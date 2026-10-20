//! Deterministic synthetic order flow.
//!
//! A fixed seed always produces the same command sequence. Prices follow a
//! bounded random walk; sizes are skewed small. The feed is clock-free;
//! pacing is handled by the server.

use engine::{Event, Order, OrderBook, OrderId, Price, Side};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// One order-book command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Limit(Order),
    Market { id: OrderId, side: Side, qty: u64 },
    Cancel(OrderId),
}

impl Command {
    /// Submits this command to `book` and returns the resulting events.
    pub fn apply(&self, book: &mut OrderBook) -> Vec<Event> {
        match self {
            Command::Limit(order) => book.submit_limit(order.clone()),
            Command::Market { id, side, qty } => book.submit_market(*id, *side, *qty),
            Command::Cancel(id) => book.cancel(*id),
        }
    }
}

/// Tunables for [`SyntheticFeed`].
#[derive(Clone, Debug)]
pub struct FeedConfig {
    /// RNG seed — same seed, same stream.
    pub seed: u64,
    /// Starting mid price; bids rest below it, asks above.
    pub mid: u64,
    /// Maximum distance in ticks of a resting price from the current mid.
    /// Resting bids live in `[mid - spread, mid - 1]`, asks in
    /// `[mid + 1, mid + spread]`.
    pub spread: u64,
    /// Half-width of the mid-price random walk.
    pub walk_range: u64,
    /// Largest order quantity (sizes skew toward 1).
    pub max_qty: u64,
    /// Percentage of commands that are resting limit orders.
    pub limit_pct: u8,
    /// Percentage that are cancels (`limit_pct + cancel_pct <= 100`; the
    /// remainder are market orders).
    pub cancel_pct: u8,
    /// Maximum live cancel candidates retained in memory (bounds candidate collection size).
    pub max_live_candidates: usize,
}

impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            seed: 1,
            mid: 100_000,
            spread: 50,
            walk_range: 200,
            max_qty: 100,
            limit_pct: 70,
            cancel_pct: 20,
            max_live_candidates: 10_000,
        }
    }
}

/// Seeded generator of [`Command`]s.
pub struct SyntheticFeed {
    rng: ChaCha8Rng,
    config: FeedConfig,
    /// Current mid price (walks over time).
    mid: u64,
    next_id: u64,
    live: Vec<OrderId>,
}

impl SyntheticFeed {
    pub fn new(config: FeedConfig) -> Self {
        assert!(
            config.limit_pct as u16 + config.cancel_pct as u16 <= 100,
            "limit_pct + cancel_pct must be <= 100"
        );
        assert!(config.mid > config.spread, "mid must exceed spread");
        assert!(config.max_qty > 0, "max_qty must be positive");
        assert!(config.spread > 0, "spread must be positive");
        let mid = config.mid;
        let rng = ChaCha8Rng::seed_from_u64(config.seed);
        Self {
            rng,
            config,
            mid,
            next_id: 1,
            live: Vec::new(),
        }
    }

    pub fn seeded(seed: u64) -> Self {
        Self::new(FeedConfig {
            seed,
            ..FeedConfig::default()
        })
    }

    pub fn mid(&self) -> u64 {
        self.mid
    }

    fn take_id(&mut self) -> OrderId {
        let id = OrderId(self.next_id);
        self.next_id += 1;
        id
    }

    /// Bounded, mean-reverting-ish random walk of the mid.
    fn walk_mid(&mut self) {
        let step: i64 = self.rng.random_range(-1..=1);
        let low = self.config.mid.saturating_sub(self.config.walk_range);
        let high = self.config.mid + self.config.walk_range;
        let next = (self.mid as i64 + step).clamp(low as i64, high as i64) as u64;
        // Never let the mid collide with the 0-price / spread guards.
        self.mid = next.max(self.config.spread + 1);
    }

    /// Geometric-like distance from the touch, capped at `spread`.
    fn touch_offset(&mut self) -> u64 {
        let mut offset = 1;
        while offset < self.config.spread && self.rng.random_bool(0.5) {
            offset += 1;
        }
        offset
    }

    /// Skewed-small integer size in `1..=max_qty + 1`.
    fn size(&mut self) -> u64 {
        let r = self.rng.random_range(1..=self.config.max_qty);
        1 + (r * r) / self.config.max_qty
    }

    fn limit_command(&mut self) -> Command {
        self.walk_mid();
        let side = if self.rng.random_bool(0.5) {
            Side::Bid
        } else {
            Side::Ask
        };
        let offset = self.touch_offset();
        let price = match side {
            Side::Bid => self.mid - offset,
            Side::Ask => self.mid + offset,
        };
        let qty = self.size();
        let id = self.take_id();
        let max_candidates = self.config.max_live_candidates.max(100);
        if self.live.len() < max_candidates {
            self.live.push(id);
        } else {
            let idx = self.rng.random_range(0..self.live.len());
            self.live[idx] = id;
        }
        Command::Limit(Order {
            id,
            side,
            price: Price(price),
            qty,
        })
    }

    fn market_command(&mut self) -> Command {
        let side = if self.rng.random_bool(0.5) {
            Side::Bid
        } else {
            Side::Ask
        };
        let qty = self.size();
        Command::Market {
            id: self.take_id(),
            side,
            qty,
        }
    }

    fn cancel_command(&mut self) -> Command {
        if self.live.is_empty() {
            return self.limit_command();
        }
        let idx = self.rng.random_range(0..self.live.len());
        let id = self.live.swap_remove(idx);
        Command::Cancel(id)
    }

    pub fn next_command(&mut self) -> Command {
        let roll = self.rng.random_range(0u8..100);
        if roll < self.config.limit_pct {
            self.limit_command()
        } else if roll < self.config.limit_pct + self.config.cancel_pct {
            self.cancel_command()
        } else {
            self.market_command()
        }
    }

    pub fn live_hint(&self) -> usize {
        self.live.len()
    }
}

/// Convenience: apply a command and translate its events to wire messages.
pub fn apply_to_wire(book: &mut OrderBook, command: &Command) -> Vec<crate::wire::Message> {
    crate::wire::messages_from_events(&command.apply(book))
}
