//! Differential tests against a simple reference matcher.
//!
//! The reference stores orders in flat lists and sorts them before matching.
//! Comparing randomized command sequences checks execution behavior independently
//! of the production book's data structures.

use engine::{Event, Fill, Order, OrderBook, OrderId, Price, RejectReason, Side};
use proptest::prelude::*;

#[derive(Clone, Debug)]
struct NaiveOrder {
    order: Order,
    arrival_seq: u64,
}

/// Reference matcher using flat order lists.
#[derive(Default, Debug, Clone)]
struct NaiveMatcher {
    bids: Vec<NaiveOrder>,
    asks: Vec<NaiveOrder>,
    seq: u64,
    order_counter: u64,
}

impl NaiveMatcher {
    fn new() -> Self {
        Self::default()
    }

    fn has_order(&self, id: OrderId) -> bool {
        self.bids.iter().any(|o| o.order.id == id) || self.asks.iter().any(|o| o.order.id == id)
    }

    fn submit_limit(&mut self, mut order: Order) -> Vec<Event> {
        self.seq += 1;
        if order.qty == 0 {
            return vec![Event::Reject {
                seq: self.seq,
                id: order.id,
                reason: RejectReason::ZeroQuantity,
            }];
        }
        if order.price.0 == 0 {
            return vec![Event::Reject {
                seq: self.seq,
                id: order.id,
                reason: RejectReason::ZeroPrice,
            }];
        }
        if self.has_order(order.id) {
            return vec![Event::Reject {
                seq: self.seq,
                id: order.id,
                reason: RejectReason::DuplicateOrderId,
            }];
        }

        let mut events = vec![Event::Ack {
            seq: self.seq,
            id: order.id,
        }];

        // Matching: sort opposing orders by price-time priority
        if order.side == Side::Bid {
            // Match against asks: lowest price first, then earliest arrival
            self.asks.sort_by(|a, b| {
                a.order
                    .price
                    .cmp(&b.order.price)
                    .then_with(|| a.arrival_seq.cmp(&b.arrival_seq))
            });
            let mut i = 0;
            while i < self.asks.len() && order.qty > 0 {
                if self.asks[i].order.price > order.price {
                    break; // No longer crosses
                }
                let fill_qty = order.qty.min(self.asks[i].order.qty);
                self.seq += 1;
                events.push(Event::Fill {
                    seq: self.seq,
                    fill: Fill {
                        maker_id: self.asks[i].order.id,
                        taker_id: order.id,
                        price: self.asks[i].order.price,
                        qty: fill_qty,
                    },
                    taker_side: Side::Bid,
                });

                order.qty -= fill_qty;
                self.asks[i].order.qty -= fill_qty;

                if self.asks[i].order.qty == 0 {
                    self.asks.remove(i);
                } else {
                    i += 1;
                }
            }
        } else {
            // Match against bids: highest price first, then earliest arrival
            self.bids.sort_by(|a, b| {
                b.order
                    .price
                    .cmp(&a.order.price)
                    .then_with(|| a.arrival_seq.cmp(&b.arrival_seq))
            });
            let mut i = 0;
            while i < self.bids.len() && order.qty > 0 {
                if self.bids[i].order.price < order.price {
                    break; // No longer crosses
                }
                let fill_qty = order.qty.min(self.bids[i].order.qty);
                self.seq += 1;
                events.push(Event::Fill {
                    seq: self.seq,
                    fill: Fill {
                        maker_id: self.bids[i].order.id,
                        taker_id: order.id,
                        price: self.bids[i].order.price,
                        qty: fill_qty,
                    },
                    taker_side: Side::Ask,
                });

                order.qty -= fill_qty;
                self.bids[i].order.qty -= fill_qty;

                if self.bids[i].order.qty == 0 {
                    self.bids.remove(i);
                } else {
                    i += 1;
                }
            }
        }

        if order.qty > 0 {
            self.order_counter += 1;
            let resting = NaiveOrder {
                order: order.clone(),
                arrival_seq: self.order_counter,
            };
            match order.side {
                Side::Bid => self.bids.push(resting),
                Side::Ask => self.asks.push(resting),
            }
        }

        events
    }

    fn submit_market(&mut self, id: OrderId, side: Side, mut qty: u64) -> Vec<Event> {
        self.seq += 1;
        if qty == 0 {
            return vec![Event::Reject {
                seq: self.seq,
                id,
                reason: RejectReason::ZeroQuantity,
            }];
        }
        if self.has_order(id) {
            return vec![Event::Reject {
                seq: self.seq,
                id,
                reason: RejectReason::DuplicateOrderId,
            }];
        }

        let mut events = vec![Event::Ack { seq: self.seq, id }];

        if side == Side::Bid {
            self.asks.sort_by(|a, b| {
                a.order
                    .price
                    .cmp(&b.order.price)
                    .then_with(|| a.arrival_seq.cmp(&b.arrival_seq))
            });
            let mut i = 0;
            while i < self.asks.len() && qty > 0 {
                let fill_qty = qty.min(self.asks[i].order.qty);
                self.seq += 1;
                events.push(Event::Fill {
                    seq: self.seq,
                    fill: Fill {
                        maker_id: self.asks[i].order.id,
                        taker_id: id,
                        price: self.asks[i].order.price,
                        qty: fill_qty,
                    },
                    taker_side: Side::Bid,
                });

                qty -= fill_qty;
                self.asks[i].order.qty -= fill_qty;

                if self.asks[i].order.qty == 0 {
                    self.asks.remove(i);
                } else {
                    i += 1;
                }
            }
        } else {
            self.bids.sort_by(|a, b| {
                b.order
                    .price
                    .cmp(&a.order.price)
                    .then_with(|| a.arrival_seq.cmp(&b.arrival_seq))
            });
            let mut i = 0;
            while i < self.bids.len() && qty > 0 {
                let fill_qty = qty.min(self.bids[i].order.qty);
                self.seq += 1;
                events.push(Event::Fill {
                    seq: self.seq,
                    fill: Fill {
                        maker_id: self.bids[i].order.id,
                        taker_id: id,
                        price: self.bids[i].order.price,
                        qty: fill_qty,
                    },
                    taker_side: Side::Ask,
                });

                qty -= fill_qty;
                self.bids[i].order.qty -= fill_qty;

                if self.bids[i].order.qty == 0 {
                    self.bids.remove(i);
                } else {
                    i += 1;
                }
            }
        }

        if qty > 0 {
            self.seq += 1;
            events.push(Event::Reject {
                seq: self.seq,
                id,
                reason: RejectReason::MarketUnfilled,
            });
        }

        events
    }

    fn cancel(&mut self, id: OrderId) -> Vec<Event> {
        self.seq += 1;
        if let Some(pos) = self.bids.iter().position(|o| o.order.id == id) {
            self.bids.remove(pos);
            vec![Event::Cancel { seq: self.seq, id }]
        } else if let Some(pos) = self.asks.iter().position(|o| o.order.id == id) {
            self.asks.remove(pos);
            vec![Event::Cancel { seq: self.seq, id }]
        } else {
            vec![Event::Reject {
                seq: self.seq,
                id,
                reason: RejectReason::UnknownCancel,
            }]
        }
    }

    fn total_resting_qty(&self) -> u64 {
        self.bids
            .iter()
            .chain(&self.asks)
            .map(|o| o.order.qty)
            .sum()
    }
}

#[derive(Clone, Debug)]
enum TestCmd {
    Limit {
        id: u64,
        side: Side,
        price: u64,
        qty: u64,
    },
    Market {
        id: u64,
        side: Side,
        qty: u64,
    },
    Cancel {
        id: u64,
    },
}

fn cmd_gen() -> impl Strategy<Value = TestCmd> {
    let id = 0u64..30;
    let side = prop_oneof![Just(Side::Bid), Just(Side::Ask)];
    prop_oneof![
        6 => (id.clone(), side.clone(), 90u64..=110, 1u64..=20)
            .prop_map(|(id, side, price, qty)| TestCmd::Limit { id, side, price, qty }),
        3 => (id.clone(), side.clone(), 1u64..=20)
            .prop_map(|(id, side, qty)| TestCmd::Market { id, side, qty }),
        1 => id.prop_map(|id| TestCmd::Cancel { id }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 1_000, ..Default::default() })]

    #[test]
    fn differential_testing_against_naive_reference(cmds in prop::collection::vec(cmd_gen(), 1..60)) {
        let mut actual = OrderBook::new();
        let mut naive = NaiveMatcher::new();

        for cmd in cmds {
            match cmd {
                TestCmd::Limit { id, side, price, qty } => {
                    let order = Order {
                        id: OrderId(id),
                        side,
                        price: Price(price),
                        qty,
                    };
                    let actual_events = actual.submit_limit(order.clone());
                    let naive_events = naive.submit_limit(order);

                    // Compare fills generated
                    let actual_fills: Vec<_> = actual_events
                        .iter()
                        .filter_map(|e| match e {
                            Event::Fill { fill, taker_side, .. } => Some((fill.clone(), *taker_side)),
                            _ => None,
                        })
                        .collect();
                    let naive_fills: Vec<_> = naive_events
                        .iter()
                        .filter_map(|e| match e {
                            Event::Fill { fill, taker_side, .. } => Some((fill.clone(), *taker_side)),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(actual_fills, naive_fills, "fills differ");
                }
                TestCmd::Market { id, side, qty } => {
                    let actual_events = actual.submit_market(OrderId(id), side, qty);
                    let naive_events = naive.submit_market(OrderId(id), side, qty);

                    let actual_fills: Vec<_> = actual_events
                        .iter()
                        .filter_map(|e| match e {
                            Event::Fill { fill, taker_side, .. } => Some((fill.clone(), *taker_side)),
                            _ => None,
                        })
                        .collect();
                    let naive_fills: Vec<_> = naive_events
                        .iter()
                        .filter_map(|e| match e {
                            Event::Fill { fill, taker_side, .. } => Some((fill.clone(), *taker_side)),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(actual_fills, naive_fills, "market fills differ");
                }
                TestCmd::Cancel { id } => {
                    let actual_events = actual.cancel(OrderId(id));
                    let naive_events = naive.cancel(OrderId(id));

                    let actual_cancelled = actual_events.iter().any(|e| matches!(e, Event::Cancel { .. }));
                    let naive_cancelled = naive_events.iter().any(|e| matches!(e, Event::Cancel { .. }));
                    assert_eq!(actual_cancelled, naive_cancelled, "cancel outcome differ for order {id}");
                }
            }

            // Invariant comparison: total resting quantity must match exactly
            assert_eq!(
                actual.total_resting_qty(),
                naive.total_resting_qty(),
                "resting quantity divergence"
            );
        }
    }
}
