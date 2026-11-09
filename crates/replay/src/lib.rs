//! Offline recordings of commands and their deterministic engine results.
//!
//! Recordings start from an empty book. Each JSON line contains a header, a
//! command with expected events, or the final totals and complete snapshot.
//! File I/O stays here rather than in the matching core.
#![forbid(unsafe_code)]

use std::io::{self, BufRead, Read, Write};

use engine::{
    Event, Fill, FlatBook, FlatBookConfig, Order, OrderBook, OrderId, Price, RejectReason, Side,
    Snapshot,
};
use feed::Command;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

pub const FORMAT_VERSION: u32 = 1;
/// Bounds memory used while reading a single record, including its newline.
pub const MAX_LINE_BYTES: usize = 8 * 1024 * 1024;

/// Identifies the matching engine implementation for recording and replay verification.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    clap::ValueEnum,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
pub enum EngineType {
    /// Standard dynamic BTreeMap + VecDeque reference engine.
    #[default]
    Reference,
    /// Ultra-low latency, zero-allocation preallocated arena + bitmap engine.
    Flat,
}

impl EngineType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Reference => "reference",
            Self::Flat => "flat",
        }
    }
}

impl std::fmt::Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Configuration options for replay matching engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayEngineConfig {
    pub engine: EngineType,
    pub flat_base_price: u64,
    pub flat_tick_count: usize,
    pub flat_max_orders: usize,
}

impl Default for ReplayEngineConfig {
    fn default() -> Self {
        Self {
            engine: EngineType::Reference,
            flat_base_price: 1,
            flat_tick_count: 4_096,
            flat_max_orders: 16_384,
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum ReplayEngineKind {
    Reference(OrderBook),
    Flat(FlatBook),
}

struct ReplayEngine {
    kind: ReplayEngineKind,
    events_buf: Vec<Event>,
}

impl ReplayEngine {
    fn new(config: ReplayEngineConfig) -> Result<Self, Error> {
        match config.engine {
            EngineType::Reference => Ok(Self {
                kind: ReplayEngineKind::Reference(OrderBook::new()),
                events_buf: Vec::new(),
            }),
            EngineType::Flat => {
                let fb_config = FlatBookConfig {
                    base_price: config.flat_base_price,
                    tick_count: config.flat_tick_count,
                    max_orders: config.flat_max_orders,
                };
                let book = FlatBook::new(fb_config)
                    .map_err(|err| invalid(1, format!("FlatBook configuration error: {err:?}")))?;
                let events_buf = Vec::with_capacity(book.worst_case_event_capacity());
                Ok(Self {
                    kind: ReplayEngineKind::Flat(book),
                    events_buf,
                })
            }
        }
    }

    fn apply(&mut self, command: &Command) -> Result<&[Event], Error> {
        self.events_buf.clear();
        match &mut self.kind {
            ReplayEngineKind::Reference(book) => {
                self.events_buf.extend(command.apply(book));
                Ok(&self.events_buf)
            }
            ReplayEngineKind::Flat(flat) => {
                let res = match command {
                    Command::Limit(order) => {
                        flat.submit_limit_into(order.clone(), &mut self.events_buf)
                    }
                    Command::Market { id, side, qty } => {
                        flat.submit_market_into(*id, *side, *qty, &mut self.events_buf)
                    }
                    Command::Cancel(id) => flat.cancel_into(*id, &mut self.events_buf),
                    Command::Modify {
                        id,
                        new_qty,
                        new_price,
                    } => flat.modify_into(*id, *new_qty, *new_price, &mut self.events_buf),
                };
                if let Err(err) = res {
                    return Err(invalid(0, format!("FlatBook execution error: {err:?}")));
                }
                Ok(&self.events_buf)
            }
        }
    }

    fn snapshot(&self) -> Snapshot {
        match &self.kind {
            ReplayEngineKind::Reference(book) => book.snapshot(0),
            ReplayEngineKind::Flat(flat) => flat.snapshot(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("recording I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("invalid JSON on line {line}: {source}")]
    Json {
        line: usize,
        #[source]
        source: serde_json::Error,
    },
    #[error("line {line}: {message}")]
    Invalid { line: usize, message: String },
    #[error("line {line} exceeds the {MAX_LINE_BYTES}-byte record limit")]
    LineTooLong { line: usize },
    #[error("line {line} is incomplete (missing newline)")]
    IncompleteLine { line: usize },
    #[error("command {index}: engine event {event} differs from the recording")]
    EventMismatch { index: u64, event: usize },
    #[error("recorder cannot continue after a write failure")]
    Poisoned,
}

fn invalid(line: usize, message: impl Into<String>) -> Error {
    Error::Invalid {
        line,
        message: message.into(),
    }
}

/// Versioned command representation, separate from both the engine and WS APIs.
/// Engine-invalid commands are retained so replays also check rejection behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordedCommand {
    Limit {
        id: OrderId,
        side: Side,
        price: Price,
        qty: u64,
    },
    Market {
        id: OrderId,
        side: Side,
        qty: u64,
    },
    Cancel {
        id: OrderId,
    },
    Modify {
        id: OrderId,
        new_qty: u64,
        new_price: Price,
    },
}

impl From<&Command> for RecordedCommand {
    fn from(command: &Command) -> Self {
        match command {
            Command::Limit(order) => Self::Limit {
                id: order.id,
                side: order.side,
                price: order.price,
                qty: order.qty,
            },
            Command::Market { id, side, qty } => Self::Market {
                id: *id,
                side: *side,
                qty: *qty,
            },
            Command::Cancel(id) => Self::Cancel { id: *id },
            Command::Modify {
                id,
                new_qty,
                new_price,
            } => Self::Modify {
                id: *id,
                new_qty: *new_qty,
                new_price: *new_price,
            },
        }
    }
}

impl From<RecordedCommand> for Command {
    fn from(command: RecordedCommand) -> Self {
        match command {
            RecordedCommand::Limit {
                id,
                side,
                price,
                qty,
            } => Self::Limit(Order {
                id,
                side,
                price,
                qty,
            }),
            RecordedCommand::Market { id, side, qty } => Self::Market { id, side, qty },
            RecordedCommand::Cancel { id } => Self::Cancel(id),
            RecordedCommand::Modify {
                id,
                new_qty,
                new_price,
            } => Self::Modify {
                id,
                new_qty,
                new_price,
            },
        }
    }
}

/// All deterministic event fields; unlike market-data trades, fills retain IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordedEvent {
    Ack {
        seq: u64,
        id: OrderId,
    },
    Fill {
        seq: u64,
        fill: Fill,
        taker_side: Side,
    },
    Cancel {
        seq: u64,
        id: OrderId,
    },
    Reject {
        seq: u64,
        id: OrderId,
        reason: RejectReason,
    },
    Delta {
        seq: u64,
        side: Side,
        price: Price,
        qty: u64,
    },
}

impl From<&Event> for RecordedEvent {
    fn from(event: &Event) -> Self {
        match event {
            Event::Ack { seq, id } => Self::Ack { seq: *seq, id: *id },
            Event::Fill {
                seq,
                fill,
                taker_side,
            } => Self::Fill {
                seq: *seq,
                fill: fill.clone(),
                taker_side: *taker_side,
            },
            Event::Cancel { seq, id } => Self::Cancel { seq: *seq, id: *id },
            Event::Reject { seq, id, reason } => Self::Reject {
                seq: *seq,
                id: *id,
                reason: *reason,
            },
            Event::Delta {
                seq,
                side,
                price,
                new_total_qty,
            } => Self::Delta {
                seq: *seq,
                side: *side,
                price: *price,
                qty: *new_total_qty,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
enum Record {
    Header {
        version: u32,
        producer_version: String,
    },
    Command {
        index: u64,
        command: RecordedCommand,
        events: Vec<RecordedEvent>,
    },
    Footer {
        commands: u64,
        events: u64,
        snapshot: Snapshot,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Summary {
    pub commands: u64,
    pub events: u64,
    pub snapshot: Snapshot,
}

/// Records commands applied to its own fresh book. A write error poisons the
/// recorder: it cannot produce a successful footer after losing a record.
/// Call `finish` to write the footer and flush the output; dropping is incomplete.
pub struct Recorder<W: Write> {
    writer: W,
    engine: ReplayEngine,
    commands: u64,
    events: u64,
    poisoned: bool,
}

impl<W: Write> Recorder<W> {
    pub fn new(writer: W) -> Result<Self, Error> {
        Self::with_config(writer, ReplayEngineConfig::default())
    }

    pub fn with_config(mut writer: W, config: ReplayEngineConfig) -> Result<Self, Error> {
        write_record(
            &mut writer,
            &Record::Header {
                version: FORMAT_VERSION,
                producer_version: env!("CARGO_PKG_VERSION").to_owned(),
            },
            1,
        )?;
        Ok(Self {
            writer,
            engine: ReplayEngine::new(config)?,
            commands: 0,
            events: 0,
            poisoned: false,
        })
    }

    pub fn submit(&mut self, command: &Command) -> Result<Vec<Event>, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let events = self.engine.apply(command)?;
        let index = self.commands + 1;
        let record = Record::Command {
            index,
            command: RecordedCommand::from(command),
            events: events.iter().map(RecordedEvent::from).collect(),
        };
        if let Err(error) = write_record(&mut self.writer, &record, index as usize + 1) {
            self.poisoned = true;
            return Err(error);
        }
        self.commands = index;
        self.events += events.len() as u64;
        Ok(events.to_vec())
    }

    pub fn finish(mut self) -> Result<Summary, Error> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let summary = Summary {
            commands: self.commands,
            events: self.events,
            snapshot: self.engine.snapshot(),
        };
        write_record(
            &mut self.writer,
            &Record::Footer {
                commands: summary.commands,
                events: summary.events,
                snapshot: summary.snapshot.clone(),
            },
            self.commands as usize + 2,
        )?;
        self.writer.flush()?;
        Ok(summary)
    }
}

fn write_record(writer: &mut impl Write, record: &Record, line: usize) -> Result<(), Error> {
    let mut bytes = serde_json::to_vec(record).map_err(|source| Error::Json { line, source })?;
    bytes.push(b'\n');
    if bytes.len() > MAX_LINE_BYTES {
        return Err(Error::LineTooLong { line });
    }
    writer.write_all(&bytes)?;
    Ok(())
}

fn read_line<T: DeserializeOwned>(
    reader: &mut impl BufRead,
    line: usize,
) -> Result<Option<T>, Error> {
    let mut bytes = Vec::new();
    let count = reader
        .take(MAX_LINE_BYTES as u64 + 1)
        .read_until(b'\n', &mut bytes)?;
    if count == 0 {
        return Ok(None);
    }
    if count > MAX_LINE_BYTES {
        return Err(Error::LineTooLong { line });
    }
    if bytes.last() != Some(&b'\n') {
        return Err(Error::IncompleteLine { line });
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|source| Error::Json { line, source })
}

/// Applies a JSONL command file and records its results using the default reference engine.
pub fn record(commands: impl BufRead, output: impl Write) -> Result<Summary, Error> {
    record_with_config(commands, output, ReplayEngineConfig::default())
}

/// Applies a JSONL command file and records its results using the specified engine configuration.
pub fn record_with_config(
    mut commands: impl BufRead,
    output: impl Write,
    config: ReplayEngineConfig,
) -> Result<Summary, Error> {
    let mut recorder = Recorder::with_config(output, config)?;
    let mut line = 1;
    while let Some(command) = read_line::<RecordedCommand>(&mut commands, line)? {
        recorder.submit(&Command::from(command))?;
        line += 1;
    }
    recorder.finish()
}

/// Replays every command against the default reference book, comparing complete events and
/// the final snapshot.
pub fn verify(input: impl BufRead) -> Result<Summary, Error> {
    verify_with_config(input, ReplayEngineConfig::default())
}

/// Replays every command against the configured matching engine, comparing complete events and
/// the final snapshot.
pub fn verify_with_config(
    mut input: impl BufRead,
    config: ReplayEngineConfig,
) -> Result<Summary, Error> {
    match read_line::<Record>(&mut input, 1)? {
        Some(Record::Header { version, .. }) if version == FORMAT_VERSION => {}
        Some(Record::Header { version, .. }) => {
            return Err(invalid(1, format!("unsupported format version {version}")))
        }
        _ => return Err(invalid(1, "expected recording header")),
    }
    let mut engine = ReplayEngine::new(config)?;
    let mut commands = 0;
    let mut events = 0;
    let mut line = 2;
    loop {
        match read_line::<Record>(&mut input, line)? {
            Some(Record::Command {
                index,
                command,
                events: expected,
            }) => {
                if index != commands + 1 {
                    return Err(invalid(
                        line,
                        format!("expected command {}, found {index}", commands + 1),
                    ));
                }
                let actual_events = engine.apply(&Command::from(command))?;
                let actual: Vec<_> = actual_events.iter().map(RecordedEvent::from).collect();
                if actual != expected {
                    let event = actual
                        .iter()
                        .zip(&expected)
                        .position(|(a, b)| a != b)
                        .unwrap_or(actual.len().min(expected.len()))
                        + 1;
                    return Err(Error::EventMismatch { index, event });
                }
                commands = index;
                events += actual.len() as u64;
            }
            Some(Record::Footer {
                commands: expected_commands,
                events: expected_events,
                snapshot,
            }) => {
                if commands != expected_commands || events != expected_events {
                    return Err(invalid(line, "footer totals do not match replay"));
                }
                if snapshot != engine.snapshot() {
                    return Err(invalid(line, "final snapshot differs from replay"));
                }
                if read_line::<Record>(&mut input, line + 1)?.is_some() {
                    return Err(invalid(line + 1, "unexpected record after footer"));
                }
                return Ok(Summary {
                    commands,
                    events,
                    snapshot,
                });
            }
            Some(Record::Header { .. }) => return Err(invalid(line, "unexpected second header")),
            None => return Err(invalid(line, "missing footer: recording is incomplete")),
        }
        line += 1;
    }
}
