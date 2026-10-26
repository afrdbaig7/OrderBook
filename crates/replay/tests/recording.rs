use std::io::{self, Cursor, Write};

use engine::{Event, Order, OrderId, Price, RejectReason, Side};
use feed::Command;
use replay::{record, verify, Error, Recorder, MAX_LINE_BYTES};
use serde_json::{json, Value};

const COMMANDS: &str = include_str!("../../../docs/recordings/basic.commands.jsonl");

fn recording() -> Vec<u8> {
    let mut output = Vec::new();
    record(Cursor::new(COMMANDS), &mut output).unwrap();
    output
}

fn records() -> Vec<Value> {
    String::from_utf8(recording())
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn encode(records: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, record).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

#[test]
fn round_trip_checks_partial_fills_fifo_rejections_and_final_state() {
    let mut bytes = Vec::new();
    let recorded = record(Cursor::new(COMMANDS), &mut bytes).unwrap();
    let replayed = verify(Cursor::new(&bytes)).unwrap();
    assert_eq!(recorded, replayed);
    assert_eq!(replayed.commands, 9);
    assert_eq!(replayed.events, 20);
    assert_eq!(replayed.snapshot.seq, 20);
    assert_eq!(replayed.snapshot.bids, vec![(Price(99), 4)]);
    assert!(replayed.snapshot.asks.is_empty());

    let rows = records();
    let fills = rows[3]["events"].as_array().unwrap();
    assert_eq!(fills[1]["fill"]["maker_id"], 1);
    assert_eq!(fills[2]["fill"]["maker_id"], 2);
    assert_eq!(fills[2]["fill"]["qty"], 2);
    assert_eq!(rows[6]["events"][3]["reason"], "market_unfilled");
    assert_eq!(rows[7]["events"][0]["reason"], "unknown_cancel");
    assert_eq!(rows[8]["events"][0]["reason"], "zero_price");
}

#[test]
fn recorder_returns_the_original_engine_events() {
    let mut bytes = Vec::new();
    let mut recorder = Recorder::new(&mut bytes).unwrap();
    let events = recorder.submit(&Command::Cancel(OrderId(42))).unwrap();
    assert_eq!(
        events,
        vec![Event::Reject {
            seq: 1,
            id: OrderId(42),
            reason: RejectReason::UnknownCancel,
        }]
    );
    recorder.finish().unwrap();
    verify(Cursor::new(bytes)).unwrap();
}

#[test]
fn identical_commands_produce_identical_recordings() {
    assert_eq!(recording(), recording());
}

#[test]
fn checked_in_recording_matches_the_example_commands() {
    let fixture = include_bytes!("../../../docs/recordings/basic.recording.jsonl");
    assert_eq!(
        verify(Cursor::new(fixture)).unwrap(),
        verify(Cursor::new(recording())).unwrap()
    );
}

#[test]
fn windows_line_endings_are_accepted() {
    let commands = COMMANDS.replace('\n', "\r\n");
    let mut bytes = Vec::new();
    let summary = record(Cursor::new(commands), &mut bytes).unwrap();
    let recording = String::from_utf8(bytes).unwrap().replace('\n', "\r\n");
    assert_eq!(verify(Cursor::new(recording)).unwrap(), summary);
}

#[test]
fn empty_recording_has_a_verified_footer() {
    let mut bytes = Vec::new();
    let summary = Recorder::new(&mut bytes).unwrap().finish().unwrap();
    assert_eq!(summary.commands, 0);
    assert_eq!(verify(Cursor::new(bytes)).unwrap(), summary);
}

#[test]
fn changed_fill_identity_is_detected_even_when_prices_and_quantities_match() {
    let mut rows = records();
    rows[3]["events"][1]["fill"]["maker_id"] = json!(999);
    assert!(matches!(
        verify(Cursor::new(encode(&rows))),
        Err(Error::EventMismatch { index: 3, event: 2 })
    ));
}

#[test]
fn changed_command_or_missing_event_fails_verification() {
    let mut rows = records();
    rows[1]["command"]["qty"] = json!(9);
    assert!(matches!(
        verify(Cursor::new(encode(&rows))),
        Err(Error::EventMismatch { .. })
    ));

    let mut rows = records();
    rows[3]["events"].as_array_mut().unwrap().pop();
    assert!(matches!(
        verify(Cursor::new(encode(&rows))),
        Err(Error::EventMismatch { .. })
    ));
}

#[test]
fn missing_duplicate_and_reordered_commands_are_detected() {
    let mut rows = records();
    rows.remove(2);
    assert!(verify(Cursor::new(encode(&rows)))
        .unwrap_err()
        .to_string()
        .contains("expected command 2"));

    let mut rows = records();
    rows.insert(2, rows[1].clone());
    assert!(verify(Cursor::new(encode(&rows))).is_err());

    let mut rows = records();
    rows.swap(1, 2);
    assert!(verify(Cursor::new(encode(&rows))).is_err());
}

#[test]
fn footer_totals_and_complete_snapshot_are_verified() {
    let mut rows = records();
    let footer = rows.last_mut().unwrap();
    footer["commands"] = json!(1);
    assert!(verify(Cursor::new(encode(&rows)))
        .unwrap_err()
        .to_string()
        .contains("footer totals"));

    let mut rows = records();
    rows.last_mut().unwrap()["snapshot"]["bids"] = json!([]);
    assert!(verify(Cursor::new(encode(&rows)))
        .unwrap_err()
        .to_string()
        .contains("final snapshot"));
}

#[test]
fn truncated_or_unfinished_recordings_do_not_pass() {
    let mut rows = records();
    rows.pop();
    assert!(verify(Cursor::new(encode(&rows)))
        .unwrap_err()
        .to_string()
        .contains("missing footer"));

    let mut bytes = recording();
    bytes.pop();
    assert!(matches!(
        verify(Cursor::new(bytes)),
        Err(Error::IncompleteLine { .. })
    ));

    assert!(verify(Cursor::new(b"{\n")).is_err());
    assert!(verify(Cursor::new(b"")).is_err());
}

#[test]
fn unsupported_versions_extra_headers_and_trailing_records_are_rejected() {
    let mut rows = records();
    rows[0]["version"] = json!(2);
    assert!(verify(Cursor::new(encode(&rows)))
        .unwrap_err()
        .to_string()
        .contains("unsupported format"));

    let mut rows = records();
    rows.insert(1, rows[0].clone());
    assert!(verify(Cursor::new(encode(&rows))).is_err());

    let mut rows = records();
    rows.push(rows.last().unwrap().clone());
    assert!(verify(Cursor::new(encode(&rows)))
        .unwrap_err()
        .to_string()
        .contains("after footer"));
}

#[test]
fn malformed_command_input_leaves_an_incomplete_recording() {
    let mut bytes = Vec::new();
    let error = record(
        Cursor::new(b"{\"type\":\"cancel\",\"id\":1,\"typo\":true}\n"),
        &mut bytes,
    )
    .unwrap_err();
    assert!(matches!(error, Error::Json { line: 1, .. }));
    assert!(verify(Cursor::new(bytes)).is_err());
}

#[test]
fn record_reader_rejects_oversized_lines_without_reading_the_whole_input() {
    let bytes = vec![b' '; MAX_LINE_BYTES + 100];
    let mut input = Cursor::new(bytes);
    assert!(matches!(
        verify(&mut input),
        Err(Error::LineTooLong { line: 1 })
    ));
    assert_eq!(input.position(), MAX_LINE_BYTES as u64 + 1);
}

struct FailingWriter {
    bytes: Vec<u8>,
    remaining: usize,
    fail_flush: bool,
}

impl Write for FailingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Err(io::Error::other("disk full"));
        }
        let count = bytes.len().min(self.remaining);
        self.bytes.extend_from_slice(&bytes[..count]);
        self.remaining -= count;
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.fail_flush {
            Err(io::Error::other("flush failed"))
        } else {
            Ok(())
        }
    }
}

#[test]
fn a_partial_write_poisons_the_recorder_and_cannot_be_finalized() {
    let mut header = Vec::new();
    drop(Recorder::new(&mut header).unwrap());
    // Allow the header and part of the first command, then simulate disk full.
    let mut writer = FailingWriter {
        bytes: Vec::new(),
        remaining: header.len() + 10,
        fail_flush: false,
    };
    let mut recorder = Recorder::new(&mut writer).unwrap();
    assert!(matches!(
        recorder.submit(&Command::Cancel(OrderId(1))),
        Err(Error::Io(_))
    ));
    assert!(matches!(
        recorder.submit(&Command::Cancel(OrderId(2))),
        Err(Error::Poisoned)
    ));
    assert!(matches!(recorder.finish(), Err(Error::Poisoned)));
    assert!(verify(Cursor::new(writer.bytes)).is_err());
}

#[test]
fn header_and_flush_failures_are_reported() {
    let writer = FailingWriter {
        bytes: Vec::new(),
        remaining: 0,
        fail_flush: false,
    };
    assert!(matches!(Recorder::new(writer), Err(Error::Io(_))));
    let writer = FailingWriter {
        bytes: Vec::new(),
        remaining: usize::MAX,
        fail_flush: true,
    };
    assert!(matches!(
        Recorder::new(writer).unwrap().finish(),
        Err(Error::Io(_))
    ));
}

#[test]
fn seeded_synthetic_commands_round_trip() {
    let mut feed = feed::SyntheticFeed::seeded(42);
    let mut bytes = Vec::new();
    let mut recorder = Recorder::new(&mut bytes).unwrap();
    for _ in 0..2_000 {
        recorder.submit(&feed.next_command()).unwrap();
    }
    let summary = recorder.finish().unwrap();
    assert_eq!(verify(Cursor::new(bytes)).unwrap(), summary);
}

#[test]
fn complete_snapshot_includes_more_than_the_display_depth() {
    let mut bytes = Vec::new();
    let mut recorder = Recorder::new(&mut bytes).unwrap();
    for id in 1..=30 {
        recorder
            .submit(&Command::Limit(Order {
                id: OrderId(id),
                side: Side::Bid,
                price: Price(id),
                qty: 1,
            }))
            .unwrap();
    }
    recorder.finish().unwrap();
    assert_eq!(verify(Cursor::new(bytes)).unwrap().snapshot.bids.len(), 30);
}
