# Command recording and replay

The `replay` crate records offline scenarios and verifies that the current
engine produces the recorded results. It needs no server, sockets, or database.

## Run the example

```bash
cargo run --locked -p replay -- verify docs/recordings/basic.recording.jsonl
```

Expected summary:

```text
9 commands, 20 events, final seq 20, 1 bid levels, 0 ask levels
```

To generate a new recording:

```bash
cargo run --locked -p replay -- record --commands docs/recordings/basic.commands.jsonl --output my-recording.jsonl
cargo run --locked -p replay -- verify my-recording.jsonl
```

These commands also work in PowerShell. The output path must not already exist.
A failed run may leave an unfinished output file; keep it for diagnosis or remove
it explicitly before retrying. Treat any recording or flush error as a failed run,
even if some output is readable.

## Command input

Input is UTF-8 JSON Lines, one command per line:

```json
{"type":"limit","id":1,"side":"ask","price":105,"qty":10}
{"type":"market","id":2,"side":"bid","qty":4}
{"type":"cancel","id":1}
```

Prices are integer ticks and quantities are integer units. `side` is `bid` or
`ask`. Unknown command fields and malformed values fail parsing. Engine-invalid
requests, such as zero quantity or cancelling a missing ID, remain valid input:
their rejection events are part of the scenario.

Every line, including the last, must end in a newline. LF and CRLF both work.
Blank lines are rejected. An empty command file produces an empty, finalized
recording.

## Recording format, version 1

A recording is also JSON Lines and contains, in order:

1. A header with `record: "header"`, `version: 1`, and `producer_version`.
2. Command records with consecutive `index` values starting at 1, a `command`,
   and an `events` array containing all engine results.
3. One footer with command/event totals and the complete `snapshot`.

The header starts a fresh empty book with engine sequence zero. The producer
version identifies the recorder package; format version controls schema
compatibility. A different producer version is permitted, but every result must
still match the current engine.

Events retain sequence numbers, order IDs, maker/taker IDs, prices, quantities,
sides, and rejection reasons. They contain no server wall-clock timestamps.
The representation is separate from both engine internals and the WebSocket
market-data format.

The verifier applies each command in order and checks exact event equality.
It then checks footer totals and the full price-level snapshot. Missing,
duplicated, reordered, or trailing records fail verification. Each line is
limited to 8 MiB including its newline; the reader enforces that bound before
parsing. Recording a command whose event record exceeds the limit fails and
leaves an incomplete recording.

## Library use

```rust
use feed::Command;
use engine::OrderId;
use replay::{Recorder, verify};
use std::io::Cursor;

fn main() -> Result<(), replay::Error> {
    let mut bytes = Vec::new();
    let mut recorder = Recorder::new(&mut bytes)?;
    // Rejected commands are recorded too.
    recorder.submit(&Command::Cancel(OrderId(42)))?;
    let summary = recorder.finish()?;
    assert_eq!(verify(Cursor::new(bytes))?, summary);
    Ok(())
}
```

`Recorder` owns its fresh book. `submit` applies one command, writes its results,
and returns the original engine events. After a write error, the recorder is
poisoned: further submission and finalization fail. `finish` writes the footer
and flushes the writer. Dropping a recorder does not finalize it.

## Scope and guarantees

- This is a synchronous offline debugging tool. File writes are outside the
  matching crate and are not on the live server's matching path.
- Successful verification means the commands and expected engine results agree.
  It is not proof that the engine implements the intended exchange rules.
- Recordings are not authenticated. Someone who rewrites commands and recomputes
  their results can create another valid recording.
- Final snapshots contain aggregate price levels, not a recoverable serialization
  of every resting order. Replaying the complete command sequence reconstructs
  those orders; loading only the footer does not.
- Flushing a writer does not promise `fsync` or power-loss durability. This is
  not a write-ahead log or crash-recovery mechanism.
- Live server capture is a later milestone. It must record commands actually
  processed by the engine and detect incomplete output if its writer falls behind.
