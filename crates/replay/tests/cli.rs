use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "orderbook-replay-test-{}-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn cli(args: &[&std::ffi::OsStr]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_replay"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn cli_records_verifies_and_refuses_to_overwrite() {
    let dir = TestDir::new();
    let commands = dir.0.join("commands.jsonl");
    let recording = dir.0.join("recording.jsonl");
    std::fs::write(
        &commands,
        include_bytes!("../../../docs/recordings/basic.commands.jsonl"),
    )
    .unwrap();
    let args = [
        "record".as_ref(),
        "--commands".as_ref(),
        commands.as_os_str(),
        "--output".as_ref(),
        recording.as_os_str(),
    ];
    let result = cli(&args);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let original = std::fs::read(&recording).unwrap();
    assert!(!cli(&args).status.success());
    assert_eq!(std::fs::read(&recording).unwrap(), original);
    let result = cli(&["verify".as_ref(), recording.as_os_str()]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("9 commands, 20 events"));
}

#[test]
fn cli_returns_failure_for_incomplete_or_missing_input() {
    let dir = TestDir::new();
    let recording = dir.0.join("incomplete.jsonl");
    assert!(!cli(&["verify".as_ref(), recording.as_os_str()])
        .status
        .success());
    std::fs::write(
        &recording,
        b"{\"record\":\"header\",\"version\":1,\"producer_version\":\"test\"}\n",
    )
    .unwrap();
    let result = cli(&["verify".as_ref(), recording.as_os_str()]);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("missing footer"));
}
