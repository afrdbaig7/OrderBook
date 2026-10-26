use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "Record and verify deterministic matching-engine scenarios")]
struct Args {
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Apply a JSONL command file and record its events and final snapshot.
    Record {
        #[arg(long)]
        commands: PathBuf,
        /// New output file; existing files are never overwritten.
        #[arg(long)]
        output: PathBuf,
    },
    /// Replay a recording and compare every event and the final snapshot.
    Verify { recording: PathBuf },
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let summary = match args.command {
        Action::Record { commands, output } => {
            let input = BufReader::new(File::open(&commands)?);
            let writer = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&output)?,
            );
            replay::record(input, writer)?
        }
        Action::Verify { recording } => replay::verify(BufReader::new(File::open(recording)?))?,
    };
    println!(
        "{} commands, {} events, final seq {}, {} bid levels, {} ask levels",
        summary.commands,
        summary.events,
        summary.snapshot.seq,
        summary.snapshot.bids.len(),
        summary.snapshot.asks.len()
    );
    Ok(())
}

fn main() -> ExitCode {
    match run(Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("replay: {error}");
            ExitCode::FAILURE
        }
    }
}
