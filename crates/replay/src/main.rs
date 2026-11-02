use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use replay::{EngineType, ReplayEngineConfig};

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
        /// Engine implementation to record with: "reference" (default) or "flat".
        #[arg(long, value_enum, default_value_t = EngineType::Reference)]
        engine: EngineType,
        /// Base price for FlatBook ladder when --engine flat is used.
        #[arg(long = "flat-base-price", default_value_t = 1)]
        flat_base_price: u64,
        /// Number of tick levels for FlatBook ladder when --engine flat is used.
        #[arg(long = "flat-tick-count", default_value_t = 4_096)]
        flat_tick_count: usize,
        /// Maximum resting orders for FlatBook arena when --engine flat is used.
        #[arg(long = "flat-max-orders", default_value_t = 16_384)]
        flat_max_orders: usize,
    },
    /// Replay a recording and compare every event and the final snapshot.
    Verify {
        recording: PathBuf,
        /// Engine implementation to verify against: "reference" (default) or "flat".
        #[arg(long, value_enum, default_value_t = EngineType::Reference)]
        engine: EngineType,
        /// Base price for FlatBook ladder when --engine flat is used.
        #[arg(long = "flat-base-price", default_value_t = 1)]
        flat_base_price: u64,
        /// Number of tick levels for FlatBook ladder when --engine flat is used.
        #[arg(long = "flat-tick-count", default_value_t = 4_096)]
        flat_tick_count: usize,
        /// Maximum resting orders for FlatBook arena when --engine flat is used.
        #[arg(long = "flat-max-orders", default_value_t = 16_384)]
        flat_max_orders: usize,
    },
}

fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let (summary, engine) = match args.command {
        Action::Record {
            commands,
            output,
            engine,
            flat_base_price,
            flat_tick_count,
            flat_max_orders,
        } => {
            let input = BufReader::new(File::open(&commands)?);
            let writer = BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&output)?,
            );
            let config = ReplayEngineConfig {
                engine,
                flat_base_price,
                flat_tick_count,
                flat_max_orders,
            };
            (replay::record_with_config(input, writer, config)?, engine)
        }
        Action::Verify {
            recording,
            engine,
            flat_base_price,
            flat_tick_count,
            flat_max_orders,
        } => {
            let input = BufReader::new(File::open(recording)?);
            let config = ReplayEngineConfig {
                engine,
                flat_base_price,
                flat_tick_count,
                flat_max_orders,
            };
            (replay::verify_with_config(input, config)?, engine)
        }
    };
    println!(
        "{} commands, {} events, final seq {}, {} bid levels, {} ask levels (engine: {})",
        summary.commands,
        summary.events,
        summary.snapshot.seq,
        summary.snapshot.bids.len(),
        summary.snapshot.asks.len(),
        engine
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
