//! Main benchmark harness CLI supporting calibration, micro-profiling, and 3-pass evaluation.

use clap::{Parser, Subcommand};

use bench::calibrate::{calibrate_timer, print_calibration_report};
use bench::harness::{evaluate_primary_workloads, print_evaluation_table};
use bench::profile_costs::run_all_cost_profiles;

#[derive(Parser, Debug)]
#[command(
    about = "Engine benchmarking, calibration, micro-profiling, and evaluation",
    long_about = None
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Calibrates system timer resolution, bracket overhead, and harness costs (QPC & TSC)
    Calibrate {
        #[arg(long, default_value_t = 100_000)]
        iterations: usize,
    },
    /// Runs micro-architectural isolated cost and bottleneck profiling experiments
    Profile,
    /// Runs the full evaluation suite collecting up to 1,000,000 samples per primary workload
    Evaluate {
        #[arg(long, default_value_t = 1_000_000)]
        samples: usize,
    },
}

fn run_evaluation(samples: usize) {
    println!("=== PHASE 1: Running High-Resolution Timer Calibration ===");
    let cal = calibrate_timer(100_000);
    print_calibration_report(&cal);

    println!("=== PHASE 2: Running 1,000,000-Sample Primary Workload Evaluation ===");
    println!("(State replenished outside timing; hardware TSC cycle counter timing; strict failure assertions)\n");

    let reports = evaluate_primary_workloads(&cal, samples);
    print_evaluation_table(&reports);
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Calibrate { iterations }) => {
            let cal = calibrate_timer(iterations);
            print_calibration_report(&cal);
        }
        Some(Commands::Profile) => {
            run_all_cost_profiles();
        }
        Some(Commands::Evaluate { samples }) => {
            run_evaluation(samples);
        }
        None => {
            // Default: run full evaluation with 1,000,000 samples
            run_evaluation(1_000_000);
        }
    }
}
