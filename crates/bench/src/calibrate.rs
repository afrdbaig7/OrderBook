//! System timer (QPC) and hardware-counter (TSC) calibration and resolution analysis.
//!
//! Measures and compares:
//! 1. Clock resolution (smallest non-zero delta) and zero-duration sample frequency.
//! 2. Empty measurement bracket duration (raw timestamp pairs without storage).
//! 3. Complete harness cost (bracket duration + sample array write).
//! 4. Hardware invariant TSC frequency, resolution, and empty bracket cycles.
//!
//! Preallocates all sample storage and processes distributions strictly after collection.
//! Zero-duration samples are recorded honestly and never discarded or replaced with 1 ns.

use std::hint::black_box;
use std::time::{Duration, Instant};

use crate::timer::{calibrate_tsc, read_tsc, TscCalibration};

#[derive(Clone, Debug)]
pub struct TimerCalibration {
    pub iterations: usize,
    // --- QPC Metrics ---
    pub qpc_resolution_ns: u64,
    pub qpc_zero_freq: f64,
    pub qpc_empty_bracket_mean_ns: f64,
    pub qpc_harness_mean_ns: f64,
    pub qpc_p50_ns: u64,
    pub qpc_p90_ns: u64,
    pub qpc_p99_ns: u64,
    pub qpc_p99_9_ns: u64,
    pub qpc_max_ns: u64,
    pub qpc_can_resolve_25ns: bool,

    // --- Hardware TSC Metrics ---
    pub tsc: Option<TscCalibration>,
    pub tsc_harness_mean_ns: f64,
    pub tsc_p50_ns: f64,
    pub tsc_p90_ns: f64,
    pub tsc_p99_ns: f64,
    pub tsc_p99_9_ns: f64,
    pub tsc_max_ns: f64,
    pub tsc_can_resolve_25ns: bool,
}

pub fn calibrate_timer(iterations: usize) -> TimerCalibration {
    // 1. Warm up CPU execution pipeline
    for _ in 0..50_000 {
        let t0 = Instant::now();
        black_box(t0.elapsed().as_nanos());
        black_box(read_tsc());
    }

    // Preallocate all buffers before any timing
    let mut qpc_raw_samples = vec![0u64; iterations];
    let mut qpc_harness_samples = vec![0u64; iterations];

    // --- Part A: QPC Calibration ---
    let mut qpc_zero_count = 0usize;
    let mut min_nonzero_delta = u64::MAX;

    for sample in &mut qpc_raw_samples {
        let t0 = Instant::now();
        let t1 = Instant::now();
        let dt = t1.duration_since(t0).as_nanos() as u64;
        *sample = dt;
        if dt == 0 {
            qpc_zero_count += 1;
        } else if dt < min_nonzero_delta {
            min_nonzero_delta = dt;
        }
    }
    let qpc_zero_freq = qpc_zero_count as f64 / iterations as f64;
    let qpc_resolution_ns = if min_nonzero_delta == u64::MAX {
        0
    } else {
        min_nonzero_delta
    };

    // QPC empty bracket mean (no storage)
    let t_empty_start = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let t1 = Instant::now();
        black_box((t0, t1));
    }
    let qpc_empty_bracket_wall = t_empty_start.elapsed();
    let qpc_empty_bracket_mean_ns = qpc_empty_bracket_wall.as_nanos() as f64 / iterations as f64;

    // QPC complete harness cost (bracket + preallocated array write)
    let t_harness_start = Instant::now();
    for sample in &mut qpc_harness_samples {
        let t0 = Instant::now();
        let dt = t0.elapsed().as_nanos() as u64;
        *sample = dt;
    }
    let qpc_harness_wall = t_harness_start.elapsed();
    let qpc_harness_mean_ns = qpc_harness_wall.as_nanos() as f64 / iterations as f64;

    qpc_harness_samples.sort_unstable();
    let qpc_p50_ns = qpc_harness_samples[(iterations * 50) / 100];
    let qpc_p90_ns = qpc_harness_samples[(iterations * 90) / 100];
    let qpc_p99_ns = qpc_harness_samples[(iterations * 99) / 100];
    let qpc_p99_9_ns = qpc_harness_samples[(iterations * 999) / 1000];
    let qpc_max_ns = qpc_harness_samples[iterations - 1];

    let qpc_can_resolve_25ns =
        qpc_empty_bracket_mean_ns < 15.0 && qpc_p50_ns < 20 && qpc_resolution_ns <= 5;

    // --- Part B: Hardware TSC Calibration ---
    let tsc_cal = calibrate_tsc(Duration::from_millis(200));

    let mut tsc_harness_samples = vec![0u64; iterations];
    let t_tsc_start = Instant::now();
    for sample in &mut tsc_harness_samples {
        let c0 = read_tsc();
        let c1 = read_tsc();
        *sample = c1.saturating_sub(c0);
    }
    let tsc_harness_wall = t_tsc_start.elapsed();
    let tsc_harness_mean_ns = tsc_harness_wall.as_nanos() as f64 / iterations as f64;

    tsc_harness_samples.sort_unstable();
    let (tsc_p50_ns, tsc_p90_ns, tsc_p99_ns, tsc_p99_9_ns, tsc_max_ns, tsc_can_resolve_25ns) =
        if let Some(ref cal) = tsc_cal {
            let p50 = tsc_harness_samples[(iterations * 50) / 100] as f64 / cal.cycles_per_ns;
            let p90 = tsc_harness_samples[(iterations * 90) / 100] as f64 / cal.cycles_per_ns;
            let p99 = tsc_harness_samples[(iterations * 99) / 100] as f64 / cal.cycles_per_ns;
            let p99_9 = tsc_harness_samples[(iterations * 999) / 1000] as f64 / cal.cycles_per_ns;
            let max = tsc_harness_samples[iterations - 1] as f64 / cal.cycles_per_ns;
            let can_resolve = cal.empty_bracket_ns < 15.0 && cal.resolution_ns < 1.0;
            (p50, p90, p99, p99_9, max, can_resolve)
        } else {
            (0.0, 0.0, 0.0, 0.0, 0.0, false)
        };

    TimerCalibration {
        iterations,
        qpc_resolution_ns,
        qpc_zero_freq,
        qpc_empty_bracket_mean_ns,
        qpc_harness_mean_ns,
        qpc_p50_ns,
        qpc_p90_ns,
        qpc_p99_ns,
        qpc_p99_9_ns,
        qpc_max_ns,
        qpc_can_resolve_25ns,
        tsc: tsc_cal,
        tsc_harness_mean_ns,
        tsc_p50_ns,
        tsc_p90_ns,
        tsc_p99_ns,
        tsc_p99_9_ns,
        tsc_max_ns,
        tsc_can_resolve_25ns,
    }
}

pub fn print_calibration_report(cal: &TimerCalibration) {
    println!("=================================================================");
    println!("             TIMER CALIBRATION & RESOLUTION COMPARISON          ");
    println!("=================================================================");
    println!("Iterations: {}", cal.iterations);
    println!();
    println!("--- Backend 1: Windows System Clock (QPC / Instant::now) ---");
    println!(
        "  Observed Clock Resolution:         {} ns",
        cal.qpc_resolution_ns
    );
    println!(
        "  Zero-Duration Sample Frequency:    {:.2}%",
        cal.qpc_zero_freq * 100.0
    );
    println!(
        "  Empty Measurement Bracket Mean:    {:.2} ns",
        cal.qpc_empty_bracket_mean_ns
    );
    println!(
        "  Complete Harness Cost (w/ storage): {:.2} ns",
        cal.qpc_harness_mean_ns
    );
    println!("  Raw Harness Distribution (QPC):");
    println!("    p50 (median):                    {} ns", cal.qpc_p50_ns);
    println!("    p90:                             {} ns", cal.qpc_p90_ns);
    println!("    p99:                             {} ns", cal.qpc_p99_ns);
    println!(
        "    p99.9:                           {} ns",
        cal.qpc_p99_9_ns
    );
    println!("    Max Observed:                    {} ns", cal.qpc_max_ns);
    if cal.qpc_can_resolve_25ns {
        println!("  Timer Status for <25 ns target:    RELIABLE");
    } else {
        println!(
            "  Timer Status for <25 ns target:    UNRESOLVED (bracket overhead {:.1} ns > 25 ns)",
            cal.qpc_empty_bracket_mean_ns
        );
    }
    println!();

    println!("--- Backend 2: Hardware Invariant Counter (x86_64 TSC) ---");
    if let Some(ref tsc) = cal.tsc {
        println!(
            "  Calibrated Frequency:              {:.3} GHz ({:.3} cycles/ns)",
            tsc.tsc_ghz, tsc.cycles_per_ns
        );
        println!(
            "  Effective Resolution:              {:.3} ns / cycle",
            tsc.resolution_ns
        );
        println!(
            "  Empty Bracket Cycles:              {} cycles ({:.2} ns)",
            tsc.empty_bracket_cycles, tsc.empty_bracket_ns
        );
        println!(
            "  Complete Harness Cost (w/ storage): {:.2} ns",
            cal.tsc_harness_mean_ns
        );
        println!("  Raw Harness Distribution (TSC):");
        println!(
            "    p50 (median):                    {:.2} ns",
            cal.tsc_p50_ns
        );
        println!(
            "    p90:                             {:.2} ns",
            cal.tsc_p90_ns
        );
        println!(
            "    p99:                             {:.2} ns",
            cal.tsc_p99_ns
        );
        println!(
            "    p99.9:                           {:.2} ns",
            cal.tsc_p99_9_ns
        );
        println!(
            "    Max Observed:                    {:.2} ns",
            cal.tsc_max_ns
        );
        if cal.tsc_can_resolve_25ns {
            println!("  Timer Status for <25 ns target:    RELIABLE ({:.2} ns bracket overhead < 25 ns target)", tsc.empty_bracket_ns);
        } else {
            println!("  Timer Status for <25 ns target:    UNRESOLVED");
        }
    } else {
        println!("  Hardware TSC unavailable on this target architecture.");
    }
    println!("=================================================================\n");
}
