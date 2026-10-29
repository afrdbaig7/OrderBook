//! Hardware invariant Time-Stamp Counter (TSC) and OS timer backends.
//!
//! Enforces:
//! 1. Serializing fences (`_mm_lfence` + `_rdtsc` + `_mm_lfence` at start,
//!    `__rdtscp` + `_mm_lfence` at stop) according to Intel IA-32 benchmark guidelines.
//! 2. Thread core affinity pinning (`SetThreadAffinityMask`) to keep execution on a dedicated core.
//! 3. Hardware core ID tracking via `RDTSCP` aux register to detect and report core migrations.
//! 4. Accurate calibration against high-resolution monotonic time over a dedicated calibration interval.

use std::hint::black_box;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerBackendKind {
    Qpc,
    Tsc,
}

#[derive(Clone, Debug)]
pub struct TscCalibration {
    pub tsc_ghz: f64,
    pub cycles_per_ns: f64,
    pub empty_bracket_cycles: u64,
    pub empty_bracket_ns: f64,
    pub resolution_ns: f64,
    pub core_migrations_detected: usize,
}

/// Pins the current calling thread to a single logical core to eliminate core-switching jitter.
pub fn pin_thread_to_core(core_id: usize) -> bool {
    #[cfg(target_os = "windows")]
    {
        extern "system" {
            fn GetCurrentThread() -> isize;
            fn SetThreadAffinityMask(hThread: isize, dwThreadAffinityMask: usize) -> usize;
        }
        unsafe {
            let mask = 1usize << core_id;
            SetThreadAffinityMask(GetCurrentThread(), mask) != 0
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = core_id;
        false
    }
}

/// Reads the raw TSC cycle counter using ordered instructions.
#[inline(always)]
pub fn read_tsc() -> u64 {
    start_tsc().0
}

/// Starts an ordered, serialized TSC measurement bracket.
/// Prevents instructions from before or after from leaking into or out of the measurement.
#[inline(always)]
pub fn start_tsc() -> (u64, u32) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_lfence();
        let mut aux = 0u32;
        let tsc = core::arch::x86_64::__rdtscp(&mut aux);
        core::arch::x86_64::_mm_lfence();
        (tsc, aux)
    }
    #[cfg(not(target_arch = "x86_64"))]
    (0, 0)
}

/// Stops an ordered, serialized TSC measurement bracket.
/// Waits for all preceding instructions to retire and globally complete before reading TSC.
#[inline(always)]
pub fn stop_tsc() -> (u64, u32) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        let mut aux = 0u32;
        let tsc = core::arch::x86_64::__rdtscp(&mut aux);
        core::arch::x86_64::_mm_lfence();
        (tsc, aux)
    }
    #[cfg(not(target_arch = "x86_64"))]
    (0, 0)
}

/// Calibrates the invariant TSC frequency, resolution, and ordered bracket overhead.
pub fn calibrate_tsc(calibration_duration: Duration) -> Option<TscCalibration> {
    #[cfg(target_arch = "x86_64")]
    {
        // Pin to core 2 for stable invariant benchmarking
        let _ = pin_thread_to_core(2);

        // Warm up CPU pipelines and instruction caches
        for _ in 0..50_000 {
            black_box(start_tsc());
            black_box(stop_tsc());
        }

        // Measure TSC ticks over calibration window against monotonic clock
        let t_start = Instant::now();
        let (tsc_start, _) = start_tsc();

        while t_start.elapsed() < calibration_duration {
            std::hint::spin_loop();
        }

        let (tsc_end, _) = stop_tsc();
        let t_end = Instant::now();

        let elapsed_ns = t_end.duration_since(t_start).as_nanos() as f64;
        let elapsed_cycles = tsc_end.saturating_sub(tsc_start) as f64;

        if elapsed_ns <= 0.0 || elapsed_cycles <= 0.0 {
            return None;
        }

        let cycles_per_ns = elapsed_cycles / elapsed_ns;
        let tsc_ghz = cycles_per_ns;
        let resolution_ns = 1.0 / cycles_per_ns;

        // Measure ordered empty bracket cycles (100,000 samples)
        let sample_count = 100_000;
        let mut samples = vec![0u64; sample_count];
        let mut core_migrations = 0usize;

        for s in &mut samples {
            let (c0, aux0) = start_tsc();
            let (c1, aux1) = stop_tsc();
            *s = c1.saturating_sub(c0);
            if aux0 != aux1 {
                core_migrations += 1;
            }
        }

        samples.sort_unstable();
        let empty_bracket_cycles = samples[sample_count / 2];
        let empty_bracket_ns = empty_bracket_cycles as f64 / cycles_per_ns;

        Some(TscCalibration {
            tsc_ghz,
            cycles_per_ns,
            empty_bracket_cycles,
            empty_bracket_ns,
            resolution_ns,
            core_migrations_detected: core_migrations,
        })
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tsc_calibration_and_ordering() {
        let cal = calibrate_tsc(Duration::from_millis(50));
        #[cfg(target_arch = "x86_64")]
        {
            let cal = cal.expect("TSC calibration must succeed on x86_64");
            println!("Calibrated Ordered TSC: {cal:?}");
            assert!(cal.tsc_ghz > 1.0 && cal.tsc_ghz < 6.0);
            assert!(cal.empty_bracket_cycles > 0 && cal.empty_bracket_cycles < 300);
            assert!(cal.resolution_ns < 1.0);
        }
    }
}
