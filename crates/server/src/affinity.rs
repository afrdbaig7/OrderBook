//! Dedicated CPU core pinning and thread affinity.
//!
//! Provides deterministic affinity assignment for the single-writer matching engine thread
//! across Windows (`SetThreadAffinityMask`) and Linux (`sched_setaffinity`).

/// Pins the current calling thread to a single logical core to eliminate core-switching jitter.
pub fn pin_thread_to_core(core_id: usize) -> bool {
    #[cfg(target_os = "windows")]
    {
        extern "system" {
            fn GetCurrentThread() -> isize;
            fn SetThreadAffinityMask(hThread: isize, dwThreadAffinityMask: usize) -> usize;
        }
        unsafe {
            if core_id >= usize::BITS as usize {
                return false;
            }
            let mask = 1usize << core_id;
            SetThreadAffinityMask(GetCurrentThread(), mask) != 0
        }
    }
    #[cfg(target_os = "linux")]
    {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(core_id, &mut set);
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0
        }
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        let _ = core_id;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pin_thread_to_core_bounds() {
        // Core ID beyond bit width should safely return false without crashing
        assert!(!pin_thread_to_core(usize::MAX));
    }
}

