use serde::Serialize;

/// Kernel lifetime process high-water mark, including the in-process fixture.
#[cfg(windows)]
pub fn peak_working_set_bytes() -> Option<u64> {
    use windows_sys::Win32::System::{
        ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::GetCurrentProcess,
    };
    let mut counters: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
    counters.cb = std::mem::size_of_val(&counters) as u32;
    // Current-process pseudo handle and a correctly sized writable output.
    if unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    } == 0
    {
        None
    } else {
        Some(counters.PeakWorkingSetSize as u64)
    }
}
#[cfg(target_os = "linux")]
pub fn peak_working_set_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}
#[cfg(not(any(windows, target_os = "linux")))]
pub fn peak_working_set_bytes() -> Option<u64> {
    None
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct CpuSample {
    pub total: u64,
    pub idle: u64,
    pub process: u64,
    pub child_process: u64,
}

#[derive(Serialize)]
pub struct LoadWindow {
    pub start: CpuSample,
    pub end: CpuSample,
    pub child_cpu: u64,
    pub total_percent: f64,
    pub controlled_percent: f64,
    pub background_percent: f64,
    pub attribution_excess_ticks: u64,
}

impl LoadWindow {
    pub fn between(start: CpuSample, end: CpuSample) -> Option<Self> {
        let total = end.total.checked_sub(start.total)?;
        let idle = end.idle.checked_sub(start.idle)?;
        let own = end.process.checked_sub(start.process)?;
        if total == 0 || idle > total {
            return None;
        }
        let busy = total - idle;
        let child_cpu = end.child_process.checked_sub(start.child_process)?;
        let controlled = own.checked_add(child_cpu)?;
        let background = busy.saturating_sub(controlled);
        Some(Self {
            start,
            end,
            child_cpu,
            total_percent: busy as f64 * 100.0 / total as f64,
            controlled_percent: controlled as f64 * 100.0 / total as f64,
            background_percent: background as f64 * 100.0 / total as f64,
            attribution_excess_ticks: controlled.saturating_sub(busy),
        })
    }
}

#[cfg(windows)]
pub fn sample() -> Option<CpuSample> {
    system_processors()?;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetSystemTimes};
    let mut idle = Default::default();
    let mut kernel = Default::default();
    let mut user = Default::default();
    // GetSystemTimes writes three FILETIMEs. Kernel time includes idle time.
    if unsafe { GetSystemTimes(&mut idle, &mut kernel, &mut user) } == 0 {
        return None;
    }
    Some(CpuSample {
        total: ticks(kernel) + ticks(user),
        idle: ticks(idle),
        process: process_cpu(unsafe { GetCurrentProcess() })?,
        child_process: 0,
    })
}

#[cfg(windows)]
fn ticks(time: windows_sys::Win32::Foundation::FILETIME) -> u64 {
    u64::from(time.dwLowDateTime) | (u64::from(time.dwHighDateTime) << 32)
}

#[cfg(windows)]
fn process_cpu(handle: windows_sys::Win32::Foundation::HANDLE) -> Option<u64> {
    use windows_sys::Win32::System::Threading::GetProcessTimes;
    let mut created = Default::default();
    let mut exited = Default::default();
    let mut kernel = Default::default();
    let mut user = Default::default();
    // The borrowed process handle remains live for the duration of this call.
    if unsafe { GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user) } == 0 {
        return None;
    }
    Some(ticks(kernel) + ticks(user))
}

#[cfg(not(windows))]
pub fn sample() -> Option<CpuSample> {
    None
}

#[cfg(windows)]
fn system_processors() -> Option<usize> {
    use windows_sys::Win32::System::Threading::{
        ALL_PROCESSOR_GROUPS, GetActiveProcessorCount, GetActiveProcessorGroupCount,
    };
    // GetSystemTimes is group-scoped on large systems, while process time may
    // span groups. Do not accept their subtraction without group attribution.
    if unsafe { GetActiveProcessorGroupCount() } != 1 {
        return None;
    }
    let count = unsafe { GetActiveProcessorCount(ALL_PROCESSOR_GROUPS) } as usize;
    (count > 0).then_some(count)
}
#[cfg(not(windows))]
fn system_processors() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn useful_work_on_many_cores_is_not_external_load() {
        let start = CpuSample {
            total: 0,
            idle: 0,
            process: 0,
            child_process: 0,
        };
        let end = CpuSample {
            total: 24_000,
            idle: 12_000,
            process: 12_000,
            child_process: 0,
        };
        let window = LoadWindow::between(start, end).unwrap();
        assert_eq!(window.total_percent, 50.0);
        assert_eq!(window.controlled_percent, 50.0);
        assert_eq!(window.background_percent, 0.0);
    }
    #[test]
    fn one_busy_processor_is_normalized_over_all_processors() {
        let window = LoadWindow::between(
            CpuSample {
                total: 0,
                idle: 0,
                process: 0,
                child_process: 0,
            },
            CpuSample {
                total: 24_000,
                idle: 23_000,
                process: 0,
                child_process: 0,
            },
        )
        .unwrap();
        assert!((window.background_percent - 100.0 / 24.0).abs() < 1e-12);
    }
    #[test]
    fn attribution_discrepancies_remain_auditable() {
        let window = LoadWindow::between(
            CpuSample {
                total: 0,
                idle: 0,
                process: 0,
                child_process: 0,
            },
            CpuSample {
                total: 1000,
                idle: 900,
                process: 110,
                child_process: 20,
            },
        )
        .unwrap();
        assert_eq!(window.background_percent, 0.0);
        assert_eq!(window.attribution_excess_ticks, 30);
        assert_eq!(window.controlled_percent, 13.0);
    }
    #[test]
    fn counter_regressions_and_invalid_intervals_are_rejected() {
        let start = CpuSample {
            total: 100,
            idle: 50,
            process: 20,
            child_process: 0,
        };
        for end in [
            CpuSample {
                total: 99,
                idle: 50,
                process: 20,
                child_process: 0,
            },
            CpuSample {
                total: 100,
                idle: 50,
                process: 20,
                child_process: 0,
            },
            CpuSample {
                total: 110,
                idle: 70,
                process: 20,
                child_process: 0,
            },
            CpuSample {
                total: 110,
                idle: 50,
                process: 19,
                child_process: 0,
            },
        ] {
            assert!(LoadWindow::between(start, end).is_none());
        }
    }
    #[test]
    fn registered_children_are_differenced_independently() {
        let start = CpuSample {
            total: 1000,
            idle: 500,
            process: 200,
            child_process: 50,
        };
        let end = CpuSample {
            total: 2000,
            idle: 1000,
            process: 300,
            child_process: 350,
        };
        let window = LoadWindow::between(start, end).unwrap();
        assert_eq!(window.child_cpu, 300);
        assert_eq!(window.controlled_percent, 40.0);
        assert_eq!(window.background_percent, 10.0);
    }
    #[test]
    fn cumulative_cpu_counters_are_differenced_and_own_work_is_excluded() {
        let start = CpuSample {
            total: 10_000,
            idle: 5_000,
            process: 1_000,
            child_process: 0,
        };
        let end = CpuSample {
            total: 11_000,
            idle: 5_500,
            process: 1_200,
            child_process: 0,
        };
        assert_eq!(
            LoadWindow::between(
                start,
                CpuSample {
                    child_process: 100,
                    ..end
                }
            )
            .unwrap()
            .background_percent,
            20.0
        );
    }
}

/// Independent 100 ms observations. All percentages use aggregate processor time;
/// one busy processor on a 24-processor host contributes about 4.17 percent.
#[derive(Serialize)]
pub struct LoadObservation {
    pub idle_before: Option<LoadWindow>,
    pub during: Option<LoadWindow>,
    pub intervals: Vec<LoadWindow>,
    pub idle_after: Option<LoadWindow>,
    pub sampling_period_ms: u64,
    pub system_logical_processors: Option<usize>,
    pub elapsed_seconds: f64,
}
impl LoadObservation {
    pub fn limitations(&self) -> Vec<String> {
        let mut reasons = Vec::new();
        if self.system_logical_processors.is_none()
            || self.idle_before.is_none()
            || self.during.is_none()
            || self.idle_after.is_none()
            || self.intervals.is_empty()
        {
            reasons.push("incomplete CPU observations".into());
        }
        if let Some(window) = &self.during
            && window.attribution_excess_ticks as f64
                > (window.end.total - window.start.total) as f64 * 0.01
        {
            reasons.push("controlled CPU exceeds system busy time by more than one percentage point; attribution uncertain".into());
        }
        let processors = self.system_logical_processors.unwrap_or(1) as u64;
        if self.intervals.iter().any(|window| {
            let span = window.end.total - window.start.total;
            span >= processors * 250_000
                && window.attribution_excess_ticks as f64 > span as f64 * 0.01
        }) {
            reasons.push("attribution uncertain in one or more sampled intervals".into());
        }
        reasons
    }
}

pub struct Monitor {
    stop: std::sync::mpsc::Sender<()>,
    thread: std::thread::JoinHandle<(Option<LoadWindow>, Vec<LoadWindow>, f64)>,
    idle_before: Option<LoadWindow>,
    children: std::sync::Arc<std::sync::Mutex<Vec<ControlledProcess>>>,
}
#[cfg(windows)]
type ControlledProcess = std::os::windows::io::OwnedHandle;
#[cfg(not(windows))]
type ControlledProcess = ();

fn controlled_sample(children: &std::sync::Mutex<Vec<ControlledProcess>>) -> Option<CpuSample> {
    let result = sample()?;
    #[cfg(windows)]
    let result = {
        let mut result = result;
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            for handle in children.lock().ok()?.iter() {
                result.child_process = result
                    .child_process
                    .checked_add(process_cpu(handle.as_raw_handle())?)?;
            }
        }
        result
    };
    #[cfg(not(windows))]
    let _ = children;
    Some(result)
}
fn idle_window() -> Option<LoadWindow> {
    let start = sample();
    std::thread::sleep(std::time::Duration::from_millis(250));
    start
        .zip(sample())
        .and_then(|(a, b)| LoadWindow::between(a, b))
}
impl Monitor {
    pub fn start() -> Self {
        let idle_before = idle_window();
        let children = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = children.clone();
        let (stop, receiver) = std::sync::mpsc::channel();
        let (ready, started) = std::sync::mpsc::sync_channel(0);
        let thread = std::thread::spawn(move || {
            let start = controlled_sample(&observed);
            let mut previous = start;
            let clock = std::time::Instant::now();
            let mut intervals = Vec::new();
            let _ = ready.send(());
            loop {
                let done = !matches!(
                    receiver.recv_timeout(std::time::Duration::from_millis(100)),
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                );
                let next = controlled_sample(&observed);
                if let Some(window) = previous
                    .zip(next)
                    .and_then(|(a, b)| LoadWindow::between(a, b))
                {
                    intervals.push(window);
                }
                previous = next;
                if done {
                    return (
                        start.zip(next).and_then(|(a, b)| LoadWindow::between(a, b)),
                        intervals,
                        clock.elapsed().as_secs_f64(),
                    );
                }
            }
        });
        started.recv().expect("CPU observer started");
        Self {
            stop,
            thread,
            idle_before,
            children,
        }
    }
    pub fn register_child(&self, child: &std::process::Child) -> std::io::Result<()> {
        #[cfg(windows)]
        {
            use std::os::windows::io::FromRawHandle;
            use windows_sys::Win32::System::Threading::{
                OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            };
            let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, child.id()) };
            if handle.is_null() {
                return Err(std::io::Error::last_os_error());
            }
            self.children
                .lock()
                .expect("CPU process registry")
                .push(unsafe { ControlledProcess::from_raw_handle(handle) });
        }
        #[cfg(not(windows))]
        let _ = (child, &self.children);
        Ok(())
    }
    pub fn finish(self) -> LoadObservation {
        let _ = self.stop.send(());
        let (during, intervals, elapsed_seconds) =
            self.thread.join().expect("CPU observer finished");
        LoadObservation {
            idle_before: self.idle_before,
            during,
            intervals,
            idle_after: idle_window(),
            sampling_period_ms: 100,
            system_logical_processors: system_processors(),
            elapsed_seconds,
        }
    }
}
