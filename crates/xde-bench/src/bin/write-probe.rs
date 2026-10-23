use clap::{Parser, ValueEnum};
use compio::{
    buf::{BufResult, IntoInner, IoBufExt, Slice},
    driver::{AsRawFd, Key, Proactor, PushEntry, SharedFd, op::WriteAt},
    io::AsyncWriteAtExt,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde::Serialize;
use std::{
    cell::Cell,
    io::{self, Read},
    path::{Path, PathBuf},
    rc::Rc,
    time::Instant,
};

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Proactor,
    Runtime,
    Busy,
}
#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, default_value_t = 1)]
    writers: usize,
    #[arg(long, default_value_t = 64)]
    size_mib_per_writer: u64,
    #[arg(long, default_value_t = 64)]
    write_kib: usize,
    #[arg(long)]
    separate_files: bool,
    #[arg(long, default_value_t = 64)]
    busy_tasks: usize,
    #[arg(long, default_value_t = 30)]
    pressure_seconds: u64,
}
#[derive(Serialize)]
struct Measurements {
    #[serde(skip)]
    load: Option<xde_bench::load::LoadObservation>,
    driver: String,
    pressure_expired: bool,
    pressure_polls_during_writes: u64,
    bytes: u64,
    operations: usize,
    seconds: f64,
    mib_s: f64,
    mean_write_us: f64,
    p50_write_us: f64,
    p90_write_us: f64,
    p99_write_us: f64,
    max_write_us: f64,
    effective_inflight: f64,
    operations_per_second: f64,
    driver_submissions_ready: Option<u64>,
    driver_submissions_pending: Option<u64>,
    driver_polls: Option<u64>,
    driver_completions: Option<u64>,
    driver_pop_calls: Option<u64>,
    driver_pop_pending: Option<u64>,
    driver_partial_completions: Option<u64>,
    write_future_polls: Option<u64>,
    write_future_first_ready: Option<u64>,
    write_future_first_pending: Option<u64>,
}
#[derive(Serialize)]
struct Report {
    schema_version: u32,
    validity_policy: &'static str,
    measurement_notes: Vec<String>,
    source_id: &'static str,
    compio_revision: &'static str,
    lock_blake3: String,
    compiler: &'static str,
    build_profile: &'static str,
    build_target: &'static str,
    os: String,
    processor_identifier: Option<String>,
    logical_processors: usize,
    mode: Mode,
    writers: usize,
    bytes_per_writer: u64,
    write_bytes: usize,
    separate_files: bool,
    busy_tasks: usize,
    measurements: Measurements,
    load: xde_bench::load::LoadObservation,
    validity: xde_bench::Validity,
}
fn paths(root: &Path, writers: usize, separate: bool) -> Vec<PathBuf> {
    (0..writers)
        .map(|n| root.join(format!("{}.bin", if separate { n } else { 0 })))
        .collect()
}
fn prepare(paths: &[PathBuf], bytes: u64, separate: bool) -> io::Result<()> {
    for (n, path) in paths.iter().enumerate() {
        if separate || n == 0 {
            std::fs::File::create(path)?.set_len(if separate {
                bytes
            } else {
                bytes * paths.len() as u64
            })?;
        }
    }
    Ok(())
}
fn verify(paths: &[PathBuf], bytes: u64, separate: bool) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut buffer = vec![0; 64 * 1024];
    for (writer, path) in paths.iter().enumerate() {
        let mut file = std::fs::File::open(path)?;
        let expected_len = if separate {
            bytes
        } else {
            bytes * paths.len() as u64
        };
        if file.metadata()?.len() != expected_len {
            return Err(io::Error::other("wrong file length"));
        }
        file.seek(SeekFrom::Start(if separate {
            0
        } else {
            writer as u64 * bytes
        }))?;
        let mut remaining = bytes;
        while remaining > 0 {
            let len = remaining.min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..len])?;
            if buffer[..len].iter().any(|&byte| byte != writer as u8 + 1) {
                return Err(io::Error::other("file payload mismatch"));
            }
            remaining -= len as u64;
        }
    }
    Ok(())
}
fn summarize(bytes: u64, seconds: f64, mut latencies: Vec<f64>) -> Measurements {
    latencies.sort_by(f64::total_cmp);
    let count = latencies.len();
    let sum: f64 = latencies.iter().sum();
    let percentile = |p: f64| latencies[((count - 1) as f64 * p).round() as usize] * 1e6;
    Measurements {
        load: None,
        driver: String::new(),
        pressure_expired: false,
        pressure_polls_during_writes: 0,
        bytes,
        operations: count,
        seconds,
        mib_s: bytes as f64 / seconds / 1048576.0,
        mean_write_us: sum * 1e6 / count as f64,
        p50_write_us: percentile(0.5),
        p90_write_us: percentile(0.9),
        p99_write_us: percentile(0.99),
        max_write_us: latencies[count - 1] * 1e6,
        effective_inflight: sum / seconds,
        operations_per_second: count as f64 / seconds,
        driver_submissions_ready: None,
        driver_submissions_pending: None,
        driver_polls: None,
        driver_completions: None,
        driver_pop_calls: None,
        driver_pop_pending: None,
        driver_partial_completions: None,
        write_future_polls: None,
        write_future_first_ready: None,
        write_future_first_pending: None,
    }
}

type Write = WriteAt<Slice<Vec<u8>>, SharedFd<std::fs::File>>;
struct Lane {
    file: SharedFd<std::fs::File>,
    key: Option<Key<Write>>,
    ready: Option<BufResult<usize, Write>>,
    offset: u64,
    done: u64,
    partial: usize,
    started: Instant,
    ready_submissions: u64,
    pending_submissions: u64,
}
fn submit(driver: &mut Proactor, lane: &mut Lane, buffer: Vec<u8>, write_bytes: usize) {
    let op = WriteAt::new(
        lane.file.clone(),
        lane.offset + lane.done + lane.partial as u64,
        buffer.slice(lane.partial..write_bytes),
    );
    match driver.push(op) {
        PushEntry::Pending(key) => {
            lane.pending_submissions += 1;
            lane.key = Some(key);
        }
        PushEntry::Ready(result) => {
            lane.ready_submissions += 1;
            lane.ready = Some(result);
        }
    }
}
fn proactor(
    paths: &[PathBuf],
    bytes: u64,
    write_bytes: usize,
    separate: bool,
) -> io::Result<Measurements> {
    let mut driver = Proactor::new()?;
    let mut lanes = Vec::new();
    for (n, path) in paths.iter().enumerate() {
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED);
        }
        let file = SharedFd::new(options.open(path)?);
        driver.attach(file.as_raw_fd())?;
        lanes.push(Lane {
            file,
            key: None,
            ready: None,
            offset: if separate { 0 } else { n as u64 * bytes },
            done: 0,
            partial: 0,
            started: Instant::now(),
            ready_submissions: 0,
            pending_submissions: 0,
        });
    }
    let buffers: Vec<_> = (0..paths.len())
        .map(|n| vec![n as u8 + 1; write_bytes])
        .collect();
    let mut latencies = Vec::with_capacity((bytes / write_bytes as u64) as usize * paths.len());
    let monitor = xde_bench::load::Monitor::start();
    let start = Instant::now();
    for (lane, buffer) in lanes.iter_mut().zip(buffers) {
        lane.started = Instant::now();
        submit(&mut driver, lane, buffer, write_bytes);
    }
    let mut polls = 0;
    let mut completions = 0;
    let mut pops = 0;
    let mut pending_pops = 0;
    let mut partial_completions = 0;
    while lanes.iter().any(|lane| lane.done < bytes) {
        let mut progressed = false;
        for lane in &mut lanes {
            if let Some(key) = lane.key.take() {
                pops += 1;
                match driver.pop(key) {
                    PushEntry::Pending(key) => {
                        pending_pops += 1;
                        lane.key = Some(key);
                    }
                    PushEntry::Ready(result) => lane.ready = Some(result),
                }
            }
            if let Some(BufResult(result, operation)) = lane.ready.take() {
                progressed = true;
                completions += 1;
                let written = result?;
                if written == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                let buffer = operation.into_inner().into_inner();
                lane.partial += written;
                if lane.partial < write_bytes {
                    partial_completions += 1;
                }
                if lane.partial == write_bytes {
                    latencies.push(lane.started.elapsed().as_secs_f64());
                    lane.done += write_bytes as u64;
                    lane.partial = 0;
                    lane.started = Instant::now();
                }
                if lane.done < bytes {
                    submit(&mut driver, lane, buffer, write_bytes);
                }
            }
        }
        if !progressed && lanes.iter().any(|lane| lane.done < bytes) {
            polls += 1;
            driver.poll(None)?;
        }
    }
    let seconds = start.elapsed().as_secs_f64();
    let load = monitor.finish();
    let mut result = summarize(bytes * paths.len() as u64, seconds, latencies);
    result.load = Some(load);
    result.driver_pop_calls = Some(pops);
    result.driver_pop_pending = Some(pending_pops);
    result.driver_partial_completions = Some(partial_completions);
    result.driver_polls = Some(polls);
    result.driver_completions = Some(completions);
    result.driver_submissions_ready = Some(lanes.iter().map(|lane| lane.ready_submissions).sum());
    result.driver_submissions_pending =
        Some(lanes.iter().map(|lane| lane.pending_submissions).sum());
    result.driver = format!("{:?}", driver.driver_type());
    Ok(result)
}
fn runtime(
    paths: &[PathBuf],
    bytes: u64,
    write_bytes: usize,
    separate: bool,
    busy_tasks: usize,
    pressure_seconds: u64,
) -> io::Result<Measurements> {
    compio::runtime::Runtime::new()?.block_on(async {
        let mut files = Vec::new();
        for path in paths {
            files.push(
                compio::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .await?,
            );
        }
        let stop = Rc::new(Cell::new(false));
        let expired = Rc::new(Cell::new(false));
        let started = Rc::new(Cell::new(0));
        let pressure_polls = Rc::new(Cell::new(0u64));
        let pressure_deadline = Instant::now() + std::time::Duration::from_secs(pressure_seconds);
        let tasks: Vec<_> = (0..busy_tasks)
            .map(|_| {
                let stop = stop.clone();
                let expired = expired.clone();
                let started = started.clone();
                let pressure_polls = pressure_polls.clone();
                let mut first_poll = true;
                compio::runtime::spawn(std::future::poll_fn(move |cx| {
                    if first_poll {
                        started.set(started.get() + 1);
                        first_poll = false;
                    }
                    pressure_polls.set(pressure_polls.get() + 1);
                    if stop.get() {
                        return std::task::Poll::Ready(());
                    }
                    if Instant::now() >= pressure_deadline {
                        expired.set(true);
                        return std::task::Poll::Ready(());
                    }
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }))
            })
            .collect();
        std::future::poll_fn(|cx| {
            if started.get() == busy_tasks {
                return std::task::Poll::Ready(());
            }
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        })
        .await;
        let mut requests = FuturesUnordered::new();
        let future_polls = Rc::new(Cell::new(0u64));
        let first_ready = Rc::new(Cell::new(0u64));
        let first_pending = Rc::new(Cell::new(0u64));
        // Construct and initialize buffers before entering the measured interval.
        let buffers: Vec<_> = (0..paths.len())
            .map(|n| vec![n as u8 + 1; write_bytes])
            .collect();
        let mut latencies = Vec::with_capacity((bytes / write_bytes as u64) as usize * paths.len());
        let mut completed_files = Vec::with_capacity(paths.len());
        let monitor = xde_bench::load::Monitor::start();
        let pressure_before = pressure_polls.get();
        let start = Instant::now();
        for (writer, (mut file, mut buffer)) in files.into_iter().zip(buffers).enumerate() {
            let future_polls = future_polls.clone();
            let first_ready = first_ready.clone();
            let first_pending = first_pending.clone();
            requests.push(async move {
                let mut latencies = Vec::with_capacity((bytes / write_bytes as u64) as usize);
                let base = if separate { 0 } else { writer as u64 * bytes };
                let mut done = 0;
                while done < bytes {
                    let start = Instant::now();
                    let mut write = std::pin::pin!(file.write_all_at(buffer, base + done));
                    let mut first = true;
                    let BufResult(result, returned) = std::future::poll_fn(|cx| {
                        future_polls.set(future_polls.get() + 1);
                        let result = std::future::Future::poll(write.as_mut(), cx);
                        if first {
                            let counter = if result.is_ready() {
                                &first_ready
                            } else {
                                &first_pending
                            };
                            counter.set(counter.get() + 1);
                            first = false;
                        }
                        result
                    })
                    .await;
                    buffer = returned;
                    result?;
                    latencies.push(start.elapsed().as_secs_f64());
                    done += write_bytes as u64;
                }
                Ok::<_, io::Error>((file, latencies))
            });
        }
        let mut error = None;
        while let Some(result) = requests.next().await {
            match result {
                Ok((file, samples)) => {
                    completed_files.push(file);
                    latencies.extend(samples);
                }
                Err(failure) => {
                    error.get_or_insert(failure);
                }
            }
        }
        let seconds = start.elapsed().as_secs_f64();
        let pressure_during = pressure_polls.get() - pressure_before;
        stop.set(true);
        let load = monitor.finish();
        for task in tasks {
            let _ = task.await;
        }
        drop(completed_files);
        if let Some(error) = error {
            return Err(error);
        }
        let mut result = summarize(bytes * paths.len() as u64, seconds, latencies);
        result.driver = compio::runtime::Runtime::with_current(|runtime| {
            format!("{:?}", runtime.driver_type())
        });
        result.load = Some(load);
        result.write_future_polls = Some(future_polls.get());
        result.write_future_first_ready = Some(first_ready.get());
        result.write_future_first_pending = Some(first_pending.get());
        result.pressure_expired = expired.get();
        result.pressure_polls_during_writes = pressure_during;
        Ok(result)
    })
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let bytes = args
        .size_mib_per_writer
        .checked_mul(1048576)
        .ok_or("size overflow")?;
    let write_bytes = args
        .write_kib
        .checked_mul(1024)
        .ok_or("write size overflow")?;
    if !(1..=8).contains(&args.writers)
        || bytes == 0
        || write_bytes == 0
        || !bytes.is_multiple_of(write_bytes as u64)
        || args.busy_tasks > 4096
        || (matches!(args.mode, Mode::Busy) && args.busy_tasks == 0)
    {
        return Err(
            "require 1..8 writers, positive aligned sizes and at most 4096 busy tasks".into(),
        );
    }
    bytes
        .checked_mul(args.writers as u64)
        .ok_or("total size overflow")?;
    let directory = tempfile::tempdir()?;
    let paths = paths(directory.path(), args.writers, args.separate_files);
    prepare(&paths, bytes, args.separate_files)?;
    let busy_tasks = if matches!(args.mode, Mode::Busy) {
        args.busy_tasks
    } else {
        0
    };
    let mut measurements = match args.mode {
        Mode::Proactor => proactor(&paths, bytes, write_bytes, args.separate_files)?,
        Mode::Runtime | Mode::Busy => runtime(
            &paths,
            bytes,
            write_bytes,
            args.separate_files,
            busy_tasks,
            args.pressure_seconds,
        )?,
    };
    let load = measurements
        .load
        .take()
        .expect("write interval observations");
    verify(&paths, bytes, args.separate_files)?;
    let validity = if measurements.pressure_expired {
        xde_bench::Validity::Invalid(vec![
            "runnable pressure expired before write completion".into(),
        ])
    } else if busy_tasks > 0 && measurements.pressure_polls_during_writes == 0 {
        xde_bench::Validity::Invalid(vec!["no runnable pressure observed during writes".into()])
    } else {
        xde_bench::Validity::Valid
    };
    let mut measurement_notes = load.limitations();
    if measurements.seconds < 1.0 {
        measurement_notes.push(
            "sub-second write interval; timing and CPU sampling resolution are limited".into(),
        );
    }
    let report = Report {
        schema_version: 4,
        validity_policy: "workstation_v1",
        measurement_notes,
        source_id: env!("XDE_SOURCE_ID"),
        compio_revision: env!("XDE_COMPIO_REV"),
        lock_blake3: blake3::hash(include_bytes!("../../../../Cargo.lock"))
            .to_hex()
            .to_string(),
        compiler: env!("XDE_COMPILER"),
        build_profile: env!("XDE_BUILD_PROFILE"),
        build_target: env!("XDE_BUILD_TARGET"),
        os: if cfg!(windows) {
            std::process::Command::new("cmd")
                .args(["/c", "ver"])
                .output()
        } else {
            std::process::Command::new("uname").arg("-a").output()
        }
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|error| error.to_string()),
        processor_identifier: std::env::var("PROCESSOR_IDENTIFIER").ok(),
        logical_processors: std::thread::available_parallelism().map_or(1, usize::from),
        mode: args.mode,
        writers: args.writers,
        bytes_per_writer: bytes,
        write_bytes,
        separate_files: args.separate_files,
        busy_tasks,
        measurements,
        load,
        validity,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_modes_materialize_disjoint_writer_regions() {
        for separate in [false, true] {
            for mode in [Mode::Proactor, Mode::Runtime, Mode::Busy] {
                let root = tempfile::tempdir().unwrap();
                let paths = paths(root.path(), 2, separate);
                let bytes = 1024 * 1024;
                prepare(&paths, bytes, separate).unwrap();
                let result = match mode {
                    Mode::Proactor => proactor(&paths, bytes, 4096, separate),
                    Mode::Runtime => runtime(&paths, bytes, 4096, separate, 0, 2),
                    Mode::Busy => runtime(&paths, bytes, 4096, separate, 8, 2),
                }
                .unwrap();
                assert!(
                    !result.pressure_expired,
                    "writes must progress while tasks remain runnable"
                );
                if matches!(mode, Mode::Busy) {
                    assert!(result.pressure_polls_during_writes > 0);
                }
                assert_eq!(result.bytes, 2 * bytes);
                assert_eq!(result.operations, 512);
                verify(&paths, bytes, separate).unwrap();
            }
        }
    }
}
