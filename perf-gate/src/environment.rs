//! Host fingerprint recorded with every run (Linux `/proc` and `/sys`).

use std::path::Path;

use crate::schema::Environment;

/// Capture the host state at the start of a run.
pub fn capture(data_root: &Path, server_cpus: Option<String>, build: String) -> Environment {
    let root = data_root
        .canonicalize()
        .unwrap_or_else(|_| data_root.to_path_buf());
    Environment {
        hostname: read_trimmed("/proc/sys/kernel/hostname"),
        kernel: read_trimmed("/proc/sys/kernel/osrelease"),
        cpu_model: cpu_model(),
        logical_cpus: logical_cpus(),
        mem_total_kb: mem_total_kb(),
        cpu_governor: read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        loadavg_start: loadavg(),
        loadavg_end: String::new(),
        data_fs: filesystem_of(&root),
        data_root: root.display().to_string(),
        server_cpus,
        build,
    }
}

/// `/proc/loadavg`, for the start and end of a run.
pub fn loadavg() -> String {
    read_trimmed("/proc/loadavg")
}

fn read_trimmed(path: &str) -> String {
    std::fs::read_to_string(path).map_or_else(|_| "unknown".to_string(), |s| s.trim().to_string())
}

fn cpu_model() -> String {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find_map(|line| line.strip_prefix("model name"))
                .map(|rest| rest.trim_start_matches([' ', '\t', ':']).to_string())
        })
        .unwrap_or_else(|| "unknown".to_string())
}

/// The host's logical CPUs. Not `available_parallelism`: that is this
/// process's affinity, which `--gate-cpus` narrows.
fn logical_cpus() -> usize {
    std::fs::read_to_string("/proc/cpuinfo")
        .map(|info| {
            info.lines()
                .filter(|line| line.starts_with("processor"))
                .count()
        })
        .ok()
        .filter(|count| *count > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(0, usize::from))
}

fn mem_total_kb() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find_map(|line| line.strip_prefix("MemTotal:"))
                .and_then(|rest| rest.trim().trim_end_matches("kB").trim().parse().ok())
        })
        .unwrap_or(0)
}

/// Type of the filesystem whose mount point is the longest prefix of `path`.
fn filesystem_of(path: &Path) -> String {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return "unknown".to_string();
    };
    mounts
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let (_, mount, fs) = (fields.next()?, fields.next()?, fields.next()?);
            path.starts_with(mount)
                .then(|| (mount.len(), fs.to_string()))
        })
        .max_by_key(|(len, _)| *len)
        .map_or_else(|| "unknown".to_string(), |(_, fs)| fs)
}
