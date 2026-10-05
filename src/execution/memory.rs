//! Per-thread heap accounting for the per-query memory limit and the
//! server's query memory budget.
//!
//! [`MeteredAllocator`], the process's global allocator, forwards to the
//! system allocator and keeps a running count of the bytes each thread has
//! allocated minus the bytes it has freed. The count is a plain thread-local
//! cell: no lock and no atomic on the allocation path.
//!
//! A request's computation runs on the threads evaluating it (Differential
//! Dataflow executes in place), so the change in their counts since the
//! request started is what the request holds. At the evaluator's cooperative checkpoints each
//! thread charges its growth since the last one through
//! [`RequestControl::charge_memory`], which sums it over the request's
//! threads and over all requests in flight and stops a request over its limit
//! or growing past the server's budget, as a deadline would.
//!
//! [`RequestControl::charge_memory`]: super::RequestControl::charge_memory

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Bytes allocated minus bytes freed by this thread since it started.
    static NET_BYTES: Cell<i64> = const { Cell::new(0) };
}

#[inline]
fn charge(bytes: i64) {
    // A const-initialised cell without a destructor is never torn down, so
    // this never fails and never allocates; `try_with` keeps it panic-free.
    let _ = NET_BYTES.try_with(|net| net.set(net.get().wrapping_add(bytes)));
}

/// Bytes this thread has allocated minus bytes it has freed, since it
/// started. Only differences between two readings on one thread mean
/// anything.
pub fn thread_net_bytes() -> i64 {
    NET_BYTES.try_with(Cell::get).unwrap_or(0)
}

/// Peak bytes past which a finished computation returns the memory it freed
/// to the operating system; see [`release_freed_memory`].
pub const RELEASE_AFTER_PEAK_BYTES: i64 = 64 << 20;

/// Return the heap memory the process has freed to the operating system.
/// The system allocator keeps freed memory for reuse, so without this a
/// server that once ran a query to its memory limit stays that large after
/// the query ended. It walks the whole heap: call it only after a
/// computation that held more than [`RELEASE_AFTER_PEAK_BYTES`].
pub fn release_freed_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> std::ffi::c_int;
        }
        // SAFETY: glibc's `malloc_trim` takes no pointers and only returns
        // free pages of the allocator's own heaps; it is thread-safe.
        unsafe {
            malloc_trim(0);
        }
    }
}

/// The memory limit of the container the process runs in, if one is set:
/// see [`process_memory_limit`].
pub fn container_memory_limit() -> Option<u64> {
    let own = std::fs::read_to_string("/proc/self/cgroup").ok();
    process_memory_limit(std::path::Path::new("/sys/fs/cgroup"), own.as_deref())
}

/// The memory limit on a process whose `/proc/<pid>/cgroup` reads `own`,
/// under the cgroup filesystem mounted at `root`: the tightest of the
/// limit at `root` (a container's own mount, or v1) and the v2 limits of
/// the process's cgroup and every cgroup above it, where a unit's
/// `MemoryMax=` lands when systemd runs the server.
pub fn process_memory_limit(root: &std::path::Path, own: Option<&str>) -> Option<u64> {
    let path = own.and_then(|text| text.lines().find_map(|line| line.strip_prefix("0::")));
    let mut dir = root.to_path_buf();
    let mut tightest = cgroup_memory_limit(root);
    for part in path.into_iter().flat_map(|path| path.split('/')) {
        if part.is_empty() || part == "." || part == ".." {
            continue;
        }
        dir.push(part);
        if let Some(limit) = read_limit(&dir.join("memory.max")) {
            tightest = Some(tightest.map_or(limit, |t| t.min(limit)));
        }
    }
    tightest
}

/// The memory limit the cgroup filesystem mounted at `root` sets (v2, else
/// v1), if any.
pub fn cgroup_memory_limit(root: &std::path::Path) -> Option<u64> {
    read_limit(&root.join("memory.max"))
        .or_else(|| read_limit(&root.join("memory/memory.limit_in_bytes")))
}

/// The limit a cgroup memory limit file holds, if it sets one.
fn read_limit(file: &std::path::Path) -> Option<u64> {
    let text = std::fs::read_to_string(file).ok()?;
    // v2 says `max` when unlimited, v1 a number near 2^63.
    let limit: u64 = text.trim().parse().ok()?;
    (limit < 1 << 60).then_some(limit)
}

/// The system allocator, metering each thread's net allocation.
pub struct MeteredAllocator;

// SAFETY: every call forwards unchanged to `System`, which upholds the
// `GlobalAlloc` contract; the accounting only touches a thread-local cell and
// never allocates.
unsafe impl GlobalAlloc for MeteredAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's guarantees.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            charge(layout.size() as i64);
        }
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded with the caller's guarantees.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            charge(layout.size() as i64);
        }
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded with the caller's guarantees.
        unsafe { System.dealloc(ptr, layout) };
        charge(-(layout.size() as i64));
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded with the caller's guarantees.
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            charge(new_size as i64 - layout.size() as i64);
        }
        new
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_allocation_is_charged_and_its_free_refunded() {
        let before = thread_net_bytes();
        let held = vec![0u8; 1 << 20];
        let holding = thread_net_bytes() - before;
        assert!(holding >= 1 << 20, "charged {holding}");
        drop(held);
        let after = thread_net_bytes() - before;
        assert!(after < 1 << 10, "refunded to {after}");
    }

    #[test]
    fn another_threads_allocations_are_not_charged_here() {
        let before = thread_net_bytes();
        let held = std::thread::spawn(|| vec![0u8; 1 << 20]).join();
        assert!(thread_net_bytes() - before < 1 << 20);
        drop(held);
    }
}
