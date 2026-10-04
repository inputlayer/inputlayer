//! The primary's retained log of replication events.
//!
//! Every committed change is appended as one encoded [`Event`](super::Event)
//! line and numbered with the next LSN. Appends happen under the lock that
//! orders the change (a knowledge graph's write lock, the WAL mutex inside
//! it, or the graph-set lock for creation), so two changes to one graph get
//! LSNs in commit order and LSN order is a valid order to apply them in.
//!
//! The log keeps the most recent events up to a byte budget. A follower
//! whose position is still retained catches up from here; one further
//! behind, or from another incarnation of the primary (`stream_id`), is
//! brought up to date from a checkpoint instead.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::watch;

/// One retained event line.
pub type Line = Arc<[u8]>;

/// What [`ReplicationLog::read_after`] found.
#[derive(Debug, PartialEq, Eq)]
pub enum Read {
    /// Events from `first` on, in LSN order.
    Events {
        /// LSN of the first line.
        first: u64,
        /// The lines.
        lines: Vec<Line>,
    },
    /// The position is the head: nothing new yet.
    UpToDate,
    /// Events after the position were dropped from the log (or the position
    /// is past the head): the reader needs a checkpoint.
    Unavailable,
}

#[derive(Debug)]
struct Retained {
    /// LSN of the newest event (0 before the first).
    head: u64,
    /// Events `head - lines.len() + 1 ..= head`.
    lines: VecDeque<Line>,
    bytes: usize,
}

/// The primary's numbered, bounded log of recent replication events.
#[derive(Debug)]
pub struct ReplicationLog {
    stream_id: u64,
    retain_bytes: usize,
    retained: Mutex<Retained>,
    head: watch::Sender<u64>,
}

impl ReplicationLog {
    /// An empty log under a fresh random stream id, keeping up to
    /// `retain_bytes` of events (always at least the newest one).
    pub fn new(retain_bytes: usize) -> Self {
        Self {
            // Never 0, so a follower's "no position" can never match.
            stream_id: rand::random::<u64>().max(1),
            retain_bytes,
            retained: Mutex::new(Retained {
                head: 0,
                lines: VecDeque::new(),
                bytes: 0,
            }),
            head: watch::channel(0).0,
        }
    }

    /// This incarnation's stream id: positions from another one are void.
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// LSN of the newest event (0 before the first).
    pub fn head(&self) -> u64 {
        self.retained.lock().head
    }

    /// Wakes whenever the head moves.
    pub fn watch_head(&self) -> watch::Receiver<u64> {
        self.head.subscribe()
    }

    /// Append one encoded event line and return its LSN. Drops the oldest
    /// lines while over the byte budget.
    pub fn append(&self, line: Vec<u8>) -> u64 {
        let mut retained = self.retained.lock();
        retained.head += 1;
        retained.bytes += line.len();
        retained.lines.push_back(line.into());
        while retained.bytes > self.retain_bytes && retained.lines.len() > 1 {
            if let Some(old) = retained.lines.pop_front() {
                retained.bytes -= old.len();
            }
        }
        let head = retained.head;
        drop(retained);
        self.head.send_replace(head);
        head
    }

    /// The events after `lsn`, up to about `max_bytes` (always at least one
    /// when any is available).
    pub fn read_after(&self, lsn: u64, max_bytes: usize) -> Read {
        let retained = self.retained.lock();
        if lsn == retained.head {
            return Read::UpToDate;
        }
        let oldest = retained.head + 1 - retained.lines.len() as u64;
        if lsn > retained.head || lsn + 1 < oldest {
            return Read::Unavailable;
        }
        let skip = usize::try_from(lsn + 1 - oldest).unwrap_or(usize::MAX);
        let mut lines = Vec::new();
        let mut bytes = 0;
        for line in retained.lines.iter().skip(skip) {
            if !lines.is_empty() && bytes + line.len() > max_bytes {
                break;
            }
            bytes += line.len();
            lines.push(Arc::clone(line));
        }
        Read::Events {
            first: lsn + 1,
            lines,
        }
    }

    /// Whether a follower at `lsn` of stream `stream_id` can catch up from
    /// the retained events.
    pub fn can_serve(&self, stream_id: u64, lsn: u64) -> bool {
        stream_id == self.stream_id && self.read_after(lsn, 0) != Read::Unavailable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(text: &str) -> Vec<u8> {
        text.as_bytes().to_vec()
    }

    fn texts(read: Read) -> (u64, Vec<String>) {
        match read {
            Read::Events { first, lines } => (
                first,
                lines
                    .iter()
                    .map(|l| String::from_utf8(l.to_vec()).unwrap())
                    .collect(),
            ),
            other => panic!("expected events, got {other:?}"),
        }
    }

    #[test]
    fn appends_are_numbered_from_one_and_read_in_order() {
        let log = ReplicationLog::new(1024);
        assert_eq!(log.read_after(0, 1024), Read::UpToDate);
        assert_eq!(log.append(line("a")), 1);
        assert_eq!(log.append(line("b")), 2);
        assert_eq!(
            texts(log.read_after(0, 1024)),
            (1, vec!["a".into(), "b".into()])
        );
        assert_eq!(texts(log.read_after(1, 1024)), (2, vec!["b".into()]));
        assert_eq!(log.read_after(2, 1024), Read::UpToDate);
        assert_eq!(log.read_after(3, 1024), Read::Unavailable);
    }

    #[test]
    fn reads_are_capped_by_bytes_but_return_at_least_one_line() {
        let log = ReplicationLog::new(1024);
        log.append(line("aaaa"));
        log.append(line("bbbb"));
        assert_eq!(texts(log.read_after(0, 6)), (1, vec!["aaaa".into()]));
        assert_eq!(texts(log.read_after(0, 1)), (1, vec!["aaaa".into()]));
    }

    #[test]
    fn the_oldest_lines_go_once_over_budget_and_their_positions_become_unavailable() {
        let log = ReplicationLog::new(8);
        log.append(line("aaaa"));
        log.append(line("bbbb"));
        log.append(line("cccc"));
        assert_eq!(log.read_after(0, 100), Read::Unavailable);
        assert_eq!(
            texts(log.read_after(1, 100)),
            (2, vec!["bbbb".into(), "cccc".into()])
        );
        assert!(log.can_serve(log.stream_id(), 1));
        assert!(!log.can_serve(log.stream_id(), 0));
        assert!(!log.can_serve(log.stream_id().wrapping_add(1), 1));
    }

    #[test]
    fn a_line_over_the_whole_budget_is_still_kept() {
        let log = ReplicationLog::new(2);
        log.append(line("aaaa"));
        assert_eq!(texts(log.read_after(0, 100)), (1, vec!["aaaa".into()]));
    }

    #[test]
    fn the_head_watch_follows_appends() {
        let log = ReplicationLog::new(64);
        let watch = log.watch_head();
        log.append(line("a"));
        assert_eq!(*watch.borrow(), 1);
        assert_eq!(log.head(), 1);
    }
}
