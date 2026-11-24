//! Running shred fetch totals, one publisher per fetch modifier thread.

use agave_event_system::{
    event,
    event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
    stream_name::StreamName,
};

pub const SHRED_FETCH_STREAM: StreamName = agave_event_system::stream_name!("tvu.shred_fetch");

pub const SHRED_FETCH_PATH_TURBINE: u8 = 0;
pub const SHRED_FETCH_PATH_REPAIR: u8 = 1;

/// Packet totals since the modifier thread started, sent at its one second stats cadence.
/// The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct ShredFetchCounters {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// `SHRED_FETCH_PATH_TURBINE` or `SHRED_FETCH_PATH_REPAIR`.
    pub path: u8,
    /// Packets taken off the receiver channel.
    pub packets: u64,
    /// Packets that passed the nonce and shred filters.
    pub valid: u64,
    /// Packets dropped by those filters, repair pings included.
    pub discarded: u64,
    /// Packets in batches dropped because the outgoing channel was full.
    pub overflow: u64,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ShredFetchTotals {
    pub(crate) packets: u64,
    pub(crate) valid: u64,
    pub(crate) discarded: u64,
    pub(crate) overflow: u64,
}

impl ShredFetchTotals {
    pub(crate) fn add_batch(&mut self, packets: usize, valid: usize) {
        let packets = packets as u64;
        let valid = valid as u64;
        self.packets = self.packets.saturating_add(packets);
        self.valid = self.valid.saturating_add(valid);
        self.discarded = self.discarded.saturating_add(packets.saturating_sub(valid));
    }

    pub(crate) fn add_overflow(&mut self, packets: usize) {
        self.overflow = self.overflow.saturating_add(packets as u64);
    }

    pub(crate) fn event(&self, path: u8) -> ShredFetchCounters {
        let (tid, pid) = cached_thread_and_process_id();
        ShredFetchCounters {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            path,
            packets: self.packets,
            valid: self.valid,
            discarded: self.discarded,
            overflow: self.overflow,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_totals_only_grow() {
        let mut totals = ShredFetchTotals::default();
        totals.add_batch(10, 7);
        let first = totals.event(SHRED_FETCH_PATH_REPAIR);
        totals.add_batch(4, 4);
        totals.add_overflow(4);
        let second = totals.event(SHRED_FETCH_PATH_REPAIR);
        assert_eq!(
            (first.packets, first.valid, first.discarded, first.overflow),
            (10, 7, 3, 0)
        );
        assert_eq!(
            (
                second.packets,
                second.valid,
                second.discarded,
                second.overflow
            ),
            (14, 11, 3, 4)
        );
        assert_eq!(second.path, SHRED_FETCH_PATH_REPAIR);
        assert!(second.timestamp_ns >= first.timestamp_ns);
    }
}
