//! When the node caught up with the cluster after starting.

use {
    super::reader::Reader,
    solana_clock::Slot,
    std::time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// The tip counts the slot still arriving, so a node in step reads a slot or two behind it.
pub const IN_STEP_SLOTS: u64 = 4;
/// Parents walked down from the tip before the rest of the distance is counted in slots.
const BEHIND_WALK_BLOCKS: u64 = 32;
const CHECK_EVERY: Duration = Duration::from_secs(1);
/// In step this long from the first reading without trailing, the node is taken never to trail.
const STEADY_FOR: Duration = Duration::from_secs(600);

/// Latched once: the node trailed the cluster, then came within `IN_STEP_SLOTS` of its tip. The tip
/// read before the first shreds arrive is the ledger's own, so a restarted node reads in step
/// first. Checked no more once latched, or after `STEADY_FOR` in step without trailing.
#[derive(Debug, Default)]
pub(crate) struct CatchUp {
    trailed: bool,
    /// Unix milliseconds, 0 before.
    caught_up_ms: u64,
    first: Option<Instant>,
    checked: Option<Instant>,
}

impl CatchUp {
    pub(crate) fn caught_up_ms(&self) -> u64 {
        self.caught_up_ms
    }

    pub(crate) fn check(&mut self, reader: &Reader, now: Instant) {
        if self.done(now)
            || self
                .checked
                .is_some_and(|at| now.duration_since(at) < CHECK_EVERY)
        {
            return;
        }
        self.checked = Some(now);
        let Some(tip) = reader.cluster_tip() else {
            return;
        };
        let completed = reader
            .bank_forks
            .read()
            .unwrap()
            .frozen_banks()
            .map(|(slot, _)| slot)
            .max()
            .unwrap_or(0);
        let behind = walk_behind(|slot| reader.parent_of(slot), tip, completed);
        self.observe(behind, now, unix_millis());
    }

    fn done(&self, now: Instant) -> bool {
        self.caught_up_ms != 0
            || (!self.trailed
                && self
                    .first
                    .is_some_and(|first| now.duration_since(first) >= STEADY_FOR))
    }

    fn observe(&mut self, behind: u64, now: Instant, now_ms: u64) {
        if self.done(now) {
            return;
        }
        self.first.get_or_insert(now);
        if behind > IN_STEP_SLOTS {
            self.trailed = true;
        } else if self.trailed {
            self.caught_up_ms = now_ms.max(1);
        }
    }
}

/// Blocks on the tip's chain that replay has yet to complete, so slots the cluster skipped are not
/// distance. Past the walk, or below a slot whose parent is unknown, the rest is counted in slots.
pub fn walk_behind(parent_of: impl Fn(Slot) -> Option<Slot>, tip: Slot, completed: Slot) -> u64 {
    let mut slot = tip;
    let mut blocks = 0u64;
    while slot > completed && blocks < BEHIND_WALK_BLOCKS {
        let Some(parent) = parent_of(slot) else {
            break;
        };
        blocks = blocks.saturating_add(1);
        slot = parent;
    }
    blocks.saturating_add(slot.saturating_sub(completed))
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_node_in_step_for_long_after_starting_is_taken_never_to_trail() {
        let start = Instant::now();
        let mut catch_up = CatchUp::default();
        catch_up.observe(1, start, 100);
        assert!(!catch_up.done(start + STEADY_FOR - Duration::from_secs(1)));
        assert!(catch_up.done(start + STEADY_FOR));
        catch_up.observe(50, start + STEADY_FOR, 200);
        catch_up.observe(0, start + STEADY_FOR, 300);
        assert_eq!(catch_up.caught_up_ms(), 0);
    }

    #[test]
    fn test_a_restart_reads_in_step_then_trails_then_catches_up() {
        // Before the first shreds the tip is the ledger's own.
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        let mut catch_up = CatchUp::default();
        catch_up.observe(0, at(0), 100);
        catch_up.observe(900, at(1), 200);
        assert_eq!(catch_up.caught_up_ms(), 0);
        catch_up.observe(3, at(90), 300);
        assert_eq!(catch_up.caught_up_ms(), 300);
        catch_up.observe(60, at(91), 400);
        catch_up.observe(0, at(92), 500);
        assert_eq!(catch_up.caught_up_ms(), 300);
    }

    #[test]
    fn test_the_walk_counts_blocks_then_slots() {
        // Tip 10's chain: 10 -> 8 -> 5, 9 and 6 skipped.
        let parent = |slot| match slot {
            10 => Some(8),
            8 => Some(5),
            _ => None,
        };
        assert_eq!(walk_behind(parent, 10, 5), 2);
        assert_eq!(walk_behind(parent, 10, 3), 4);
    }
}
