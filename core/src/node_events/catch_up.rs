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

/// Latched once: the node trailed the cluster, then came within `IN_STEP_SLOTS` of its tip. A node
/// in step at its first reading never trailed and has no catch-up time. Checked no more after the
/// first reading in step.
#[derive(Debug, Default)]
pub(crate) struct CatchUp {
    trailed: bool,
    in_step: bool,
    /// Unix milliseconds, 0 before.
    caught_up_ms: u64,
    checked: Option<Instant>,
}

impl CatchUp {
    pub(crate) fn caught_up_ms(&self) -> u64 {
        self.caught_up_ms
    }

    pub(crate) fn check(&mut self, reader: &Reader, now: Instant) {
        if self.in_step
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
        self.observe(behind, unix_millis());
    }

    fn observe(&mut self, behind: u64, now_ms: u64) {
        if self.in_step {
            return;
        }
        if behind > IN_STEP_SLOTS {
            self.trailed = true;
            return;
        }
        self.in_step = true;
        if self.trailed {
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
    fn test_a_node_that_never_trailed_has_no_catch_up_time() {
        let mut catch_up = CatchUp::default();
        catch_up.observe(1, 100);
        assert!(catch_up.in_step);
        // A later stall and recovery is not a catch-up after starting.
        catch_up.observe(50, 200);
        catch_up.observe(0, 300);
        assert_eq!(catch_up.caught_up_ms(), 0);
    }

    #[test]
    fn test_catch_up_latches_once_after_trailing() {
        let mut catch_up = CatchUp::default();
        catch_up.observe(50, 100);
        assert_eq!(catch_up.caught_up_ms(), 0);
        catch_up.observe(4, 200);
        assert_eq!(catch_up.caught_up_ms(), 200);
        catch_up.observe(60, 300);
        catch_up.observe(0, 400);
        assert_eq!(catch_up.caught_up_ms(), 200);
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
