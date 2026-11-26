//! How far the node trails the cluster's tip.

use solana_clock::Slot;

/// The tip counts the slot still arriving, so a node in step reads a slot or two behind it.
pub const IN_STEP_SLOTS: u64 = 4;
/// Parents walked down from the tip before the rest of the distance is counted in slots.
const BEHIND_WALK_BLOCKS: u64 = 32;

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

#[cfg(test)]
mod tests {
    use super::*;

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
