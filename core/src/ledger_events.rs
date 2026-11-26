//! Each slot's first shred and its filling, as the blockstore records them.

use {
    crate::{
        bank_state_events::publisher,
        node_events::types::{LEDGER_SLOT_STREAM, LedgerSlotEvent},
    },
    agave_event_system::{
        EventSystem,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
    },
    solana_ledger::blockstore::{SlotInsert, SlotInsertObserver},
    std::sync::Mutex,
};

pub fn ledger_slot_event(insert: &SlotInsert) -> LedgerSlotEvent {
    let (tid, pid) = cached_thread_and_process_id();
    let fill = insert.fill.as_ref();
    LedgerSlotEvent {
        pid,
        tid,
        timestamp_ns: monotonic_timestamp_ns(),
        slot: insert.slot,
        has_parent: insert.parent.is_some(),
        parent_slot: insert.parent.unwrap_or_default(),
        first_shred_ms: insert.first_shred_ms,
        full: fill.is_some(),
        full_ms: fill.map_or(0, |fill| fill.full_ms),
        last_index: fill.map_or(0, |fill| fill.last_index),
        num_repaired: fill.map_or(0, |fill| {
            u32::try_from(fill.num_repaired).unwrap_or(u32::MAX)
        }),
        num_recovered: fill.map_or(0, |fill| {
            u32::try_from(fill.num_recovered).unwrap_or(u32::MAX)
        }),
    }
}

/// Publishes from the blockstore's insert threads, two events a slot.
pub struct LedgerSlotObserver {
    publisher: Mutex<Publisher<LedgerSlotEvent>>,
}

impl LedgerSlotObserver {
    pub fn new(event_system: &EventSystem) -> Result<Self, String> {
        Ok(Self {
            publisher: Mutex::new(publisher(event_system, LEDGER_SLOT_STREAM, 1024)?),
        })
    }
}

impl SlotInsertObserver for LedgerSlotObserver {
    fn observe(&self, insert: &SlotInsert) {
        let Ok(mut publisher) = self.publisher.lock() else {
            return;
        };
        if publisher.is_enabled() {
            let _ = publisher.publish(&ledger_slot_event(insert));
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_ledger::blockstore::SlotFill};

    #[test]
    fn test_a_filled_slot_carries_its_fill() {
        let first = SlotInsert {
            slot: 9,
            parent: Some(8),
            first_shred_ms: 1_000,
            first: true,
            fill: None,
        };
        let event = ledger_slot_event(&first);
        assert_eq!(
            (event.slot, event.parent_slot, event.first_shred_ms),
            (9, 8, 1_000)
        );
        assert!(event.has_parent && !event.full);
        let full = ledger_slot_event(&SlotInsert {
            first: false,
            fill: Some(SlotFill {
                full_ms: 1_400,
                last_index: 63,
                num_repaired: 2,
                num_recovered: 1,
            }),
            ..first
        });
        assert!(full.full);
        assert_eq!(
            (
                full.full_ms,
                full.last_index,
                full.num_repaired,
                full.num_recovered
            ),
            (1_400, 63, 2, 1)
        );
    }
}
