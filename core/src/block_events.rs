//! Block footers as events: each frozen bank's as replay reads it or as we produce it, and our
//! blocks this epoch as one set, read from the ledger, for subscribers that join late.

use {
    crate::node_events::{
        blocks::{Block, read_block},
        types::{
            BITMAP_BYTES, BLOCK_FOOTER, BLOCK_MISSING, BLOCK_OPAQUE, BlockEvent, OwnBlockEvent,
            OwnSetPlace,
        },
    },
    agave_event_system::event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
    solana_clock::{BankId, Epoch, Slot},
    solana_entry::block_component::BlockFooterV1,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    std::time::Instant,
};

/// Our footer as produced, handed from block creation to replay.
pub type OwnFooter = (Slot, BankId, BlockFooterV1);

/// Slots under the root within which a block of ours may not yet be stored: a leader's own block
/// can be rooted before its last shreds are. The own set reads only below.
pub const OWN_SETTLED: u64 = 16;
/// How often our blocks go out whole, besides at turn-on and at a new epoch or identity.
const OWN_EVERY: std::time::Duration = std::time::Duration::from_secs(10);
/// Reading per tick, so an epoch of our blocks is read well inside a subscriber's wait for it.
const OWN_READ_BUDGET: std::time::Duration = std::time::Duration::from_millis(150);

pub fn block_event(slot: Slot, bank_id: BankId, block: &Block) -> BlockEvent {
    let (tid, pid) = cached_thread_and_process_id();
    let mut event = BlockEvent {
        pid,
        tid,
        timestamp_ns: monotonic_timestamp_ns(),
        slot,
        bank_id,
        kind: BLOCK_OPAQUE,
        close_nanos: 0,
        has_notar: false,
        notar_len: 0,
        notar: [0u8; BITMAP_BYTES],
        has_skip: false,
        skip_len: 0,
        skip: [0u8; BITMAP_BYTES],
        has_fast: false,
        fast: false,
    };
    let pack = |bitmap: &[u8], into: &mut [u8; BITMAP_BYTES]| -> Option<u16> {
        into.get_mut(..bitmap.len())?.copy_from_slice(bitmap);
        u16::try_from(bitmap.len()).ok()
    };
    match block {
        Block::Missing => event.kind = BLOCK_MISSING,
        Block::Pending | Block::Opaque => {}
        Block::Footer(footer) => {
            let notar = match &footer.notar_bitmap {
                Some(bitmap) => pack(bitmap, &mut event.notar).map(Some),
                None => Some(None),
            };
            let skip = match &footer.skip_bitmap {
                Some(bitmap) => pack(bitmap, &mut event.skip).map(Some),
                None => Some(None),
            };
            // A bitmap too long for the event leaves the block unread, as an unreadable one does.
            if let (Some(notar), Some(skip)) = (notar, skip) {
                event.kind = BLOCK_FOOTER;
                event.close_nanos = footer.close_nanos;
                event.has_notar = notar.is_some();
                event.notar_len = notar.unwrap_or(0);
                event.has_skip = skip.is_some();
                event.skip_len = skip.unwrap_or(0);
                event.has_fast = footer.fast.is_some();
                event.fast = footer.fast.unwrap_or(false);
            }
        }
    }
    event
}

/// A frozen bank's footer as an event.
pub fn footer_event(bank: &Bank, footer: &BlockFooterV1) -> BlockEvent {
    block_event(
        bank.slot(),
        bank.bank_id().into(),
        &Block::Footer(Box::new(footer.into())),
    )
}

/// Our blocks as one set; with none, one event with `count` 0.
pub fn own_block_events(
    (epoch, identity): (Epoch, Pubkey),
    generation: u64,
    covered_to: Slot,
    blocks: &[BlockEvent],
) -> Vec<OwnBlockEvent> {
    let count = u32::try_from(blocks.len()).unwrap_or(u32::MAX);
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let place = |index| OwnSetPlace {
        pid,
        tid,
        timestamp_ns,
        epoch,
        identity: identity.to_bytes(),
        generation,
        index,
        count,
        covered_to,
    };
    if blocks.is_empty() {
        return vec![OwnBlockEvent::new(
            place(0),
            &block_event(0, 0, &Block::Opaque),
        )];
    }
    blocks
        .iter()
        .zip(0u32..)
        .map(|(block, index)| OwnBlockEvent::new(place(index), block))
        .collect()
}

/// A slot under the ledger's first slot reads as skipped once cleaned; it is unreadable instead.
fn held_block(slot: Slot, lowest: Slot, block: Block) -> Block {
    match block {
        Block::Missing if slot < lowest => Block::Opaque,
        block => block,
    }
}

/// Our turn slots of `epoch`, where its schedule is known.
fn leader_slots(
    leader_schedule_cache: &LeaderScheduleCache,
    bank: &Bank,
    epoch: Epoch,
    me: &Pubkey,
) -> Option<Vec<Slot>> {
    let schedule = leader_schedule_cache.get_epoch_leader_schedule(epoch)?;
    let first = bank.epoch_schedule().get_first_slot_in_epoch(epoch);
    Some(
        schedule
            .get_slot_leaders()
            .zip(first..)
            .filter(|(leader, _)| leader.id == *me)
            .map(|(_, slot)| slot)
            .collect(),
    )
}

/// Our blocks of one epoch, read from the ledger a tick's worth at a time.
pub(crate) struct OwnBlocks {
    key: (Epoch, Pubkey),
    leader_slots: Vec<Slot>,
    next: usize,
    events: Vec<BlockEvent>,
    generation: u64,
    sent: Option<Instant>,
}

impl OwnBlocks {
    pub(crate) fn new(
        leader_schedule_cache: &LeaderScheduleCache,
        bank: &Bank,
        key: (Epoch, Pubkey),
        generation: u64,
    ) -> Option<Self> {
        let (epoch, me) = key;
        Some(Self::with_slots(
            key,
            leader_slots(leader_schedule_cache, bank, epoch, &me)?,
            generation,
        ))
    }

    fn with_slots(key: (Epoch, Pubkey), leader_slots: Vec<Slot>, generation: u64) -> Self {
        Self {
            key,
            leader_slots,
            next: 0,
            events: Vec::new(),
            generation,
            sent: None,
        }
    }

    pub(crate) fn key(&self) -> (Epoch, Pubkey) {
        self.key
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// Sends the set again at the next read, as for a subscriber turning the stream on.
    pub(crate) fn resend(&mut self) {
        self.sent = None;
    }

    /// Reads more of our slots up to `to` until `deadline`, from a ledger holding slots from
    /// `lowest`; whether all are read.
    fn read_to(
        &mut self,
        read: impl Fn(Slot) -> Block,
        to: Slot,
        lowest: Slot,
        deadline: Instant,
    ) -> bool {
        while Instant::now() < deadline {
            let Some(slot) = self.leader_slots.get(self.next).copied() else {
                break;
            };
            if slot > to {
                break;
            }
            self.events
                .push(block_event(slot, 0, &held_block(slot, lowest, read(slot))));
            self.next = self.next.saturating_add(1);
        }
        self.leader_slots
            .get(self.next)
            .is_none_or(|slot| *slot > to)
    }

    /// The set to send now, if any: once first read up to the settled root, then every `OWN_EVERY`.
    pub(crate) fn due(
        &mut self,
        blockstore: &Blockstore,
        root: Slot,
        now: Instant,
    ) -> Option<Vec<OwnBlockEvent>> {
        let to = root.saturating_sub(OWN_SETTLED);
        if !self.read_to(
            |slot| read_block(blockstore, slot, root),
            to,
            blockstore.lowest_slot(),
            now + OWN_READ_BUDGET,
        ) {
            return None;
        }
        if self
            .sent
            .is_some_and(|at| now.duration_since(at) < OWN_EVERY)
        {
            return None;
        }
        self.generation = self.generation.saturating_add(1);
        self.sent = Some(now);
        Some(own_block_events(
            self.key,
            self.generation,
            to,
            &self.events,
        ))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::node_events::blocks::FooterFacts};

    fn footer(close_nanos: u64) -> Block {
        Block::Footer(Box::new(FooterFacts {
            close_nanos,
            notar_bitmap: Some(vec![1, 2]),
            skip_bitmap: None,
            fast: Some(true),
        }))
    }

    #[test]
    fn test_a_footer_keeps_its_figures_and_a_long_bitmap_leaves_it_unread() {
        let event = block_event(7, 3, &footer(42));
        assert_eq!(
            (event.slot, event.bank_id, event.kind),
            (7, 3, BLOCK_FOOTER)
        );
        assert_eq!((event.close_nanos, event.notar_len), (42, 2));
        assert!(event.has_notar && !event.has_skip && event.has_fast && event.fast);
        let long = Block::Footer(Box::new(FooterFacts {
            close_nanos: 1,
            notar_bitmap: Some(vec![0; BITMAP_BYTES.saturating_add(1)]),
            skip_bitmap: None,
            fast: None,
        }));
        assert_eq!(block_event(7, 3, &long).kind, BLOCK_OPAQUE);
        assert_eq!(block_event(7, 0, &Block::Missing).kind, BLOCK_MISSING);
    }

    fn far() -> Instant {
        Instant::now() + std::time::Duration::from_secs(60)
    }

    #[test]
    fn test_the_set_reads_to_the_settled_root_and_a_cleaned_slot_is_unread() {
        let mut own = OwnBlocks::with_slots((0, Pubkey::new_unique()), vec![4, 5, 30, 60], 0);
        let read = |slot| {
            if slot < 6 {
                Block::Missing
            } else {
                footer(slot)
            }
        };
        // Slot 4 is below the ledger's first slot, so its skip is a clean; slot 60 is past 40.
        assert!(own.read_to(read, 40, 5, far()));
        let kinds: Vec<_> = own
            .events
            .iter()
            .map(|event| (event.slot, event.kind))
            .collect();
        assert_eq!(
            kinds,
            vec![(4, BLOCK_OPAQUE), (5, BLOCK_MISSING), (30, BLOCK_FOOTER)]
        );
        assert!(own.read_to(read, 60, 5, far()));
        assert_eq!(own.events.len(), 4);
        // Past its deadline a read takes nothing more.
        let mut late = OwnBlocks::with_slots((0, Pubkey::new_unique()), vec![4], 0);
        assert!(!late.read_to(read, 40, 5, Instant::now()));
        assert!(late.events.is_empty());
    }
}
