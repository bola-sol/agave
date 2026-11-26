//! What the dashboard reads of the validator, as plain values. [`Direct`] reads a running
//! validator in process; a mirror rebuilt from event streams can stand in for it.

pub use crate::bank_view::*;
use {
    crate::{context::DashboardContext, snapshot, validator_info::ValidatorInfo, versions},
    agave_snapshots::snapshot_config::SnapshotConfig,
    solana_clock::{Epoch, Slot},
    solana_gossip::crds_data::SnapshotHashes,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_pubkey::Pubkey,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationReceiver,
    },
    solana_runtime::bank_forks::SharableBanks,
    std::{
        net::SocketAddr,
        path::PathBuf,
        sync::{Arc, Mutex},
    },
};

/// What the dashboard reads of the node: identity, gossip, ledger, schedule and banks.
pub trait Source: Send + Sync {
    fn identity(&self) -> Pubkey;
    fn vote_account(&self) -> Pubkey;
    fn cluster_name(&self) -> &'static str;
    fn cluster_tip(&self) -> Option<Slot>;
    fn shred_version(&self) -> u16;
    fn my_contact(&self) -> PeerRow;
    fn all_peers(&self) -> Vec<(PeerRow, u64)>;
    fn peer_gossip(&self, key: &Pubkey) -> Option<SocketAddr>;
    /// The full and newest incremental snapshot slot each peer advertises.
    fn snapshot_slots(&self, keys: &[Pubkey]) -> Vec<(Option<Slot>, Option<Slot>)>;
    /// Peers advertising an RPC address gossip would accept, ourselves excluded.
    fn rpc_node_count(&self, peers: &[(PeerRow, u64)]) -> usize;
    /// Whether gossip accepts only globally routable addresses.
    fn global_addresses_only(&self) -> bool;
    /// Unix milliseconds this validator process caught up with the cluster, as recorded by an
    /// earlier run of the dashboard.
    fn caught_up_millis(&self) -> Option<u64> {
        None
    }
    /// Keeps the catch-up time for a later run of the dashboard against the same process.
    fn record_caught_up(&self, _at_millis: u64) {}
    fn banks(&self) -> Banks;
    fn root_bank(&self) -> Arc<dyn BankView>;
    fn working_bank(&self) -> Arc<dyn BankView>;
    /// Bank forks' highest slot.
    fn highest_slot(&self) -> Slot;
    /// Banks frozen since the last call, each once; `None` where the source cannot say.
    fn take_frozen(&self) -> Option<Vec<Arc<dyn BankView>>>;
    fn commitment(&self) -> Commitment;
    fn first_shred_time(&self, slot: Slot) -> Option<u64>;
    fn parent_of(&self, slot: Slot) -> Option<Slot>;
    fn is_root(&self, slot: Slot) -> bool;
    fn is_full(&self, slot: Slot) -> bool;
    fn lowest_slot(&self) -> Slot;
    fn ledger_path(&self) -> PathBuf;
    fn message_versions(&self, slot: Slot) -> Option<versions::TxVersions>;
    /// The non-vote transactions of a full block: all of ours, only `followed` payers' otherwise.
    fn tx_outcomes(
        &self,
        slot: Slot,
        bank: &dyn BankView,
        ours: bool,
        followed: &dyn Fn(&Pubkey) -> bool,
    ) -> Option<Vec<crate::collect::TxOutcome>>;
    /// The block at `slot` as the certificate walks read it, against `root`.
    fn block(&self, slot: Slot, root: Slot) -> crate::certs::Block;
    fn slot_leader(&self, slot: Slot, bank: &dyn BankView) -> Option<Pubkey>;
    fn next_leader_slot(
        &self,
        me: &Pubkey,
        from: Slot,
        bank: &dyn BankView,
        range: u64,
    ) -> Option<(Slot, Slot)>;
    /// The leader of each turn of `epoch`, where the schedule is known.
    fn epoch_turn_leaders(&self, epoch: Epoch) -> Option<Vec<Pubkey>>;
    /// Slot indexes below `slots` that `leader` leads in `epoch`, where the schedule is known.
    fn leader_slot_indexes(&self, epoch: Epoch, leader: &Pubkey, slots: u64) -> Option<Vec<u64>>;
    fn snapshot_config(&self) -> Option<SnapshotConfig>;
    fn snapshots(&self) -> Option<snapshot::Snapshots> {
        self.snapshot_config().as_ref().and_then(snapshot::read)
    }
    fn account_paths(&self) -> Vec<PathBuf>;
    /// Every validator info entry and the slot read at, once at attach.
    fn validator_info_all(&self) -> (Slot, Vec<(Pubkey, ValidatorInfo)>);
    /// The validator's handles, where this is a running validator.
    fn direct(&self) -> Option<&DashboardContext> {
        None
    }
}

/// Reads a running validator through its handles.
pub struct Direct {
    ctx: DashboardContext,
    /// The working bank without bank forks' lock.
    sharable_banks: SharableBanks,
    frozen_banks: Option<Mutex<BankNotificationReceiver>>,
}

impl Direct {
    pub fn new(ctx: DashboardContext, frozen_banks: Option<BankNotificationReceiver>) -> Self {
        let sharable_banks = ctx.bank_forks.read().unwrap().sharable_banks();
        Self {
            ctx,
            sharable_banks,
            frozen_banks: frozen_banks.map(Mutex::new),
        }
    }
}

impl Source for Direct {
    fn identity(&self) -> Pubkey {
        self.ctx.identity()
    }
    fn vote_account(&self) -> Pubkey {
        self.ctx.vote_account
    }
    fn cluster_name(&self) -> &'static str {
        self.ctx.cluster_name()
    }
    fn cluster_tip(&self) -> Option<Slot> {
        self.ctx.cluster_tip()
    }
    fn shred_version(&self) -> u16 {
        self.ctx.cluster_info.my_shred_version()
    }
    fn my_contact(&self) -> PeerRow {
        PeerRow::from(&self.ctx.cluster_info.my_contact_info())
    }
    fn all_peers(&self) -> Vec<(PeerRow, u64)> {
        self.ctx
            .cluster_info
            .all_peers()
            .iter()
            .map(|(contact, at)| (PeerRow::from(contact), *at))
            .collect()
    }
    fn peer_gossip(&self, key: &Pubkey) -> Option<SocketAddr> {
        self.ctx
            .cluster_info
            .lookup_contact_info(key, |contact| contact.gossip())
            .flatten()
    }
    fn snapshot_slots(&self, keys: &[Pubkey]) -> Vec<(Option<Slot>, Option<Slot>)> {
        // One short hold of the gossip table's lock.
        let crds = self.ctx.cluster_info.gossip.crds.read();
        keys.iter()
            .map(|key| {
                let hashes = crds.get::<&SnapshotHashes>(*key);
                (
                    hashes.map(|hashes| hashes.full.0),
                    hashes
                        .and_then(|hashes| hashes.incremental.iter().map(|(slot, _)| *slot).max()),
                )
            })
            .collect()
    }
    fn rpc_node_count(&self, peers: &[(PeerRow, u64)]) -> usize {
        crate::collect::count_rpc_nodes(&self.ctx.cluster_info, peers)
    }
    fn global_addresses_only(&self) -> bool {
        matches!(
            self.ctx.cluster_info.socket_addr_space(),
            solana_net_utils::SocketAddrSpace::Global
        )
    }
    fn banks(&self) -> Banks {
        // Held only to clone the handles out: replay takes this lock to advance.
        let bank_forks = self.ctx.bank_forks.read().unwrap();
        Banks {
            root: bank_forks.root_bank(),
            working: bank_forks.working_bank(),
            highest_slot: bank_forks.highest_slot(),
            frozen: bank_forks
                .frozen_banks()
                .map(|(slot, bank)| (slot, bank as Arc<dyn BankView>))
                .collect(),
        }
    }
    fn root_bank(&self) -> Arc<dyn BankView> {
        self.sharable_banks.root()
    }
    fn working_bank(&self) -> Arc<dyn BankView> {
        self.sharable_banks.working()
    }
    fn highest_slot(&self) -> Slot {
        self.ctx.bank_forks.read().unwrap().highest_slot()
    }
    fn take_frozen(&self) -> Option<Vec<Arc<dyn BankView>>> {
        let receiver = self.frozen_banks.as_ref()?.lock().unwrap();
        Some(
            receiver
                .try_iter()
                .filter_map(|(notification, _)| match notification {
                    BankNotification::Frozen(bank) => Some(bank as Arc<dyn BankView>),
                    _ => None,
                })
                .collect(),
        )
    }
    fn commitment(&self) -> Commitment {
        let commitment = self.ctx.block_commitment_cache.read().unwrap();
        Commitment {
            root: commitment.root(),
            confirmed: commitment.highest_confirmed_slot(),
            finalized: commitment.highest_super_majority_root(),
        }
    }
    fn first_shred_time(&self, slot: Slot) -> Option<u64> {
        match self.ctx.blockstore.meta(slot) {
            Ok(Some(meta)) if meta.first_shred_timestamp > 0 => Some(meta.first_shred_timestamp),
            _ => None,
        }
    }
    fn parent_of(&self, slot: Slot) -> Option<Slot> {
        self.ctx
            .blockstore
            .meta(slot)
            .ok()
            .flatten()
            .and_then(|meta| meta.parent_slot)
    }
    fn is_root(&self, slot: Slot) -> bool {
        self.ctx.blockstore.is_root(slot)
    }
    fn is_full(&self, slot: Slot) -> bool {
        self.ctx.blockstore.is_full(slot)
    }
    fn lowest_slot(&self) -> Slot {
        self.ctx.blockstore.lowest_slot()
    }
    fn ledger_path(&self) -> PathBuf {
        self.ctx.blockstore.ledger_path().clone()
    }
    fn message_versions(&self, slot: Slot) -> Option<versions::TxVersions> {
        let entries = self.ctx.blockstore.get_slot_entries(slot, 0).ok()?;
        Some(versions::tally(
            entries.iter().flat_map(|entry| &entry.transactions),
        ))
    }
    fn tx_outcomes(
        &self,
        slot: Slot,
        bank: &dyn BankView,
        ours: bool,
        followed: &dyn Fn(&Pubkey) -> bool,
    ) -> Option<Vec<crate::collect::TxOutcome>> {
        let entries = self.ctx.blockstore.get_slot_entries(slot, 0).ok()?;
        Some(crate::collect::tx_outcomes(
            bank.as_bank()?,
            &entries,
            ours,
            followed,
        ))
    }
    fn block(&self, slot: Slot, root: Slot) -> crate::certs::Block {
        crate::certs::read_block(&self.ctx.blockstore, slot, root)
    }
    fn slot_leader(&self, slot: Slot, bank: &dyn BankView) -> Option<Pubkey> {
        self.ctx
            .leader_schedule_cache
            .slot_leader_at(slot, bank.as_bank())
            .map(|leader| leader.id)
    }
    fn next_leader_slot(
        &self,
        me: &Pubkey,
        from: Slot,
        bank: &dyn BankView,
        range: u64,
    ) -> Option<(Slot, Slot)> {
        self.ctx.leader_schedule_cache.next_leader_slot(
            me,
            from,
            bank.as_bank()?,
            Some(self.ctx.blockstore.as_ref()),
            range,
        )
    }
    fn epoch_turn_leaders(&self, epoch: Epoch) -> Option<Vec<Pubkey>> {
        let schedule = self
            .ctx
            .leader_schedule_cache
            .get_epoch_leader_schedule(epoch)?;
        Some(
            schedule
                .get_slot_leaders()
                .step_by(NUM_CONSECUTIVE_LEADER_SLOTS.get())
                .map(|leader| leader.id)
                .collect(),
        )
    }
    fn leader_slot_indexes(&self, epoch: Epoch, leader: &Pubkey, slots: u64) -> Option<Vec<u64>> {
        let schedule = self
            .ctx
            .leader_schedule_cache
            .get_epoch_leader_schedule(epoch)?;
        // The schedule repeats endlessly, so the epoch's length bounds it.
        Some(
            schedule
                .get_leader_upcoming_slots(leader, 0)
                .map(|index| index as u64)
                .take_while(|index| *index < slots)
                .collect(),
        )
    }
    fn snapshot_config(&self) -> Option<SnapshotConfig> {
        self.ctx.snapshot_config.clone()
    }
    fn account_paths(&self) -> Vec<PathBuf> {
        self.ctx.account_paths.clone()
    }
    fn validator_info_all(&self) -> (Slot, Vec<(Pubkey, ValidatorInfo)>) {
        let bank = self.ctx.bank_forks.read().unwrap().root_bank();
        (bank.slot(), crate::validator_info::scan_all(&bank))
    }
    fn direct(&self) -> Option<&DashboardContext> {
        Some(&self.ctx)
    }
}
