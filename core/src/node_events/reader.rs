//! What the node events read of the running validator, through its handles.

use {
    super::{
        bank_view::{BankView, Banks, Commitment, PeerRow},
        blocks::{self, Block, TxOutcome},
        versions::{self, TxVersions},
    },
    agave_snapshots::snapshot_config::SnapshotConfig,
    solana_clock::{Epoch, Slot},
    solana_cluster_type::ClusterType,
    solana_gossip::{cluster_info::ClusterInfo, crds_data::SnapshotHashes},
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank_forks::{BankForks, SharableBanks},
        commitment::BlockCommitmentCache,
        validated_block_finalization::ValidatedBlockFinalizationCert,
    },
    std::{
        path::PathBuf,
        sync::{Arc, RwLock},
    },
};

/// The validator's handles the node events read.
pub struct Reader {
    cluster_info: Arc<ClusterInfo>,
    pub(crate) bank_forks: Arc<RwLock<BankForks>>,
    block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    vote_account: Pubkey,
    /// Votor's newest finalization certificate.
    highest_finalized: Arc<RwLock<Option<ValidatedBlockFinalizationCert>>>,
    account_paths: Vec<PathBuf>,
    snapshot_config: Option<SnapshotConfig>,
    /// The working and root banks without bank forks' lock.
    sharable_banks: SharableBanks,
}

impl Reader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<BankForks>>,
        block_commitment_cache: Arc<RwLock<BlockCommitmentCache>>,
        blockstore: Arc<Blockstore>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        vote_account: Pubkey,
        highest_finalized: Arc<RwLock<Option<ValidatedBlockFinalizationCert>>>,
        account_paths: Vec<PathBuf>,
        snapshot_config: Option<SnapshotConfig>,
    ) -> Self {
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
        Self {
            cluster_info,
            bank_forks,
            block_commitment_cache,
            blockstore,
            leader_schedule_cache,
            vote_account,
            highest_finalized,
            account_paths,
            snapshot_config,
            sharable_banks,
        }
    }

    pub fn identity(&self) -> Pubkey {
        self.cluster_info.id()
    }

    pub fn vote_account(&self) -> Pubkey {
        self.vote_account
    }

    pub fn cluster_name(&self) -> &'static str {
        match self.bank_forks.read().unwrap().root_bank().cluster_type() {
            ClusterType::Testnet => "testnet",
            ClusterType::MainnetBeta => "mainnet",
            ClusterType::Devnet => "devnet",
            ClusterType::Development => "development",
        }
    }

    /// Votor's last certificate under Alpenglow, the blockstore's latest optimistic slot before
    /// it, and in both the highest slot with shreds, which turbine keeps current while replay lags.
    pub fn cluster_tip(&self) -> Option<Slot> {
        let migration_status = self.bank_forks.read().unwrap().migration_status();
        let decided = if migration_status.is_alpenglow_enabled() {
            self.highest_finalized
                .read()
                .ok()
                .and_then(|cert| cert.as_ref().map(|cert| cert.block().slot))
        } else {
            self.blockstore
                .get_latest_optimistic_slots(1)
                .ok()
                .and_then(|mut slots| slots.pop())
                .map(|(slot, _, _)| slot)
        };
        let shredded = self.blockstore.highest_slot().ok().flatten();
        decided.into_iter().chain(shredded).max()
    }

    pub fn shred_version(&self) -> u16 {
        self.cluster_info.my_shred_version()
    }

    pub fn my_contact(&self) -> PeerRow {
        PeerRow::from(&self.cluster_info.my_contact_info())
    }

    pub fn all_peers(&self) -> Vec<(PeerRow, u64)> {
        self.cluster_info
            .all_peers()
            .iter()
            .map(|(contact, at)| (PeerRow::from(contact), *at))
            .collect()
    }

    /// The full and newest incremental snapshot slot each peer advertises.
    pub fn snapshot_slots(&self, keys: &[Pubkey]) -> Vec<(Option<Slot>, Option<Slot>)> {
        // One short hold of the gossip table's lock.
        let crds = self.cluster_info.gossip.crds.read();
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

    /// Whether gossip accepts only globally routable addresses.
    pub fn global_addresses_only(&self) -> bool {
        matches!(
            self.cluster_info.socket_addr_space(),
            solana_net_utils::SocketAddrSpace::Global
        )
    }

    pub fn banks(&self) -> Banks {
        // Held only to clone the handles out: replay takes this lock to advance.
        let bank_forks = self.bank_forks.read().unwrap();
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

    pub fn root_bank(&self) -> Arc<dyn BankView> {
        self.sharable_banks.root()
    }

    pub fn working_bank(&self) -> Arc<dyn BankView> {
        self.sharable_banks.working()
    }

    /// Bank forks' highest slot.
    pub fn highest_slot(&self) -> Slot {
        self.bank_forks.read().unwrap().highest_slot()
    }

    pub fn commitment(&self) -> Commitment {
        let commitment = self.block_commitment_cache.read().unwrap();
        Commitment {
            root: commitment.root(),
            confirmed: commitment.highest_confirmed_slot(),
            finalized: commitment.highest_super_majority_root(),
        }
    }

    pub fn first_shred_time(&self, slot: Slot) -> Option<u64> {
        match self.blockstore.meta(slot) {
            Ok(Some(meta)) if meta.first_shred_timestamp > 0 => Some(meta.first_shred_timestamp),
            _ => None,
        }
    }

    pub fn parent_of(&self, slot: Slot) -> Option<Slot> {
        self.blockstore
            .meta(slot)
            .ok()
            .flatten()
            .and_then(|meta| meta.parent_slot)
    }

    pub fn is_full(&self, slot: Slot) -> bool {
        self.blockstore.is_full(slot)
    }

    pub fn lowest_slot(&self) -> Slot {
        self.blockstore.lowest_slot()
    }

    pub fn ledger_path(&self) -> PathBuf {
        self.blockstore.ledger_path().clone()
    }

    pub fn message_versions(&self, slot: Slot) -> Option<TxVersions> {
        let entries = self.blockstore.get_slot_entries(slot, 0).ok()?;
        Some(versions::tally(
            entries.iter().flat_map(|entry| &entry.transactions),
        ))
    }

    /// The non-vote transactions of a full block: all of ours, only `followed` payers' otherwise.
    pub fn tx_outcomes(
        &self,
        slot: Slot,
        bank: &dyn BankView,
        ours: bool,
        followed: &dyn Fn(&Pubkey) -> bool,
    ) -> Option<Vec<TxOutcome>> {
        let entries = self.blockstore.get_slot_entries(slot, 0).ok()?;
        Some(blocks::tx_outcomes(
            bank.as_bank()?,
            &entries,
            ours,
            followed,
        ))
    }

    /// The block at `slot` as the certificate walks read it, against `root`.
    pub fn block(&self, slot: Slot, root: Slot) -> Block {
        blocks::read_block(&self.blockstore, slot, root)
    }

    pub fn slot_leader(&self, slot: Slot, bank: &dyn BankView) -> Option<Pubkey> {
        self.leader_schedule_cache
            .slot_leader_at(slot, bank.as_bank())
            .map(|leader| leader.id)
    }

    /// Slot indexes below `slots` that `leader` leads in `epoch`, where the schedule is known.
    pub fn leader_slot_indexes(
        &self,
        epoch: Epoch,
        leader: &Pubkey,
        slots: u64,
    ) -> Option<Vec<u64>> {
        let schedule = self
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

    /// The slots `leader` leads in `epoch`, where the schedule is known.
    pub fn leader_slots_in_epoch(
        &self,
        bank: &dyn BankView,
        epoch: Epoch,
        leader: &Pubkey,
    ) -> Option<Vec<Slot>> {
        let epoch_schedule = bank.epoch_schedule();
        let start_slot = epoch_schedule.get_first_slot_in_epoch(epoch);
        let end_slot = epoch_schedule.get_last_slot_in_epoch(epoch);
        let slots = end_slot.saturating_sub(start_slot).saturating_add(1);
        self.leader_slot_indexes(epoch, leader, slots)
            .map(|indexes| {
                indexes
                    .into_iter()
                    .map(|index| start_slot.saturating_add(index))
                    .collect()
            })
    }

    pub fn snapshot_config(&self) -> Option<SnapshotConfig> {
        self.snapshot_config.clone()
    }

    pub fn account_paths(&self) -> Vec<PathBuf> {
        self.account_paths.clone()
    }
}
