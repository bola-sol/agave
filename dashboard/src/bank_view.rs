//! A bank and gossip contacts as plain values, read by the collectors from a running node or from
//! a mirror rebuilt from events.

use {
    crate::validator_info::ValidatorInfo,
    solana_clock::{Clock, Epoch, Slot},
    solana_epoch_schedule::EpochSchedule,
    solana_gossip::contact_info::{ContactInfo, Protocol},
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::{Bank, VATHealthError},
        epoch_stakes::BLSPubkeyToRankMap,
    },
    solana_slot_history::SlotHistory,
    std::{collections::HashMap, net::SocketAddr, sync::Arc},
};

/// A vote account as of one bank.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VoteRow {
    pub vote: Pubkey,
    pub identity: Pubkey,
    pub stake: u64,
    pub commission: u8,
    pub inflation_rewards_commission_bps: u16,
    pub last_voted_slot: Option<Slot>,
    /// The newest epoch credits entry: epoch, credits, previous credits.
    pub last_epoch_credits: Option<(Epoch, u64, u64)>,
    pub pending_delegator_rewards: u64,
    pub has_bls_key: bool,
}

impl VoteRow {
    pub fn from_account(
        vote: Pubkey,
        stake: u64,
        account: &solana_vote::vote_account::VoteAccount,
    ) -> Self {
        let view = account.vote_state_view();
        Self {
            vote,
            identity: *account.node_pubkey(),
            stake,
            commission: view.commission(),
            inflation_rewards_commission_bps: view.inflation_rewards_commission(),
            last_voted_slot: view.last_voted_slot(),
            last_epoch_credits: newest_credits(view),
            pending_delegator_rewards: view.pending_delegator_rewards(),
            has_bls_key: view.bls_pubkey_compressed().is_some(),
        }
    }

    /// Credits earned in `epoch`, which counts only while it is the newest entry.
    pub fn credits_in(&self, epoch: Epoch) -> u64 {
        self.last_epoch_credits
            .filter(|(at, _, _)| *at == epoch)
            .map_or(0, |(_, credits, prev)| credits.saturating_sub(prev))
    }
}

/// The vote state's newest epoch credits entry: epoch, credits, previous credits.
pub fn newest_credits(
    view: &solana_vote::vote_state_view::VoteStateView,
) -> Option<(Epoch, u64, u64)> {
    view.epoch_credits_iter()
        .last()
        .map(|item| (item.epoch(), item.credits(), item.prev_credits()))
}

/// One rank of an epoch's admitted set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RankEntry {
    pub vote: Pubkey,
    pub identity: Pubkey,
    pub stake: u64,
}

/// An epoch's rank map: the admitted set in rank order.
pub trait Ranks: Send + Sync {
    fn len(&self) -> usize;
    fn entry(&self, rank: usize) -> Option<RankEntry>;
    fn rank_of(&self, vote: &Pubkey) -> Option<u16>;
    fn total_stake(&self) -> u64;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Ranks for BLSPubkeyToRankMap {
    fn len(&self) -> usize {
        BLSPubkeyToRankMap::len(self)
    }
    fn entry(&self, rank: usize) -> Option<RankEntry> {
        self.get_pubkey_stake_entry(rank).map(|entry| RankEntry {
            vote: entry.vote_account_pubkey,
            identity: entry.node_pubkey,
            stake: entry.stake.get().get(),
        })
    }
    fn rank_of(&self, vote: &Pubkey) -> Option<u16> {
        self.get_rank_for_vote_pubkey(vote).copied()
    }
    fn total_stake(&self) -> u64 {
        BLSPubkeyToRankMap::total_stake(self).get().get()
    }
}

/// A rank map rebuilt from rows.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RankRows {
    entries: Vec<RankEntry>,
    by_vote: HashMap<Pubkey, u16>,
    total_stake: u64,
}

impl RankRows {
    pub fn new(entries: Vec<RankEntry>) -> Self {
        let by_vote = entries
            .iter()
            .enumerate()
            .filter_map(|(rank, entry)| Some((entry.vote, u16::try_from(rank).ok()?)))
            .collect();
        let total_stake = entries
            .iter()
            .fold(0u64, |sum, entry| sum.saturating_add(entry.stake));
        Self {
            entries,
            by_vote,
            total_stake,
        }
    }
}

impl Ranks for RankRows {
    fn len(&self) -> usize {
        self.entries.len()
    }
    fn entry(&self, rank: usize) -> Option<RankEntry> {
        self.entries.get(rank).copied()
    }
    fn rank_of(&self, vote: &Pubkey) -> Option<u16> {
        self.by_vote.get(vote).copied()
    }
    fn total_stake(&self) -> u64 {
        self.total_stake
    }
}

/// An epoch's stakes as the rewards read them.
#[derive(Clone)]
pub struct EpochStakeView {
    pub total_stake: u64,
    pub ranks: Arc<dyn Ranks>,
}

/// A block's cost and its limits; zeros where the tracker could not be read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CostFacts {
    pub block_cost: u64,
    pub block_limit: u64,
    pub account_limit: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FeeTotals {
    pub total: u64,
    pub priority: u64,
}

/// What the node events read of a bank.
pub trait BankView: Send + Sync {
    fn slot(&self) -> Slot;
    fn epoch(&self) -> Epoch;
    fn epoch_schedule(&self) -> &EpochSchedule;
    fn is_alpenglow(&self) -> bool;
    fn parent_slot(&self) -> Slot;
    fn parent(&self) -> Option<Arc<dyn BankView>>;
    fn is_frozen(&self) -> bool;
    fn block_height(&self) -> u64;
    fn clock(&self) -> Clock;
    fn ns_per_slot_at_slot(&self, slot: Slot) -> u128;
    fn last_blockhash(&self) -> Hash;
    fn get_lamports_per_signature(&self) -> u64;
    fn transaction_count(&self) -> u64;
    fn non_vote_transaction_count_since_restart(&self) -> u64;
    fn transaction_error_count(&self) -> u64;
    fn transaction_entries_count(&self) -> u64;
    fn get_balance(&self, key: &Pubkey) -> u64;
    fn cost(&self) -> CostFacts;
    fn fees(&self) -> FeeTotals;
    fn vote_row(&self, vote: &Pubkey) -> Option<VoteRow>;
    fn for_each_vote_row(&self, each: &mut dyn FnMut(&VoteRow));
    fn rank_map(&self, slot: Slot) -> Option<Arc<dyn Ranks>>;
    fn epoch_stakes(&self, epoch: Epoch) -> Option<EpochStakeView>;
    fn epoch_stakes_from_slot(&self, slot: Slot) -> Option<EpochStakeView>;
    /// How far the vote account falls short of next epoch's ticket, where it falls short.
    fn vat_ticket_short(&self, vote: &Pubkey) -> Option<u64>;
    fn minimum_vote_account_balance_for_vat(&self) -> u64;
    fn get_minimum_balance_for_rent_exemption(&self, len: usize) -> u64;
    fn get_slot_history(&self) -> Option<SlotHistory>;
    fn account_data(&self, key: &Pubkey) -> Option<Vec<u8>>;
    /// Validator info entries this bank changed.
    fn validator_info_changed(&self) -> Vec<(Pubkey, ValidatorInfo)>;
    /// The bank itself, where it is one.
    fn as_bank(&self) -> Option<&Bank> {
        None
    }
}

/// So a held bank passes where a view is asked for.
macro_rules! forward_bank_view {
    ($held:ty) => {
        impl BankView for $held {
            fn slot(&self) -> Slot {
                BankView::slot(&**self)
            }
            fn epoch(&self) -> Epoch {
                BankView::epoch(&**self)
            }
            fn epoch_schedule(&self) -> &EpochSchedule {
                BankView::epoch_schedule(&**self)
            }
            fn is_alpenglow(&self) -> bool {
                BankView::is_alpenglow(&**self)
            }
            fn parent_slot(&self) -> Slot {
                BankView::parent_slot(&**self)
            }
            fn parent(&self) -> Option<Arc<dyn BankView>> {
                BankView::parent(&**self)
            }
            fn is_frozen(&self) -> bool {
                BankView::is_frozen(&**self)
            }
            fn block_height(&self) -> u64 {
                BankView::block_height(&**self)
            }
            fn clock(&self) -> Clock {
                BankView::clock(&**self)
            }
            fn ns_per_slot_at_slot(&self, slot: Slot) -> u128 {
                BankView::ns_per_slot_at_slot(&**self, slot)
            }
            fn last_blockhash(&self) -> Hash {
                BankView::last_blockhash(&**self)
            }
            fn get_lamports_per_signature(&self) -> u64 {
                BankView::get_lamports_per_signature(&**self)
            }
            fn transaction_count(&self) -> u64 {
                BankView::transaction_count(&**self)
            }
            fn non_vote_transaction_count_since_restart(&self) -> u64 {
                BankView::non_vote_transaction_count_since_restart(&**self)
            }
            fn transaction_error_count(&self) -> u64 {
                BankView::transaction_error_count(&**self)
            }
            fn transaction_entries_count(&self) -> u64 {
                BankView::transaction_entries_count(&**self)
            }
            fn get_balance(&self, key: &Pubkey) -> u64 {
                BankView::get_balance(&**self, key)
            }
            fn cost(&self) -> CostFacts {
                BankView::cost(&**self)
            }
            fn fees(&self) -> FeeTotals {
                BankView::fees(&**self)
            }
            fn vote_row(&self, vote: &Pubkey) -> Option<VoteRow> {
                BankView::vote_row(&**self, vote)
            }
            fn rank_map(&self, slot: Slot) -> Option<Arc<dyn Ranks>> {
                BankView::rank_map(&**self, slot)
            }
            fn epoch_stakes(&self, epoch: Epoch) -> Option<EpochStakeView> {
                BankView::epoch_stakes(&**self, epoch)
            }
            fn epoch_stakes_from_slot(&self, slot: Slot) -> Option<EpochStakeView> {
                BankView::epoch_stakes_from_slot(&**self, slot)
            }
            fn vat_ticket_short(&self, vote: &Pubkey) -> Option<u64> {
                BankView::vat_ticket_short(&**self, vote)
            }
            fn minimum_vote_account_balance_for_vat(&self) -> u64 {
                BankView::minimum_vote_account_balance_for_vat(&**self)
            }
            fn get_minimum_balance_for_rent_exemption(&self, len: usize) -> u64 {
                BankView::get_minimum_balance_for_rent_exemption(&**self, len)
            }
            fn get_slot_history(&self) -> Option<SlotHistory> {
                BankView::get_slot_history(&**self)
            }
            fn account_data(&self, key: &Pubkey) -> Option<Vec<u8>> {
                BankView::account_data(&**self, key)
            }
            fn validator_info_changed(&self) -> Vec<(Pubkey, ValidatorInfo)> {
                BankView::validator_info_changed(&**self)
            }
            fn for_each_vote_row(&self, each: &mut dyn FnMut(&VoteRow)) {
                BankView::for_each_vote_row(&**self, each)
            }
            fn as_bank(&self) -> Option<&Bank> {
                BankView::as_bank(&**self)
            }
        }
    };
}

forward_bank_view!(Arc<dyn BankView>);
forward_bank_view!(Arc<Bank>);

impl BankView for Bank {
    fn slot(&self) -> Slot {
        Bank::slot(self)
    }
    fn epoch(&self) -> Epoch {
        Bank::epoch(self)
    }
    fn epoch_schedule(&self) -> &EpochSchedule {
        Bank::epoch_schedule(self)
    }
    fn is_alpenglow(&self) -> bool {
        Bank::is_alpenglow(self)
    }
    fn parent_slot(&self) -> Slot {
        Bank::parent_slot(self)
    }
    fn parent(&self) -> Option<Arc<dyn BankView>> {
        Bank::parent(self).map(|bank| bank as Arc<dyn BankView>)
    }
    fn is_frozen(&self) -> bool {
        Bank::is_frozen(self)
    }
    fn block_height(&self) -> u64 {
        Bank::block_height(self)
    }
    fn clock(&self) -> Clock {
        Bank::clock(self)
    }
    fn ns_per_slot_at_slot(&self, slot: Slot) -> u128 {
        Bank::ns_per_slot_at_slot(self, slot)
    }
    fn last_blockhash(&self) -> Hash {
        Bank::last_blockhash(self)
    }
    fn get_lamports_per_signature(&self) -> u64 {
        Bank::get_lamports_per_signature(self)
    }
    fn transaction_count(&self) -> u64 {
        Bank::transaction_count(self)
    }
    fn non_vote_transaction_count_since_restart(&self) -> u64 {
        Bank::non_vote_transaction_count_since_restart(self)
    }
    fn transaction_error_count(&self) -> u64 {
        Bank::transaction_error_count(self)
    }
    fn transaction_entries_count(&self) -> u64 {
        Bank::transaction_entries_count(self)
    }
    fn get_balance(&self, key: &Pubkey) -> u64 {
        Bank::get_balance(self, key)
    }
    fn cost(&self) -> CostFacts {
        match self.read_cost_tracker() {
            Ok(tracker) => CostFacts {
                block_cost: tracker.block_cost(),
                block_limit: tracker.get_block_limit(),
                account_limit: tracker.get_account_limit(),
            },
            Err(_) => CostFacts::default(),
        }
    }
    fn fees(&self) -> FeeTotals {
        let fees = self.get_collector_fee_details();
        FeeTotals {
            total: fees.total_transaction_fee(),
            priority: fees.total_priority_fee(),
        }
    }
    fn vote_row(&self, vote: &Pubkey) -> Option<VoteRow> {
        let accounts = self.vote_accounts();
        let (stake, account) = accounts.get(vote)?;
        Some(VoteRow::from_account(*vote, *stake, account))
    }
    fn for_each_vote_row(&self, each: &mut dyn FnMut(&VoteRow)) {
        for (vote, (stake, account)) in self.vote_accounts().iter() {
            each(&VoteRow::from_account(*vote, *stake, account));
        }
    }
    fn rank_map(&self, slot: Slot) -> Option<Arc<dyn Ranks>> {
        self.get_rank_map(slot)
            .map(|map| map.clone() as Arc<dyn Ranks>)
    }
    fn epoch_stakes(&self, epoch: Epoch) -> Option<EpochStakeView> {
        Bank::epoch_stakes(self, epoch).map(|stakes| EpochStakeView {
            total_stake: stakes.total_stake(),
            ranks: stakes.bls_pubkey_to_rank_map().clone(),
        })
    }
    fn epoch_stakes_from_slot(&self, slot: Slot) -> Option<EpochStakeView> {
        Bank::epoch_stakes_from_slot(self, slot).map(|stakes| EpochStakeView {
            total_stake: stakes.total_stake(),
            ranks: stakes.bls_pubkey_to_rank_map().clone(),
        })
    }
    fn vat_ticket_short(&self, vote: &Pubkey) -> Option<u64> {
        match self.get_vat_health_for_next_epoch(vote) {
            Err(VATHealthError::InsufficientFundsInVoteAccount(balance, minimum)) => {
                Some(minimum.saturating_sub(balance))
            }
            Ok(()) | Err(_) => None,
        }
    }
    fn minimum_vote_account_balance_for_vat(&self) -> u64 {
        Bank::minimum_vote_account_balance_for_vat(self)
    }
    fn get_minimum_balance_for_rent_exemption(&self, len: usize) -> u64 {
        Bank::get_minimum_balance_for_rent_exemption(self, len)
    }
    fn get_slot_history(&self) -> Option<SlotHistory> {
        Bank::get_slot_history(self)
    }
    fn account_data(&self, key: &Pubkey) -> Option<Vec<u8>> {
        use solana_account::ReadableAccount;
        self.get_account(key).map(|account| account.data().to_vec())
    }
    fn validator_info_changed(&self) -> Vec<(Pubkey, ValidatorInfo)> {
        crate::validator_info::scan_slot(self)
    }
    fn as_bank(&self) -> Option<&Bank> {
        Some(self)
    }
}

/// A gossip contact as the node events carry it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerRow {
    pub pubkey: Pubkey,
    pub client: String,
    pub version: String,
    pub wallclock: u64,
    pub outset: u64,
    pub shred_version: u16,
    pub gossip: Option<SocketAddr>,
    pub rpc: Option<SocketAddr>,
    pub tvu_udp: Option<SocketAddr>,
    pub tpu_quic: Option<SocketAddr>,
    pub tpu_forwards_quic: Option<SocketAddr>,
    pub tpu_vote_udp: Option<SocketAddr>,
    pub tpu_vote_quic: Option<SocketAddr>,
    pub serve_repair_udp: Option<SocketAddr>,
    pub alpenglow: Option<SocketAddr>,
}

impl From<&ContactInfo> for PeerRow {
    fn from(contact: &ContactInfo) -> Self {
        Self {
            pubkey: *contact.pubkey(),
            client: contact.version().client().to_string(),
            version: contact.version().to_string(),
            wallclock: contact.wallclock(),
            outset: contact.outset(),
            shred_version: contact.shred_version(),
            gossip: contact.gossip(),
            rpc: contact.rpc(),
            tvu_udp: contact.tvu(Protocol::UDP),
            tpu_quic: contact.tpu(Protocol::QUIC),
            tpu_forwards_quic: contact.tpu_forwards(Protocol::QUIC),
            tpu_vote_udp: contact.tpu_vote(Protocol::UDP),
            tpu_vote_quic: contact.tpu_vote(Protocol::QUIC),
            serve_repair_udp: contact.serve_repair(Protocol::UDP),
            alpenglow: contact.alpenglow(),
        }
    }
}

/// The banks one tick reads, cloned out together.
pub struct Banks {
    pub root: Arc<dyn BankView>,
    pub working: Arc<dyn BankView>,
    pub highest_slot: Slot,
    pub frozen: Vec<(Slot, Arc<dyn BankView>)>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Commitment {
    pub root: Slot,
    pub confirmed: Slot,
    pub finalized: Slot,
}
