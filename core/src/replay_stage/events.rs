use {
    crate::{
        block_events::{OwnFooter, footer_event},
        frozen_bank_events::FrozenBankSender,
        node_events::types::BlockEvent,
    },
    agave_event_system::{
        event,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
    crossbeam_channel::Receiver,
    solana_clock::{BankId, Epoch, Slot, UnixTimestamp},
    solana_entry::block_component::{BlockFooterV1, VersionedBlockFooter},
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    std::sync::Arc,
};

pub const SLOT_EVENT_STREAM: StreamName = agave_event_system::stream_name!("replay.slot_event");
pub const BANK_FROZEN_STREAM: StreamName = agave_event_system::stream_name!("replay.bank_frozen");

/// Slot lifecycle events observed by replay.
/// All timestamps are monotonic nanoseconds.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub enum SlotEvent {
    /// Replay inserted a bank for this slot and parent into BankForks.
    /// The slot may begin again if its bank is discarded and recreated.
    Begin {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        parent: Slot,
        bank_id: BankId,
    },
    /// Execution stopped for this bank; `succeeded` records replay and verification success.
    ExecutionFinished {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: BankId,
        succeeded: bool,
    },
    /// The bank was removed from BankForks, but may still have other owners.
    Removed {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: BankId,
    },
    /// Replay applied a BankForks root that includes this bank.
    Rooted {
        pid: i32,
        tid: i32,
        timestamp_ns: u64,
        slot: Slot,
        bank_id: BankId,
    },
}

impl SlotEvent {
    pub(crate) fn execution_finished(slot: Slot, bank_id: BankId, succeeded: bool) -> Self {
        let (tid, pid) = cached_thread_and_process_id();
        Self::ExecutionFinished {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            slot,
            bank_id,
            succeeded,
        }
    }

    pub(crate) fn rooted(slot: Slot, bank_id: BankId) -> Self {
        let (tid, pid) = cached_thread_and_process_id();
        Self::Rooted {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            slot,
            bank_id,
        }
    }

    pub(crate) fn removed(slot: Slot, bank_id: BankId) -> Self {
        let (tid, pid) = cached_thread_and_process_id();
        Self::Removed {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            slot,
            bank_id,
        }
    }
}

/// A bank replay froze, with the counts of this block alone and the fork's running totals.
/// The timestamp is monotonic nanoseconds.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BankFrozenEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub bank_id: BankId,
    pub parent_slot: Slot,
    pub leader: [u8; 32],
    pub blockhash: [u8; 32],
    /// Transactions processed, failed ones included.
    pub transaction_count: u64,
    /// Non-vote transactions processed, failed ones included.
    pub non_vote_transaction_count: u64,
    pub failed_transaction_count: u64,
    /// Entries that held at least one processed transaction.
    pub transaction_entry_count: u64,
    pub block_cost: u64,
    pub block_cost_limit: u64,
    pub account_cost_limit: u64,
    /// Base and priority fees, in lamports.
    pub total_fee: u64,
    pub priority_fee: u64,
    pub block_height: u64,
    /// The fork's running totals, as the bank keeps them.
    pub transaction_count_total: u64,
    pub non_vote_transaction_count_total: u64,
    pub epoch: Epoch,
    /// The clock sysvar's timestamps and leader schedule epoch.
    pub clock_epoch_start_timestamp: UnixTimestamp,
    pub clock_unix_timestamp: UnixTimestamp,
    pub clock_leader_schedule_epoch: Epoch,
    /// Our identity's and vote account's balances, in lamports.
    pub identity_balance: u64,
    pub vote_balance: u64,
}

impl BankFrozenEvent {
    pub fn new(bank: &Bank, identity: &Pubkey, vote_account: &Pubkey) -> Self {
        let (tid, pid) = cached_thread_and_process_id();
        let parent_non_vote_transaction_count = bank.parent().map_or(0, |parent| {
            parent.non_vote_transaction_count_since_restart()
        });
        let (block_cost, block_cost_limit, account_cost_limit) = {
            let cost_tracker = bank.read_cost_tracker().unwrap();
            (
                cost_tracker.block_cost(),
                cost_tracker.get_block_limit(),
                cost_tracker.get_account_limit(),
            )
        };
        let fee_details = bank.get_collector_fee_details();
        let clock = bank.clock();
        Self {
            pid,
            tid,
            timestamp_ns: monotonic_timestamp_ns(),
            slot: bank.slot(),
            bank_id: bank.bank_id().into(),
            parent_slot: bank.parent_slot(),
            leader: bank.leader_id().to_bytes(),
            blockhash: bank.last_blockhash().to_bytes(),
            transaction_count: bank.executed_transaction_count(),
            non_vote_transaction_count: bank
                .non_vote_transaction_count_since_restart()
                .saturating_sub(parent_non_vote_transaction_count),
            failed_transaction_count: bank.transaction_error_count(),
            transaction_entry_count: bank.transaction_entries_count(),
            block_cost,
            block_cost_limit,
            account_cost_limit,
            total_fee: fee_details.total_transaction_fee(),
            priority_fee: fee_details.total_priority_fee(),
            block_height: bank.block_height(),
            transaction_count_total: bank.transaction_count(),
            non_vote_transaction_count_total: bank.non_vote_transaction_count_since_restart(),
            epoch: bank.epoch(),
            clock_epoch_start_timestamp: clock.epoch_start_timestamp,
            clock_unix_timestamp: clock.unix_timestamp,
            clock_leader_schedule_epoch: clock.leader_schedule_epoch,
            identity_balance: bank.get_balance(identity),
            vote_balance: bank.get_balance(vote_account),
        }
    }
}

/// Our footers kept for their banks' freeze, at most this many.
const OWN_FOOTERS_KEPT: usize = 64;

/// What replay sends as each bank freezes: its own figures, its footer, and the bank to the
/// readers that take longer.
pub(crate) struct FrozenOutput {
    pub(crate) publisher: Publisher<BankFrozenEvent>,
    pub(crate) blocks: Publisher<BlockEvent>,
    pub(crate) vote_account: Pubkey,
    pub(crate) readers: FrozenBankSender,
    /// Our footers as block creation produced them, since our banks are not replayed from shreds.
    pub(crate) own_footers: Receiver<OwnFooter>,
    pub(crate) own_held: Vec<OwnFooter>,
}

impl FrozenOutput {
    #[cfg(test)]
    pub(crate) fn stub() -> Self {
        Self {
            publisher: Publisher::stub(),
            blocks: Publisher::stub(),
            vote_account: Pubkey::default(),
            readers: FrozenBankSender::stub(),
            own_footers: crossbeam_channel::never(),
            own_held: Vec::new(),
        }
    }

    /// Whether the footer stream is on, so replay reads the footer only then.
    pub(crate) fn wants_footers(&mut self) -> bool {
        self.blocks.is_enabled()
    }

    /// `footer` is the one replay read; ours comes from block creation.
    pub(crate) fn frozen(
        &mut self,
        bank: &Arc<Bank>,
        identity: &Pubkey,
        footer: Option<&VersionedBlockFooter>,
        ours: bool,
    ) {
        if self.publisher.is_enabled() {
            let _ =
                self.publisher
                    .publish(&BankFrozenEvent::new(bank, identity, &self.vote_account));
        }
        let own = ours.then(|| self.take_own(bank)).flatten();
        if self.blocks.is_enabled() {
            let footer = footer
                .map(|VersionedBlockFooter::V1(footer)| footer)
                .or(own.as_ref());
            if let Some(footer) = footer {
                let _ = self.blocks.publish(&footer_event(bank, footer));
            }
        }
        self.readers.send(bank);
    }

    /// Our footer for `bank`, from those block creation sent.
    fn take_own(&mut self, bank: &Bank) -> Option<BlockFooterV1> {
        self.own_held.extend(self.own_footers.try_iter());
        let (slot, bank_id) = (bank.slot(), BankId::from(bank.bank_id()));
        let at = self
            .own_held
            .iter()
            .position(|(held, id, _)| *held == slot && *id == bank_id);
        let footer = at.map(|at| self.own_held.swap_remove(at).2);
        self.own_held.retain(|(held, _, _)| *held > slot);
        if self.own_held.len() > OWN_FOOTERS_KEPT {
            let excess = self.own_held.len().saturating_sub(OWN_FOOTERS_KEPT);
            self.own_held.drain(..excess);
        }
        footer
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_fee_calculator::FeeRateGovernor, solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader, solana_pubkey::Pubkey,
        solana_runtime::genesis_utils::create_genesis_config_with_leader, solana_signer::Signer,
        solana_system_transaction as system_transaction,
    };

    #[test]
    fn test_bank_frozen_event_counts_only_its_own_block() {
        let leader_id = Pubkey::new_unique();
        let mut genesis = create_genesis_config_with_leader(1_000_000, &leader_id, 1_000);
        genesis.genesis_config.fee_rate_governor = FeeRateGovernor::new(5_000, 0);
        let mint = genesis.mint_keypair;
        let (parent, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis.genesis_config);
        parent
            .process_transaction(&system_transaction::transfer(
                &mint,
                &Pubkey::new_unique(),
                10_000,
                parent.last_blockhash(),
            ))
            .unwrap();
        parent.freeze();

        let leader = SlotLeader {
            id: leader_id,
            vote_address: genesis.voting_keypair.pubkey(),
        };
        let bank = Bank::new_from_parent_with_bank_forks(&bank_forks, parent, leader, 1);
        bank.process_transaction(&system_transaction::transfer(
            &mint,
            &Pubkey::new_unique(),
            10_000,
            bank.last_blockhash(),
        ))
        .unwrap();
        let poor = Keypair::new();
        bank.process_transaction(&system_transaction::transfer(
            &mint,
            &poor.pubkey(),
            100_000,
            bank.last_blockhash(),
        ))
        .unwrap();
        // Pays its fee, then fails to send more than it holds.
        assert!(
            bank.process_transaction(&system_transaction::transfer(
                &poor,
                &Pubkey::new_unique(),
                1_000_000,
                bank.last_blockhash(),
            ))
            .is_err()
        );
        bank.freeze();

        let event = BankFrozenEvent::new(&bank, &mint.pubkey(), &genesis.voting_keypair.pubkey());
        assert_eq!(event.slot, 1);
        assert_eq!(event.bank_id, u64::from(bank.bank_id()));
        assert_eq!(event.parent_slot, 0);
        assert_eq!(event.leader, leader.id.to_bytes());
        assert_eq!(event.blockhash, bank.last_blockhash().to_bytes());
        assert_eq!(event.transaction_count, 3);
        assert_eq!(event.non_vote_transaction_count, 3);
        assert_eq!(event.failed_transaction_count, 1);
        assert_eq!(event.transaction_entry_count, 3);
        assert_eq!(event.total_fee, 15_000);
        assert_eq!(event.priority_fee, 0);
        assert!(event.block_cost_limit > 0);
        assert!(event.account_cost_limit > 0);
        assert_eq!(event.epoch, bank.epoch());
        assert_eq!(event.clock_unix_timestamp, bank.clock().unix_timestamp);
        assert_eq!(event.identity_balance, bank.get_balance(&mint.pubkey()));
        assert_eq!(
            event.vote_balance,
            bank.get_balance(&genesis.voting_keypair.pubkey())
        );
    }

    #[test]
    fn test_our_footer_is_taken_for_its_own_bank_only() {
        let genesis = solana_runtime::genesis_utils::create_genesis_config(1_000);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        let footer = |close| BlockFooterV1 {
            bank_hash: solana_hash::Hash::default(),
            block_producer_time_nanos: close,
            block_user_agent: Vec::new(),
            block_final_cert: None,
            skip_reward_cert: None,
            notar_reward_cert: None,
        };
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut output = FrozenOutput {
            own_footers: receiver,
            ..FrozenOutput::stub()
        };
        let bank_id = BankId::from(bank.bank_id());
        // Another bank of the same slot, then this one, then a later slot.
        sender
            .send((bank.slot(), bank_id.saturating_add(1), footer(1)))
            .unwrap();
        sender.send((bank.slot(), bank_id, footer(2))).unwrap();
        sender
            .send((bank.slot().saturating_add(1), 9, footer(3)))
            .unwrap();
        let taken = output.take_own(&bank).unwrap();
        assert_eq!(taken.block_producer_time_nanos, 2);
        assert_eq!(output.own_held.len(), 1);
        assert!(output.take_own(&bank).is_none());
    }
}
