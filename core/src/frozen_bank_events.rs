//! Readings of each frozen bank that cost too much for replay's thread: which ranks' last votes it
//! held, and the config accounts it changed, where validator names are kept. Replay hands each
//! bank over as it freezes; the whole config account set also goes out every minute for
//! subscribers that join late.

use {
    crate::{
        bank_state_events::{publish_paced, publisher},
        node_events::types::{
            CONFIG_DATA_BYTES, FINALIZATION_RANKS, FINALIZATION_STREAM, FinalizationEvent,
            INFO_CHANGE, INFO_FULL_SET, VALIDATOR_INFO_STREAM, ValidatorInfoEvent,
        },
    },
    agave_event_system::{
        EventSystem,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TrySendError, bounded},
    solana_account::{AccountSharedData, ReadableAccount},
    solana_accounts_db::accounts_index::IndexKey,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    std::{
        collections::HashMap,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Frozen banks waiting to be read; past this replay drops them rather than wait.
const QUEUE: usize = 256;
const TICK: Duration = Duration::from_secs(1);
/// How often every config account goes out.
const CONFIG_EVERY: Duration = Duration::from_secs(60);
/// A last vote this close to the newest one in the bank counts as carried.
pub const NEAR_SLOTS: u64 = 3;

/// Replay's end: hands a frozen bank over while either stream is on.
pub struct FrozenBankSender {
    sender: Sender<Arc<Bank>>,
    wanted: Arc<AtomicBool>,
}

impl FrozenBankSender {
    #[cfg(test)]
    pub(crate) fn stub() -> Self {
        Self {
            sender: bounded(0).0,
            wanted: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn send(&self, bank: &Arc<Bank>) {
        if !self.wanted.load(Ordering::Relaxed) {
            return;
        }
        if let Err(TrySendError::Full(_)) = self.sender.try_send(bank.clone()) {
            log::debug!("frozen bank events: slot {} dropped", bank.slot());
        }
    }
}

/// Which ranks' last votes the bank held, and which were within `NEAR_SLOTS` of the newest.
pub fn finalization_event(bank: &Bank) -> Option<FinalizationEvent> {
    if !bank.is_alpenglow() {
        return None;
    }
    let ranks = bank.get_rank_map(bank.slot())?;
    let mut votes: Vec<(usize, Slot)> = Vec::new();
    for (vote, (_, account)) in bank.vote_accounts().iter() {
        if let (Some(rank), Some(last)) = (
            ranks.get_rank_for_vote_pubkey(vote),
            account.vote_state_view().last_voted_slot(),
        ) {
            votes.push((usize::from(*rank), last));
        }
    }
    let front = votes.iter().map(|(_, slot)| *slot).max()?;
    let (tid, pid) = cached_thread_and_process_id();
    let mut event = FinalizationEvent {
        pid,
        tid,
        timestamp_ns: monotonic_timestamp_ns(),
        slot: bank.slot(),
        front,
        ranks: u16::try_from(ranks.len().min(FINALIZATION_RANKS)).unwrap_or(0),
        seen: [0u8; FINALIZATION_RANKS / 8],
        carried: [0u8; FINALIZATION_RANKS / 8],
    };
    for (rank, slot) in votes {
        let (byte, bit) = (rank / 8, rank % 8);
        let mask = 1u8
            .checked_shl(u32::try_from(bit).unwrap_or(0))
            .unwrap_or(0);
        if let Some(seen) = event.seen.get_mut(byte) {
            *seen |= mask;
        }
        if front.saturating_sub(slot) <= NEAR_SLOTS
            && let Some(carried) = event.carried.get_mut(byte)
        {
            *carried |= mask;
        }
    }
    Some(event)
}

/// Config accounts as events of `reason`; one too large for a validator info account is left out.
pub fn config_events(
    reason: u8,
    generation: u64,
    slot: Slot,
    accounts: &[(Pubkey, AccountSharedData)],
) -> Vec<ValidatorInfoEvent> {
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let fitting: Vec<_> = accounts
        .iter()
        .filter(|(_, account)| account.data().len() <= CONFIG_DATA_BYTES)
        .collect();
    let count = u32::try_from(fitting.len()).unwrap_or(u32::MAX);
    fitting
        .into_iter()
        .zip(0u32..)
        .map(|((key, account), index)| {
            let bytes = account.data();
            let mut data = [0u8; CONFIG_DATA_BYTES];
            if let Some(slot) = data.get_mut(..bytes.len()) {
                slot.copy_from_slice(bytes);
            }
            ValidatorInfoEvent {
                pid,
                tid,
                timestamp_ns,
                reason,
                generation,
                index,
                count,
                slot,
                account: key.to_bytes(),
                data_len: u16::try_from(bytes.len()).unwrap_or(0),
                data,
            }
        })
        .collect()
}

/// The config accounts this bank wrote.
fn config_changes(bank: &Bank) -> Vec<(Pubkey, AccountSharedData)> {
    bank.get_program_accounts_modified_since_parent(&solana_sdk_ids::config::id())
}

/// Every config account, by the secondary index. Minutes on a real cluster: run off-thread.
fn config_scan(bank: &Bank) -> Vec<(Pubkey, AccountSharedData)> {
    let config_id = solana_sdk_ids::config::id();
    // Without the config program in the index, the indexed call reads every account on the
    // validator, which takes hours and cannot be interrupted.
    if !bank.account_indexes_include_key(&config_id) {
        log::info!(
            "frozen bank events: the config program is excluded from the account index, so \
             validator names are unavailable"
        );
        return Vec::new();
    }
    match bank.get_filtered_indexed_accounts(
        &IndexKey::ProgramId(config_id),
        |account| account.owner() == &config_id,
        None,
    ) {
        Ok(accounts) => {
            // An index never built answers none rather than failing.
            if accounts.is_empty() {
                log::info!(
                    "frozen bank events: found no config accounts. Start the validator with \
                     --account-index program-id --account-index-include-key {config_id} to read \
                     validator names"
                );
            }
            accounts
        }
        Err(err) => {
            log::warn!("frozen bank events: could not read config accounts: {err}");
            Vec::new()
        }
    }
}

type Scan = JoinHandle<(Slot, Vec<(Pubkey, AccountSharedData)>)>;

/// Every config account at the newest slot read, kept current from frozen banks.
#[derive(Default)]
struct ConfigSet {
    accounts: HashMap<Pubkey, (Slot, AccountSharedData)>,
    /// The scan started at turn-on, and whether it finished.
    scan: Option<Scan>,
    scanned: bool,
    /// Banks frozen while the scan ran, applied over its result.
    while_scanning: Vec<(Slot, Vec<(Pubkey, AccountSharedData)>)>,
    sent: Option<(u64, Instant)>,
}

impl ConfigSet {
    fn merge(&mut self, slot: Slot, accounts: Vec<(Pubkey, AccountSharedData)>) {
        for (key, account) in accounts {
            let newer = self
                .accounts
                .get(&key)
                .is_none_or(|(held, _)| slot >= *held);
            if newer {
                self.accounts.insert(key, (slot, account));
            }
        }
    }
}

pub struct FrozenBankEvents {
    thread_hdl: JoinHandle<()>,
}

impl FrozenBankEvents {
    pub fn new(
        event_system: &EventSystem,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> Result<(Self, FrozenBankSender), String> {
        let finalization = publisher::<FinalizationEvent>(event_system, FINALIZATION_STREAM, 1024)?;
        let configs = publisher::<ValidatorInfoEvent>(event_system, VALIDATOR_INFO_STREAM, 8192)?;
        let (sender, receiver) = bounded(QUEUE);
        let wanted = Arc::new(AtomicBool::new(false));
        let mut reader = FrozenReader {
            finalization,
            configs,
            config_set: ConfigSet::default(),
            wanted: wanted.clone(),
        };
        let thread_hdl = Builder::new()
            .name("solEvtFrozen".to_string())
            .spawn(move || reader.run(&receiver, &bank_forks, &exit))
            .map_err(|err| format!("Failed to spawn solEvtFrozen: {err}"))?;
        Ok((Self { thread_hdl }, FrozenBankSender { sender, wanted }))
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

struct FrozenReader {
    finalization: Publisher<FinalizationEvent>,
    configs: Publisher<ValidatorInfoEvent>,
    config_set: ConfigSet,
    wanted: Arc<AtomicBool>,
}

impl FrozenReader {
    fn run(
        &mut self,
        receiver: &Receiver<Arc<Bank>>,
        bank_forks: &RwLock<BankForks>,
        exit: &AtomicBool,
    ) {
        let mut next_tick = Instant::now();
        while !exit.load(Ordering::Relaxed) {
            match receiver.recv_timeout(TICK) {
                Ok(bank) => self.read(&bank),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            let now = Instant::now();
            if now >= next_tick {
                next_tick = now + TICK;
                self.tick(bank_forks, exit, now);
            }
        }
    }

    fn read(&mut self, bank: &Bank) {
        if self.finalization.is_enabled()
            && let Some(event) = finalization_event(bank)
        {
            let _ = self.finalization.publish(&event);
        }
        if !self.configs.is_enabled() {
            return;
        }
        let changed = config_changes(bank);
        if changed.is_empty() {
            return;
        }
        let generation = self.config_set.sent.map_or(0, |(generation, _)| generation);
        for event in config_events(INFO_CHANGE, generation, bank.slot(), &changed) {
            let _ = self.configs.publish(&event);
        }
        if self.config_set.scanned {
            self.config_set.merge(bank.slot(), changed);
        } else {
            self.config_set.while_scanning.push((bank.slot(), changed));
        }
    }

    fn tick(&mut self, bank_forks: &RwLock<BankForks>, exit: &AtomicBool, now: Instant) {
        let configs_on = self.configs.is_enabled();
        self.wanted.store(
            configs_on || self.finalization.is_enabled(),
            Ordering::Relaxed,
        );
        let set = &mut self.config_set;
        if !configs_on {
            // A running scan is left to finish and is dropped unread.
            let scan = set.scan.take();
            *set = ConfigSet::default();
            drop(scan);
            return;
        }
        if !set.scanned {
            match &set.scan {
                None => {
                    let bank = bank_forks.read().unwrap().root_bank();
                    set.scan = Builder::new()
                        .name("solEvtConfig".to_string())
                        .spawn(move || (bank.slot(), config_scan(&bank)))
                        .ok();
                    return;
                }
                Some(scan) if !scan.is_finished() => return,
                Some(_) => {
                    if let Some(Ok((slot, accounts))) = set.scan.take().map(JoinHandle::join) {
                        set.merge(slot, accounts);
                    }
                    for (slot, changed) in std::mem::take(&mut set.while_scanning) {
                        set.merge(slot, changed);
                    }
                    set.scanned = true;
                }
            }
        }
        if set.accounts.is_empty() {
            return;
        }
        let generation = match set.sent {
            Some((_, at)) if now.duration_since(at) < CONFIG_EVERY => return,
            Some((generation, _)) => generation.saturating_add(1),
            None => 1,
        };
        set.sent = Some((generation, now));
        let slot = set
            .accounts
            .values()
            .map(|(slot, _)| *slot)
            .max()
            .unwrap_or_default();
        let accounts: Vec<_> = set
            .accounts
            .iter()
            .map(|(key, (_, account))| (*key, account.clone()))
            .collect();
        publish_paced(
            &mut self.configs,
            &config_events(INFO_FULL_SET, generation, slot, &accounts),
            exit,
        );
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::genesis_utils::{
            ValidatorVoteKeypairs, create_genesis_config_with_vote_accounts,
        },
    };

    #[test]
    fn test_config_accounts_go_out_whole_and_one_too_large_is_left_out() {
        let config = solana_sdk_ids::config::id();
        let small = AccountSharedData::new(1, 40, &config);
        let large = AccountSharedData::new(1, CONFIG_DATA_BYTES.saturating_add(1), &config);
        let (first, second) = (Pubkey::new_unique(), Pubkey::new_unique());
        let events = config_events(
            INFO_FULL_SET,
            3,
            9,
            &[
                (first, small.clone()),
                (Pubkey::new_unique(), large),
                (second, small),
            ],
        );
        assert_eq!(events.len(), 2);
        for (event, (index, key)) in events.iter().zip([(0u32, first), (1, second)]) {
            assert_eq!((event.index, event.count, event.generation), (index, 2, 3));
            assert_eq!((event.slot, event.data_len), (9, 40));
            assert_eq!(event.account, key.to_bytes());
        }
    }

    #[test]
    fn test_a_bank_without_last_votes_has_no_finalization_reading() {
        let keypairs: Vec<_> = (0..2).map(|_| ValidatorVoteKeypairs::new_rand()).collect();
        let genesis =
            create_genesis_config_with_vote_accounts(1_000_000_000, &keypairs, vec![100, 200]);
        let bank = Bank::new_for_tests(&genesis.genesis_config);
        assert!(finalization_event(&bank).is_none());
    }

    #[test]
    fn test_a_bank_is_handed_over_only_while_wanted() {
        let (sender, receiver) = bounded(1);
        let wanted = Arc::new(AtomicBool::new(false));
        let handle = FrozenBankSender {
            sender,
            wanted: wanted.clone(),
        };
        let genesis = solana_runtime::genesis_utils::create_genesis_config(1_000);
        let bank = Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        handle.send(&bank);
        assert!(receiver.try_recv().is_err());
        wanted.store(true, Ordering::Relaxed);
        handle.send(&bank);
        // Full: dropped rather than waited for.
        handle.send(&bank);
        assert_eq!(receiver.try_recv().unwrap().slot(), bank.slot());
        assert!(receiver.try_recv().is_err());
    }
}
