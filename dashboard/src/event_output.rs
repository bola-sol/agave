//! Publishes what the dashboard reads of the node as event streams, from inside the validator, so
//! a dashboard in its own process can rebuild it. Test branch only: each stream is read through
//! the same [`Source`] the collector reads, so this tests the event shapes, not where Agave would
//! publish them.

use {
    crate::{
        context::StartProgress,
        events::{
            ACCOUNT_PATHS, ADDR_ALPENGLOW, ADDR_BYTES, ADDR_GOSSIP, ADDR_RPC,
            ADDR_SERVE_REPAIR_UDP, ADDR_TPU_FORWARDS_QUIC, ADDR_TPU_QUIC, ADDR_TPU_VOTE_QUIC,
            ADDR_TPU_VOTE_UDP, ADDR_TVU_UDP, ADDRS, BITMAP_BYTES, BLOCK_FOOTER, BLOCK_MISSING,
            BLOCK_OPAQUE, BLOCK_STREAM, BlockEvent, EPOCH_STREAM, EpochEvent, LEDGER_SLOT_STREAM,
            LedgerSlotEvent, NODE_INFO_STREAM, NodeInfoEvent, PATH_BYTES, ROLE_ROOT, ROLE_WORKING,
            SLOT_CLOCK_STREAM, SLOT_STATUS_STREAM, START_PROGRESS_STREAM, SlotClockEvent,
            SlotStatusEvent, StartProgressEvent, pack_addr, pack_path, pack_str,
        },
        metric_events::{
            METRIC_ACCOUNTS_STREAM, METRIC_GOSSIP_STREAM, METRIC_LEADER_STREAM, METRIC_NET_STREAM,
            METRIC_ONCE_STREAM, METRIC_SLOT_STREAM, METRIC_TPU_STREAM, METRIC_VOTOR_STREAM,
            MetricAccounts, MetricEvent, MetricGossip, MetricLeader, MetricNet, MetricOnce,
            MetricSlot, MetricTpu, MetricVotor,
        },
        metrics_tap::MetricsTap,
        source::{BankView, Source},
    },
    agave_event_system::{
        Event, EventSystem, StreamConfig,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
        stream_name::StreamName,
    },
    agave_snapshots::SnapshotInterval,
    solana_core::validator::ValidatorStartProgress,
    solana_metrics::datapoint::DataPoint,
    solana_vote_interface::state::VoteStateV4,
    std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle},
        time::{Duration, Instant},
    },
};

const TICK: Duration = Duration::from_millis(200);
/// Unchanged events go out again this often, so a late subscriber has them soon.
const RESEND: Duration = Duration::from_secs(5);

/// `publishers` publishers on a new stream.
fn publishers<E: Event>(
    event_system: &EventSystem,
    name: StreamName,
    capacity: usize,
    publishers: usize,
) -> io::Result<Vec<Publisher<E>>> {
    let factory = event_system
        .create_stream::<E>(
            name,
            StreamConfig {
                capacity,
                publisher_slots: publishers,
                subscriber_slots: 8,
            },
        )
        .map_err(io::Error::other)?;
    (0..publishers)
        .map(|_| {
            factory
                .try_create_publisher()
                .ok_or_else(|| io::Error::other("no publisher slot on a new stream"))
        })
        .collect()
}

fn stream<E: Event>(
    event_system: &EventSystem,
    name: StreamName,
    capacity: usize,
) -> io::Result<Publisher<E>> {
    publishers(event_system, name, capacity, 1)?
        .pop()
        .ok_or_else(|| io::Error::other("no publisher"))
}

/// What decides whether an event changed: everything but its header.
trait Keyed {
    fn key(&self) -> Vec<u8>;
}

macro_rules! keyed {
    ($($event:ty),*) => {$(
        impl Keyed for $event {
            fn key(&self) -> Vec<u8> {
                format!(
                    "{:?}",
                    Self {
                        pid: 0,
                        tid: 0,
                        timestamp_ns: 0,
                        ..self.clone()
                    }
                )
                .into_bytes()
            }
        }
    )*};
}

keyed!(
    NodeInfoEvent,
    StartProgressEvent,
    SlotStatusEvent,
    EpochEvent,
    SlotClockEvent
);

/// Sends an event when its content changes, when its stream turns on, and every [`RESEND`].
struct Resent<E: Event> {
    publisher: Publisher<E>,
    sent: Option<(Vec<u8>, Instant)>,
}

impl<E: Event + Keyed> Resent<E> {
    fn new(publisher: Publisher<E>) -> Self {
        Self {
            publisher,
            sent: None,
        }
    }

    fn enabled(&mut self) -> bool {
        let on = self.publisher.is_enabled();
        if !on {
            self.sent = None;
        }
        on
    }

    fn offer(&mut self, event: E, now: Instant) {
        let key = event.key();
        let due = self
            .sent
            .as_ref()
            .is_none_or(|(held, at)| *held != key || now.duration_since(*at) >= RESEND);
        if due {
            let _ = self.publisher.publish(&event);
            self.sent = Some((key, now));
        }
    }
}

/// Slots below the highest whose blockstore records are watched.
const LEDGER_WINDOW: u64 = 64;
/// Blocks read per tick behind the root.
const BLOCKS_PER_TICK: u64 = 64;

pub struct EventOutput {
    ledger: Publisher<LedgerSlotEvent>,
    /// What has been sent of each watched slot: its first shred, and that it filled.
    ledger_sent: std::collections::BTreeMap<solana_clock::Slot, (bool, bool)>,
    blocks: Publisher<BlockEvent>,
    /// The next rooted slot whose block goes out.
    next_block: Option<solana_clock::Slot>,
    node: Resent<NodeInfoEvent>,
    progress: Resent<StartProgressEvent>,
    status: Resent<SlotStatusEvent>,
    epoch: Resent<EpochEvent>,
    root: Resent<SlotClockEvent>,
    working: Resent<SlotClockEvent>,
}

impl EventOutput {
    pub fn new(event_system: &EventSystem) -> io::Result<Self> {
        let mut clock = publishers::<SlotClockEvent>(event_system, SLOT_CLOCK_STREAM, 256, 2)?;
        let (Some(working), Some(root)) = (clock.pop(), clock.pop()) else {
            return Err(io::Error::other("no publishers"));
        };
        Ok(Self {
            ledger: stream(event_system, LEDGER_SLOT_STREAM, 1024)?,
            ledger_sent: std::collections::BTreeMap::new(),
            blocks: stream(event_system, BLOCK_STREAM, 512)?,
            next_block: None,
            node: Resent::new(stream(event_system, NODE_INFO_STREAM, 16)?),
            progress: Resent::new(stream(event_system, START_PROGRESS_STREAM, 64)?),
            status: Resent::new(stream(event_system, SLOT_STATUS_STREAM, 256)?),
            epoch: Resent::new(stream(event_system, EPOCH_STREAM, 16)?),
            working: Resent::new(working),
            root: Resent::new(root),
        })
    }

    /// Runs on its own thread until `exit`.
    pub fn spawn(
        mut self,
        src: Arc<dyn Source>,
        start_progress: StartProgress,
        exit: Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name("solEvtDash".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    self.tick(src.as_ref(), &start_progress);
                    thread::sleep(TICK);
                }
            })
    }

    fn tick(&mut self, src: &dyn Source, start_progress: &StartProgress) {
        let now = Instant::now();
        self.send_ledger(src);
        self.send_blocks(src);
        if self.progress.enabled() {
            let progress = *start_progress.read().unwrap();
            self.progress.offer(progress_event(progress), now);
        }
        if self.node.enabled() {
            self.node.offer(node_event(src), now);
        }
        let wants_banks = [
            self.status.enabled(),
            self.epoch.enabled(),
            self.root.enabled(),
            self.working.enabled(),
        ];
        if !wants_banks.contains(&true) {
            return;
        }
        if wants_banks[0] {
            self.status.offer(status_event(src), now);
        }
        let working = src.working_bank();
        let identity = src.identity();
        let vote_account = src.vote_account();
        if wants_banks[1] {
            self.epoch
                .offer(epoch_event(working.as_ref(), &vote_account), now);
        }
        if wants_banks[3] {
            self.working.offer(
                clock_event(ROLE_WORKING, working.as_ref(), &identity, &vote_account),
                now,
            );
        }
        if wants_banks[2] {
            let root = src.root_bank();
            self.root.offer(
                clock_event(ROLE_ROOT, root.as_ref(), &identity, &vote_account),
                now,
            );
        }
    }
}

impl EventOutput {
    /// Each watched slot once its first shred arrives, and again once it fills.
    fn send_ledger(&mut self, src: &dyn Source) {
        if !self.ledger.is_enabled() {
            self.ledger_sent.clear();
            return;
        }
        let highest = src
            .highest_slot()
            .max(src.cluster_tip().unwrap_or_default());
        let from = highest.saturating_sub(LEDGER_WINDOW);
        self.ledger_sent = self.ledger_sent.split_off(&from);
        for slot in from..=highest {
            let (first_sent, full_sent) = self.ledger_sent.get(&slot).copied().unwrap_or_default();
            if full_sent {
                continue;
            }
            let Some(first_shred_ms) = src.first_shred_time(slot) else {
                continue;
            };
            let full = src.is_full(slot);
            if first_sent && !full {
                continue;
            }
            let parent = src.parent_of(slot);
            let (pid, tid, timestamp_ns) = header();
            let _ = self.ledger.publish(&LedgerSlotEvent {
                pid,
                tid,
                timestamp_ns,
                slot,
                has_parent: parent.is_some(),
                parent_slot: parent.unwrap_or_default(),
                first_shred_ms,
                full,
            });
            self.ledger_sent.insert(slot, (true, full));
        }
    }

    /// Each rooted slot's block, from the root at turn-on.
    fn send_blocks(&mut self, src: &dyn Source) {
        if !self.blocks.is_enabled() {
            self.next_block = None;
            return;
        }
        let root = src.commitment().root.max(src.root_bank().slot());
        let next = *self.next_block.get_or_insert(root);
        let last = root.min(next.saturating_add(BLOCKS_PER_TICK).saturating_sub(1));
        for slot in next..=last {
            let _ = self
                .blocks
                .publish(&block_event(slot, src.block(slot, root)));
        }
        if last >= next {
            self.next_block = Some(last.saturating_add(1));
        }
    }
}

fn block_event(slot: solana_clock::Slot, block: crate::certs::Block) -> BlockEvent {
    let (pid, tid, timestamp_ns) = header();
    let mut event = BlockEvent {
        pid,
        tid,
        timestamp_ns,
        slot,
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
        crate::certs::Block::Missing => event.kind = BLOCK_MISSING,
        crate::certs::Block::Pending | crate::certs::Block::Opaque => {}
        crate::certs::Block::Footer(footer) => {
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

fn header() -> (i32, i32, u64) {
    let (tid, pid) = cached_thread_and_process_id();
    (pid, tid, monotonic_timestamp_ns())
}

pub(crate) fn node_event(src: &dyn Source) -> NodeInfoEvent {
    let (pid, tid, timestamp_ns) = header();
    let contact = src.my_contact();
    let mut addrs = [0u8; ADDRS * ADDR_BYTES];
    for (at, addr) in [
        (ADDR_GOSSIP, contact.gossip),
        (ADDR_RPC, contact.rpc),
        (ADDR_TVU_UDP, contact.tvu_udp),
        (ADDR_TPU_QUIC, contact.tpu_quic),
        (ADDR_TPU_FORWARDS_QUIC, contact.tpu_forwards_quic),
        (ADDR_TPU_VOTE_UDP, contact.tpu_vote_udp),
        (ADDR_TPU_VOTE_QUIC, contact.tpu_vote_quic),
        (ADDR_SERVE_REPAIR_UDP, contact.serve_repair_udp),
        (ADDR_ALPENGLOW, contact.alpenglow),
    ] {
        let start = at.saturating_mul(ADDR_BYTES);
        addrs[start..start.saturating_add(ADDR_BYTES)].copy_from_slice(&pack_addr(addr));
    }
    let mut account_paths = [0u8; ACCOUNT_PATHS * PATH_BYTES];
    for (packed, path) in account_paths
        .as_chunks_mut::<PATH_BYTES>()
        .0
        .iter_mut()
        .zip(src.account_paths())
    {
        packed.copy_from_slice(&pack_path(&path));
    }
    let snapshots = src.snapshot_config();
    let slots = |interval: SnapshotInterval| match interval {
        SnapshotInterval::Disabled => 0,
        SnapshotInterval::Slots(slots) => slots.get(),
    };
    NodeInfoEvent {
        pid,
        tid,
        timestamp_ns,
        identity: src.identity().to_bytes(),
        vote_account: src.vote_account().to_bytes(),
        cluster: match src.cluster_name() {
            "testnet" => 0,
            "mainnet" => 1,
            "devnet" => 2,
            _ => 3,
        },
        shred_version: src.shred_version(),
        version: pack_str(&contact.version),
        client: pack_str(&contact.client),
        wallclock: 0,
        outset: contact.outset,
        addrs,
        ledger_path: pack_path(&src.ledger_path()),
        account_paths,
        has_snapshots: snapshots.is_some(),
        snapshots_generated: snapshots
            .as_ref()
            .is_some_and(|config| config.should_generate_snapshots()),
        full_snapshot_dir: snapshots.as_ref().map_or([0u8; PATH_BYTES], |config| {
            pack_path(&config.full_snapshot_archives_dir)
        }),
        incremental_snapshot_dir: snapshots.as_ref().map_or([0u8; PATH_BYTES], |config| {
            pack_path(&config.incremental_snapshot_archives_dir)
        }),
        full_snapshot_interval: snapshots
            .as_ref()
            .map_or(0, |config| slots(config.full_snapshot_archive_interval)),
        incremental_snapshot_interval: snapshots.as_ref().map_or(0, |config| {
            slots(config.incremental_snapshot_archive_interval)
        }),
    }
}

fn progress_event(progress: ValidatorStartProgress) -> StartProgressEvent {
    let (pid, tid, timestamp_ns) = header();
    let mut event = StartProgressEvent {
        pid,
        tid,
        timestamp_ns,
        phase: 0,
        slot: 0,
        max_slot: 0,
        gossip_stake_percent: 0,
        rpc_addr: [0u8; ADDR_BYTES],
    };
    event.phase = match progress {
        ValidatorStartProgress::Initializing => 0,
        ValidatorStartProgress::SearchingForRpcService => 1,
        ValidatorStartProgress::DownloadingSnapshot { slot, rpc_addr } => {
            event.slot = slot;
            event.rpc_addr = pack_addr(Some(rpc_addr));
            2
        }
        ValidatorStartProgress::CleaningBlockStore => 3,
        ValidatorStartProgress::CleaningAccounts => 4,
        ValidatorStartProgress::LoadingLedger => 5,
        ValidatorStartProgress::ProcessingLedger { slot, max_slot } => {
            event.slot = slot;
            event.max_slot = max_slot;
            6
        }
        ValidatorStartProgress::StartingServices => 7,
        ValidatorStartProgress::Halted => 8,
        ValidatorStartProgress::WaitingForSupermajority {
            slot,
            gossip_stake_percent,
        } => {
            event.slot = slot;
            event.gossip_stake_percent = gossip_stake_percent;
            9
        }
        ValidatorStartProgress::Running => 10,
    };
    event
}

pub(crate) fn status_event(src: &dyn Source) -> SlotStatusEvent {
    let (pid, tid, timestamp_ns) = header();
    let commitment = src.commitment();
    let tip = src.cluster_tip();
    SlotStatusEvent {
        pid,
        tid,
        timestamp_ns,
        root: commitment.root,
        confirmed: commitment.confirmed,
        finalized: commitment.finalized,
        highest_slot: src.highest_slot(),
        has_cluster_tip: tip.is_some(),
        cluster_tip: tip.unwrap_or(0),
        lowest_slot: src.lowest_slot(),
    }
}

pub(crate) fn epoch_event(bank: &dyn BankView, vote_account: &solana_pubkey::Pubkey) -> EpochEvent {
    let (pid, tid, timestamp_ns) = header();
    let schedule = bank.epoch_schedule();
    let cost = bank.cost();
    let ticket_short = bank.vat_ticket_short(vote_account);
    EpochEvent {
        pid,
        tid,
        timestamp_ns,
        epoch: bank.epoch(),
        slots_per_epoch: schedule.slots_per_epoch,
        leader_schedule_slot_offset: schedule.leader_schedule_slot_offset,
        warmup: schedule.warmup,
        first_normal_epoch: schedule.first_normal_epoch,
        first_normal_slot: schedule.first_normal_slot,
        ns_per_slot: u64::try_from(bank.ns_per_slot_at_slot(bank.slot())).unwrap_or(u64::MAX),
        alpenglow: bank.is_alpenglow(),
        lamports_per_signature: bank.get_lamports_per_signature(),
        block_cost_limit: cost.block_limit,
        account_cost_limit: cost.account_limit,
        minimum_vat_balance: bank.minimum_vote_account_balance_for_vat(),
        vote_account_rent: bank.get_minimum_balance_for_rent_exemption(VoteStateV4::size_of()),
        has_ticket_short: ticket_short.is_some(),
        ticket_short: ticket_short.unwrap_or(0),
    }
}

pub(crate) fn clock_event(
    role: u8,
    bank: &dyn BankView,
    identity: &solana_pubkey::Pubkey,
    vote_account: &solana_pubkey::Pubkey,
) -> SlotClockEvent {
    let (pid, tid, timestamp_ns) = header();
    let clock = bank.clock();
    let cost = bank.cost();
    let fees = bank.fees();
    SlotClockEvent {
        pid,
        tid,
        timestamp_ns,
        role,
        slot: bank.slot(),
        parent_slot: bank.parent_slot(),
        epoch: bank.epoch(),
        frozen: bank.is_frozen(),
        block_height: bank.block_height(),
        clock_slot: clock.slot,
        clock_epoch_start_timestamp: clock.epoch_start_timestamp,
        clock_epoch: clock.epoch,
        clock_leader_schedule_epoch: clock.leader_schedule_epoch,
        clock_unix_timestamp: clock.unix_timestamp,
        blockhash: bank.last_blockhash().to_bytes(),
        transaction_count: bank.transaction_count(),
        non_vote_transaction_count: bank.non_vote_transaction_count_since_restart(),
        transaction_error_count: bank.transaction_error_count(),
        entries_count: bank.transaction_entries_count(),
        block_cost: cost.block_cost,
        block_cost_limit: cost.block_limit,
        account_cost_limit: cost.account_limit,
        total_fees: fees.total,
        priority_fees: fees.priority,
        identity_balance: bank.get_balance(identity),
        vote_balance: bank.get_balance(vote_account),
    }
}

/// Publishes the datapoints the tap forwards, each on its row's stream.
pub struct MetricOutput {
    slot: Publisher<MetricSlot>,
    leader: Publisher<MetricLeader>,
    votor: Publisher<MetricVotor>,
    tpu: Publisher<MetricTpu>,
    net: Publisher<MetricNet>,
    accounts: Publisher<MetricAccounts>,
    gossip: Publisher<MetricGossip>,
    once: Publisher<MetricOnce>,
}

impl MetricOutput {
    pub fn new(event_system: &EventSystem) -> io::Result<Self> {
        Ok(Self {
            slot: stream(event_system, METRIC_SLOT_STREAM, 4096)?,
            // Workers report every 20 ms each.
            leader: stream(event_system, METRIC_LEADER_STREAM, 8192)?,
            // Votor reports every vote account at once.
            votor: stream(event_system, METRIC_VOTOR_STREAM, 8192)?,
            tpu: stream(event_system, METRIC_TPU_STREAM, 1024)?,
            net: stream(event_system, METRIC_NET_STREAM, 1024)?,
            accounts: stream(event_system, METRIC_ACCOUNTS_STREAM, 2048)?,
            gossip: stream(event_system, METRIC_GOSSIP_STREAM, 256)?,
            once: stream(event_system, METRIC_ONCE_STREAM, 16)?,
        })
    }

    /// Runs on its own thread until `exit`, turning the tap's forwarding on while any stream is.
    pub fn spawn(
        mut self,
        tap: Arc<MetricsTap>,
        points: crossbeam_channel::Receiver<(DataPoint, u64)>,
        exit: Arc<AtomicBool>,
    ) -> io::Result<JoinHandle<()>> {
        thread::Builder::new()
            .name("solEvtMetric".to_string())
            .spawn(move || {
                let mut checked = Instant::now();
                tap.set_forwarding(self.enabled());
                while !exit.load(Ordering::Relaxed) {
                    match points.recv_timeout(TICK) {
                        Ok((point, observed_ms)) => self.publish(&point, observed_ms),
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                    }
                    if checked.elapsed() >= TICK {
                        checked = Instant::now();
                        tap.set_forwarding(self.enabled());
                    }
                }
                tap.set_forwarding(false);
            })
    }

    fn enabled(&mut self) -> bool {
        // Every stream is asked, so each one's turn-on is seen.
        [
            self.slot.is_enabled(),
            self.leader.is_enabled(),
            self.votor.is_enabled(),
            self.tpu.is_enabled(),
            self.net.is_enabled(),
            self.accounts.is_enabled(),
            self.gossip.is_enabled(),
            self.once.is_enabled(),
        ]
        .contains(&true)
    }

    fn publish(&mut self, point: &DataPoint, observed_ms: u64) {
        if let Some(event) = MetricSlot::from_point(point, observed_ms) {
            let _ = self.slot.publish(&event);
        } else if let Some(event) = MetricLeader::from_point(point, observed_ms) {
            let _ = self.leader.publish(&event);
        } else if let Some(event) = MetricVotor::from_point(point, observed_ms) {
            let _ = self.votor.publish(&event);
        } else if let Some(event) = MetricTpu::from_point(point, observed_ms) {
            let _ = self.tpu.publish(&event);
        } else if let Some(event) = MetricNet::from_point(point, observed_ms) {
            let _ = self.net.publish(&event);
        } else if let Some(event) = MetricAccounts::from_point(point, observed_ms) {
            let _ = self.accounts.publish(&event);
        } else if let Some(event) = MetricGossip::from_point(point, observed_ms) {
            let _ = self.gossip.publish(&event);
        } else if let Some(event) = MetricOnce::from_point(point, observed_ms) {
            let _ = self.once.publish(&event);
        }
    }
}
