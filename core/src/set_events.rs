//! State a late subscriber needs that is too large or too slow for the state thread: the gossip
//! table, and votor's newest certificate with the ledger's floor.

use {
    crate::{
        bank_state_events::{Latest, publish_paced, publisher},
        node_events::types::{
            GOSSIP_PEER_STREAM, GossipPeerEvent, SLOT_STATUS_STREAM, SlotStatusEvent, pack_contact,
            pack_str,
        },
    },
    agave_event_system::{
        EventSystem,
        event_metadata::{cached_thread_and_process_id, monotonic_timestamp_ns},
        publisher::Publisher,
    },
    solana_gossip::{cluster_info::ClusterInfo, crds_data::SnapshotHashes},
    solana_ledger::blockstore::Blockstore,
    solana_runtime::validated_block_finalization::ValidatedBlockFinalizationCert,
    std::{
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const TICK: Duration = Duration::from_secs(1);
/// How often the gossip table goes out whole.
const PEERS_EVERY: Duration = Duration::from_secs(15);
/// How often the status goes out unchanged.
const STATUS_EVERY: Duration = Duration::from_secs(5);

/// Every gossip peer with its contact and advertised snapshots, as one set.
pub fn peer_events(cluster_info: &ClusterInfo, generation: u64) -> Vec<GossipPeerEvent> {
    let (tid, pid) = cached_thread_and_process_id();
    let timestamp_ns = monotonic_timestamp_ns();
    let peers = cluster_info.all_peers();
    let snapshots: Vec<_> = {
        // One short hold of the gossip table's lock.
        let crds = cluster_info.gossip.crds.read();
        peers
            .iter()
            .map(|(contact, _)| {
                let hashes = crds.get::<&SnapshotHashes>(*contact.pubkey());
                (
                    hashes.map(|hashes| hashes.full.0),
                    hashes
                        .and_then(|hashes| hashes.incremental.iter().map(|(slot, _)| *slot).max()),
                )
            })
            .collect()
    };
    let count = u32::try_from(peers.len()).unwrap_or(u32::MAX);
    peers
        .iter()
        .zip(snapshots)
        .zip(0u32..)
        .map(
            |(((contact, heard_ms), (full, incremental)), index)| GossipPeerEvent {
                pid,
                tid,
                timestamp_ns,
                generation,
                index,
                count,
                pubkey: contact.pubkey().to_bytes(),
                client: pack_str(&contact.version().client().to_string()),
                version: pack_str(&contact.version().to_string()),
                wallclock: contact.wallclock(),
                outset: contact.outset(),
                shred_version: contact.shred_version(),
                addrs: pack_contact(contact),
                heard_ms: *heard_ms,
                has_full_snapshot: full.is_some(),
                full_snapshot: full.unwrap_or(0),
                has_incremental_snapshot: incremental.is_some(),
                incremental_snapshot: incremental.unwrap_or(0),
            },
        )
        .collect()
}

pub fn status_event(
    highest_finalized: &RwLock<Option<ValidatedBlockFinalizationCert>>,
    blockstore: &Blockstore,
) -> SlotStatusEvent {
    let (tid, pid) = cached_thread_and_process_id();
    let certificate_slot = highest_finalized
        .read()
        .ok()
        .and_then(|cert| cert.as_ref().map(|cert| cert.block().slot));
    SlotStatusEvent {
        pid,
        tid,
        timestamp_ns: monotonic_timestamp_ns(),
        has_certificate_slot: certificate_slot.is_some(),
        certificate_slot: certificate_slot.unwrap_or(0),
        lowest_slot: blockstore.lowest_slot(),
    }
}

pub struct SetEvents {
    thread_hdl: JoinHandle<()>,
}

impl SetEvents {
    pub fn new(
        event_system: &EventSystem,
        cluster_info: Arc<ClusterInfo>,
        blockstore: Arc<Blockstore>,
        highest_finalized: Arc<RwLock<Option<ValidatedBlockFinalizationCert>>>,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, String> {
        let mut peers = publisher::<GossipPeerEvent>(event_system, GOSSIP_PEER_STREAM, 16_384)?;
        let mut status = Latest::new(publisher::<SlotStatusEvent>(
            event_system,
            SLOT_STATUS_STREAM,
            256,
        )?);
        let thread_hdl = Builder::new()
            .name("solEvtSets".to_string())
            .spawn(move || {
                let mut sets = SetLoop::default();
                while !exit.load(Ordering::Relaxed) {
                    sets.send_peers(&mut peers, &cluster_info, &exit);
                    sets.send_status(&mut status, &highest_finalized, &blockstore);
                    thread::sleep(TICK);
                }
            })
            .map_err(|err| format!("Failed to spawn solEvtSets: {err}"))?;
        Ok(Self { thread_hdl })
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[derive(Default)]
struct SetLoop {
    /// The generation and time of the last whole gossip table.
    peers_sent: Option<(u64, Instant)>,
    status_sent: Option<Instant>,
}

impl SetLoop {
    fn send_peers(
        &mut self,
        publisher: &mut Publisher<GossipPeerEvent>,
        cluster_info: &ClusterInfo,
        exit: &AtomicBool,
    ) {
        if !publisher.is_enabled() {
            self.peers_sent = None;
            return;
        }
        let now = Instant::now();
        let generation = match self.peers_sent {
            Some((_, at)) if now.duration_since(at) < PEERS_EVERY => return,
            Some((generation, _)) => generation.saturating_add(1),
            None => 1,
        };
        self.peers_sent = Some((generation, now));
        publish_paced(publisher, &peer_events(cluster_info, generation), exit);
    }

    fn send_status(
        &mut self,
        status: &mut Latest<SlotStatusEvent>,
        highest_finalized: &RwLock<Option<ValidatedBlockFinalizationCert>>,
        blockstore: &Blockstore,
    ) {
        if !status.enabled() {
            self.status_sent = None;
            return;
        }
        let now = Instant::now();
        let due = self
            .status_sent
            .is_none_or(|at| now.duration_since(at) >= STATUS_EVERY);
        if due {
            self.status_sent = Some(now);
        }
        status.offer(status_event(highest_finalized, blockstore), due);
    }
}
