//! The node event types, from which a process of its own can rebuild the node's state.
//! Timestamps are monotonic nanoseconds.

use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::{Epoch, Slot},
    std::{net::SocketAddr, path::Path},
};

pub const NODE_INFO_STREAM: StreamName = agave_event_system::stream_name!("node.info");
pub const SLOT_STATUS_STREAM: StreamName = agave_event_system::stream_name!("bank.slot_status");
pub const EPOCH_STREAM: StreamName = agave_event_system::stream_name!("bank.epoch");
pub const LEDGER_SLOT_STREAM: StreamName = agave_event_system::stream_name!("ledger.first_shred");
pub const BLOCK_STREAM: StreamName = agave_event_system::stream_name!("ledger.reward_cert");
pub const OWN_BLOCK_STREAM: StreamName = agave_event_system::stream_name!("ledger.own_reward_cert");
pub const ADMITTED_STREAM: StreamName = agave_event_system::stream_name!("bank.admitted");
pub const SLOT_HISTORY_STREAM: StreamName = agave_event_system::stream_name!("bank.slot_history");
pub const TX_OUTCOME_STREAM: StreamName = agave_event_system::stream_name!("ledger.tx_outcome");
pub const GOSSIP_PEER_STREAM: StreamName = agave_event_system::stream_name!("gossip.peer");
pub const VALIDATOR_INFO_STREAM: StreamName =
    agave_event_system::stream_name!("bank.validator_info");
pub const FINALIZATION_STREAM: StreamName = agave_event_system::stream_name!("bank.finalization");
pub const MESSAGE_VERSIONS_STREAM: StreamName =
    agave_event_system::stream_name!("ledger.message_versions");

/// Path bytes a node info event holds; a longer path is left empty.
pub const PATH_BYTES: usize = 192;
pub const ACCOUNT_PATHS: usize = 4;

/// The order of `NodeInfoEvent::addrs`.
pub const ADDR_GOSSIP: usize = 0;
pub const ADDR_RPC: usize = 1;
pub const ADDR_TVU_UDP: usize = 2;
pub const ADDR_TPU_QUIC: usize = 3;
pub const ADDR_TPU_FORWARDS_QUIC: usize = 4;
pub const ADDR_TPU_VOTE_UDP: usize = 5;
pub const ADDR_TPU_VOTE_QUIC: usize = 6;
pub const ADDR_SERVE_REPAIR_UDP: usize = 7;
pub const ADDR_ALPENGLOW: usize = 8;
pub const ADDRS: usize = 9;
pub const ADDR_BYTES: usize = 18;

/// The node: keys, build, contact, and the paths of its ledger, accounts and snapshots.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfoEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub identity: [u8; 32],
    pub vote_account: [u8; 32],
    /// 0 testnet, 1 mainnet, 2 devnet, 3 development.
    pub cluster: u8,
    pub shred_version: u16,
    pub version: [u8; 48],
    pub client: [u8; 24],
    pub wallclock: u64,
    pub outset: u64,
    /// `ADDRS` addresses of `ADDR_BYTES` each: IPv6 or v4-mapped, then the port big-endian;
    /// port 0 is none.
    pub addrs: [u8; ADDRS * ADDR_BYTES],
    pub ledger_path: [u8; PATH_BYTES],
    /// `ACCOUNT_PATHS` paths of `PATH_BYTES` each.
    pub account_paths: [u8; ACCOUNT_PATHS * PATH_BYTES],
    pub has_snapshots: bool,
    pub snapshots_generated: bool,
    pub full_snapshot_dir: [u8; PATH_BYTES],
    pub incremental_snapshot_dir: [u8; PATH_BYTES],
    /// Slots between archives; 0 is disabled.
    pub full_snapshot_interval: u64,
    pub incremental_snapshot_interval: u64,
    /// Gossip accepts only globally routable addresses, which the RPC node count follows.
    pub global_addresses_only: bool,
}

/// What no other stream carries of where the node stands: votor's newest finalization
/// certificate and the ledger's floor.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotStatusEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub has_certificate_slot: bool,
    pub certificate_slot: Slot,
    pub lowest_slot: Slot,
}

/// The epoch as the working bank holds it, and the figures fixed with it.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub epoch: Epoch,
    pub slots_per_epoch: u64,
    pub leader_schedule_slot_offset: u64,
    pub warmup: bool,
    pub first_normal_epoch: Epoch,
    pub first_normal_slot: Slot,
    pub ns_per_slot: u64,
    pub alpenglow: bool,
    pub lamports_per_signature: u64,
    pub block_cost_limit: u64,
    pub account_cost_limit: u64,
    pub minimum_vat_balance: u64,
    /// Rent exemption for a vote account.
    pub vote_account_rent: u64,
    pub has_ticket_short: bool,
    pub ticket_short: u64,
    /// The vote reward account's data, where it fits.
    pub vote_reward_len: u8,
    pub vote_reward: [u8; VOTE_REWARD_BYTES],
}

/// Bytes of the vote reward account an epoch event holds.
pub const VOTE_REWARD_BYTES: usize = 64;

/// Ranks per admitted set event.
pub const RANKS_PER_EVENT: usize = 32;

/// One part of an epoch's rank map, the admitted set in rank order. An epoch's parts share a
/// `generation`, and `index` and `count` mark when it is complete.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmittedEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub epoch: Epoch,
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    /// The epoch's stake as the rewards read it.
    pub total_stake: u64,
    pub first_rank: u32,
    pub len: u8,
    /// `RANKS_PER_EVENT` vote accounts, identities and stakes, `len` of them used.
    pub votes: [u8; RANKS_PER_EVENT * 32],
    pub identities: [u8; RANKS_PER_EVENT * 32],
    pub stakes: [u64; RANKS_PER_EVENT],
}

/// Words of the slot history per event.
pub const HISTORY_WORDS: usize = 256;

/// One part of the root bank's slot history bitmap, sent whole at turn-on and every 5 seconds.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotHistoryEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    pub next_slot: Slot,
    pub first_word: u32,
    pub len: u16,
    pub words: [u64; HISTORY_WORDS],
}

/// A slot's blockstore record: when its first shred arrived, its parent, and whether it is full.
/// Sent when the first shred is seen and again when the slot fills.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerSlotEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub has_parent: bool,
    pub parent_slot: Slot,
    /// Unix milliseconds; 0 where none has arrived.
    pub first_shred_ms: u64,
    pub full: bool,
}

pub const BLOCK_MISSING: u8 = 1;
pub const BLOCK_OPAQUE: u8 = 2;
pub const BLOCK_FOOTER: u8 = 3;

/// Bytes an encoded reward certificate bitmap holds; a longer one reads as opaque.
pub const BITMAP_BYTES: usize = 512;

/// A frozen bank's footer figures as replay read them, or as we produced them; in the own set, a
/// rooted slot of ours as the ledger holds it: skipped, unreadable or its footer's figures.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    /// The bank the footer belongs to; 0 in the own set.
    pub bank_id: u64,
    /// `BLOCK_MISSING`, `BLOCK_OPAQUE` or `BLOCK_FOOTER`.
    pub kind: u8,
    /// The leader's close time, unix nanoseconds.
    pub close_nanos: u64,
    pub has_notar: bool,
    pub notar_len: u16,
    pub notar: [u8; BITMAP_BYTES],
    pub has_skip: bool,
    pub skip_len: u16,
    pub skip: [u8; BITMAP_BYTES],
    pub has_fast: bool,
    pub fast: bool,
}

/// One of our blocks this epoch up to `covered_to`, as a member of a set sent whole every few
/// seconds. A set with none is one event with `count` 0.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnBlockEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub epoch: Epoch,
    pub identity: [u8; 32],
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    /// The slot the set was read up to; ours past it are answered by the footers sent as they
    /// froze.
    pub covered_to: Slot,
    pub slot: Slot,
    pub kind: u8,
    pub close_nanos: u64,
    pub has_notar: bool,
    pub notar_len: u16,
    pub notar: [u8; BITMAP_BYTES],
    pub has_skip: bool,
    pub skip_len: u16,
    pub skip: [u8; BITMAP_BYTES],
    pub has_fast: bool,
    pub fast: bool,
}

/// Who sent an own block set, where it belongs and the event's place in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OwnSetPlace {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub epoch: Epoch,
    pub identity: [u8; 32],
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    pub covered_to: Slot,
}

impl OwnBlockEvent {
    pub fn new(place: OwnSetPlace, block: &BlockEvent) -> Self {
        Self {
            pid: place.pid,
            tid: place.tid,
            timestamp_ns: place.timestamp_ns,
            epoch: place.epoch,
            identity: place.identity,
            generation: place.generation,
            index: place.index,
            count: place.count,
            covered_to: place.covered_to,
            slot: block.slot,
            kind: block.kind,
            close_nanos: block.close_nanos,
            has_notar: block.has_notar,
            notar_len: block.notar_len,
            notar: block.notar,
            has_skip: block.has_skip,
            skip_len: block.skip_len,
            skip: block.skip,
            has_fast: block.has_fast,
            fast: block.fast,
        }
    }

    pub fn block(&self) -> BlockEvent {
        BlockEvent {
            pid: self.pid,
            tid: self.tid,
            timestamp_ns: self.timestamp_ns,
            slot: self.slot,
            bank_id: 0,
            kind: self.kind,
            close_nanos: self.close_nanos,
            has_notar: self.has_notar,
            notar_len: self.notar_len,
            notar: self.notar,
            has_skip: self.has_skip,
            skip_len: self.skip_len,
            skip: self.skip,
            has_fast: self.has_fast,
            fast: self.fast,
        }
    }
}

pub const OUTCOME_SUCCEEDED: u8 = 0;
pub const OUTCOME_FAILED: u8 = 1;
/// The status cache held no result.
pub const OUTCOME_UNKNOWN: u8 = 2;

/// One non-vote transaction of a full block. A block's share a `count`; a block with none sends
/// one event with `count` 0.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxOutcomeEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub index: u32,
    pub count: u32,
    /// Our own block, whose fees are read too.
    pub ours: bool,
    pub has_signature: bool,
    pub signature: [u8; 64],
    pub payer: [u8; 32],
    pub outcome: u8,
    pub priority_fee: u64,
    pub base_fee: u64,
}

/// The message versions of one of our full blocks' transactions.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageVersionsEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub legacy: u64,
    pub v0: u64,
    pub v1: u64,
}

/// One gossip peer, as a member of a set every few seconds.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipPeerEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    pub pubkey: [u8; 32],
    pub client: [u8; 24],
    pub version: [u8; 48],
    pub wallclock: u64,
    pub outset: u64,
    pub shred_version: u16,
    /// As `NodeInfoEvent::addrs`.
    pub addrs: [u8; ADDRS * ADDR_BYTES],
    /// Unix milliseconds this node last took something new from the peer.
    pub heard_ms: u64,
    pub has_full_snapshot: bool,
    pub full_snapshot: Slot,
    pub has_incremental_snapshot: bool,
    pub incremental_snapshot: Slot,
}

/// Bytes of config account data an info event holds: a validator info account's largest size.
pub const CONFIG_DATA_BYTES: usize = 576 + 1 + (32 + 1) * 2;

pub const INFO_FULL_SET: u8 = 0;
pub const INFO_CHANGE: u8 = 1;

/// One config program account, where validator names are kept: the whole set at turn-on and
/// every minute, and each account a frozen bank changed.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatorInfoEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// `INFO_FULL_SET` or `INFO_CHANGE`.
    pub reason: u8,
    pub generation: u64,
    pub index: u32,
    pub count: u32,
    /// The bank the account was read from.
    pub slot: Slot,
    pub account: [u8; 32],
    pub data_len: u16,
    pub data: [u8; CONFIG_DATA_BYTES],
}

/// Ranks a finalization event's bitmaps hold.
pub const FINALIZATION_RANKS: usize = 4096;

/// Which ranks' last votes a frozen bank held, and which were within reach of the newest: what
/// the finalization share reads of each bank.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizationEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub slot: Slot,
    /// The newest last vote among the ranked accounts.
    pub front: Slot,
    pub ranks: u16,
    pub seen: [u8; FINALIZATION_RANKS / 8],
    pub carried: [u8; FINALIZATION_RANKS / 8],
}

pub fn pack_str<const N: usize>(value: &str) -> [u8; N] {
    let mut packed = [0u8; N];
    let bytes = value.as_bytes();
    let len = bytes.len().min(N);
    packed[..len].copy_from_slice(&bytes[..len]);
    packed
}

pub fn unpack_str(packed: &[u8]) -> String {
    let end = packed
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(packed.len());
    String::from_utf8_lossy(&packed[..end]).into_owned()
}

/// Empty where the path does not fit.
pub fn pack_path(path: &Path) -> [u8; PATH_BYTES] {
    let bytes = path.as_os_str().as_encoded_bytes();
    if bytes.len() >= PATH_BYTES {
        return [0u8; PATH_BYTES];
    }
    let mut packed = [0u8; PATH_BYTES];
    packed[..bytes.len()].copy_from_slice(bytes);
    packed
}

pub fn unpack_path(packed: &[u8]) -> Option<std::path::PathBuf> {
    let text = unpack_str(packed);
    (!text.is_empty()).then(|| text.into())
}

pub fn pack_addr(addr: Option<SocketAddr>) -> [u8; ADDR_BYTES] {
    let mut packed = [0u8; ADDR_BYTES];
    if let Some(addr) = addr {
        let ip = match addr.ip() {
            std::net::IpAddr::V4(ip) => ip.to_ipv6_mapped(),
            std::net::IpAddr::V6(ip) => ip,
        };
        packed[..16].copy_from_slice(&ip.octets());
        packed[16..].copy_from_slice(&addr.port().to_be_bytes());
    }
    packed
}

/// A contact's addresses in `ADDR_*` order.
pub fn pack_contact(
    contact: &solana_gossip::contact_info::ContactInfo,
) -> [u8; ADDRS * ADDR_BYTES] {
    use solana_gossip::contact_info::Protocol;
    let mut addrs = [0u8; ADDRS * ADDR_BYTES];
    for (at, addr) in [
        (ADDR_GOSSIP, contact.gossip()),
        (ADDR_RPC, contact.rpc()),
        (ADDR_TVU_UDP, contact.tvu(Protocol::UDP)),
        (ADDR_TPU_QUIC, contact.tpu(Protocol::QUIC)),
        (ADDR_TPU_FORWARDS_QUIC, contact.tpu_forwards(Protocol::QUIC)),
        (ADDR_TPU_VOTE_UDP, contact.tpu_vote(Protocol::UDP)),
        (ADDR_TPU_VOTE_QUIC, contact.tpu_vote(Protocol::QUIC)),
        (ADDR_SERVE_REPAIR_UDP, contact.serve_repair(Protocol::UDP)),
        (ADDR_ALPENGLOW, contact.alpenglow()),
    ] {
        let start = at.saturating_mul(ADDR_BYTES);
        if let Some(slot) = addrs.get_mut(start..start.saturating_add(ADDR_BYTES)) {
            slot.copy_from_slice(&pack_addr(addr));
        }
    }
    addrs
}

pub fn unpack_addr(packed: &[u8]) -> Option<SocketAddr> {
    if packed.len() < ADDR_BYTES {
        return None;
    }
    let port = u16::from_be_bytes([packed[16], packed[17]]);
    if port == 0 {
        return None;
    }
    let mut octets = [0u8; 16];
    octets.copy_from_slice(&packed[..16]);
    let ip = std::net::Ipv6Addr::from(octets);
    let ip = ip
        .to_ipv4_mapped()
        .map_or(std::net::IpAddr::V6(ip), std::net::IpAddr::V4);
    Some(SocketAddr::new(ip, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addresses_and_strings_survive_packing() {
        for addr in [
            "1.2.3.4:8001".parse().unwrap(),
            "[2001:db8::1]:9000".parse().unwrap(),
        ] {
            assert_eq!(unpack_addr(&pack_addr(Some(addr))), Some(addr));
        }
        assert_eq!(unpack_addr(&pack_addr(None)), None);
        assert_eq!(unpack_str(&pack_str::<8>("4.5.0")), "4.5.0");
        assert_eq!(unpack_str(&pack_str::<4>("toolong")), "tool");
        let path = Path::new("/mnt/ledger");
        assert_eq!(unpack_path(&pack_path(path)).as_deref(), Some(path));
        assert_eq!(
            unpack_path(&pack_path(&Path::new("/").join("x".repeat(300)))),
            None
        );
    }
}
