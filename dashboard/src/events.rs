//! Streams the dashboard publishes from inside the validator, so a dashboard in a process of its
//! own can rebuild what this one reads. Test branch only. Timestamps are monotonic nanoseconds.

use {
    agave_event_system::{event, stream_name::StreamName},
    solana_clock::{Epoch, Slot},
    std::{net::SocketAddr, path::Path},
};

pub const NODE_INFO_STREAM: StreamName = agave_event_system::stream_name!("node.info");
pub const START_PROGRESS_STREAM: StreamName =
    agave_event_system::stream_name!("node.start_progress");
pub const SLOT_STATUS_STREAM: StreamName = agave_event_system::stream_name!("bank.slot_status");
pub const EPOCH_STREAM: StreamName = agave_event_system::stream_name!("bank.epoch");
pub const SLOT_CLOCK_STREAM: StreamName = agave_event_system::stream_name!("bank.slot_clock");

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

/// The node: keys, build, contact, and the paths a dashboard reads disks and snapshots from.
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
}

/// `ValidatorStartProgress`, with its fields where it has them.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartProgressEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// The variant in declaration order.
    pub phase: u8,
    pub slot: Slot,
    pub max_slot: Slot,
    pub gossip_stake_percent: u64,
    pub rpc_addr: [u8; ADDR_BYTES],
}

/// Where the node stands: commitment, the highest slot, the tip and the ledger's floor.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotStatusEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    pub root: Slot,
    pub confirmed: Slot,
    pub finalized: Slot,
    /// Bank forks' highest slot.
    pub highest_slot: Slot,
    pub has_cluster_tip: bool,
    pub cluster_tip: Slot,
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
}

pub const ROLE_ROOT: u8 = 0;
pub const ROLE_WORKING: u8 = 1;

/// The root or the working bank's own figures, sent as either moves.
#[event]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotClockEvent {
    pub pid: i32,
    pub tid: i32,
    pub timestamp_ns: u64,
    /// `ROLE_ROOT` or `ROLE_WORKING`.
    pub role: u8,
    pub slot: Slot,
    pub parent_slot: Slot,
    pub epoch: Epoch,
    pub frozen: bool,
    pub block_height: u64,
    pub clock_slot: Slot,
    pub clock_epoch_start_timestamp: i64,
    pub clock_epoch: Epoch,
    pub clock_leader_schedule_epoch: Epoch,
    pub clock_unix_timestamp: i64,
    pub blockhash: [u8; 32],
    pub transaction_count: u64,
    pub non_vote_transaction_count: u64,
    pub transaction_error_count: u64,
    pub entries_count: u64,
    pub block_cost: u64,
    pub block_cost_limit: u64,
    pub account_cost_limit: u64,
    pub total_fees: u64,
    pub priority_fees: u64,
    pub identity_balance: u64,
    pub vote_balance: u64,
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
