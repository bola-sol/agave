/** Mirrors the payloads published by `dashboard/src/collect.rs`. */

export type SlotLevel =
  | "incomplete"
  | "completed"
  | "optimistically_confirmed"
  | "rooted"
  | "finalized"
  | "skipped";

export interface SlotEntry {
  slot: number;
  level: SlotLevel;
  leader: string | null;
  leader_name: string | null;
  leader_icon: string | null;
  mine: boolean;
  /** What replay found in the block. Null for a slot with no block. */
  block: BlockDetail | null;
  duration_nanos: number | null;
}

/**
 * Where this validator's shreds came from over the last five minutes.
 *
 * Turbine should deliver nearly all of them; repair is the fallback for what
 * never arrived. Null while none have arrived at all.
 */
export interface Shreds {
  received: number;
  repaired: number;
  repair_rate: number;
}

/**
 * How often an account replay needed was already in memory, over the last
 * minute.
 *
 * Lifted from the measurements the accounts database submits about itself,
 * which carry a second's work each. Null while nothing has been read.
 */
export interface AccountsCache {
  read: number;
  hit_rate: number;
  evictions: number;
}

/**
 * How often replay found a program already compiled, over the last minute.
 *
 * The counters behind this are reset for each bank, so `looked_up` is what was
 * seen in the window rather than since startup. Null while nothing has been
 * looked up at all.
 */
export interface ProgramCache {
  looked_up: number;
  hit_rate: number;
  evictions: number;
}

/**
 * What is known about a leader beyond the name its slot rows carry.
 *
 * Published only for the leaders on screen, so a leader may be missing from
 * this table briefly after a reconnection, before the next slow tick.
 */
export interface Peer {
  identity: string;
  version: string | null;
  stake: number;
  ip: string | null;
}

/**
 * A slot the leader schedule has assigned that has not happened yet.
 *
 * Published on the slow tier, so the front of the list has usually happened by
 * the time it is read. Filter against the completed slot before rendering.
 */
export interface UpcomingSlot {
  slot: number;
  leader: string;
  leader_name: string | null;
  leader_icon: string | null;
  mine: boolean;
}

/** What one block contained, as the collector read it off the frozen bank. */
export interface BlockDetail {
  transactions: number;
  non_vote_transactions: number;
  failed_transactions: number;
  entries: number;
  block_cost: number;
  block_cost_limit: number;
  total_fees: number;
  priority_fees: number;
}

export interface Tps {
  total: number;
  vote: number;
  non_vote_success: number;
  non_vote_failed: number;
}

export interface TpsSample extends Tps {
  slot: number;
  timestamp_nanos: number;
}

export interface StakeSummary {
  activated_stake: number;
  total_stake: number;
  /** This validator's share of total stake, in [0, 1]. */
  share: number;
}

export interface ValidatorCounts {
  total: number;
  delinquent: number;
  rpc_nodes: number;
  non_delinquent_stake: number;
  delinquent_stake: number;
}

export interface VersionShare {
  /** Null for peers reporting no version, and for the folded tail. */
  version: string | null;
  validators: number;
  stake: number;
  /** True only for the row the tail was folded into. */
  other: boolean;
}

export interface EpochInfo {
  epoch: number;
  start_slot: number;
  end_slot: number;
  slots_in_epoch: number;
  my_leader_slots: number[];
}

export interface Network {
  received_per_second: number;
  sent_per_second: number;
}

export interface NetworkSample extends Network {
  timestamp_nanos: number;
}

export interface IngestPath {
  name: string;
  port: number;
  drops_recent: number;
  drops_total: number;
  queued_bytes: number;
  /**
   * Packets the port delivered, over the same window and from the same instant
   * as the drops beside them, so that one can be divided by their sum.
   *
   * Null for a port whose traffic nothing counts in datagrams: the two QUIC
   * ports, whose counters count transactions pulled out of streams, and serve
   * repair, whose receiver keeps counters that nothing reports.
   */
  received_recent: number | null;
  received_total: number | null;
}

export interface ProducedBlock {
  slot: number;
  slot_time_millis: number | null;
  blockhash: string;
  duration_nanos: number | null;
  transactions: number;
  non_vote_transactions: number;
  failed_transactions: number;
  entries: number;
  block_cost: number;
  block_cost_limit: number;
  total_fees: number;
  priority_fees: number;
}

export interface IngestSummary {
  window_seconds: number;
  paths: IngestPath[];
}

export interface StartupProgress {
  phase: string;
  detail: string | null;
  running: boolean;
  /** Ledger replay progress from 0 to 1, on the phases that can measure it. */
  fraction: number | null;
}

export interface Health {
  replay: string;
  vote: string;
}

export interface SkipRate {
  epoch: number;
  rate: number | null;
}

/** The envelope every message arrives in. */
export interface Envelope {
  topic: string;
  key: string;
  id?: number;
  value: unknown;
}
