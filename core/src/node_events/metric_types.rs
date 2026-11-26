//! Metrics datapoints as typed events: one stream per row of the events list, one variant per
//! datapoint, one field per field a reader uses. Values pass through as the datapoint reports them.

use {
    agave_event_system::{event, stream_name::StreamName},
    solana_metrics::datapoint::DataPoint,
    std::time::{Duration, UNIX_EPOCH},
};

/// Bytes a tag holds; a longer value is cut.
pub const TEXT_BYTES: usize = 48;
/// Bytes a string field holds, enough for a NIC's model name; a longer value is cut.
pub const STRING_BYTES: usize = 128;

/// A datapoint as an event of its row's stream.
pub trait MetricEvent: Sized {
    /// `None` for a datapoint this row does not carry.
    fn from_point(point: &DataPoint, observed_ms: u64) -> Option<Self>;
    /// The datapoint as the validator reported it, and when it observed it.
    fn to_point(&self) -> (DataPoint, u64);
    fn timestamp_ns(&self) -> u64;
}

fn text<const N: usize>(value: &str) -> [u8; N] {
    super::types::pack_str(value)
}

fn untext(packed: &[u8]) -> String {
    super::types::unpack_str(packed)
}

fn number(value: &str) -> Option<i64> {
    value.strip_suffix('i')?.parse().ok()
}

fn unix_nanos(point: &DataPoint) -> u64 {
    point
        .timestamp
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| u64::try_from(since.as_nanos()).ok())
        .unwrap_or_default()
}

/// Declares a row's stream and event. A variant carries one or more datapoints with the same
/// fields, and `which` says which. Fields are integers; `bools` are fields sent as `true` or
/// `false`, `strings` quoted text fields, and `tags` the point's tags.
macro_rules! metric_row {
    (
        $(#[$doc:meta])*
        $event:ident, $stream:ident = $name:literal;
        $(
            $variant:ident = [$($point:literal),+ $(,)?],
            tags [$($tag:ident = $tag_name:literal),* $(,)?],
            strings [$($string:ident = $string_name:literal),* $(,)?],
            bools [$($flag:ident = $flag_name:literal),* $(,)?],
            fields [$($field:ident = $field_name:literal),* $(,)?];
        )+
    ) => {
        pub const $stream: StreamName = agave_event_system::stream_name!($name);

        $(#[$doc])*
        #[event]
        #[derive(Debug, Clone, PartialEq, Eq)]
        // Queue cells take the largest variant's size whatever the Rust layout.
        #[allow(clippy::large_enum_variant)]
        pub enum $event {
            $(
                $variant {
                    pid: i32,
                    tid: i32,
                    timestamp_ns: u64,
                    /// Index of the datapoint's name in the variant's list.
                    which: u8,
                    /// The point's own time, unix nanoseconds.
                    point_ns: u64,
                    /// Unix milliseconds the validator observed it.
                    observed_ms: u64,
                    /// Bit `n` set where the `n`th text, flag or field was in the point, in
                    /// declaration order.
                    present: u64,
                    $($tag: [u8; TEXT_BYTES],)*
                    $($string: [u8; STRING_BYTES],)*
                    $($flag: bool,)*
                    $($field: i64,)*
                },
            )+
        }

        impl $event {
            /// The datapoints this row carries.
            pub const POINTS: &'static [&'static str] = &[$($($point),+),+];

            /// Each datapoint with every field the row reads set, for tests.
            #[cfg(test)]
            #[allow(unused_mut, unused_variables)]
            pub fn samples() -> Vec<DataPoint> {
                let mut points = Vec::new();
                $(
                    for name in [$($point),+] {
                        let mut point = DataPoint::new(name);
                        let mut value = 0i64;
                        $(point.add_tag($tag_name, "tag");)*
                        $(point.add_field_str($string_name, "text");)*
                        $(point.add_field_bool($flag_name, true);)*
                        $(
                            value = value.saturating_add(1);
                            point.add_field_i64($field_name, value);
                        )*
                        points.push(point);
                    }
                )+
                points
            }
        }

        impl MetricEvent for $event {
            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn from_point(point: &DataPoint, observed_ms: u64) -> Option<Self> {
                $(
                    if let Some(which) = [$($point),+]
                        .iter()
                        .position(|name| *name == point.name)
                        .and_then(|which| u8::try_from(which).ok())
                    {
                        let (tid, pid) =
                            agave_event_system::event_metadata::cached_thread_and_process_id();
                        let mut present = 0u64;
                        let mut bit = 0u32;
                        $(
                            let $tag = match point.tags.iter().find(|(name, _)| *name == $tag_name) {
                                Some((_, value)) => {
                                    present |= 1u64.checked_shl(bit).unwrap_or(0);
                                    text(value)
                                }
                                None => [0u8; TEXT_BYTES],
                            };
                            bit = bit.saturating_add(1);
                        )*
                        $(
                            let $string = match point.fields.iter().find(|(name, _)| *name == $string_name) {
                                Some((_, value)) => {
                                    present |= 1u64.checked_shl(bit).unwrap_or(0);
                                    let inner = value
                                        .strip_prefix('"')
                                        .and_then(|value| value.strip_suffix('"'))
                                        .unwrap_or(value);
                                    text(&inner.replace("\\\"", "\""))
                                }
                                None => [0u8; STRING_BYTES],
                            };
                            bit = bit.saturating_add(1);
                        )*
                        $(
                            let $flag = match point.fields.iter().find(|(name, _)| *name == $flag_name) {
                                Some((_, value)) => {
                                    present |= 1u64.checked_shl(bit).unwrap_or(0);
                                    value == "true"
                                }
                                None => false,
                            };
                            bit = bit.saturating_add(1);
                        )*
                        $(
                            let $field = match point
                                .fields
                                .iter()
                                .find(|(name, _)| *name == $field_name)
                                .and_then(|(_, value)| number(value))
                            {
                                Some(value) => {
                                    present |= 1u64.checked_shl(bit).unwrap_or(0);
                                    value
                                }
                                None => 0,
                            };
                            bit = bit.saturating_add(1);
                        )*
                        return Some(Self::$variant {
                            pid,
                            tid,
                            timestamp_ns:
                                agave_event_system::event_metadata::monotonic_timestamp_ns(),
                            which,
                            point_ns: unix_nanos(point),
                            observed_ms,
                            present,
                            $($tag,)*
                            $($string,)*
                            $($flag,)*
                            $($field,)*
                        });
                    }
                )+
                None
            }

            #[allow(unused_assignments, unused_mut, unused_variables)]
            fn to_point(&self) -> (DataPoint, u64) {
                match self {
                    $(
                        Self::$variant {
                            which,
                            point_ns,
                            observed_ms,
                            present,
                            $($tag,)*
                            $($string,)*
                            $($flag,)*
                            $($field,)*
                            ..
                        } => {
                            let name = [$($point),+]
                                .get(usize::from(*which))
                                .copied()
                                .unwrap_or_default();
                            let mut point = DataPoint::new(name);
                            point.timestamp = UNIX_EPOCH
                                .checked_add(Duration::from_nanos(*point_ns))
                                .unwrap_or(UNIX_EPOCH);
                            let has = |bit: u32| present & 1u64.checked_shl(bit).unwrap_or(0) != 0;
                            let mut bit = 0u32;
                            $(
                                if has(bit) {
                                    point.add_tag($tag_name, &untext($tag));
                                }
                                bit = bit.saturating_add(1);
                            )*
                            $(
                                if has(bit) {
                                    point.add_field_str($string_name, &untext($string));
                                }
                                bit = bit.saturating_add(1);
                            )*
                            $(
                                if has(bit) {
                                    point.add_field_bool($flag_name, *$flag);
                                }
                                bit = bit.saturating_add(1);
                            )*
                            $(
                                if has(bit) {
                                    point.add_field_i64($field_name, *$field);
                                }
                                bit = bit.saturating_add(1);
                            )*
                            (point, *observed_ms)
                        }
                    )+
                }
            }

            fn timestamp_ns(&self) -> u64 {
                match self {
                    $(Self::$variant { timestamp_ns, .. } => *timestamp_ns,)+
                }
            }
        }
    };
}

metric_row! {
    /// Fields of `replay-slot-stats`, `retransmit-stage-slot-stats` and `cost_tracker_stats`: the
    /// rows Slot replayed, Turbine layers and Block cost. Block full comes with `ledger.first_shred`.
    MetricSlot, METRIC_SLOT_STREAM = "metric.slot";
    Replayed = ["replay-slot-stats"],
        tags [],
        strings [],
        bools [],
        fields [slot = "slot", fetch_entries_time = "fetch_entries_time", confirmation_without_replay_us = "confirmation_without_replay_us", confirmation_time_us = "confirmation_time_us", bank_complete_time_us = "bank_complete_time_us", entry_poh_verification_time = "entry_poh_verification_time", entry_transaction_verification_time = "entry_transaction_verification_time", task_submission_us = "task_submission_us", replay_time = "replay_time", execute_us = "execute_us", execute_details_execute_inner_us = "execute_details_execute_inner_us", execute_details_serialize_us = "execute_details_serialize_us", execute_details_deserialize_us = "execute_details_deserialize_us", load_us = "load_us", store_us = "store_us", program_cache_us = "program_cache_us", total_transactions = "total_transactions", execute_details_create_executor_load_elf_us = "execute_details_create_executor_load_elf_us", execute_details_create_executor_verify_code_us = "execute_details_create_executor_verify_code_us", execute_details_create_executor_jit_compile_us = "execute_details_create_executor_jit_compile_us", validate_transactions_us = "validate_transactions_us", validate_fees_us = "validate_fees_us", filter_executable_us = "filter_executable_us", collect_balances_us = "collect_balances_us", collect_logs_us = "collect_logs_us", update_stakes_cache_us = "update_stakes_cache_us", update_transaction_statuses = "update_transaction_statuses", check_block_limits_us = "check_block_limits_us"];
    TurbineLayers = ["retransmit-stage-slot-stats"],
        tags [],
        strings [],
        bools [],
        fields [num_shreds_received_root = "num_shreds_received_root", num_shreds_received_1st_layer = "num_shreds_received_1st_layer", num_shreds_received_2nd_layer = "num_shreds_received_2nd_layer", num_shreds_received_3rd_layer = "num_shreds_received_3rd_layer"];
    Cost = ["cost_tracker_stats"],
        tags [is_leader = "is_leader"],
        strings [costliest_account = "costliest_account"],
        bools [],
        fields [bank_slot = "bank_slot", costliest_account_cost = "costliest_account_cost", block_cost = "block_cost", number_of_accounts = "number_of_accounts", number_of_contended_accounts = "number_of_contended_accounts", allocated_accounts_data_size = "allocated_accounts_data_size", inflight_transaction_count = "inflight_transaction_count"];
}

metric_row! {
    /// Fields of `slot-metrics`, the leader bank and broadcast timings, and the banking stage's
    /// scheduler and worker reports: the rows Leader slot timings, Scheduler counts, Worker timing
    /// and Vote worker timing.
    MetricLeader, METRIC_LEADER_STREAM = "metric.leader";
    Start = ["slot-metrics"],
        tags [],
        strings [],
        bools [],
        fields [slot = "slot", slot_delay_us = "slot_delay_us", replay_is_behind_us = "replay_is_behind_us", leader_handover_fast = "leader_handover_fast", leader_handover_sad = "leader_handover_sad"];
    BankOpen = ["leader-slot-start-to-cleared-elapsed-ms"],
        tags [],
        strings [],
        bools [],
        fields [slot = "slot", elapsed = "elapsed"];
    Transmit = ["broadcast-transmit-shreds-stats"],
        tags [],
        strings [],
        bools [],
        fields [slot = "slot", end_to_end_elapsed = "end_to_end_elapsed"];
    Shredding = ["broadcast-process-shreds-stats"],
        tags [],
        strings [],
        bools [],
        fields [slot = "slot", shredding_time = "shredding_time"];
    SchedulerTiming = ["banking_stage_scheduler_slot_timing"],
        tags [id = "id"],
        strings [],
        bools [],
        fields [slot = "slot", receive_time_us = "receive_time_us", schedule_time_us = "schedule_time_us"];
    SchedulerCounts = ["banking_stage_scheduler_slot_counts"],
        tags [id = "id"],
        strings [],
        bools [],
        fields [slot = "slot", num_received = "num_received", num_dropped_on_receive = "num_dropped_on_receive", num_dropped_on_check_work_queue_full = "num_dropped_on_check_work_queue_full", num_dropped_on_parsing_and_sanitization = "num_dropped_on_parsing_and_sanitization", num_dropped_on_validate_locks = "num_dropped_on_validate_locks", num_dropped_on_receive_compute_budget = "num_dropped_on_receive_compute_budget", num_dropped_on_receive_age = "num_dropped_on_receive_age", num_dropped_on_receive_already_processed = "num_dropped_on_receive_already_processed", num_dropped_on_receive_fee_payer = "num_dropped_on_receive_fee_payer", num_dropped_on_filter_key = "num_dropped_on_filter_key", num_dropped_on_nonce_dedup = "num_dropped_on_nonce_dedup", num_buffered = "num_buffered", num_dropped_on_capacity = "num_dropped_on_capacity", num_evicted_on_nonce_dedup = "num_evicted_on_nonce_dedup", num_dropped_on_clear = "num_dropped_on_clear", num_dropped_on_clean = "num_dropped_on_clean", num_scheduled = "num_scheduled", num_unschedulable_conflicts = "num_unschedulable_conflicts", num_unschedulable_threads = "num_unschedulable_threads", num_finished = "num_finished", num_retryable = "num_retryable"];
    WorkerTiming = ["banking_stage_worker_timing"],
        tags [id = "id"],
        strings [],
        bools [],
        fields [load_execute_us_max = "load_execute_us_max", cost_model_us = "cost_model_us", load_execute_us = "load_execute_us", freeze_lock_us = "freeze_lock_us", record_us = "record_us", commit_us = "commit_us", find_and_send_votes_us = "find_and_send_votes_us"];
    VoteTiming = ["banking_stage-leader_slot_vote_execute_and_commit_timings"],
        tags [],
        strings [],
        bools [],
        fields [slot = "slot", cost_model_us = "cost_model_us", load_execute_us = "load_execute_us", freeze_lock_us = "freeze_lock_us", record_us = "record_us", commit_us = "commit_us", find_and_send_votes_us = "find_and_send_votes_us"];
}

metric_row! {
    /// Fields of `event_handler_slot_tracking` and `consensus_vote_metrics`: the rows Vote timing and
    /// Vote latency.
    MetricVotor, METRIC_VOTOR_STREAM = "metric.votor";
    Track = ["event_handler_slot_tracking"],
        tags [],
        strings [],
        bools [is_fast_finalization = "is_fast_finalization"],
        fields [slot = "slot", first_shred = "first_shred", parent_ready = "parent_ready", vote_notarize = "vote_notarize", vote_skip = "vote_skip", finalized = "finalized"];
    Arrival = ["consensus_vote_metrics"],
        tags [address = "address"],
        strings [],
        bools [],
        fields [epoch = "epoch", notar_vote_us_mean = "notar_vote_us_mean", notar_vote_us_stddev = "notar_vote_us_stddev", notar_vote_us_maximum = "notar_vote_us_maximum", notar_vote_count = "notar_vote_count", finalize_vote_us_mean = "finalize_vote_us_mean"];
}

metric_row! {
    /// The per-second QUIC, verifier and banking worker reports: the row TPU ingest.
    MetricTpu, METRIC_TPU_STREAM = "metric.tpu";
    Quic = ["quic_streamer_tpu", "quic_streamer_tpu_forwards", "quic_streamer_tpu_vote"],
        tags [],
        strings [],
        bools [],
        fields [total_incoming_connection_attempts = "total_incoming_connection_attempts", open_connections = "open_connections", active_streams = "active_streams", connection_rate_limited_across_all = "connection_rate_limited_across_all", connection_rate_limited_per_ipaddr = "connection_rate_limited_per_ipaddr", refused_connections_too_many_open_connections = "refused_connections_too_many_open_connections", connection_setup_timeout = "connection_setup_timeout", connection_setup_error = "connection_setup_error", new_connections = "new_connections", connection_add_failed = "connection_add_failed", connection_add_failed_staked_node = "connection_add_failed_staked_node", connection_add_failed_unstaked_node = "connection_add_failed_unstaked_node", connection_add_failed_banned = "connection_add_failed_banned", connection_added_from_staked_peer = "connection_added_from_staked_peer", connection_added_from_unstaked_peer = "connection_added_from_unstaked_peer", new_streams = "new_streams", throttled_staked_streams = "throttled_staked_streams", throttled_unstaked_streams = "throttled_unstaked_streams", stream_read_timeouts = "stream_read_timeouts", stream_read_errors = "stream_read_errors", invalid_stream_size = "invalid_stream_size", packets_sent_to_consumer = "packets_sent_to_consumer", total_handle_chunk_to_packet_send_full_err = "total_handle_chunk_to_packet_send_full_err", total_handle_chunk_to_packet_send_disconnected_err = "total_handle_chunk_to_packet_send_disconnected_err"];
    Verifier = ["tpu-verifier"],
        tags [],
        strings [],
        bools [],
        fields [total_packets = "total_packets", total_dedup = "total_dedup", total_dropped_below_priority_floor = "total_dropped_below_priority_floor", total_valid_packets = "total_valid_packets", eviction_drops = "eviction_drops"];
    Workers = ["banking_stage_worker_counts", "banking_stage_worker_error_metrics"],
        tags [id = "id"],
        strings [],
        bools [],
        fields [transactions_attempted_processing_count = "transactions_attempted_processing_count", cost_model_throttled_transactions_count = "cost_model_throttled_transactions_count", retryable_transaction_count = "retryable_transaction_count", retryable_expired_bank_count = "retryable_expired_bank_count", processed_transactions_count = "processed_transactions_count", processed_with_successful_result_count = "processed_with_successful_result_count", too_many_account_locks = "too_many_account_locks", account_not_found = "account_not_found", insufficient_funds = "insufficient_funds", invalid_account_for_fee = "invalid_account_for_fee", blockhash_not_found = "blockhash_not_found", blockhash_too_old = "blockhash_too_old", already_processed = "already_processed", invalid_compute_budget = "invalid_compute_budget", max_loaded_accounts_data_size_exceeded = "max_loaded_accounts_data_size_exceeded", invalid_program_for_execution = "invalid_program_for_execution", program_execution_temporarily_restricted = "program_execution_temporarily_restricted"];
}

metric_row! {
    /// The receivers' and senders' reports: the row Receivers and senders.
    MetricNet, METRIC_NET_STREAM = "metric.net";
    Received = ["shred_fetch_receiver", "shred_fetch_repair_receiver", "gossip_receiver", "tpu_vote_receiver"],
        tags [],
        strings [],
        bools [],
        fields [packets_count = "packets_count"];
    Votor = ["votor_datagram_server"],
        tags [],
        strings [],
        bools [],
        fields [datagrams_received = "datagrams_received"];
    Sent = ["Gossip", "Repair"],
        tags [],
        strings [],
        bools [],
        fields [streamer_send_bytes_total = "streamer-send-bytes_total", streamer_send_sample_duration_ms = "streamer-send-sample_duration_ms"];
    Retransmit = ["retransmit-stage"],
        tags [is_xdp = "is_xdp"],
        strings [],
        bools [],
        fields [num_shreds_dropped_xdp_full = "num_shreds_dropped_xdp_full"];
}

metric_row! {
    /// The accounts database, index, caches and blockstore column families: the row Accounts and
    /// caches, with the three ten-second size points the row leaves out.
    MetricAccounts, METRIC_ACCOUNTS_STREAM = "metric.accounts";
    StoreTimings = ["accounts_db_store_timings"],
        tags [],
        strings [],
        bools [],
        fields [total_bytes = "total_bytes", total_alive_bytes = "total_alive_bytes", total_count = "total_count", read_only_accounts_cache_data_size = "read_only_accounts_cache_data_size", read_only_accounts_cache_entries = "read_only_accounts_cache_entries", num_loaded_from_write_cache = "num_loaded_from_write_cache", num_loaded_from_read_cache = "num_loaded_from_read_cache", num_loaded_from_index_storage = "num_loaded_from_index_storage", num_accounts_stored = "num_accounts_stored", account_bytes_stored = "account_bytes_stored", read_only_accounts_cache_hits = "read_only_accounts_cache_hits", read_only_accounts_cache_misses = "read_only_accounts_cache_misses", read_only_accounts_cache_evicts = "read_only_accounts_cache_evicts"];
    Accounts = ["accounts_db_load_accounts", "accounts_db-stores", "accounts_db-flush_accounts_cache"],
        tags [],
        strings [],
        bools [],
        fields [total_bytes = "total_bytes", total_alive_bytes = "total_alive_bytes", total_count = "total_count", read_only_accounts_cache_data_size = "read_only_accounts_cache_data_size", read_only_accounts_cache_entries = "read_only_accounts_cache_entries", num_loaded_from_write_cache = "num_loaded_from_write_cache", num_loaded_from_read_cache = "num_loaded_from_read_cache", num_loaded_from_index_storage = "num_loaded_from_index_storage", num_accounts_stored = "num_accounts_stored", account_bytes_stored = "account_bytes_stored"];
    Index = ["accounts_index"],
        tags [],
        strings [],
        bools [],
        fields [estimate_mem_bytes = "estimate_mem_bytes"];
    WriteCache = ["accounts_cache_size"],
        tags [],
        strings [],
        bools [],
        fields [total_size = "total_size"];
    ColumnFamily = ["blockstore_rocksdb_cfs"],
        tags [cf_name = "cf_name"],
        strings [],
        bools [],
        fields [size_all_mem_tables = "size_all_mem_tables", block_cache_usage = "block_cache_usage", estimate_table_readers_mem = "estimate_table_readers_mem", total_sst_files_size = "total_sst_files_size"];
    ProgramCache = ["loaded-programs-cache-stats"],
        tags [],
        strings [],
        bools [],
        fields [water_level = "water_level", hits = "hits", misses = "misses", evictions = "evictions", reloads = "reloads", insertions = "insertions", lost_insertions = "lost_insertions", replace_entry = "replace_entry", one_hit_wonders = "one_hit_wonders", prunes_orphan = "prunes_orphan", prunes_environment = "prunes_environment"];
}

metric_row! {
    /// Gossip's two-second reports and its table entries by type: the row Gossip.
    MetricGossip, METRIC_GOSSIP_STREAM = "metric.gossip";
    Stats = ["cluster_info_stats", "cluster_info_stats2", "cluster_info_stats3", "cluster_info_stats4", "cluster_info_stats5"],
        tags [],
        strings [],
        bools [],
        fields [table_size = "table_size", num_pubkeys = "num_pubkeys", num_nodes = "num_nodes", num_nodes_staked = "num_nodes_staked", packets_received_count = "packets_received_count", packets_received_push_messages_count = "packets_received_push_messages_count", packets_received_pull_requests_count = "packets_received_pull_requests_count", packets_received_pull_responses_count = "packets_received_pull_responses_count", packets_received_ping_messages_count = "packets_received_ping_messages_count", packets_received_pong_messages_count = "packets_received_pong_messages_count", packets_received_prune_messages_count = "packets_received_prune_messages_count", packets_sent_push_messages_count = "packets_sent_push_messages_count", packets_sent_pull_requests_count = "packets_sent_pull_requests_count", packets_sent_pull_responses_count = "packets_sent_pull_responses_count", packets_sent_ping_messages_count = "packets_sent_ping_messages_count", packets_sent_pong_messages_count = "packets_sent_pong_messages_count", packets_sent_prune_messages_count = "packets_sent_prune_messages_count", num_duplicate_push_messages = "num_duplicate_push_messages", num_redundant_pull_responses = "num_redundant_pull_responses", purge_count = "purge_count", trim_crds_table_purged_values_count = "trim_crds_table_purged_values_count", gossip_packets_dropped_count = "gossip_packets_dropped_count", gossip_transmit_packets_dropped_count = "gossip_transmit_packets_dropped_count", gossip_pull_request_no_budget = "gossip_pull_request_no_budget", pull_request_scan_budget_exhausted = "pull_request_scan_budget_exhausted", skip_push_message_shred_version = "skip_push_message_shred_version", skip_pull_response_shred_version = "skip_pull_response_shred_version", skip_pull_shred_version = "skip_pull_shred_version", pull_request_ping_pong_check_failed_count = "pull_request_ping_pong_check_failed_count", num_unverifed_gossip_addrs = "num_unverifed_gossip_addrs", bad_prune_destination = "bad_prune_destination", handle_batch_push_messages_time = "handle_batch_push_messages_time", handle_batch_pull_requests_time = "handle_batch_pull_requests_time", handle_batch_pull_responses_time = "handle_batch_pull_responses_time", handle_batch_ping_messages_time = "handle_batch_ping_messages_time", handle_batch_pong_messages_time = "handle_batch_pong_messages_time", handle_batch_prune_messages_time = "handle_batch_prune_messages_time", verify_gossip_packets_time = "verify_gossip_packets_time", process_gossip_packets_time = "process_gossip_packets_time"];
    Entries = ["cluster_info_crds_stats", "cluster_info_crds_stats_fails"],
        tags [],
        strings [],
        bools [],
        fields [legacy_contact_info_push = "LegacyContactInfo-push", legacy_contact_info_pull = "LegacyContactInfo-pull", vote_push = "Vote-push", vote_pull = "Vote-pull", lowest_slot_push = "LowestSlot-push", lowest_slot_pull = "LowestSlot-pull", legacy_snapshot_hashes_push = "LegacySnapshotHashes-push", legacy_snapshot_hashes_pull = "LegacySnapshotHashes-pull", accounts_hashes_push = "AccountsHashes-push", accounts_hashes_pull = "AccountsHashes-pull", epoch_slots_push = "EpochSlots-push", epoch_slots_pull = "EpochSlots-pull", legacy_version_push = "LegacyVersion-push", legacy_version_pull = "LegacyVersion-pull", version_push = "Version-push", version_pull = "Version-pull", node_instance_push = "NodeInstance-push", node_instance_pull = "NodeInstance-pull", duplicate_shred_push = "DuplicateShred-push", duplicate_shred_pull = "DuplicateShred-pull", snapshot_hashes_push = "SnapshotHashes-push", snapshot_hashes_pull = "SnapshotHashes-pull", contact_info_push = "ContactInfo-push", contact_info_pull = "ContactInfo-pull", restart_last_voted_fork_slots_push = "RestartLastVotedForkSlots-push", restart_last_voted_fork_slots_pull = "RestartLastVotedForkSlots-pull", restart_heaviest_fork_push = "RestartHeaviestFork-push", restart_heaviest_fork_pull = "RestartHeaviestFork-pull", all_push = "all-push", all_pull = "all-pull"];
}

metric_row! {
    /// Fields of `xdp-network-config` and `wfsm_gossip`, sent once: the rows XDP setup and
    /// Supermajority wait.
    MetricOnce, METRIC_ONCE_STREAM = "metric.once";
    Xdp = ["xdp-network-config"],
        tags [driver = "driver", zero_copy = "zero_copy"],
        strings [vendor = "vendor", model = "model", kernel_version = "kernel_version"],
        bools [],
        fields [];
    StakeInGossip = ["wfsm_gossip"],
        tags [],
        strings [],
        bools [],
        fields [online_stake = "online_stake", total_activated_stake = "total_activated_stake"];
}

/// Every datapoint a metric stream carries.
static CARRIED: std::sync::LazyLock<std::collections::HashSet<&'static str>> =
    std::sync::LazyLock::new(|| {
        [
            MetricSlot::POINTS,
            MetricLeader::POINTS,
            MetricVotor::POINTS,
            MetricTpu::POINTS,
            MetricNet::POINTS,
            MetricAccounts::POINTS,
            MetricGossip::POINTS,
            MetricOnce::POINTS,
        ]
        .into_iter()
        .flatten()
        .copied()
        .collect()
    });

/// Whether a metric stream carries the datapoint.
pub fn carried(name: &str) -> bool {
    CARRIED.contains(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_point_comes_back_as_it_went() {
        let mut point = DataPoint::new("xdp-network-config");
        point
            .add_tag("driver", "mlx5_core")
            .add_tag("zero_copy", "true")
            .add_field_str("vendor", "Mellanox \"Tech\"")
            .add_field_str(
                "model",
                "BCM57414 NetXtreme-E 10Gb/25Gb RDMA Ethernet Controller",
            );
        let event = MetricOnce::from_point(&point, 42).unwrap();
        let (back, observed) = event.to_point();
        assert_eq!(observed, 42);
        assert_eq!(back.name, point.name);
        assert_eq!(back.tags, point.tags);
        assert_eq!(back.fields, point.fields);
        assert_eq!(
            back.timestamp,
            UNIX_EPOCH + Duration::from_nanos(unix_nanos(&point))
        );
        assert!(MetricOnce::from_point(&DataPoint::new("other"), 0).is_none());
    }

    fn round_trip<E: MetricEvent>(samples: Vec<DataPoint>) {
        for point in samples {
            let event = E::from_point(&point, 7).unwrap();
            let (back, _) = event.to_point();
            assert_eq!(
                (back.name, &back.tags, &back.fields),
                (point.name, &point.tags, &point.fields)
            );
        }
    }

    #[test]
    fn test_every_row_hands_back_every_field() {
        round_trip::<MetricSlot>(MetricSlot::samples());
        round_trip::<MetricLeader>(MetricLeader::samples());
        round_trip::<MetricVotor>(MetricVotor::samples());
        round_trip::<MetricTpu>(MetricTpu::samples());
        round_trip::<MetricNet>(MetricNet::samples());
        round_trip::<MetricAccounts>(MetricAccounts::samples());
        round_trip::<MetricGossip>(MetricGossip::samples());
        round_trip::<MetricOnce>(MetricOnce::samples());
    }
}
