# What the dashboard reads

The dashboard runs inside the validator and draws everything from what the validator already has. This is the list of those inputs: what each one is, how the dashboard gets at it, and which part of the page uses it. It is written for anyone deciding what a validator should expose to tools like this one.

There are six kinds of input. The first is the most fragile and the last is the least.

| Kind | How it is read | Already event shaped | What can break it |
| --- | --- | --- | --- |
| Metrics datapoints | An observer sees every datapoint before it is sent and keeps the ones named below | Nearly | A point or field renamed; points that only exist above a certain log level |
| Frozen banks | A channel replay writes to, named in the validator config | Yes | Nothing |
| Gossip and bank forks before the wait | One message on a channel named in the config, sent before the supermajority wait | Yes | Nothing |
| Bank and bank forks reads | Polled every 200 ms and every second | No | Lock contention; anything a pruned bank takes with it |
| Gossip, blockstore, caches | Direct calls on handles the run command passes | No | API changes between releases |
| The host | `/proc`, `statvfs`, and the validator's own threads | Not the validator's to emit | Linux only |

## Metrics datapoints

The validator reports on itself through `solana_metrics` datapoints. The dashboard installs an observer at start, matches on point name and then on field name, and keeps what it needs. Each row below is one point, with the fields read from it and the part of the page they feed. Anything else on the point is ignored. If the validator stops emitting a point, the figures that read it go quiet without an error.

### Per slot

These carry a slot and describe one block.

| Point | Fields read | Feeds |
| --- | --- | --- |
| `replay-slot-stats` | `slot`, `fetch_entries_time`, `confirmation_without_replay_us`, `bank_complete_time_us`, `entry_poh_verification_time`, `entry_transaction_verification_time`, `task_submission_us`, `execute_us`, `execute_details_execute_inner_us`, `execute_details_serialize_us`, `execute_details_deserialize_us`, `execute_details_create_vm_us`, `execute_details_create_executor_jit_compile_us`, `load_us`, `store_us`, `program_cache_us`, `validate_transactions_us`, `check_block_limits_us`, `total_transactions` | The replay card: time per slot, its three spans, and the verify and execute breakdowns. The schedule page's replay column and its received to replayed timeline. |
| `shred_insert_is_full` | `slot`, `last_index`, `num_repaired`, `total_time_ms` | Shreds and repaired shreds per slot; the first shred to block full span on the timeline |
| `cost_tracker_stats` | `bank_slot`, `block_cost`, `costliest_account`, `costliest_account_cost`, `number_of_accounts`, `number_of_contended_accounts`, `allocated_accounts_data_size`, `inflight_transaction_count` | Slot details: compute used and the costliest account against its own limit |
| `banking_stage_scheduler_slot_counts` | `slot` and the scheduler counters listed under the per second section | Slot details: the waterfall for one of our own leader slots |
| `bundle_stage-stats` (jito only) | `slot`, `num_sanitized_ok`, `execution_results_ok` | Bundles sanitised and landed per produced block |

### Per second

These are running counters. The dashboard differences them once a second.

| Point | Fields read | Feeds |
| --- | --- | --- |
| `shred_fetch_receiver`, `shred_fetch_repair_receiver` | `packets_count` | Repaired shreds on the status card; the turbine row of the socket card |
| `gossip_receiver`, `tpu_vote_receiver` | `packets_count` | Packets delivered on the gossip and UDP vote rows of the socket card |
| `Gossip`, `Repair` (the streamer senders) | `streamer-send-bytes_total`, `streamer-send-sample_duration_ms` | Network card: what the gossip and repair senders put on the wire |
| `banking_stage_scheduler_counts` | tag `id`; `num_received`, `num_dropped_on_receive`, `num_dropped_on_check_work_queue_full`, `num_dropped_on_parsing_and_sanitization`, `num_dropped_on_validate_locks`, `num_dropped_on_receive_compute_budget`, `num_dropped_on_receive_age`, `num_dropped_on_receive_already_processed`, `num_dropped_on_receive_fee_payer`, `num_dropped_on_filter_key`, `num_dropped_on_nonce_dedup`, `num_buffered`, `num_dropped_on_capacity`, `num_evicted_on_nonce_dedup`, `num_dropped_on_clear`, `num_dropped_on_clean`, `num_scheduled`, `num_unschedulable_conflicts`, `num_unschedulable_threads`, `num_finished`, `num_retryable` | The scheduler section of the TPU path card. The `id` tag tells the validator's own scheduler from BAM's, which count `num_received` in different units. |
| `banking_stage_worker_counts`, `banking_stage_worker_error_metrics` | `transactions_attempted_processing_count`, `cost_model_throttled_transactions_count`, `retryable_transaction_count`, `retryable_expired_bank_count`, `processed_transactions_count`, `processed_with_successful_result_count`, `too_many_account_locks`, `account_not_found`, `insufficient_funds`, `invalid_account_for_fee`, `blockhash_not_found`, `blockhash_too_old`, `already_processed`, `invalid_compute_budget`, `max_loaded_accounts_data_size_exceeded`, `invalid_program_for_execution`, `program_execution_temporarily_restricted` | The executed section of the TPU path card, summed over the epoch |
| `tpu-verifier` | `total_packets`, `total_dedup`, `total_dropped_below_priority_floor`, `total_valid_packets`, `eviction_drops` | The verify section of the TPU path card, summed over the epoch |
| `quic_streamer_tpu`, `quic_streamer_tpu_forwards`, `quic_streamer_tpu_vote` | `connection_rate_limited_across_all`, `connection_rate_limited_per_ipaddr`, `refused_connections_too_many_open_connections`, `connection_setup_timeout`, `connection_setup_error`, `new_connections`, `connection_add_failed`, `connection_add_failed_staked_node`, `connection_add_failed_unstaked_node`, `connection_add_failed_banned`, `connection_added_from_staked_peer`, `connection_added_from_unstaked_peer`, `new_streams`, `throttled_staked_streams`, `throttled_unstaked_streams`, `stream_read_timeouts`, `stream_read_errors`, `invalid_stream_size`, `packets_sent_to_consumer`, `bytes_sent_to_consumer`, `total_handle_chunk_to_packet_send_full_err`, `total_handle_chunk_to_packet_send_disconnected_err` | One section per QUIC port on the TPU path card: connections, then streams, then packets |
| `bundle_stage-loop_stats` (jito only) | `num_bundles_received`, `num_packets_received` | The bundles line under the executed section |
| `accounts_db_store_timings` | `total_bytes`, `total_alive_bytes`, `total_count`, `read_only_accounts_cache_data_size`, `read_only_accounts_cache_entries`, `read_only_accounts_cache_hits`, `read_only_accounts_cache_misses`, `read_only_accounts_cache_evicts` | Caches card: storage size, the live share, file count, and the read cache's size and hit rate |
| `accounts_db_load_accounts`, `accounts_db-stores`, `accounts_db-flush_accounts_cache` | `num_loaded_from_write_cache`, `num_loaded_from_read_cache`, `num_loaded_from_index_storage`, `num_accounts_stored`, `account_bytes_stored` | Caches card: where reads were answered from and what was flushed |
| `loaded-programs-cache-stats` | `hits`, `misses`, `evictions`, `reloads`, `insertions`, `lost_insertions`, `replace_entry`, `one_hit_wonders`, `prunes_orphan`, `prunes_environment`, `empty_entries` | The program cache card |

### Read once, or on change

| Point | Fields read | Feeds |
| --- | --- | --- |
| `xdp-network-config` | tags `driver`, `zero_copy`; `vendor`, `model`, `kernel_version` | Network card: how the XDP transmit path is set up |
| `wfsm_gossip` | `online_stake`, `offline_stake`, `total_activated_stake` | The exact stake figure during the supermajority wait. The validator's own progress value truncates to a whole percent. |

## Frozen banks

Replay sends a notification for every bank it freezes. The dashboard adds a sender to `ValidatorConfig::extra_bank_notification_senders`, and a small relay in the validator copies each notification to it and to the RPC tracker. Jito's tree already has a fan out of its own for this. The collector drains the channel every 200 ms and reads each bank once, at freeze, before it can be pruned.

| Read off the bank | Feeds |
| --- | --- |
| `slot`, `parent_slot` | Keys everything per slot. Counts are differenced against the parent's running totals. |
| `transaction_count`, `non_vote_transaction_count_since_restart` | Transactions and votes per block |
| `transaction_error_count`, `transaction_entries_count` | Failed transactions and entries per block |
| The cost tracker: block cost, block limit, account limit | Compute per block and the limits it is drawn against |
| Collector fee details: total and priority fees | Base and priority fees per block |
| The balance of the eight tip accounts | Tips per block, differenced against the parent (jito only) |
| `last_blockhash` | Our own produced blocks |

An event carrying these fields, sent when a block completes, would mean the dashboard never holds a bank.

## Gossip and bank forks before the wait

The wait for a supermajority happens inside the validator constructor, before anything outside it has handles. So the constructor sends gossip and bank forks down `ValidatorConfig::gossip_ready_sender` just before the wait starts. The dashboard's boot thread then walks the snapshot bank's staked validators against gossip every quarter second, the same way the validator's own check does, and publishes each one with its stake, its version, and whether gossip has a fresh contact for it. The same handles let the header show this validator's name during the wait.

The validator's own walk logs its result per node and emits only totals. If it emitted the per node verdicts, the dashboard would not need its copy of the walk.

## Bank and bank forks reads

Polled from the collector thread every 200 ms and from the meters thread every second. The bank forks read lock is held only long enough to clone handles out.

| Call | Feeds |
| --- | --- |
| `BankForks::root_bank`, `working_bank`, `highest_slot`, `frozen_banks` | The slot readouts; per slot detail where no notification channel is wired; failed transaction totals for TPS |
| `BankForks::migration_status`, `Bank::is_alpenglow` | Which consensus the cluster runs, and which of two cluster tip sources to read |
| `Bank::vote_accounts` | Our stake and commission; the validators card's counts and delinquency; the peer table's stake; the wait's validator list |
| `Bank::epoch_schedule`, `epoch`, `slot`, `block_height`, `clock`, `ns_per_slot_at_slot` | The epoch card, block height, and the nominal slot time the bars are drawn against |
| `Bank::get_rank_map` | This node's rank in the BLS rank map, to find its bit in a certificate |
| `Bank::get_program_accounts_modified_since_parent`, `get_filtered_indexed_accounts`, `account_indexes_include_key` | Validator names and icons from the config program's accounts: once before the wait, once at attach, then as they change |
| `Bank::transaction_count`, `non_vote_transaction_count_since_restart` on the working bank | TPS, differenced once a second |

## Gossip, blockstore, caches

| Call | Feeds |
| --- | --- |
| `ClusterInfo::all_peers`, `my_shred_version`, `my_contact_info`, `id` | Cluster versions, the RPC node count, the peer table's versions and addresses, our identity and shred version |
| `ClusterInfo::tvu_peers`, with each contact's wallclock | Who counts as seen during the supermajority wait |
| `Blockstore::meta`, `is_full`, `lowest_slot`, `ledger_path` | First shred times for slot durations, skipped slots, the skip rate's window, and which filesystem holds the ledger |
| `Blockstore::get_slot_components_with_shred_info` on a block's last two FEC sets | The block footer's reward certificates, read against the rank map: whether this node's vote was paid for each slot under alpenglow |
| `Blockstore::get_latest_optimistic_slots` | The cluster's tip under TowerBFT, for the distance behind it |
| `BlockCommitmentCache::highest_confirmed_slot`, `highest_super_majority_root`, `root` | The confirmed, rooted, and finalized levels on the slot strip |
| `Validator::highest_finalized` | The cluster's tip under alpenglow, from votor's last certificate |
| `LeaderScheduleCache::slot_leader_at`, `next_leader_slot` | The epoch's leader turns, who is in the peer table, and the countdown to our slot |
| `ValidatorStartProgress` | The boot phase list and its timings |
| `solana_version::Version::this_build` | The client name, version, and commit in the header |

The certificate walk is the one place the dashboard parses ledger bytes. A per slot event saying which validators each reward certificate paid would replace it. The leader already knows this when it writes the footer.

## The host

These are not the validator's to emit and are listed for completeness: `/proc/stat`, `/proc/loadavg`, `/proc/meminfo`, `/proc/diskstats`, `/proc/net/dev`, `/proc/net/udp` and `udp6` for socket drops and queues, `statvfs` on the ledger and accounts paths, and `/proc/self/task/*` for the validator's own threads. The socket rows do need one fact only the validator has: how many packets each receiver delivered, which is the denominator the kernel's drop counter lacks. That comes from the datapoints above.

## What an events system would need to carry

- Everything under metrics datapoints as typed events, with a slot where the point has one, instead of datapoints matched by name and subject to the log level.
- A frozen block event with the fields listed under frozen banks, so a consumer never needs to hold a bank.
- The supermajority wait's per node verdicts, and each reward certificate's membership per slot, from the code that already computes them.
- A way to subscribe from the run command without changing core. The two config fields the dashboard carries stand in for that today.
