import { useState } from "react";
import { count, percent, shortKey, sol, solCompact } from "../format";
import { groupByLeader, matchesQuery, type Led, type LeaderGroup } from "../schedule";
import type { Peer, SlotEntry, StakeSummary, UpcomingSlot } from "../types";
import { useStore } from "../useStore";
import { Copyable } from "./Copyable";
import { Logo } from "./Logo";

/**
 * The leader schedule, with what each block turned out to contain.
 *
 * The same slots the sidebar lists, folded into the runs the schedule hands
 * out and widened with the figures the collector reads off each bank as it
 * freezes. Everything here is already on the wire for the rest of the page;
 * this is a second reading of it rather than a second feed.
 */
export function SchedulePage() {
  const store = useStore();
  const [query, setQuery] = useState("");
  const [oursOnly, setOursOnly] = useState(false);

  const completed = store.get<number>("summary", "completed_slot");
  const stake = store.get<StakeSummary>("summary", "stake");
  const peers = store.get<Peer[]>("peers", "all");
  // Keyed for lookup by the leader column, which asks once per group.
  const byIdentity = new Map((peers ?? []).map((peer) => [peer.identity, peer]));
  const upcoming = store.get<UpcomingSlot[]>("slot", "upcoming") ?? [];
  // Newest first, matching the sidebar: a viewer wants the last block, not the
  // oldest one still held.
  const past = [...store.getSlots()].reverse();

  // Published on the slow tier, so the front of the list has usually happened
  // by the time it is read here.
  const ahead = completed === undefined ? upcoming : upcoming.filter((slot) => slot.slot > completed);

  const wanted = <T extends Led>(group: LeaderGroup<T>) =>
    matchesQuery(group, query) && (!oursOnly || group.mine);

  const aheadGroups = groupByLeader([...ahead].reverse()).filter(wanted);
  const pastGroups = groupByLeader(past).filter(wanted);

  return (
    <section className="schedule">
      <div className="schedule-controls">
        <input
          type="search"
          className="schedule-search"
          value={query}
          placeholder="Name, pubkey or slot"
          aria-label="Filter the schedule by leader name, pubkey or slot"
          onChange={(event) => setQuery(event.target.value)}
        />
        <div className="sidebar-filter" role="group" aria-label="Which leaders to list">
          <button type="button" aria-pressed={!oursOnly} onClick={() => setOursOnly(false)}>
            All
          </button>
          <button type="button" aria-pressed={oursOnly} onClick={() => setOursOnly(true)}>
            Ours
          </button>
        </div>
      </div>

      {aheadGroups.length > 0 && (
        <>
          <h2 className="schedule-heading">Upcoming</h2>
          {aheadGroups.map((group) => (
            <UpcomingGroup
              key={group.slots[0].slot}
              group={group}
              peer={group.leader ? byIdentity.get(group.leader) : undefined}
              totalStake={stake?.total_stake}
            />
          ))}
        </>
      )}

      <h2 className="schedule-heading">Past</h2>
      {pastGroups.length === 0 && (
        <div className="sidebar-empty">
          {past.length === 0 ? "waiting for slots…" : "nothing matches that"}
        </div>
      )}
      {pastGroups.map((group) => (
        <PastGroup
          key={group.slots[0].slot}
          group={group}
          peer={group.leader ? byIdentity.get(group.leader) : undefined}
          totalStake={stake?.total_stake}
        />
      ))}
    </section>
  );
}

/** Leader, name and key, shared by both kinds of group. */
function GroupLeader<T extends Led>({
  group,
  peer,
  totalStake,
}: {
  group: LeaderGroup<T>;
  peer: Peer | undefined;
  totalStake: number | undefined;
}) {
  // Missing rather than zero when the table has not caught up with a leader
  // that has only just come into view.
  const share = peer && totalStake ? peer.stake / totalStake : null;

  return (
    <div className="schedule-leader">
      <div className="schedule-leader-name">
        <Logo url={group.leader_icon} size={16} />
        {group.leader_name ?? (group.leader ? shortKey(group.leader, 6, 5) : "unknown")}
        {group.mine && <span className="schedule-mine">ours</span>}
      </div>
      {group.leader && (
        <Copyable
          text={group.leader}
          label={shortKey(group.leader, 8, 8)}
          className="schedule-leader-key"
        />
      )}
      {peer && (
        <div className="schedule-leader-meta">
          {peer.version && <span className="schedule-version">{peer.version}</span>}
          {peer.stake > 0 && (
            <span>
              {solCompact(peer.stake)} SOL
              {share !== null && <span className="schedule-share">{percent(share, 3)}</span>}
            </span>
          )}
          {peer.ip && <span className="schedule-ip">{peer.ip}</span>}
        </div>
      )}
    </div>
  );
}

/** A turn that has not come round yet: who, and which slots. */
function UpcomingGroup({
  group,
  peer,
  totalStake,
}: {
  group: LeaderGroup<UpcomingSlot>;
  peer: Peer | undefined;
  totalStake: number | undefined;
}) {
  return (
    <div className="schedule-group is-upcoming">
      <GroupLeader group={group} peer={peer} totalStake={totalStake} />
      <div className="schedule-slots">
        {group.slots.map((slot) => (
          <div className="schedule-row" key={slot.slot}>
            <span className="schedule-slot">{count(slot.slot)}</span>
            <span className="schedule-pending">scheduled</span>
          </div>
        ))}
      </div>
    </div>
  );
}

/** A turn that has been and gone, with what each of its blocks held. */
function PastGroup({
  group,
  peer,
  totalStake,
}: {
  group: LeaderGroup<SlotEntry>;
  peer: Peer | undefined;
  totalStake: number | undefined;
}) {
  return (
    <div className="schedule-group">
      <GroupLeader group={group} peer={peer} totalStake={totalStake} />
      <div className="schedule-slots">
        <div className="schedule-row schedule-head">
          <span className="schedule-slot">Slot</span>
          <span>Votes</span>
          <span>Non-votes</span>
          <span>Fees</span>
          <span>Duration</span>
          <span>Compute</span>
        </div>
        {group.slots.map((slot) => (
          <SlotRow key={slot.slot} slot={slot} />
        ))}
      </div>
    </div>
  );
}

function SlotRow({ slot }: { slot: SlotEntry }) {
  const block = slot.block;
  // Votes are what is left of the block once the rest is taken out. Clamped
  // because the two counters are differenced independently and a bank whose
  // parent has gone reports neither.
  const votes = block ? Math.max(0, block.transactions - block.non_vote_transactions) : null;
  const filled = block && block.block_cost_limit > 0 ? block.block_cost / block.block_cost_limit : null;

  return (
    <div className={`schedule-row level-${slot.level}`}>
      <span className="schedule-slot">
        {count(slot.slot)}
        <span className={`schedule-level level-${slot.level}`} title={slot.level.replace(/_/g, " ")} />
      </span>
      <span>{votes === null ? "—" : count(votes)}</span>
      <span>{block ? count(block.non_vote_transactions) : "—"}</span>
      <span>{block ? sol(block.total_fees, 4) : "—"}</span>
      <span>{slot.duration_nanos === null ? "—" : `${Math.round(slot.duration_nanos / 1e6)} ms`}</span>
      <span>
        {block ? count(block.block_cost) : "—"}
        {filled !== null && <span className="schedule-fill">{percent(filled, 0)}</span>}
      </span>
    </div>
  );
}
