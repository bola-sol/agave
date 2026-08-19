import { memo, useLayoutEffect, useRef, useState, type RefObject } from "react";
import { count, percent, shortKey, sol, solCompact } from "../format";
import {
  groupByLeader,
  hasBegun,
  matchesQuery,
  timeline,
  type Led,
  type LeaderGroup,
  type TimelineSlot,
} from "../schedule";
import type { Peer, StakeSummary, UpcomingSlot } from "../types";
import { useStore } from "../useStore";
import { Copyable } from "./Copyable";
import { Logo } from "./Logo";
import { ScrollTop } from "./ScrollTop";

/**
 * How far above the boundary the page settles, in pixels.
 *
 * Measured rather than counted in turns. A turn is not a fixed height — the
 * nearest one is a partial run, and a leader whose details have not arrived is
 * shorter than one whose have — so counting them made the resting place move
 * about, and with it the position the page kept returning to. Roughly two
 * turns, and the exact figure does not matter: it is only where the list opens.
 */
const AHEAD_PINNED_PX = 260;

/**
 * The leader schedule, with what each block turned out to contain.
 *
 * The slots are the ones the sidebar lists and the block figures are the ones
 * the collector reads off each bank as it freezes, so this is a second reading
 * of what is on the wire rather than a second feed.
 *
 * Slots that have begun and slots the schedule promises are merged into one run
 * before being grouped, so a leader's turn is drawn once and whole. Split at the
 * slot being produced it appeared on both sides of the boundary, losing a row
 * above and gaining one below several times a turn, and everything after it
 * moved each time.
 */
export function SchedulePage() {
  const store = useStore();
  const [query, setQuery] = useState("");
  const [oursOnly, setOursOnly] = useState(false);
  const list = useRef<HTMLDivElement>(null);
  const live = useRef<HTMLHeadingElement>(null);

  const completed = store.get<number>("summary", "completed_slot");
  const stake = store.get<StakeSummary>("summary", "stake");
  const peers = store.get<Peer[]>("peers", "all");
  const upcoming = store.get<UpcomingSlot[]>("slot", "upcoming") ?? [];
  const byIdentity = new Map((peers ?? []).map((peer) => [peer.identity, peer]));

  // Published on the slow tier, so the front of it has usually happened.
  const ahead =
    completed === undefined ? upcoming : upcoming.filter((slot) => slot.slot > completed);

  const wanted = <T extends Led>(group: LeaderGroup<T>) =>
    matchesQuery(group, query) && (!oursOnly || group.mine);

  const groups = groupByLeader(timeline(store.getSlots(), ahead)).filter(wanted);
  const scheduled = groups.filter((group) => !hasBegun(group));
  const produced = groups.filter(hasBegun);

  usePinToBoundary(list, live, produced.length > 0);

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

      <div className="schedule-list" ref={list}>
        <ScrollTop scroller={list} live={live} liveOffset={AHEAD_PINNED_PX} />
        {scheduled.length > 0 && <h2 className="schedule-heading">Upcoming</h2>}
        {scheduled.map((group) => (
          <Group
            key={group.slots[0].slot}
            group={group}
            peer={group.leader ? byIdentity.get(group.leader) : undefined}
            totalStake={stake?.total_stake}
          />
        ))}

        {/* The boundary, and the one element on the page that neither moves
            between renders nor changes what it is. Everything about where the
            list sits is measured from here. */}
        <h2 className="schedule-heading" ref={live}>
          Produced
        </h2>
        {produced.length === 0 && (
          <div className="sidebar-empty">
            {groups.length === 0 ? "waiting for slots…" : "nothing matches that"}
          </div>
        )}
        {produced.map((group) => (
          <Group
            key={group.slots[0].slot}
            group={group}
            peer={group.leader ? byIdentity.get(group.leader) : undefined}
            totalStake={stake?.total_stake}
          />
        ))}
      </div>
    </section>
  );
}

/**
 * Settles the list on the boundary once, when the first slots arrive.
 *
 * Once, because after that the position is the viewer's. The list is scrolled
 * rather than trimmed, so everything further ahead stays reachable by scrolling
 * up: the difference between a starting position and a limit.
 *
 * Before the paint rather than after it, so that arriving on the page does not
 * show the top of the schedule for a frame and then jump. It shares the phase
 * with the held position, which sees the move as one it did not make and stands
 * aside — the same way it treats the browser's own anchoring.
 */
function usePinToBoundary(
  list: RefObject<HTMLDivElement | null>,
  live: RefObject<HTMLHeadingElement | null>,
  ready: boolean,
): void {
  const settled = useRef(false);

  useLayoutEffect(() => {
    if (settled.current || !ready) return;
    const scroller = list.current;
    const target = live.current;
    if (!scroller || !target) return;
    // Nothing to scroll yet: the boundary is already on screen, and settling
    // now would spend the one chance on a list that has not filled.
    if (scroller.scrollHeight <= scroller.clientHeight) return;

    settled.current = true;
    const boundary =
      scroller.scrollTop +
      (target.getBoundingClientRect().top - scroller.getBoundingClientRect().top);
    scroller.scrollTop = Math.max(0, boundary - AHEAD_PINNED_PX);
  });
}

/**
 * One leader's turn, drawn the same whether or not it has started.
 *
 * Memoised on the slots themselves. The store replaces only the entries that
 * changed, so a turn whose slots have all settled is skipped rather than
 * rebuilt — which is most of them, on a page of a hundred and fifty turns that
 * was otherwise re-rendering whole several times a second.
 */
const Group = memo(
  function Group({
    group,
    peer,
    totalStake,
  }: {
    group: LeaderGroup<TimelineSlot>;
    peer: Peer | undefined;
    totalStake: number | undefined;
  }) {
    return (
      <div className={`schedule-group${hasBegun(group) ? "" : " is-upcoming"}`}>
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
  },
  (before, after) =>
    before.peer === after.peer &&
    before.totalStake === after.totalStake &&
    before.group.slots.length === after.group.slots.length &&
    before.group.slots.every((slot, index) => slot.entry === after.group.slots[index]?.entry),
);

/** Leader, name and key, with what is known about the validator behind them. */
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
      {/* Always drawn, empty or not. The peer table arrives on the slow tier
          and a turn that grew a line when it did would move every turn after
          it, several times a minute. */}
      <div className="schedule-leader-meta">
        {peer?.version && <span className="schedule-version">{peer.version}</span>}
        {peer && peer.stake > 0 && (
          <span>
            {solCompact(peer.stake)} SOL
            {share !== null && <span className="schedule-share">{percent(share, 3)}</span>}
          </span>
        )}
        {peer?.ip && <span className="schedule-ip">{peer.ip}</span>}
      </div>
    </div>
  );
}

/** One slot, empty until it has been produced. */
function SlotRow({ slot }: { slot: TimelineSlot }) {
  const entry = slot.entry;
  const block = entry?.block ?? null;
  // Votes are what is left of the block once the rest is taken out. Clamped
  // because the two counters are differenced independently and a bank whose
  // parent has gone reports neither.
  const votes = block ? Math.max(0, block.transactions - block.non_vote_transactions) : null;
  const filled =
    block && block.block_cost_limit > 0 ? block.block_cost / block.block_cost_limit : null;
  const level = entry?.level ?? "scheduled";

  return (
    <div className={`schedule-row level-${level}`}>
      <span className="schedule-slot">
        {count(slot.slot)}
        <span className={`schedule-level level-${level}`} title={level.replace(/_/g, " ")} />
      </span>
      <span>{votes === null ? "—" : count(votes)}</span>
      <span>{block ? count(block.non_vote_transactions) : "—"}</span>
      <span>{block ? sol(block.total_fees, 4) : "—"}</span>
      <span>
        {entry?.duration_nanos == null ? "—" : `${Math.round(entry.duration_nanos / 1e6)} ms`}
      </span>
      <span>
        {block ? count(block.block_cost) : "—"}
        {filled !== null && <span className="schedule-fill">{percent(filled, 0)}</span>}
      </span>
    </div>
  );
}
