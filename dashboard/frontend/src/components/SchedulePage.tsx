import {
  memo,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type RefObject,
} from "react";
import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";
import { count, percent, shortKey, sol, solCompact } from "../format";
import {
  groupByLeader,
  hasBegun,
  matchesQuery,
  prependedCount,
  rowKey,
  scheduleRows,
  timeline,
  type Led,
  type LeaderGroup,
  type TimelineSlot,
} from "../schedule";
import type { Peer, StakeSummary, UpcomingSlot } from "../types";
import { useStore } from "../useStore";
import { Copyable } from "./Copyable";
import { Logo } from "./Logo";

/**
 * Leader turns kept above the boundary when the page settles on it.
 *
 * Enough to see who is next without opening on a stretch of schedule that has
 * not happened. Everything further ahead is still there, above them.
 */
const AHEAD_PINNED = 2;

/**
 * Where the row indices start.
 *
 * The virtualised list identifies rows by an index that only ever decreases as
 * turns arrive above them, so it has to begin high enough to keep counting down
 * for as long as the page is open. At a turn every second or so this is years.
 */
const FIRST_ROW = 1_000_000;

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
 * above and gaining one below several times a turn.
 *
 * The list is virtualised, and that is what keeps it still. Turns arrive above
 * whatever is being read two and a half times a second, and the list is *told*
 * how many rather than left to infer it from the page afterwards:
 * `firstItemIndex` drops by the number prepended and the view does not move.
 * Every attempt here to work that out from pixels found a new way to be wrong —
 * a seed that read as a page-length jump, two corrections in different render
 * phases, a smooth-scroll animation racing its own correction. Firedancer's
 * schedule is built on the same library for the same reason.
 */
export function SchedulePage() {
  const store = useStore();
  const [query, setQuery] = useState("");
  const [oursOnly, setOursOnly] = useState(false);
  const list = useRef<VirtuosoHandle>(null);
  const seen = useRef<string[]>([]);
  const [firstRow, setFirstRow] = useState(FIRST_ROW);
  // Following until the viewer scrolls for themselves, and again when they ask
  // to come back. Read from what they did rather than from where the list is:
  // the list moves on its own, and it moving is not them leaving.
  const [following, setFollowing] = useState(true);
  const [scroller, setScroller] = useState<HTMLElement | null>(null);
  useUserScroll(scroller, () => setFollowing(false));

  const completed = store.get<number>("summary", "completed_slot");
  const stake = store.get<StakeSummary>("summary", "stake");
  const peers = store.get<Peer[]>("peers", "all");
  const upcoming = store.get<UpcomingSlot[]>("slot", "upcoming") ?? [];
  const slots = store.getSlots();

  const byIdentity = useMemo(
    () => new Map((peers ?? []).map((peer) => [peer.identity, peer])),
    [peers],
  );

  const rows = useMemo(() => {
    // Published on the slow tier, so the front of it has usually happened.
    const ahead =
      completed === undefined ? upcoming : upcoming.filter((slot) => slot.slot > completed);
    const groups = groupByLeader(timeline(slots, ahead)).filter(
      (group) => matchesQuery(group, query) && (!oursOnly || group.mine),
    );
    return scheduleRows(groups);
  }, [slots, upcoming, completed, query, oursOnly]);

  // Counted during the render that introduces the rows, so the list is handed
  // them and the shift that goes with them together. Told afterwards, it would
  // draw them in the wrong place for a frame first.
  const keys = rows.map(rowKey);
  const prepended = prependedCount(seen.current, keys);
  if (prepended > 0) setFirstRow((first) => first - prepended);
  seen.current = keys;

  const boundary = rows.findIndex((row) => row.kind === "heading" && row.label === "Produced");
  const settleOn = Math.max(0, boundary - AHEAD_PINNED);
  // The boundary's place in the run of every row there has ever been, which is
  // what says whether it really moved. Its place in the array shifts by one
  // every time a turn arrives above it; add back what the list has been told to
  // subtract and the two cancel, leaving a number that changes only when a turn
  // crosses. `prepended` is included because the state holding it does not
  // catch up until the next render.
  const absolute = firstRow - prepended + boundary;
  usePinToBoundary(list, settleOn, absolute, following);

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

      <div className="schedule-list">
        {!following && (
          <button
            type="button"
            className="scroll-top schedule-live"
            onClick={() => setFollowing(true)}
          >
            Live <span aria-hidden="true">↕</span>
          </button>
        )}
        {rows.length <= 1 ? (
          <div className="sidebar-empty">
            {slots.length === 0 ? "waiting for slots…" : "nothing matches that"}
          </div>
        ) : (
          <Virtuoso
            ref={list}
            data={rows}
            firstItemIndex={firstRow}
            initialTopMostItemIndex={settleOn}
            computeItemKey={(_index, row) => rowKey(row)}
            scrollerRef={(element) => setScroller(element instanceof HTMLElement ? element : null)}
            // A screen either side, so a turn is measured before it is scrolled
            // into rather than resizing the list underneath the scroll.
            increaseViewportBy={600}
            itemContent={(_index, row) =>
              row.kind === "heading" ? (
                <h2 className="schedule-heading">{row.label}</h2>
              ) : (
                <Group
                  group={row.group}
                  peer={row.group.leader ? byIdentity.get(row.group.leader) : undefined}
                  totalStake={stake?.total_stake}
                />
              )
            }
          />
        )}
      </div>
    </section>
  );
}

/**
 * One leader's turn, drawn the same whether or not it has started.
 *
 * Memoised on the slots themselves. The store replaces only the entries that
 * changed, so a turn whose slots have all settled is skipped rather than
 * rebuilt as the page updates around it.
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
          and a turn that grew a line when it did would be measured twice. */}
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

/**
 * Calls back when the viewer scrolls the list themselves.
 *
 * Listens for the input rather than for the scrolling, because the list scrolls
 * on its own all the time — holding the boundary in place is scrolling — and a
 * position change says nothing about who wanted it. A wheel, a drag or a key
 * only ever comes from a person.
 */
function useUserScroll(scroller: HTMLElement | null, leave: () => void): void {
  useEffect(() => {
    if (!scroller) return;
    const options = { passive: true } as const;
    scroller.addEventListener("wheel", leave, options);
    scroller.addEventListener("touchmove", leave, options);
    scroller.addEventListener("keydown", leave, options);
    return () => {
      scroller.removeEventListener("wheel", leave);
      scroller.removeEventListener("touchmove", leave);
      scroller.removeEventListener("keydown", leave);
    };
    // `leave` is a fresh closure each render and the listeners are cheap to
    // swap, so it is left out rather than wrapped in a callback that would have
    // to be kept in step by hand.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scroller]);
}

/**
 * Keeps the boundary at the same place on screen while the list changes.
 *
 * `absolute` decides when: it is unmoved by turns arriving above the boundary,
 * which the list absorbs on its own, and drops by one when a turn crosses —
 * which is the only thing that would otherwise walk the heading up the screen,
 * a turn's worth at a time until it left.
 *
 * `index` is where to: a position in the row array, which is what the list
 * scrolls by. Not the numbering it hands to `itemContent`, which counts from
 * `firstItemIndex` and would be a million rows past the end of the list.
 *
 * Only while following. Once the viewer has scrolled somewhere, moving the list
 * under them would be the rudest thing this page could do.
 */
function usePinToBoundary(
  list: RefObject<VirtuosoHandle | null>,
  index: number,
  absolute: number,
  following: boolean,
): void {
  const pinned = useRef<number | null>(null);

  useLayoutEffect(() => {
    if (!following) {
      // Forgotten while away, so that coming back settles wherever the
      // boundary has got to rather than where it was left.
      pinned.current = null;
      return;
    }
    if (absolute === pinned.current) return;
    pinned.current = absolute;
    list.current?.scrollToIndex({ align: "start", index });
  }, [list, index, absolute, following]);
}
