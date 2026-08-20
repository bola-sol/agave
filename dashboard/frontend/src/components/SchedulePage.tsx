import { memo, useMemo, useRef, useState } from "react";
import { Virtuoso, type VirtuosoHandle } from "react-virtuoso";
import { count, percent, shortKey, sol, solCompact } from "../format";
import {
  matchesQuery,
  prependedCount,
  turnKey,
  turnsOf,
  type Turn,
  type TurnSlot,
} from "../schedule";
import type { Peer, StakeSummary } from "../types";
import { useStore } from "../useStore";
import { Copyable } from "./Copyable";
import { Logo } from "./Logo";

/**
 * Where the turn indices start.
 *
 * The virtualised list identifies rows by an index that only ever decreases as
 * turns arrive above them, so it has to begin high enough to keep counting down
 * for as long as the page is open. At a turn every second or so this is years.
 */
const FIRST_TURN = 1_000_000;

/**
 * What each leader's turn at producing contained.
 *
 * The slots are the ones the sidebar lists and the block figures are the ones
 * the collector reads off each bank as it freezes, so this is a second reading
 * of what is on the wire rather than a second feed.
 *
 * Newest first, and a turn appears whole the moment its first slot begins: all
 * four slots share a leader by definition, so the rest are drawn as empty rows
 * and filled where they stand. Nothing below a turn moves while it fills.
 *
 * The list is virtualised, and that is what holds it still. Turns arrive at the
 * top faster than they can be read, and the list is *told* how many rather than
 * left to infer it from the page afterwards: `firstItemIndex` drops by the
 * number prepended and the view does not move. Every attempt here to work that
 * out from pixels found a new way to be wrong. Firedancer's schedule is built
 * on the same library for the same reason.
 */
export function SchedulePage() {
  const store = useStore();
  const [query, setQuery] = useState("");
  const [oursOnly, setOursOnly] = useState(false);
  const list = useRef<VirtuosoHandle>(null);
  const seen = useRef<string[]>([]);
  const [firstTurn, setFirstTurn] = useState(FIRST_TURN);
  // The newest turn is the top of the list, so the list being at its top is the
  // whole of what it means to be live. The list itself reports that.
  const [atTop, setAtTop] = useState(true);

  const stake = store.get<StakeSummary>("summary", "stake");
  const peers = store.get<Peer[]>("peers", "all");
  const slots = store.getSlots();

  const byIdentity = useMemo(
    () => new Map((peers ?? []).map((peer) => [peer.identity, peer])),
    [peers],
  );

  const turns = useMemo(
    () => turnsOf(slots).filter((turn) => matchesQuery(turn, query) && (!oursOnly || turn.mine)),
    [slots, query, oursOnly],
  );

  // Counted during the render that introduces the turns, so the list is handed
  // them and the shift that goes with them together. Told afterwards, it would
  // draw them in the wrong place for a frame first.
  const keys = turns.map(turnKey);
  const prepended = prependedCount(seen.current, keys);
  if (prepended > 0) setFirstTurn((first) => first - prepended);
  seen.current = keys;

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
        {!atTop && (
          <button
            type="button"
            className="scroll-top schedule-live"
            onClick={() => list.current?.scrollToIndex({ align: "start", index: 0 })}
          >
            Live <span aria-hidden="true">↑</span>
          </button>
        )}
        {turns.length === 0 ? (
          <div className="sidebar-empty">
            {slots.length === 0 ? "waiting for slots…" : "nothing matches that"}
          </div>
        ) : (
          <Virtuoso
            ref={list}
            data={turns}
            firstItemIndex={firstTurn}
            computeItemKey={(_index, turn) => turnKey(turn)}
            atTopStateChange={setAtTop}
            // A screen either side, so a turn is measured before it is scrolled
            // into rather than resizing the list underneath the scroll.
            increaseViewportBy={600}
            itemContent={(_index, turn) => (
              <TurnCard
                turn={turn}
                peer={turn.leader ? byIdentity.get(turn.leader) : undefined}
                totalStake={stake?.total_stake}
              />
            )}
          />
        )}
      </div>
    </section>
  );
}

/**
 * One leader's turn, the same height from the moment it appears.
 *
 * Memoised on the slots themselves. The store replaces only the entries that
 * changed, so a turn whose slots have all settled is skipped rather than
 * rebuilt as the page updates around it.
 */
const TurnCard = memo(
  function TurnCard({
    turn,
    peer,
    totalStake,
  }: {
    turn: Turn;
    peer: Peer | undefined;
    totalStake: number | undefined;
  }) {
    return (
      <div className="schedule-group">
        <TurnLeader turn={turn} peer={peer} totalStake={totalStake} />
        <div className="schedule-slots">
          <div className="schedule-row schedule-head">
            <span className="schedule-slot">Slot</span>
            <span>Votes</span>
            <span>Non-votes</span>
            <span>Fees</span>
            <span>Duration</span>
            <span>Compute</span>
          </div>
          {turn.slots.map((slot) => (
            <SlotRow key={slot.slot} slot={slot} />
          ))}
        </div>
      </div>
    );
  },
  (before, after) =>
    before.peer === after.peer &&
    before.totalStake === after.totalStake &&
    before.turn.slots.length === after.turn.slots.length &&
    before.turn.slots.every((slot, index) => slot.entry === after.turn.slots[index]?.entry),
);

/** Leader, name and key, with what is known about the validator behind them. */
function TurnLeader({
  turn,
  peer,
  totalStake,
}: {
  turn: Turn;
  peer: Peer | undefined;
  totalStake: number | undefined;
}) {
  // Missing rather than zero when the table has not caught up with a leader
  // that has only just come into view.
  const share = peer && totalStake ? peer.stake / totalStake : null;

  return (
    <div className="schedule-leader">
      <div className="schedule-leader-name">
        <Logo url={turn.leader_icon} size={16} />
        {turn.leader_name ?? (turn.leader ? shortKey(turn.leader, 6, 5) : "unknown")}
        {turn.mine && <span className="schedule-mine">ours</span>}
      </div>
      {turn.leader && (
        <Copyable
          text={turn.leader}
          label={shortKey(turn.leader, 8, 8)}
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
function SlotRow({ slot }: { slot: TurnSlot }) {
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
