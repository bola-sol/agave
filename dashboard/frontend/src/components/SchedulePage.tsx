import { memo, useEffect, useMemo, useRef, useState } from "react";
import { count, percent, shortKey, sol, solCompact } from "../format";
import { matchesQuery, SLOTS_PER_TURN, turnKey, turnsOf, type Turn, type TurnSlot } from "../schedule";
import { entriesOf, type SlotRange } from "../slotHistory";
import type { EpochInfo, Peer, SlotEntry, StakeSummary } from "../types";
import { useStore } from "../useStore";
import { Copyable } from "./Copyable";
import { Logo } from "./Logo";
import { ScrollTop } from "./ScrollTop";

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
 * The live edge is the top, which is the same arrangement as the slot list down
 * the side, so it wants the same handling and gets it from the same component:
 * arrivals are seen while the top is on screen, and held off what is being read
 * once it is not.
 */
/**
 * Slots asked for each time the reader wants more.
 *
 * Five hundred and twelve, a hundred and twenty-eight turns, which is a few
 * screenfuls. The list is not virtualised, so this bounds the DOM as much as
 * the request: the depth the validator retains is far past what a browser will
 * happily render at once, and the reader asking for more is what decides how
 * much of it is worth rendering.
 */
const OLDER_SPAN = 512;

/**
 * Turns drawn at once, however many are loaded or matched.
 *
 * The list is not virtualised, so every turn on it is about fifty elements of
 * real DOM and the browser lays all of them out. Measured on this stylesheet: a
 * thousand turns is fifty-four thousand elements, half a second to render and
 * thirty milliseconds of layout on every scroll, which is the edge of
 * comfortable. Two and a half thousand is a second and a half and sixty-seven
 * milliseconds a scroll, which is not.
 *
 * The depth beyond this is reached by searching rather than by scrolling to it.
 * Nobody walks a hundred thousand slots four at a time; they look for a
 * validator or paste a slot number, and a search narrows to a few hundred turns
 * long before this bites.
 */
const MAX_TURNS = 1000;

export function SchedulePage() {
  const store = useStore();
  const [query, setQuery] = useState("");
  const [oursOnly, setOursOnly] = useState(false);
  const list = useRef<HTMLDivElement>(null);

  const stake = store.get<StakeSummary>("summary", "stake");
  const peers = store.get<Peer[]>("peers", "all");
  const epoch = store.get<EpochInfo>("epoch", "new");
  const identity = store.get<string>("summary", "identity_key");
  const live = store.getSlots();

  // Spans fetched from the validator's packed history, oldest first, below
  // whatever the live window still holds. Held here rather than in the store:
  // they are this page's working set, and putting a hundred thousand
  // reconstructed entries into the shared slot map is the thing this design
  // exists to avoid.
  // The cluster's names and icons, fetched the first time somebody searches.
  //
  // Not on load: it is a hundred and fifty kilobytes and most visits never
  // search. Not per query either, since the store only fetches it once. Until
  // it arrives a search still matches on key and on slot number, which is what
  // most searches are; a name search before it lands finds the leaders of the
  // live window and no more.
  const searching = query.trim().length > 0;
  useEffect(() => {
    if (searching) void store.loadDisplays().catch(() => {});
  }, [searching, store]);

  const [older, setOlder] = useState<SlotEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [exhausted, setExhausted] = useState(false);
  const slots = useMemo(() => [...older, ...live], [older, live]);

  const loadOlder = async () => {
    if (loading) return;
    const earliest = slots[0]?.slot;
    if (earliest === undefined) return;
    setLoading(true);
    try {
      // Aligned down to a turn boundary so a span never begins mid-turn, and
      // clamped at nought for a cluster young enough that it could go below.
      const first = Math.max(0, Math.floor((earliest - OLDER_SPAN) / SLOTS_PER_TURN) * SLOTS_PER_TURN);
      const range = await store.request<SlotRange>("slot", "range", {
        first_slot: first,
        count: earliest - first,
      });
      const fetched = entriesOf(range, epoch, identity);
      // Nothing came back for any of it, so there is nothing older to ask for
      // and the control stops offering.
      if (fetched.length === 0) setExhausted(true);
      else setOlder((held) => [...fetched, ...held]);
    } catch {
      // A refused or lost request leaves the page as it was. The control stays,
      // so trying again is a click rather than a reload.
    } finally {
      setLoading(false);
    }
  };

  const byIdentity = useMemo(
    () => new Map((peers ?? []).map((peer) => [peer.identity, peer])),
    [peers],
  );

  const matched = useMemo(
    () =>
      turnsOf(slots, (slot, mine) => store.leaderOf(slot, mine)).filter(
        (turn) => matchesQuery(turn, query) && (!oursOnly || turn.mine),
      ),
    // `store` is stable and its revision is what re-runs this; `leaderOf`
    // answers from the epoch and peer table, both of which arrive as published
    // values and so bump that revision when they change.
    [store, slots, query, oursOnly],
  );

  // Newest first, so the cap keeps the newest and drops the tail. A search that
  // matches more than the page will draw says so rather than quietly showing
  // some of its answer.
  const turns = matched.slice(0, MAX_TURNS);
  const beyondCap = matched.length - turns.length;
  // Counted in slots because that is what a span is asked for in. The live
  // window is part of the total: it is drawn from the same list.
  const atCeiling = slots.length >= MAX_TURNS * SLOTS_PER_TURN;

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
        <ScrollTop scroller={list} />
        {turns.length === 0 && (
          <div className="sidebar-empty">
            {slots.length === 0 ? "waiting for slots…" : "nothing matches that"}
          </div>
        )}
        {turns.map((turn) => (
          <TurnCard
            key={turnKey(turn)}
            turn={turn}
            peer={turn.leader ? byIdentity.get(turn.leader) : undefined}
            totalStake={stake?.total_stake}
          />
        ))}
        {beyondCap > 0 && (
          <div className="schedule-capped">
            {count(turns.length)} of {count(matched.length)} matching turns shown.
            Narrow the search to see the rest.
          </div>
        )}
        {slots.length > 0 && !exhausted && !atCeiling && (
          <button
            type="button"
            className="schedule-older"
            disabled={loading}
            onClick={() => void loadOlder()}
          >
            {loading ? "loading…" : "load earlier turns"}
          </button>
        )}
        {atCeiling && beyondCap === 0 && (
          <div className="schedule-capped">
            As far back as this list goes. The validator keeps a great deal more;
            search a name, a key or a slot number to reach it.
          </div>
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
