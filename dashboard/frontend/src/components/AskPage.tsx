/** One box that takes a request in plain words and answers it with charts from the slot history and
 *  the host's readings, and a figure where one is asked for. The browser's own model picks the series
 *  and the operation; nothing typed leaves the browser. */

import { memo, useEffect, useMemo, useRef, useState, useSyncExternalStore, type FormEvent, type ReactElement } from "react";
import {
  asksWhy,
  BIN_COUNT,
  bucket,
  ceiling,
  figure,
  HOST_SERIES,
  HOST_STEP_MILLIS,
  hostBins,
  leftOut,
  namesSeries,
  notDrawnNote,
  onWholeSpans,
  paths,
  readWindow,
  recordedList,
  SERIES,
  SLOT_SERIES,
  windowLabel,
  windowOf,
  WINDOWS,
  windowShort,
  type AskedWindow,
  type Bins,
  type Series,
} from "../ask";
import {
  answerFigure,
  comparing,
  directed,
  readAs,
  shareFigure,
  sharePair,
  thresholdOf,
  withoutThreshold,
  type Answered as Worked,
  type FigureContext,
  type Mark,
  type Operation,
  type ThresholdRead,
} from "../askFigures";
import { askModel, useAskModel, type AskModelSnapshot } from "../askModel";
import {
  asksEarned,
  asksProduction,
  earningsLine,
  epochAsked,
  OWN_KEYS,
  ownAmount,
  ownEmpty,
  ownFigure,
  ownLabel,
  ownReadings,
  ownScope,
  ownThresholdOf,
  production,
  readOwnBlocks,
  type OwnReading,
  type OwnWindow,
} from "../askOwn";
import { useBalancesHidden } from "../balances";
import { clockTime, count } from "../format";
import { heldShortLabel, type BlockHead } from "../produced";
import type { Store } from "../store";
import type { EpochEarnings, LeaderSlotCounts, TipRates } from "../types";
import { useRequests } from "../useStore";

type Answered =
  | { kind: "asking" }
  | {
      kind: "charts";
      charts: readonly Charted[];
      asked: AskedWindow;
      pickedMillis: number;
      /** What the request asked for and did not get, or null. */
      note: string | null;
      /** What to work out beside the charts; null where it was not read. */
      operation: Operation | null | "pending";
      operationMillis: number | null;
    }
  | { kind: "why" }
  | { kind: "none" }
  | { kind: "failed"; reason: "timeout" | "model" };

/** One series of an answer; `closest` when the request names none of its words. */
interface Charted {
  series: Series;
  closest: boolean;
}

interface Item {
  id: number;
  typed: string;
  answer: Answered;
}

const EXAMPLES: readonly string[] = [
  "Shreds per second over the past hour",
  "Replay time, last 15 minutes",
  "Total fees over the past 6 hours",
  "CPU use over the past hour",
];

/** Older answers fall off the end. */
const MAX_ITEMS = 20;

export function AskPage(): ReactElement {
  const model = useAskModel();
  const [typed, setTyped] = useState("");
  const [items, setItems] = useState<Item[]>([]);
  const nextId = useRef(1);
  const stop = useRef<AbortController | null>(null);

  const ready = model.state === "ready";
  useEffect(() => {
    if (!ready) return;
    askModel.warm();
    askModel.warmFigures();
  }, [ready]);
  useEffect(() => () => stop.current?.abort(), []);

  // Until the operation is read too, so the two prompts of one request never queue behind another's.
  const busy = items.some(
    (item) => item.answer.kind === "asking" || (item.answer.kind === "charts" && item.answer.operation === "pending"),
  );
  const open = ready && !busy;

  const ask = (text: string): void => {
    const request = text.trim();
    if (!request || !open) return;
    const id = nextId.current;
    nextId.current += 1;
    const settle = (answer: Answered | null): void =>
      setItems((held) =>
        answer === null
          ? held.filter((item) => item.id !== id)
          : held.map((item) => (item.id === id ? { ...item, answer } : item)),
      );
    const add = (answer: Answered): void =>
      setItems((held) => [{ id, typed: request, answer }, ...held].slice(0, MAX_ITEMS));
    setTyped("");
    if (asksWhy(request)) {
      add({ kind: "why" });
      return;
    }
    add({ kind: "asking" });
    const controller = new AbortController();
    stop.current = controller;
    const began = performance.now();
    // Asked at once; whichever lands first waits for the other in these.
    let operation: Operation | null | "pending" = "pending";
    let operationMillis: number | null = null;
    void askModel.pickOperation(request, controller.signal).then((read) => {
      operation = read;
      operationMillis = performance.now() - began;
      setItems((held) =>
        held.map((item) =>
          item.id === id && item.answer.kind === "charts"
            ? { ...item, answer: { ...item.answer, operation: read, operationMillis } }
            : item,
        ),
      );
    });
    void askModel.pickSeries(request, controller.signal).then((picked) => {
      if (picked.kind === "series") {
        settle({
          kind: "charts",
          charts: picked.series.map((series) => ({ series, closest: !namesSeries(request, series) })),
          // The values compared against are no window, as "under 5 s" would read.
          asked: windowOf(picked.series.reduce((text, series) => withoutThreshold(text, series), request)),
          pickedMillis: performance.now() - began,
          note: notDrawnNote(leftOut(request, picked.series, SERIES), picked.unmatched),
          operation,
          operationMillis,
        });
        return;
      }
      // Nothing to work a figure out for.
      controller.abort();
      if (picked.kind === "none") settle({ kind: "none" });
      else settle(picked.reason === "stopped" ? null : { kind: "failed", reason: picked.reason });
    });
  };

  const submit = (event: FormEvent): void => {
    event.preventDefault();
    ask(typed);
  };

  if (model.state === "unavailable") {
    return (
      <div className="ask">
        <section className="card ask-box">
          <h2 className="card-title">Ask</h2>
          <p className="ask-plain">Ask needs the language model built into desktop Chrome, which this browser does not offer.</p>
        </section>
      </div>
    );
  }

  return (
    <div className="ask">
      <section className="card ask-box">
        <div className="card-head">
          <h2 className="card-title">Ask</h2>
          <span className="card-aside">answered by Chrome's built-in model, on this device</span>
        </div>
        <form className="ask-form" onSubmit={submit}>
          <input
            className="ask-input"
            type="search"
            value={typed}
            disabled={!ready}
            onChange={(event) => setTyped(event.target.value)}
            placeholder="Ask for a chart or a figure"
            aria-label="Request"
            maxLength={200}
          />
          <button type="submit" className="ask-send" disabled={!open || !typed.trim()}>
            Ask
          </button>
        </form>
        <div className="ask-examples">
          {EXAMPLES.map((example) => (
            <button key={example} type="button" className="ask-example" disabled={!open} onClick={() => ask(example)}>
              {example}
            </button>
          ))}
        </div>
        {!ready && <AskDownload model={model} />}
      </section>
      <div className="ask-answers">
        {items.map((item) => (
          <Answer key={item.id} item={item} />
        ))}
      </div>
    </div>
  );
}

function AskDownload({ model }: { model: AskModelSnapshot }): ReactElement | null {
  // The check answers within a frame or two, so a line for it would only flash.
  if (model.state === "checking") return null;
  if (model.state === "downloadable") {
    return (
      <div className="ask-download">
        <p className="ask-plain">
          {model.downloadFailed
            ? "The download did not finish."
            : "Chrome's built-in model is not on this device yet. It is a one-off download of a few gigabytes."}
        </p>
        <button type="button" className="ask-send" onClick={askModel.download}>
          {model.downloadFailed ? "Try again" : "Download"}
        </button>
      </div>
    );
  }
  if (model.state !== "downloading") return null;
  const percent = model.progress === null ? null : Math.round(model.progress * 100);
  return (
    <div className="ask-download">
      <p className="ask-plain">
        {percent === null ? "Chrome is downloading its model…" : percent >= 100 ? "Downloaded. Loading the model…" : `Downloading Chrome's model: ${percent}%`}
      </p>
      {percent !== null && (
        <div className="ask-progress" role="progressbar" aria-label="Model download" aria-valuemin={0} aria-valuemax={100} aria-valuenow={percent}>
          <span style={{ width: `${percent}%` }} />
        </div>
      )}
    </div>
  );
}

function Answer({ item }: { item: Item }): ReactElement {
  const { answer, typed } = item;
  if (answer.kind === "charts") return <ScopedAnswer typed={typed} answer={answer} />;
  return (
    <section className="card ask-answer">
      <p className="ask-asked">{typed}</p>
      <p className="ask-plain" role="status">
        {PLAIN[answer.kind === "failed" ? answer.reason : answer.kind]}
      </p>
      {answer.kind === "none" && (
        <p className="ask-source">
          Recorded per slot: {RECORDED_SLOTS}. Recorded for the host: {RECORDED_HOST}.
        </p>
      )}
    </section>
  );
}

const RECORDED_SLOTS = recordedList(SLOT_SERIES);
const RECORDED_HOST = recordedList(HOST_SERIES);

const PLAIN: Record<"asking" | "why" | "none" | "timeout" | "model", string> = {
  asking: "Asking Chrome's model…",
  why: "This version draws charts and works out figures; it does not explain causes.",
  none: "Nothing this validator records matches that.",
  timeout: "Chrome's model took too long.",
  model: "Chrome's model did not answer.",
};

type Loaded =
  | { state: "loading" }
  | { state: "empty" }
  | { state: "failed" }
  | {
      state: "ready";
      /** The window these readings were read for, which a figure names. */
      millis: number;
      from: number;
      bins: readonly (Bins | null)[];
      /** What the slot history gave, or null for a request with no per-slot series. */
      slots: { count: number; reached: boolean } | null;
      /** How the host's readings came, or null for a request with none. */
      host: { stepMillis: number; heldFrom: number | null } | null;
      /** A share's whole where it came by rule and has no panel of its own. */
      whole: Bins | null;
    };

/** Whether the validator can be asked now, and for the slot history whether its tip is known; a
 *  boolean, so a card redraws only when it flips. */
function useCanRead(live: Store, needsTip: boolean): boolean {
  // The clock offset comes with the first message after a connection opens; without it a window
  // would be laid on the viewer's clock, not the validator's.
  const read = (): boolean =>
    live.getConnection() === "open" &&
    live.getClockOffset() !== null &&
    (!needsTip || live.get("summary", "completed_slot") !== undefined);
  return useSyncExternalStore(live.subscribe, read, read);
}

const WAITING: Record<"offline" | "loading" | "empty" | "failed", string> = {
  offline: "Waiting for the validator…",
  loading: "Reading the history…",
  empty: "No readings in that window.",
  failed: "The validator did not answer.",
};

type ChartsAnswered = Extract<Answered, { kind: "charts" }>;

type Scope = "ours" | "all";

/** A request our own blocks can answer starts on them when it says so, and either way can switch. */
function ScopedAnswer({ typed, answer }: { typed: string; answer: ChartsAnswered }): ReactElement {
  const ownable = useMemo(
    () => answer.charts.some((chart) => OWN_KEYS.has(chart.series.key)) || asksProduction(typed) || asksEarned(typed),
    [answer.charts, typed],
  );
  const [scope, setScope] = useState<Scope>(() => (ownable && ownScope(typed) ? "ours" : "all"));
  // Each side is drawn once first shown and kept, so switching back keeps its window and readings.
  const [shown, setShown] = useState<ReadonlySet<Scope>>(() => new Set([scope]));
  const holder = useRef<HTMLDivElement>(null);
  const switched = useRef(false);
  const choose = (next: Scope): void => {
    switched.current = true;
    setScope(next);
    setShown((held) => (held.has(next) ? held : new Set([...held, next])));
  };
  // The pressed button of the side now shown takes the focus the other side's button had.
  useEffect(() => {
    if (!switched.current) return;
    switched.current = false;
    holder.current?.querySelector<HTMLButtonElement>(`[data-scope="${scope}"] .ask-scope [aria-pressed="true"]`)?.focus();
  }, [scope]);
  const toggle = ownable ? <ScopeToggle scope={scope} onScope={choose} /> : null;
  if (!ownable) return <ChartsAnswer typed={typed} answer={answer} toggle={null} />;
  return (
    <div ref={holder} className="ask-scoped">
      {shown.has("ours") && (
        <div data-scope="ours" hidden={scope !== "ours"}>
          <OwnAnswer typed={typed} answer={answer} toggle={toggle} />
        </div>
      )}
      {shown.has("all") && (
        <div data-scope="all" hidden={scope !== "all"}>
          <ChartsAnswer typed={typed} answer={answer} toggle={toggle} />
        </div>
      )}
    </div>
  );
}

function ScopeToggle({ scope, onScope }: { scope: Scope; onScope: (scope: Scope) => void }): ReactElement {
  return (
    <div className="toggle-group ask-scope" role="group" aria-label="Blocks">
      <button type="button" aria-pressed={scope === "ours"} onClick={() => onScope("ours")}>
        Our blocks
      </button>
      <button type="button" aria-pressed={scope === "all"} onClick={() => onScope("all")}>
        Every leader
      </button>
    </div>
  );
}

function ChartsAnswer({
  typed,
  answer,
  toggle,
}: {
  typed: string;
  answer: ChartsAnswered;
  toggle: ReactElement | null;
}): ReactElement {
  const { charts, asked, pickedMillis, operation, operationMillis } = answer;
  // Asked of our own blocks with nothing they measure: every leader's chart, said so.
  const perSlotAsked = charts.some((chart) => chart.series.source === "slots");
  const note =
    [answer.note, toggle === null && perSlotAsked && ownScope(typed) ? NOT_OURS : null].filter(Boolean).join(" ") || null;
  // Always the live validator: a request for the past hour means the hour up to now.
  const live = useRequests();
  const perSlot = charts.some((chart) => chart.series.source === "slots");
  const canRead = useCanRead(live, perSlot);
  // A window and how often it was picked: picking the one shown reads it again.
  const [pick, setPick] = useState({ millis: asked.millis, turn: 0 });
  const { millis } = pick;
  const [loaded, setLoaded] = useState<Loaded>({ state: "loading" });
  const drawn = useRef<typeof pick | null>(null);
  // Read from the request alone, so the window's blocks can be counted against them as it loads.
  const thresholds = useMemo(() => charts.map((chart) => thresholdOf(typed, chart.series)), [typed, charts]);
  const pair = useMemo(() => sharePair(charts.map((chart) => chart.series)), [charts]);

  useEffect(() => {
    // A reconnect reads again only what is not yet drawn, and leaves a drawn chart at its own time.
    if (!canRead || drawn.current === pick) return;
    const hostKeys = charts.filter((chart) => chart.series.source === "host").map((chart) => chart.series.key);
    const tip = live.get("summary", "completed_slot");
    if (perSlot && tip === undefined) return;
    let stale = false;
    // Row times are the validator's clock. Both ends are whole host steps, so every chart of the
    // request covers the same span.
    const now = Date.now() - (live.getClockOffset() ?? 0);
    const to = Math.floor(now / HOST_STEP_MILLIS) * HOST_STEP_MILLIS;
    const from = Math.floor((to - pick.millis) / HOST_STEP_MILLIS) * HOST_STEP_MILLIS;
    setLoaded({ state: "loading" });
    // One read of each source for every chart of the request.
    const rows =
      perSlot && tip !== undefined
        ? readWindow((first, size) => live.request("slot.range", { first_slot: first, count: size }), tip, from)
        : null;
    const host =
      hostKeys.length > 0 ? live.request("host.range", { series: hostKeys, from, to, points: BIN_COUNT }) : null;
    Promise.all([rows, host])
      .then(([slotRows, range]) => {
        if (stale) return;
        // A refusal comes back as a value too, without the series.
        const readings = range && Array.isArray(range.series) ? range : null;
        if (host !== null && readings === null) {
          setLoaded({ state: "failed" });
          return;
        }
        const bins = charts.map(({ series }, index) => {
          const compared = thresholds[index];
          const fits = compared && !("wrong" in compared) ? compared : null;
          if (series.source === "host") return readings && hostBins(readings, series);
          const bins = slotRows && bucket(slotRows.rows, series, from, to, BIN_COUNT, fits?.value, fits?.inclusive);
          // A rate's line and figures leave out spans that held blocks for part of their seconds.
          return bins && series.rate ? onWholeSpans(bins) : bins;
        });
        const whole =
          pair?.byRule && slotRows && pair.whole.source === "slots" ? bucket(slotRows.rows, pair.whole, from, to) : null;
        drawn.current = pick;
        setLoaded(
          bins.some((held) => held !== null)
            ? {
                state: "ready",
                millis: pick.millis,
                from,
                bins,
                slots: slotRows && { count: slotRows.rows.length, reached: slotRows.reached },
                host: readings && { stepMillis: readings.step_ms, heldFrom: readings.held_from },
                whole,
              }
            : { state: "empty" },
        );
      })
      .catch(() => {
        if (!stale) setLoaded({ state: "failed" });
      });
    return () => {
      stale = true;
    };
  }, [canRead, live, pick, charts, perSlot, thresholds, pair]);

  const read = operation === "pending" ? null : operation;
  const worked = useMemo(() => {
    if (loaded.state !== "ready" || read === null) return { panels: charts.map(() => null), share: null };
    const base: Omit<FigureContext, "threshold"> = {
      window: windowLabel(loaded.millis),
      comparing: comparing(typed),
      clock: clockTime,
    };
    const panels = charts.map((chart, index) =>
      answerFigure(directed(read, typed, chart.series), chart.series, loaded.bins[index] ?? null, {
        ...base,
        threshold: thresholds[index],
      }),
    );
    let share: Worked = null;
    if (read === "share") {
      const part = pair ? (loaded.bins[charts.findIndex((chart) => chart.series === pair.part)] ?? null) : null;
      const whole = pair
        ? pair.byRule
          ? loaded.whole
          : (loaded.bins[charts.findIndex((chart) => chart.series === pair.whole)] ?? null)
        : null;
      share = shareFigure(pair, part, whole, { ...base, threshold: null });
    }
    return { panels, share };
  }, [loaded, read, charts, thresholds, pair, typed]);

  const choose = (next: number): void => setPick((held) => ({ millis: next, turn: held.turn + 1 }));
  const own = WINDOWS.some((entry) => entry.millis === asked.millis);
  const waiting = loaded.state === "ready" ? "empty" : loaded.state === "loading" && !canRead ? "offline" : loaded.state;
  return (
    <section className="card ask-answer">
      <div className="ask-head">
        <p className="ask-asked">{typed}</p>
        {toggle}
        <div className="toggle-group ask-windows" role="group" aria-label="Window">
          {!own && (
            <button type="button" aria-pressed={asked.millis === millis} onClick={() => choose(asked.millis)}>
              {windowShort(asked.millis)}
            </button>
          )}
          {WINDOWS.map((entry) => (
            <button key={entry.label} type="button" aria-pressed={entry.millis === millis} onClick={() => choose(entry.millis)}>
              {entry.label}
            </button>
          ))}
        </div>
      </div>
      {note !== null && (
        <p className="ask-note" role="status">
          {note}
        </p>
      )}
      <p className="ask-announce" role="status">
        {announcement(worked)}
      </p>
      {worked.share && <FigureLine worked={worked.share} />}
      <div className="ask-charts">
        {charts.map((chart, index) => (
          <SeriesPanel
            key={chart.series.key}
            chart={chart}
            bins={loaded.state === "ready" ? loaded.bins[index] : null}
            waiting={WAITING[waiting]}
            announce={loaded.state === "ready" || index === 0}
            operation={directed(read, typed, chart.series)}
            threshold={thresholds[index]}
            worked={worked.panels[index]}
          />
        ))}
      </div>
      <p className="ask-source">
        {[
          ...(loaded.state === "ready" ? sources(loaded, loaded.millis) : []),
          modelLine(pickedMillis, operation, operationMillis),
          ...(loaded.state === "ready" ? shortfalls(loaded) : []),
          ...(asked.clamped ? ["windows run from 5 minutes to 6 hours"] : []),
        ].join(" · ")}
      </p>
    </section>
  );
}

type Ready = Extract<Loaded, { state: "ready" }>;

const NOT_OURS = "Not measured for our own blocks; this is every leader's.";

/** A window over our blocks: this epoch or the last by slot, or a span of time. */
type OwnPick = { window: "this" | "last" | number; turn: number };

type OwnLoaded =
  | { state: "loading" }
  | { state: "failed" }
  | {
      state: "ready";
      label: string;
      /** The epoch the window is, or null for a span of time. */
      epoch: number | null;
      current: boolean;
      blocks: readonly BlockHead[];
      /** False where the block list ran out before the window began. */
      reached: boolean;
      counts: LeaderSlotCounts | undefined;
      /** The This epoch card's line, or null where the card would not show it. */
      earnings: EpochEarnings | null;
      rates: TipRates | undefined;
    };

const EPOCH_WINDOWS: readonly { key: "this" | "last"; label: string }[] = [
  { key: "this", label: "This epoch" },
  { key: "last", label: "Last epoch" },
];

/** The fees series, which a request for what we earned answers when the model picked none. */
const FEES = SERIES.find((series) => series.key === "fees");

/** Figures over the blocks this validator built and the cluster kept, from the Slots page's list. */
function OwnAnswer({ typed, answer, toggle }: { typed: string; answer: ChartsAnswered; toggle: ReactElement | null }): ReactElement {
  const { charts, asked, pickedMillis, operation, operationMillis } = answer;
  const live = useRequests();
  const canRead = useCanRead(live, false);
  const hasEpoch = useSyncExternalStore(live.subscribe, () => live.get("epoch", "new") !== undefined);
  const balancesHidden = useBalancesHidden();
  // An epoch or window the request named stands; otherwise this epoch.
  const [pick, setPick] = useState<OwnPick>(() => ({
    window: epochAsked(typed) ?? (asked.given ? asked.millis : "this"),
    turn: 0,
  }));
  const [loaded, setLoaded] = useState<OwnLoaded>({ state: "loading" });
  const drawn = useRef<OwnPick | null>(null);
  const earned = asksEarned(typed);
  const shown = useMemo(() => {
    const own = charts.filter((chart) => OWN_KEYS.has(chart.series.key)).map((chart) => chart.series);
    return own.length === 0 && earned && FEES ? [FEES] : own;
  }, [charts, earned]);
  const others = charts.filter((chart) => !OWN_KEYS.has(chart.series.key)).map((chart) => chart.series.label);
  const thresholds = useMemo(() => shown.map((series) => ownThresholdOf(typed, series)), [typed, shown]);

  useEffect(() => {
    if (!canRead || drawn.current === pick) return;
    const info = live.get("epoch", "new");
    let window: OwnWindow;
    let label: string;
    let epoch: number | null = null;
    if (typeof pick.window === "number") {
      const now = Date.now() - (live.getClockOffset() ?? 0);
      window = { kind: "time", from: now - pick.window, to: now };
      label = windowLabel(pick.window);
    } else {
      if (!info) return;
      const last = pick.window === "last";
      epoch = last ? info.epoch - 1 : info.epoch;
      const first = last ? info.start_slot - info.slots_in_epoch : info.start_slot;
      window = { kind: "epoch", epoch, first, last: last ? info.start_slot - 1 : info.end_slot };
      label = last ? `epoch ${count(epoch)}` : `epoch ${count(epoch)} so far`;
    }
    let stale = false;
    setLoaded({ state: "loading" });
    readOwnBlocks((before) => live.request("produced.figures", before === undefined ? {} : { before }), window)
      .then((read) => {
        if (stale) return;
        drawn.current = pick;
        const admission = live.get("summary", "admission");
        const seated = !admission || admission.seat;
        setLoaded({
          state: "ready",
          label,
          epoch,
          current: epoch !== null && epoch === info?.epoch,
          blocks: read.blocks,
          reached: read.reached,
          counts: epoch === null ? undefined : live.get("summary", "leader_slot_counts")?.find((held) => held.epoch === epoch),
          earnings: seated ? (live.get("summary", "epoch_earnings") ?? null) : null,
          rates: live.get("summary", "tip_rates"),
        });
      })
      .catch(() => {
        if (!stale) setLoaded({ state: "failed" });
      });
    return () => {
      stale = true;
    };
  }, [canRead, hasEpoch, live, pick]);

  const ready = loaded.state === "ready" ? loaded : null;
  const short = ready
    ? ready.epoch === null
      ? ready.reached
        ? null
        : "the block list does not reach back that far"
      : heldShortLabel(ready.blocks.length, ready.counts)
    : null;
  const worked = useMemo(() => {
    if (!ready || operation === "pending") return shown.map(() => null);
    return shown.map((series, index) => {
      const directedOp = directed(operation, typed, series);
      // What we earned is a total where nothing else was asked.
      const op = earned && (directedOp === null || directedOp === "chart") ? "total" : directedOp;
      return ownFigure(op, series, ready.blocks, ready.rates, {
        window: ready.label,
        threshold: thresholds[index],
        clock: clockTime,
        short,
      });
    });
  }, [ready, operation, shown, typed, earned, thresholds, short]);
  const readings = useMemo(
    () => (ready ? shown.map((series) => ownReadings(ready.blocks, series.key, ready.rates)) : null),
    [ready, shown],
  );
  const headlines: Worked[] = [];
  if (ready && asksProduction(typed)) {
    headlines.push(
      ready.epoch === null
        ? { unfit: "Leader slot counts are kept by epoch: This epoch or Last epoch gives them." }
        : production(ready.counts, { window: ready.label, short: null }),
    );
  }
  if (ready && earned && ready.current && !balancesHidden) headlines.push(earningsLine(ready.earnings, ready.epoch));

  const choose = (next: OwnPick["window"]): void => setPick((held) => ({ window: next, turn: held.turn + 1 }));
  const named = asked.given && !WINDOWS.some((entry) => entry.millis === asked.millis);
  const waiting = loaded.state === "failed" ? WAITING.failed : canRead ? WAITING.loading : WAITING.offline;
  return (
    <section className="card ask-answer">
      <div className="ask-head">
        <p className="ask-asked">{typed}</p>
        {toggle}
        <div className="toggle-group ask-windows" role="group" aria-label="Epoch">
          {EPOCH_WINDOWS.map((entry) => (
            <button key={entry.key} type="button" aria-pressed={pick.window === entry.key} onClick={() => choose(entry.key)}>
              {entry.label}
            </button>
          ))}
        </div>
        <div className="toggle-group ask-windows" role="group" aria-label="Window">
          {named && (
            <button type="button" aria-pressed={pick.window === asked.millis} onClick={() => choose(asked.millis)}>
              {windowShort(asked.millis)}
            </button>
          )}
          {WINDOWS.map((entry) => (
            <button key={entry.label} type="button" aria-pressed={pick.window === entry.millis} onClick={() => choose(entry.millis)}>
              {entry.label}
            </button>
          ))}
        </div>
      </div>
      {others.length > 0 && (
        <p className="ask-note" role="status">
          {others.join(", ")}: not measured for our own blocks; Every leader shows {others.length === 1 ? "it" : "them"}.
        </p>
      )}
      <p className="ask-announce" role="status">
        {announcement({ panels: worked, share: null })}
        {headlines.length > 0 ? ` ${announcement({ panels: headlines, share: null })}` : ""}
      </p>
      {headlines.map((headline, index) => (
        <FigureLine key={index} worked={headline} />
      ))}
      {shown.length > 0 && (
        <div className="ask-charts">
          {shown.map((series, index) => (
            <OwnPanel
              key={series.key}
              series={series}
              readings={readings?.[index] ?? null}
              waiting={ready ? ownEmpty(series, ready.blocks.length, ready.label) : waiting}
              worked={worked[index]}
              threshold={thresholds[index]}
            />
          ))}
        </div>
      )}
      {shown.length === 0 && !ready && <p className="ask-plain">{waiting}</p>}
      <p className="ask-source">
        {[
          ready ? `${count(ready.blocks.length)} of our blocks from the Slots page's list, ${ready.label}` : null,
          modelLine(pickedMillis, operation, operationMillis),
          short,
          asked.clamped ? "windows run from 5 minutes to 6 hours" : null,
        ]
          .filter(Boolean)
          .join(" · ")}
      </p>
    </section>
  );
}

/** One series over our blocks: what it was read as, the figure, its tiles and a bar per block. */
function OwnPanel({
  series,
  readings,
  waiting,
  worked,
  threshold,
}: {
  series: Series;
  readings: readonly OwnReading[] | null;
  waiting: string;
  worked: Worked;
  threshold: ThresholdRead;
}): ReactElement {
  const as = ownLabel(series);
  const held = readings && readings.length > 0 ? readings : null;
  const fits = threshold && !("wrong" in threshold) ? threshold.value : undefined;
  const tiles = useMemo(() => {
    if (!held) return null;
    let low = held[0];
    let high = held[0];
    let sum = 0;
    for (const reading of held) {
      if (reading.value < low.value) low = reading;
      if (reading.value > high.value) high = reading;
      sum += reading.value;
    }
    return { latest: held[held.length - 1].value, mean: sum / held.length, low: low.value, high: high.value };
  }, [held]);
  return (
    <div className="ask-series">
      <div className="ask-read">
        <span className="ask-read-label">Read as</span>
        <span className="ask-read-series">
          {as.head} {as.unit && <span className="ask-read-unit">{as.unit}</span>}
        </span>
      </div>
      {held && <FigureLine worked={worked} />}
      {held && tiles ? (
        <>
          <div className="ask-figures">
            <OwnTile label="Latest block" value={tiles.latest} unit={as.unit} />
            <OwnTile label="Mean per block" value={tiles.mean} unit={as.unit} />
            <OwnTile label="Lowest block" value={tiles.low} unit={as.unit} />
            <OwnTile label="Highest block" value={tiles.high} unit={as.unit} />
          </div>
          <OwnChart readings={held} unit={as.unit} threshold={fits} />
        </>
      ) : (
        <p className="ask-plain ask-waiting" role="status">
          {waiting}
        </p>
      )}
    </div>
  );
}

function OwnTile({ label, value, unit }: { label: string; value: number; unit: string }): ReactElement {
  return (
    <div className="ask-figure">
      <span className="ask-figure-label">{label}</span>
      <span className="ask-figure-value">{ownAmount(value, unit)}</span>
    </div>
  );
}

/** A bar per block of ours, oldest first, spaced evenly whatever the time between them. */
const OwnChart = memo(function OwnChart({
  readings,
  unit,
  threshold,
}: {
  readings: readonly OwnReading[];
  unit: string;
  threshold: number | undefined;
}): ReactElement {
  const max = readings.reduce((high, reading) => Math.max(high, reading.value), 0);
  const shown = threshold !== undefined && threshold > 0 && threshold <= max * 4 ? threshold : undefined;
  const top = ceiling(Math.max(max, shown ?? 0));
  const slot = WIDTH / readings.length;
  // Bars under three units wide touch; a gap there would only thin them.
  const gap = slot > 3 ? Math.min(2, slot / 4) : 0;
  const y = (value: number): number => HEIGHT - (Math.max(0, value) / top) * HEIGHT;
  return (
    <div className="ask-chart">
      <div className="ask-plot">
        {[top, top / 2, 0].map((tick) => (
          <span key={tick} className="ask-tick" style={{ top: `${(1 - tick / top) * 100}%` }}>
            {ownAmount(tick, unit)}
          </span>
        ))}
        <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none" role="img" aria-label="A bar per block of ours">
          <line className="ask-grid" x1={0} x2={WIDTH} y1={0.5} y2={0.5} />
          <line className="ask-grid" x1={0} x2={WIDTH} y1={HEIGHT / 2} y2={HEIGHT / 2} />
          {readings.map((reading, index) => (
            <rect
              key={reading.slot}
              className="ask-bar"
              x={index * slot + gap / 2}
              y={y(reading.value)}
              width={slot - gap}
              height={HEIGHT - y(reading.value)}
            />
          ))}
          <line className="ask-base" x1={0} x2={WIDTH} y1={HEIGHT - 0.5} y2={HEIGHT - 0.5} />
          {shown !== undefined && <line className="ask-threshold" x1={0} x2={WIDTH} y1={y(shown)} y2={y(shown)} />}
        </svg>
        {shown !== undefined && (
          <span className="ask-threshold-label" aria-hidden="true" style={{ top: `${(y(shown) / HEIGHT) * 100}%` }}>
            {ownAmount(shown, unit)}
          </span>
        )}
      </div>
      <div className="ask-x" aria-hidden="true">
        <span>slot {figure(readings[0].slot)}</span>
        <span>slot {figure(readings[readings.length - 1].slot)}</span>
      </div>
    </div>
  );
});

/** What the model read and how long it took, as the source line says it. */
function modelLine(pickedMillis: number, operation: Operation | null | "pending", operationMillis: number | null): string {
  const seconds = (millis: number): string => `${(millis / 1000).toFixed(1)} s`;
  if (operation === "pending") return `series chosen by Chrome's built-in model in ${seconds(pickedMillis)} · reading the operation…`;
  if (operation === null) return `series chosen by Chrome's built-in model in ${seconds(pickedMillis)} · the operation was not read`;
  return `series and operation read by Chrome's built-in model in ${seconds(Math.max(pickedMillis, operationMillis ?? 0))}`;
}

/** The figures of a card as one line, for a screen reader to say when they arrive. */
function announcement(worked: { panels: readonly Worked[]; share: Worked }): string {
  const line = (one: Worked): string | null =>
    one === null ? null : "unfit" in one ? one.unfit : [one.label, one.value].filter(Boolean).join(": ");
  return [worked.share, ...worked.panels].map(line).filter(Boolean).join(". ");
}

/** A worked-out figure above a chart, or why there is none. */
function FigureLine({ worked }: { worked: Worked }): ReactElement | null {
  if (worked === null) return null;
  if ("unfit" in worked) return <p className="ask-unfit">{worked.unfit}</p>;
  return (
    <div className={`ask-headline${worked.warn ? " is-warn" : ""}`}>
      <span className="ask-headline-label">{worked.label}</span>
      {worked.value && <span className="ask-headline-value">{worked.value}</span>}
      {worked.detail && <span className="ask-headline-detail">{worked.detail}</span>}
    </div>
  );
}

/** A host step's length, in seconds below a minute. */
function stepLabel(millis: number): string {
  const seconds = Math.round(millis / 1000);
  return seconds < 60 ? `${seconds} s` : `${figure(seconds / 60)} min`;
}

/** Where a drawn answer's readings came from, as the source line opens. */
function sources(loaded: Ready, millis: number): string[] {
  const parts: string[] = [];
  if (loaded.slots) parts.push(`${count(loaded.slots.count)} slots from the slot history`);
  if (loaded.host) parts.push(`host readings in ${stepLabel(loaded.host.stepMillis)} steps`);
  return [`${parts.join(" and ")}, ${windowLabel(millis)}`];
}

/** What a drawn answer's window asked for and the server does not hold. */
function shortfalls(loaded: Ready): string[] {
  const parts: string[] = [];
  if (loaded.slots && !loaded.slots.reached) parts.push("the slot history does not reach back that far");
  if (loaded.host) {
    const { heldFrom } = loaded.host;
    if (heldFrom === null) parts.push("no host readings are held yet");
    else if (heldFrom > loaded.from) parts.push(`host readings begin ${clockTime(heldFrom)}`);
  }
  return parts;
}

/** One series of a request: what it was read as and worked out, then its figures and chart, or why
 *  there are none. */
function SeriesPanel({
  chart,
  bins,
  waiting,
  announce,
  operation,
  threshold,
  worked,
}: {
  chart: Charted;
  bins: Bins | null;
  waiting: string;
  /** False where a neighbouring panel already says the same of the whole answer. */
  announce: boolean;
  operation: Operation | null;
  threshold: ThresholdRead;
  worked: Worked;
}): ReactElement {
  const { series, closest } = chart;
  const as = readAs(operation, series, threshold);
  const mark = worked && !("unfit" in worked) ? worked.mark : undefined;
  // A per-block series' tiles come from the blocks, so they agree with a figure above them.
  const blocks = series.source === "slots" && !series.rate ? bins?.blocks : undefined;
  return (
    <div className="ask-series">
      <div className="ask-read">
        <span className="ask-read-label">Read as</span>
        <span className="ask-read-series">
          {as.head} {as.unit && <span className="ask-read-unit">{as.unit}</span>}
        </span>
        {closest && <span className="ask-closest">closest match</span>}
      </div>
      {bins && <FigureLine worked={worked} />}
      {bins ? (
        <>
          <div className="ask-figures">
            <Figure label="Latest" value={bins.latest} />
            <Figure label={blocks ? "Mean per block" : "Mean"} value={blocks ? blocks.mean : bins.mean} />
            <Figure label={blocks ? "Lowest block" : "Lowest"} value={blocks ? blocks.low.value : bins.min} />
            {bins.peak === undefined ? (
              <Figure label={blocks ? "Highest block" : "Highest"} value={blocks ? blocks.high.value : bins.max} />
            ) : (
              <Figure label="Peak, 1 s" value={bins.peak} />
            )}
          </div>
          <Chart bins={bins} mark={mark} />
        </>
      ) : (
        <p className="ask-plain ask-waiting" role={announce ? "status" : undefined}>
          {waiting}
        </p>
      )}
    </div>
  );
}

function Figure({ label, value }: { label: string; value: number }): ReactElement {
  return (
    <div className="ask-figure">
      <span className="ask-figure-label">{label}</span>
      <span className="ask-figure-value">{figure(value)}</span>
    </div>
  );
}

const WIDTH = 600;
const HEIGHT = 150;

const Chart = memo(function Chart({ bins, mark }: { bins: Bins; mark?: Mark }): ReactElement {
  // A value far off the line is told in words, not drawn.
  const shown = mark && mark.threshold > 0 && mark.threshold <= bins.max * 4 ? mark : undefined;
  const top = ceiling(Math.max(bins.max, shown?.threshold ?? 0));
  const { line, area } = paths(bins.values, top, WIDTH, HEIGHT);
  const step = bins.values.length > 1 ? WIDTH / (bins.values.length - 1) : 0;
  const x = (time: number): number => Math.min(WIDTH, Math.max(0, ((time - bins.from) / bins.binMillis) * step));
  const y = (value: number): number => HEIGHT - (value / top) * HEIGHT;
  return (
    <div className="ask-chart">
      <div className="ask-plot">
        {[top, top / 2, 0].map((tick) => (
          <span key={tick} className="ask-tick" style={{ top: `${(1 - tick / top) * 100}%` }}>
            {figure(tick)}
          </span>
        ))}
        <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} preserveAspectRatio="none" role="img" aria-label="The readings over the window">
          <line className="ask-grid" x1={0} x2={WIDTH} y1={0.5} y2={0.5} />
          <line className="ask-grid" x1={0} x2={WIDTH} y1={HEIGHT / 2} y2={HEIGHT / 2} />
          <line className="ask-base" x1={0} x2={WIDTH} y1={HEIGHT - 0.5} y2={HEIGHT - 0.5} />
          {mark?.spans.map((span) => (
            <rect
              key={span.start}
              className="ask-span"
              x={Math.min(x(span.start), WIDTH - 1)}
              y={0}
              width={Math.max(1, x(span.end) - x(span.start))}
              height={HEIGHT}
            />
          ))}
          <path className="ask-area" d={area} />
          <path className="ask-line" d={line} />
          {shown && <line className="ask-threshold" x1={0} x2={WIDTH} y1={y(shown.threshold)} y2={y(shown.threshold)} />}
        </svg>
        {shown && (
          <span className="ask-threshold-label" aria-hidden="true" style={{ top: `${(y(shown.threshold) / HEIGHT) * 100}%` }}>
            {figure(shown.threshold)}
          </span>
        )}
      </div>
      <div className="ask-x" aria-hidden="true">
        <span>{clockTime(bins.from)}</span>
        <span>{clockTime((bins.from + bins.to) / 2)}</span>
        <span>{clockTime(bins.to)}</span>
      </div>
    </div>
  );
});
