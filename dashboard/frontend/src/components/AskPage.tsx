/** One box that takes a request in plain words and answers it with a chart from the slot history.
 *  The browser's own model picks the series; nothing typed is sent to the validator. */

import { memo, useEffect, useRef, useState, useSyncExternalStore, type FormEvent, type ReactElement } from "react";
import {
  asksWhy,
  bucket,
  ceiling,
  figure,
  leftOut,
  namesSeries,
  notDrawnNote,
  paths,
  readWindow,
  recordedList,
  SERIES,
  windowLabel,
  windowOf,
  WINDOWS,
  windowShort,
  type AskedWindow,
  type Bins,
  type Series,
} from "../ask";
import { askModel, useAskModel, type AskModelSnapshot } from "../askModel";
import { clockTime, count } from "../format";
import type { Store } from "../store";
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
  "Failed transactions over the past 6 hours",
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
    if (ready) askModel.warm();
  }, [ready]);
  useEffect(() => () => stop.current?.abort(), []);

  const busy = items.some((item) => item.answer.kind === "asking");
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
    void askModel.pickSeries(request, controller.signal).then((picked) => {
      if (picked.kind === "series") {
        settle({
          kind: "charts",
          charts: picked.series.map((series) => ({ series, closest: !namesSeries(request, series) })),
          asked: windowOf(request),
          pickedMillis: performance.now() - began,
          note: notDrawnNote(leftOut(request, picked.series, SERIES), picked.unmatched),
        });
      } else if (picked.kind === "none") settle({ kind: "none" });
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
            placeholder="Ask for a chart"
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
  if (answer.kind === "charts") return <ChartsAnswer typed={typed} answer={answer} />;
  return (
    <section className="card ask-answer">
      <p className="ask-asked">{typed}</p>
      <p className="ask-plain" role="status">
        {PLAIN[answer.kind === "failed" ? answer.reason : answer.kind]}
      </p>
      {answer.kind === "none" && <p className="ask-source">Recorded per slot: {RECORDED}.</p>}
    </section>
  );
}

const RECORDED = recordedList(SERIES);

const PLAIN: Record<"asking" | "why" | "none" | "timeout" | "model", string> = {
  asking: "Asking Chrome's model…",
  why: "This version only draws charts.",
  none: "Nothing this validator records matches that.",
  timeout: "Chrome's model took too long.",
  model: "Chrome's model did not answer.",
};

type Loaded =
  | { state: "loading" }
  | { state: "empty" }
  | { state: "failed" }
  | { state: "ready"; bins: readonly (Bins | null)[]; slots: number; reached: boolean };

/** Whether the validator can be asked now; a boolean, so a card redraws only when it flips. */
function useCanRead(live: Store): boolean {
  const read = (): boolean => live.getConnection() === "open" && live.get("summary", "completed_slot") !== undefined;
  return useSyncExternalStore(live.subscribe, read, read);
}

const WAITING: Record<"offline" | "loading" | "empty" | "failed", string> = {
  offline: "Waiting for the validator…",
  loading: "Reading the slot history…",
  empty: "No readings in that window.",
  failed: "The validator did not answer.",
};

function ChartsAnswer({ typed, answer }: { typed: string; answer: Extract<Answered, { kind: "charts" }> }): ReactElement {
  const { charts, asked, pickedMillis, note } = answer;
  // Always the live validator: a request for the past hour means the hour up to now.
  const live = useRequests();
  const canRead = useCanRead(live);
  // A window and how often it was picked: picking the one shown reads it again.
  const [pick, setPick] = useState({ millis: asked.millis, turn: 0 });
  const { millis } = pick;
  const [loaded, setLoaded] = useState<Loaded>({ state: "loading" });
  const drawn = useRef<typeof pick | null>(null);

  useEffect(() => {
    // A reconnect reads again only what is not yet drawn, and leaves a drawn chart at its own time.
    if (!canRead || drawn.current === pick) return;
    const tip = live.get("summary", "completed_slot");
    if (tip === undefined) return;
    let stale = false;
    // Row times are the validator's clock.
    const to = Date.now() - (live.getClockOffset() ?? 0);
    setLoaded({ state: "loading" });
    // One read for every chart of the request, so they share a time axis.
    readWindow((first, size) => live.request("slot.range", { first_slot: first, count: size }), tip, to - pick.millis)
      .then(({ rows, reached }) => {
        if (stale) return;
        const bins = charts.map((chart) => bucket(rows, chart.series, to - pick.millis, to));
        drawn.current = pick;
        setLoaded(bins.some((held) => held !== null) ? { state: "ready", bins, slots: rows.length, reached } : { state: "empty" });
      })
      .catch(() => {
        if (!stale) setLoaded({ state: "failed" });
      });
    return () => {
      stale = true;
    };
  }, [canRead, live, pick, charts]);

  const choose = (next: number): void => setPick((held) => ({ millis: next, turn: held.turn + 1 }));
  const own = WINDOWS.some((entry) => entry.millis === asked.millis);
  const waiting = loaded.state === "ready" ? "empty" : loaded.state === "loading" && !canRead ? "offline" : loaded.state;
  return (
    <section className="card ask-answer">
      <div className="ask-head">
        <p className="ask-asked">{typed}</p>
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
      <div className="ask-charts">
        {charts.map((chart, index) => (
          <SeriesPanel
            key={chart.series.key}
            chart={chart}
            bins={loaded.state === "ready" ? loaded.bins[index] : null}
            waiting={WAITING[waiting]}
            announce={loaded.state === "ready" || index === 0}
          />
        ))}
      </div>
      <p className="ask-source">
        {loaded.state === "ready" && `${count(loaded.slots)} slots from the slot history, ${windowLabel(millis)} · `}
        series chosen by Chrome's built-in model in {(pickedMillis / 1000).toFixed(1)} s
        {loaded.state === "ready" && !loaded.reached && " · the history does not reach back that far"}
        {asked.clamped && " · windows run from 5 minutes to 6 hours"}
      </p>
    </section>
  );
}

/** One series of a request: what it was read as, then its figures and chart, or why there are none. */
function SeriesPanel({
  chart,
  bins,
  waiting,
  announce,
}: {
  chart: Charted;
  bins: Bins | null;
  waiting: string;
  /** False where a neighbouring panel already says the same of the whole answer. */
  announce: boolean;
}): ReactElement {
  const { series, closest } = chart;
  return (
    <div className="ask-series">
      <div className="ask-read">
        <span className="ask-read-label">Read as</span>
        <span className="ask-read-series">
          {series.label} <span className="ask-read-unit">{series.unit}</span>
        </span>
        {closest && <span className="ask-closest">closest match</span>}
      </div>
      {bins ? (
        <>
          <div className="ask-figures">
            <Figure label="Latest" value={bins.latest} />
            <Figure label="Mean" value={bins.mean} />
            <Figure label="Lowest" value={bins.min} />
            <Figure label="Highest" value={bins.max} />
          </div>
          <Chart bins={bins} />
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

const Chart = memo(function Chart({ bins }: { bins: Bins }): ReactElement {
  const top = ceiling(bins.max);
  const { line, area } = paths(bins.values, top, WIDTH, HEIGHT);
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
          <path className="ask-area" d={area} />
          <path className="ask-line" d={line} />
        </svg>
      </div>
      <div className="ask-x" aria-hidden="true">
        <span>{clockTime(bins.from)}</span>
        <span>{clockTime((bins.from + bins.to) / 2)}</span>
        <span>{clockTime(bins.to)}</span>
      </div>
    </div>
  );
});
