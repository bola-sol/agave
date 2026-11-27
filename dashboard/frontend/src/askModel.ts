/** The browser's built-in language model, as the Ask page uses it: whether it can be used here, and
 *  one question at a time put to it. Nothing typed leaves the browser. */

import { useSyncExternalStore } from "react";
import { parseReply, SERIES, seriesSchema, systemPrompt, type Series } from "./ask";
import { OPERATION_PROMPT, operationSchema, parseOperation, type Operation } from "./askFigures";

interface PromptOptions {
  responseConstraint?: object;
  signal?: AbortSignal;
}

export interface ModelSession {
  prompt(text: string, options?: PromptOptions): Promise<string>;
  clone(): Promise<ModelSession>;
  destroy(): void;
}

interface Languages {
  expectedInputs: { type: "text"; languages: string[] }[];
  expectedOutputs: { type: "text"; languages: string[] }[];
}

interface CreateOptions extends Languages {
  initialPrompts: { role: "system"; content: string }[];
  monitor?: (monitor: EventTarget) => void;
}

export interface LanguageModelApi {
  availability(options: Languages): Promise<string>;
  create(options: CreateOptions): Promise<ModelSession>;
}

/** The same for the check and the session, or the check answers for a different model. */
const LANGUAGES: Languages = {
  expectedInputs: [{ type: "text", languages: ["en"] }],
  expectedOutputs: [{ type: "text", languages: ["en"] }],
};

export type AskState = "checking" | "ready" | "downloadable" | "downloading" | "unavailable";

export interface AskModelSnapshot {
  state: AskState;
  /** Of a download this page started, from nought to one. */
  progress: number | null;
  downloadFailed: boolean;
}

export function modelState(hasApi: boolean, secure: boolean, availability: string | null): AskState {
  if (!hasApi || !secure) return "unavailable";
  switch (availability) {
    case null:
      return "checking";
    case "available":
      return "ready";
    case "downloadable":
    case "downloading":
      return availability;
    default:
      return "unavailable";
  }
}

/** The Ask tab shows wherever the model is usable now or after a download the page can start. */
export function tabShown(state: AskState): boolean {
  return state === "ready" || state === "downloadable" || state === "downloading";
}

export type Picked =
  | { kind: "series"; series: Series[]; unmatched: boolean }
  | { kind: "none" }
  | { kind: "failed"; reason: "timeout" | "model" | "stopped" };

export const PROMPT_TIMEOUT_MS = 10_000;
/** The first prompt also waits for the session and the model to load. */
export const FIRST_PROMPT_TIMEOUT_MS = 30_000;
/** The operation runs after the series pick on the device, so its wait covers both. */
export const OPERATION_TIMEOUT_MS = 8_000;
/** How often a download started elsewhere is looked at again. */
const RECHECK_MS = 5_000;

const SCHEMA = seriesSchema(SERIES);
const OPERATION_SCHEMA = operationSchema();

function aborted(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    const fail = () => reject(new Error("aborted"));
    if (signal.aborted) fail();
    else signal.addEventListener("abort", fail, { once: true });
  });
}

export interface AskModel {
  subscribe: (listener: () => void) => () => void;
  get: () => AskModelSnapshot;
  /** Opens the session ahead of the first request. */
  warm: () => void;
  pickSeries: (text: string, stop?: AbortSignal) => Promise<Picked>;
  /** Opens the operation's own session ahead of the first request. */
  warmFigures: () => void;
  /** The operation a request asks for, or null where it could not be read; never an error. */
  pickOperation: (text: string, stop?: AbortSignal) => Promise<Operation | null>;
  /** Called from a click, which the browser requires before it fetches the model. */
  download: () => void;
}

type Asked = { reply: string } | { failed: "timeout" | "model" | "stopped" };

/** One session with its prompt and schema, kept open and copied per request. */
interface Lane {
  hold: (opening: Promise<ModelSession>) => Promise<ModelSession>;
  drop: () => void;
  warm: () => void;
  /** One request, with one new session tried after an error before giving up. */
  run: (text: string, stop: AbortSignal | undefined) => Promise<Asked>;
}

function lane(
  create: () => Promise<ModelSession>,
  schema: object,
  waits: { first: number; later: number },
): Lane {
  let base: Promise<ModelSession> | null = null;
  let opened = false;
  let prompted = false;

  const hold = (opening: Promise<ModelSession>): Promise<ModelSession> => {
    opened = false;
    base = opening;
    opening.then(
      () => {
        if (base === opening) opened = true;
      },
      () => undefined,
    );
    return opening;
  };

  /** Forgets the session and closes it, now or once it opens. */
  const drop = (): void => {
    const old = base;
    base = null;
    opened = false;
    // The next session loads the model again, so its first prompt gets the longer wait.
    prompted = false;
    old?.then(
      (held) => held.destroy(),
      () => undefined,
    );
  };

  const session = (): Promise<ModelSession> => base ?? hold(create());

  const ask = async (text: string, signal: AbortSignal): Promise<string> => {
    const held = await session();
    // A copy per request, so one request's text never reaches the next and the context never fills.
    const copy = await held.clone();
    try {
      return await copy.prompt(text, { responseConstraint: schema, signal });
    } finally {
      copy.destroy();
    }
  };

  const attempt = async (text: string, stop: AbortSignal | undefined): Promise<Asked | "retry"> => {
    if (stop?.aborted) return { failed: "stopped" };
    const controller = new AbortController();
    let late = false;
    const timer = setTimeout(
      () => {
        late = true;
        controller.abort();
      },
      prompted ? waits.later : waits.first,
    );
    const halt = (): void => controller.abort();
    stop?.addEventListener("abort", halt);
    try {
      const work = ask(text, controller.signal);
      // Still running when the wait ends; its late failure is not this request's.
      work.catch(() => undefined);
      const reply = await Promise.race([work, aborted(controller.signal)]);
      prompted = true;
      return { reply };
    } catch {
      // A session that never opened would hold up every later request.
      if (late && !opened) drop();
      if (late) return { failed: "timeout" };
      if (stop?.aborted) return { failed: "stopped" };
      return "retry";
    } finally {
      clearTimeout(timer);
      stop?.removeEventListener("abort", halt);
    }
  };

  return {
    hold,
    drop,
    warm() {
      const opening = session();
      opening.catch(() => {
        if (base === opening) drop();
      });
    },
    async run(text, stop) {
      const first = await attempt(text, stop);
      if (first !== "retry") return first;
      // The browser can close a session; one new one is tried before giving up.
      drop();
      const second = await attempt(text, stop);
      if (second !== "retry") return second;
      drop();
      return { failed: "model" };
    },
  };
}

export function createAskModel(api: () => LanguageModelApi | undefined, secure: () => boolean): AskModel {
  let snapshot: AskModelSnapshot = { state: "checking", progress: null, downloadFailed: false };
  const listeners = new Set<() => void>();
  let started = false;
  let downloading = false;

  const set = (next: Partial<AskModelSnapshot>): void => {
    snapshot = { ...snapshot, ...next };
    for (const listener of listeners) listener();
  };

  const detect = async (): Promise<void> => {
    const model = api();
    let availability: string | null = null;
    if (model && secure()) {
      try {
        availability = await model.availability(LANGUAGES);
      } catch {
        availability = "unavailable";
      }
    }
    if (downloading) return;
    const state = modelState(model !== undefined, secure(), availability);
    if (state !== snapshot.state) set({ state });
    if (state === "downloading" && listeners.size > 0) setTimeout(() => void detect(), RECHECK_MS);
  };

  const create = (prompt: string, monitor?: (monitor: EventTarget) => void): Promise<ModelSession> => {
    const model = api();
    if (!model) return Promise.reject(new Error("no model"));
    return model.create({ ...LANGUAGES, initialPrompts: [{ role: "system", content: prompt }], monitor });
  };

  const series = lane(() => create(systemPrompt(SERIES)), SCHEMA, {
    first: FIRST_PROMPT_TIMEOUT_MS,
    later: PROMPT_TIMEOUT_MS,
  });
  const operations = lane(() => create(OPERATION_PROMPT), OPERATION_SCHEMA, {
    first: FIRST_PROMPT_TIMEOUT_MS,
    later: OPERATION_TIMEOUT_MS,
  });

  return {
    subscribe(listener) {
      listeners.add(listener);
      if (!started) {
        started = true;
        void detect();
      }
      return () => {
        listeners.delete(listener);
      };
    },
    get: () => snapshot,
    warm() {
      if (snapshot.state === "ready") series.warm();
    },
    warmFigures() {
      if (snapshot.state === "ready") operations.warm();
    },
    async pickSeries(text, stop) {
      const asked = await series.run(text, stop);
      if ("failed" in asked) return { kind: "failed", reason: asked.failed };
      const parsed = parseReply(asked.reply, SERIES);
      if (parsed === null) return { kind: "failed", reason: "model" };
      return parsed.series.length === 0 ? { kind: "none" } : { kind: "series", ...parsed };
    },
    async pickOperation(text, stop) {
      const asked = await operations.run(text, stop);
      return "failed" in asked ? null : parseOperation(asked.reply);
    },
    download() {
      if (snapshot.state !== "downloadable" || downloading) return;
      downloading = true;
      set({ state: "downloading", progress: 0, downloadFailed: false });
      const fetched = create(systemPrompt(SERIES), (monitor) => {
        monitor.addEventListener("downloadprogress", (event) => {
          const loaded = (event as ProgressEvent).loaded;
          if (downloading && Number.isFinite(loaded)) set({ progress: Math.min(1, Math.max(0, loaded)) });
        });
      });
      series.hold(fetched).then(
        () => {
          downloading = false;
          set({ state: "ready", progress: null });
        },
        () => {
          downloading = false;
          series.drop();
          set({ state: "downloadable", progress: null, downloadFailed: true });
        },
      );
    },
  };
}

export const askModel: AskModel = createAskModel(
  () => (globalThis as { LanguageModel?: LanguageModelApi }).LanguageModel,
  () => globalThis.isSecureContext === true,
);

export function useAskModel(): AskModelSnapshot {
  return useSyncExternalStore(askModel.subscribe, askModel.get, askModel.get);
}
