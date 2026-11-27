import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createAskModel,
  FIRST_PROMPT_TIMEOUT_MS,
  modelState,
  PROMPT_TIMEOUT_MS,
  tabShown,
  type AskState,
  type LanguageModelApi,
  type ModelSession,
} from "./askModel";

describe("modelState", () => {
  it("is unavailable without the API or outside a secure page", () => {
    expect(modelState(false, true, null)).toBe("unavailable");
    expect(modelState(true, false, "available")).toBe("unavailable");
  });

  it("is checking until the browser has answered", () => {
    expect(modelState(true, true, null)).toBe("checking");
  });

  it("follows the browser's answer", () => {
    expect(modelState(true, true, "available")).toBe("ready");
    expect(modelState(true, true, "downloadable")).toBe("downloadable");
    expect(modelState(true, true, "downloading")).toBe("downloading");
    expect(modelState(true, true, "unavailable")).toBe("unavailable");
    expect(modelState(true, true, "something new")).toBe("unavailable");
  });
});

describe("tabShown", () => {
  it("shows the tab where the model is usable now or after a download", () => {
    const shown = (["checking", "ready", "downloadable", "downloading", "unavailable"] as AskState[]).filter(tabShown);
    expect(shown).toEqual(["ready", "downloadable", "downloading"]);
  });
});

type Reply = string | Error | "hang";

/** A model whose copies answer with `replies` in turn, counting what is opened and closed. */
function fake(availability: string, replies: Reply[] = []) {
  const counts = { created: 0, cloned: 0, destroyed: 0, closed: 0, prompts: [] as string[] };
  const answers = { availability, stuck: 0 };
  let progress: ((loaded: number) => void) | null = null;
  let finish: (() => void) | null = null;
  let fail: (() => void) | null = null;
  const copy = (): ModelSession => ({
    prompt: (text, options) => {
      counts.prompts.push(text);
      const reply = replies.shift() ?? '{"series":["shreds"]}';
      if (reply instanceof Error) return Promise.reject(reply);
      if (reply !== "hang") return Promise.resolve(reply);
      return new Promise((_, reject) => {
        options?.signal?.addEventListener("abort", () => reject(new Error("aborted")));
      });
    },
    clone: () => Promise.reject(new Error("a copy is not copied")),
    destroy: () => {
      counts.destroyed += 1;
    },
  });
  const base: ModelSession = {
    prompt: () => Promise.reject(new Error("the base session is never prompted")),
    clone: () => {
      counts.cloned += 1;
      return Promise.resolve(copy());
    },
    destroy: () => {
      counts.closed += 1;
    },
  };
  const api: LanguageModelApi = {
    availability: () => Promise.resolve(answers.availability),
    create: (options) => {
      counts.created += 1;
      if (answers.stuck > 0) {
        answers.stuck -= 1;
        return new Promise(() => undefined);
      }
      if (!options.monitor) return Promise.resolve(base);
      const target = new EventTarget();
      options.monitor(target);
      progress = (loaded) => {
        const event = new Event("downloadprogress");
        Object.defineProperty(event, "loaded", { value: loaded });
        target.dispatchEvent(event);
      };
      return new Promise((resolve, reject) => {
        finish = () => resolve(base);
        fail = () => reject(new Error("no room"));
      });
    },
  };
  return {
    counts,
    answers,
    api,
    progress: (loaded: number) => progress?.(loaded),
    finish: () => finish?.(),
    fail: () => fail?.(),
  };
}

async function settled(model: ReturnType<typeof createAskModel>): Promise<void> {
  model.subscribe(() => undefined);
  await vi.waitFor(() => expect(model.get().state).not.toBe("checking"));
}

afterEach(() => {
  vi.useRealTimers();
});

describe("createAskModel", () => {
  it("asks the browser once, on the first subscriber", async () => {
    const { api } = fake("available");
    const asked = vi.spyOn(api, "availability");
    const model = createAskModel(() => api, () => true);
    expect(model.get().state).toBe("checking");
    expect(asked).not.toHaveBeenCalled();
    await settled(model);
    model.subscribe(() => undefined);
    expect(model.get().state).toBe("ready");
    expect(asked).toHaveBeenCalledTimes(1);
  });

  it("is unavailable where the browser has no model", async () => {
    const model = createAskModel(() => undefined, () => true);
    await settled(model);
    expect(model.get().state).toBe("unavailable");
  });

  it("is unavailable when the check itself fails", async () => {
    const { api } = fake("available");
    vi.spyOn(api, "availability").mockRejectedValue(new Error("blocked"));
    const model = createAskModel(() => api, () => true);
    await settled(model);
    expect(model.get().state).toBe("unavailable");
  });

  it("returns the series the model names, on a copy closed afterwards", async () => {
    const { api, counts } = fake("available", ['{"series":["replay"]}']);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    const picked = await model.pickSeries("replay time, last hour");
    expect(picked).toMatchObject({ kind: "series", series: [{ key: "replay" }], unmatched: false });
    expect(counts).toMatchObject({ created: 1, cloned: 1, destroyed: 1, prompts: ["replay time, last hour"] });
  });

  it("keeps one session across requests", async () => {
    const { api, counts } = fake("available");
    const model = createAskModel(() => api, () => true);
    await settled(model);
    model.warm();
    await model.pickSeries("shreds");
    await model.pickSeries("shreds again");
    expect(counts).toMatchObject({ created: 1, cloned: 2, destroyed: 2 });
  });

  it("returns several series in the model's order, and whether part was not recorded", async () => {
    const { api } = fake("available", ['{"series":["shreds","replay"]}', '{"series":["fees","none"]}']);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    expect(await model.pickSeries("shreds and replay time")).toMatchObject({
      kind: "series",
      series: [{ key: "shreds" }, { key: "replay" }],
      unmatched: false,
    });
    expect(await model.pickSeries("fees and the price of SOL")).toMatchObject({
      kind: "series",
      series: [{ key: "fees" }],
      unmatched: true,
    });
  });

  it("says so when nothing recorded matches", async () => {
    const { api } = fake("available", ['{"series":["none"]}']);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    expect(await model.pickSeries("the price of SOL")).toEqual({ kind: "none" });
  });

  it("fails without a second try on an answer outside the schema", async () => {
    const { api, counts } = fake("available", ['{"series":["gpu"]}']);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    expect(await model.pickSeries("cpu")).toEqual({ kind: "failed", reason: "model" });
    expect(counts.cloned).toBe(1);
  });

  it("opens a new session once after an error, then gives up", async () => {
    const once = fake("available", [new Error("session destroyed"), '{"series":["fees"]}']);
    const recovered = createAskModel(() => once.api, () => true);
    await settled(recovered);
    expect(await recovered.pickSeries("fees")).toMatchObject({ kind: "series", series: [{ key: "fees" }] });
    expect(once.counts).toMatchObject({ created: 2, cloned: 2, destroyed: 2, closed: 1 });

    const twice = fake("available", [new Error("gone"), new Error("gone")]);
    const lost = createAskModel(() => twice.api, () => true);
    await settled(lost);
    expect(await lost.pickSeries("fees")).toEqual({ kind: "failed", reason: "model" });
    expect(twice.counts).toMatchObject({ created: 2, destroyed: 2 });
    await vi.waitFor(() => expect(twice.counts.closed).toBe(2));
  });

  it("replaces a session that never opened, once its request has run out of time", async () => {
    const { api, answers, counts } = fake("available");
    answers.stuck = 1;
    const model = createAskModel(() => api, () => true);
    await settled(model);
    vi.useFakeTimers();
    const first = model.pickSeries("shreds");
    await vi.advanceTimersByTimeAsync(FIRST_PROMPT_TIMEOUT_MS);
    expect(await first).toEqual({ kind: "failed", reason: "timeout" });
    expect(counts).toMatchObject({ created: 1, cloned: 0 });
    expect(await model.pickSeries("shreds")).toMatchObject({ kind: "series" });
    expect(counts).toMatchObject({ created: 2, cloned: 1 });
  });

  it("gives a session opened after an error the first prompt's longer wait", async () => {
    const { api, answers, counts } = fake("available", ['{"series":["shreds"]}', new Error("session closed")]);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    expect(await model.pickSeries("shreds")).toMatchObject({ kind: "series" });
    answers.stuck = 1;
    vi.useFakeTimers();
    let settledWith: unknown = null;
    const second = model.pickSeries("shreds").then((picked) => (settledWith = picked));
    await vi.advanceTimersByTimeAsync(PROMPT_TIMEOUT_MS);
    expect(settledWith).toBeNull();
    await vi.advanceTimersByTimeAsync(FIRST_PROMPT_TIMEOUT_MS - PROMPT_TIMEOUT_MS);
    expect(await second).toEqual({ kind: "failed", reason: "timeout" });
    expect(counts.created).toBe(2);
  });

  it("keeps a session that opened when only the prompt ran out of time", async () => {
    const { api, counts } = fake("available", ["hang"]);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    vi.useFakeTimers();
    const first = model.pickSeries("shreds");
    await vi.advanceTimersByTimeAsync(FIRST_PROMPT_TIMEOUT_MS);
    expect(await first).toEqual({ kind: "failed", reason: "timeout" });
    expect(counts).toMatchObject({ created: 1, closed: 0 });
  });

  it("looks again while a download started elsewhere runs, and is ready when it ends", async () => {
    vi.useFakeTimers();
    const { api, answers } = fake("downloading");
    const model = createAskModel(() => api, () => true);
    model.subscribe(() => undefined);
    await vi.advanceTimersByTimeAsync(0);
    expect(model.get()).toMatchObject({ state: "downloading", progress: null });
    answers.availability = "available";
    await vi.advanceTimersByTimeAsync(5000);
    expect(model.get().state).toBe("ready");
  });

  it("gives up on a prompt that outlasts its wait, the first given longer", async () => {
    const { api, counts } = fake("available", ["hang", '{"series":["shreds"]}', "hang"]);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    vi.useFakeTimers();

    const first = model.pickSeries("shreds");
    await vi.advanceTimersByTimeAsync(FIRST_PROMPT_TIMEOUT_MS - 1);
    expect(counts.destroyed).toBe(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(await first).toEqual({ kind: "failed", reason: "timeout" });
    expect(counts.destroyed).toBe(1);

    expect(await model.pickSeries("shreds")).toMatchObject({ kind: "series" });
    const later = model.pickSeries("shreds");
    await vi.advanceTimersByTimeAsync(PROMPT_TIMEOUT_MS);
    expect(await later).toEqual({ kind: "failed", reason: "timeout" });
    expect(counts.created).toBe(1);
  });

  it("stops a prompt the page no longer wants, and closes its copy", async () => {
    const { api, counts } = fake("available", ["hang"]);
    const model = createAskModel(() => api, () => true);
    await settled(model);
    const stop = new AbortController();
    const picked = model.pickSeries("shreds", stop.signal);
    await vi.waitFor(() => expect(counts.prompts).toHaveLength(1));
    stop.abort();
    expect(await picked).toEqual({ kind: "failed", reason: "stopped" });
    await vi.waitFor(() => expect(counts.destroyed).toBe(1));
    expect(counts.created).toBe(1);
  });

  it("downloads on request, reporting progress, and is ready when the session opens", async () => {
    const { api, counts, progress, finish } = fake("downloadable");
    const model = createAskModel(() => api, () => true);
    await settled(model);
    expect(model.get().state).toBe("downloadable");
    model.download();
    model.download();
    expect(model.get()).toEqual({ state: "downloading", progress: 0, downloadFailed: false });
    progress(0.4);
    expect(model.get().progress).toBe(0.4);
    finish();
    await vi.waitFor(() => expect(model.get().state).toBe("ready"));
    expect(model.get().progress).toBeNull();
    await model.pickSeries("shreds");
    expect(counts.created).toBe(1);
  });

  it("goes back to offering the download when it fails", async () => {
    const { api, fail } = fake("downloadable");
    const model = createAskModel(() => api, () => true);
    await settled(model);
    model.download();
    fail();
    await vi.waitFor(() => expect(model.get().state).toBe("downloadable"));
    expect(model.get()).toEqual({ state: "downloadable", progress: null, downloadFailed: true });
  });

  it("does not download where the model is already there", async () => {
    const { api, counts } = fake("available");
    const model = createAskModel(() => api, () => true);
    await settled(model);
    model.download();
    expect(model.get().state).toBe("ready");
    expect(counts.created).toBe(0);
  });
});
