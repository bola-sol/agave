import { count, percent } from "../format";
import type { ProgramCache } from "../types";
import { useStore } from "../useStore";
import { Card, Explain, Meter, Stat } from "./primitives";

/**
 * How the program cache is faring, over the last minute.
 *
 * Every figure here is a rate rather than a standing total. The cache resets
 * its counters each time a bank is made from a parent — two or three times a
 * second — and reports them as it does, so what is summed is a minute of real
 * work rather than anything the cache is holding.
 *
 * The one exception is the entry peak, which is a level and is treated as one:
 * it is written only when an eviction runs, so the highest reading across the
 * window is taken rather than the latest.
 *
 * Firedancer's equivalent panel shows this as bytes used against a total,
 * because their cache is a fixed-size arena. Agave's is a map on the heap
 * bounded by a number of entries and with no byte budget at all, so there is no
 * honest way to draw the same gauge, and entries against the entry limit is
 * what stands in for it.
 */
export function ProgramCacheCard() {
  const store = useStore();
  const cache = store.get<ProgramCache | null>("summary", "program_cache");
  if (!cache) return null;

  const filled =
    cache.peak_entries !== null && cache.entry_limit > 0
      ? cache.peak_entries / cache.entry_limit
      : null;

  return (
    <Card title="Program Cache" className="cache-body">
      <div className="stat-grid">
        <Stat
          label="Hit rate"
          explain="How often replay found a program already compiled rather than having to build it, over the last minute. A low rate means replay spends its time compiling, which slows a block down and leaves less room to pack the next one. Evictions are the usual cause."
          value={percent(cache.hit_rate, 2)}
          tone={cache.hit_rate >= 0.98 ? "good" : cache.hit_rate >= 0.9 ? undefined : "bad"}
          sub={`${count(cache.hits)} hits · ${count(cache.misses)} misses`}
        />
        <Stat
          label="Loads"
          explain="Every program lookup replay made in the window, hits and misses together. Small numbers here are ordinary — a block touches few distinct programs — which is why the rate is summed over a minute rather than read off a single slot."
          value={count(cache.looked_up)}
        />
      </div>

      {/* Drawn whether or not an eviction has happened, so the card keeps its
          height. The bar is empty until one has, which is honest: nothing has
          reported where the cache stood. */}
      <div className="cache-storage">
        <div className="cache-storage-head">
          <Explain text="The most entries seen loaded at any eviction in the last minute, against the limit eviction keeps them under. Only measured when an eviction runs, so it is a high-water mark rather than a live reading, and it is empty on a validator that has not had to evict anything. Approaching the limit is what precedes a falling hit rate.">
            <span className="cache-storage-label">Peak entries</span>
          </Explain>
          <span className="cache-storage-value">
            {cache.peak_entries === null ? "—" : count(cache.peak_entries)}
            <span className="cache-storage-limit"> / {count(cache.entry_limit)}</span>
          </span>
        </div>
        <Meter fraction={filled ?? 0} />
      </div>

      <div className="stat-grid">
        <Stat
          label="Insertions"
          explain="Programs compiled and added to the cache in the window. Lost insertions are ones thrown away because the fork they were compiled for had gone by the time they were ready — wasted work, but not a fault."
          value={count(cache.insertions)}
          sub={cache.lost_insertions > 0 ? `${count(cache.lost_insertions)} lost` : undefined}
        />
        <Stat
          label="Evictions"
          explain="Compiled programs dropped to keep the cache within its entry limit. This is the usual reason a hit rate falls, and reloads below are what it costs: the same program compiled again the next time a block wants it."
          value={count(cache.evictions)}
          sub={cache.reloads > 0 ? `${count(cache.reloads)} reloaded` : undefined}
        />
        <Stat
          label="One-hit wonders"
          explain="Programs compiled, used once, and then evicted. Cache space and compilation time spent for a single use. A high figure alongside a healthy hit rate is ordinary — the network has a long tail of rarely used programs."
          value={count(cache.one_hit_wonders)}
        />
        <Stat
          label="Pruned"
          explain="Entries dropped because the fork they belonged to was abandoned, or because they had not been recompiled for the incoming epoch. Neither is a fault; both are the cache keeping up with the chain. The epoch figure rises sharply around an epoch boundary and is expected to."
          value={count(cache.prunes_orphan + cache.prunes_environment)}
          sub={`${count(cache.prunes_orphan)} orphaned · ${count(cache.prunes_environment)} epoch`}
        />
      </div>

      <div className="card-footnote">
        One minute of the cache's own counters, which it resets and reports at
        every bank.{" "}
        {cache.replacements > 0 && (
          <Explain text="An entry already in the cache compiled a second time. Not harmful, but it is work that need not have happened, and a persistent figure here is worth reporting upstream.">
            {count(cache.replacements)} recompiled needlessly.
          </Explain>
        )}
      </div>
    </Card>
  );
}
