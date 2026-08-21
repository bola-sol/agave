import { bytes, count, percent } from "../format";
import type { AccountsCache } from "../types";
import { useStore } from "../useStore";
import { Card, Explain, Meter, Stat } from "./primitives";

/**
 * What the accounts database is holding, where its reads come from, and what it
 * is writing.
 *
 * Firedancer's equivalent draws disk throughput in bytes a second both ways,
 * because their accounts database instruments its own file access in bytes.
 * Agave counts the read path in accounts and only the write path in bytes, so
 * that is what is shown: a rate of accounts read from storage, and a rate of
 * bytes written to it. The asymmetry is real rather than an omission here.
 *
 * Not built from `/proc/self/io`, which would give true bytes for both and be
 * the wrong number: it is process-wide, so the blockstore's writes, snapshot
 * archiving and the log would all land in a panel labelled Accounts.
 */
export function AccountsCard() {
  const store = useStore();
  const accounts = store.get<AccountsCache | null>("summary", "accounts_cache");
  if (!accounts) return null;

  const perSecond = (total: number) =>
    accounts.window_seconds > 0 ? total / accounts.window_seconds : 0;
  const disk = accounts.disk;
  const live = disk && disk.allocated > 0 ? disk.used / disk.allocated : null;

  return (
    <Card title="Accounts" className="cache-body">
      <div className="stat-grid">
        <Stat
          label="Cache hit rate"
          explain="How often an account replay needed was already in memory, over the last minute. Reads that miss go to a storage file, which is orders of magnitude slower, so a falling rate here shows up as slower replay."
          value={percent(accounts.hit_rate, 2)}
          tone={accounts.hit_rate >= 0.95 ? "good" : accounts.hit_rate >= 0.8 ? undefined : "bad"}
          sub={`${count(accounts.read)} reads · ${count(accounts.evictions)} evicted`}
        />
        <Stat
          label="Cache size"
          explain="What the read cache is holding right now, and how many accounts that is. A level rather than a rate — it is read as it stands rather than summed over the window. There is no configured maximum reported alongside it, so this is a size and not a share."
          value={bytes(accounts.cache_bytes)}
          sub={`${count(accounts.cache_entries)} accounts`}
        />
      </div>

      <div className="cache-section">
        <Explain text="Every account read in the window, split by where it was answered from. The write cache holds accounts this validator has just written and has not yet flushed; the read cache holds ones it fetched earlier. Only the third goes to a file, which makes it the closest thing to a disk read rate — counted in accounts, because nothing on that path counts bytes.">
          <span className="cache-section-title">Reads answered from</span>
        </Explain>
        <div className="stat-grid">
          <Stat label="Write cache" value={count(accounts.from_write_cache)} />
          <Stat label="Read cache" value={count(accounts.from_read_cache)} />
          <Stat
            label="Storage"
            value={count(accounts.from_storage)}
            sub={`${count(Math.round(perSecond(accounts.from_storage)))}/s`}
          />
        </div>
      </div>

      <div className="cache-section">
        <Explain text="Accounts written out of the cache into storage files over the window. This is the one path the accounts database measures in bytes as well as in accounts, which is why the write side has a throughput figure and the read side does not.">
          <span className="cache-section-title">Written to storage</span>
        </Explain>
        <div className="stat-grid">
          <Stat
            label="Throughput"
            value={`${bytes(Math.round(perSecond(accounts.stored_bytes)))}/s`}
            sub={bytes(accounts.stored_bytes)}
          />
          <Stat
            label="Accounts"
            value={count(accounts.stored_accounts)}
            sub={`${count(Math.round(perSecond(accounts.stored_accounts)))}/s`}
          />
        </div>
      </div>

      {disk && (
        <div className="cache-section">
          <Explain text="How much space the storage files occupy and how much of it is still referenced by a live account. The gap is dead account data that shrink has not yet reclaimed. Agave shrinks continuously as candidates appear rather than on a schedule, so unlike Firedancer's panel there is no next-compaction time to count down to.">
            <span className="cache-section-title">On disk</span>
          </Explain>
          <div className="cache-storage">
            <div className="cache-storage-head">
              <span className="cache-storage-label">Live of allocated</span>
              <span className="cache-storage-value">
                {bytes(disk.used)}
                <span className="cache-storage-limit"> / {bytes(disk.allocated)}</span>
              </span>
            </div>
            <Meter fraction={live ?? 0} />
          </div>
          <div className="stat-grid">
            <Stat
              label="Fragmented"
              explain="Allocated bytes no longer referenced by any live account. Shrink rewrites storage files to reclaim this, continuously rather than on a schedule, so a steady figure here is normal and only a growing one is worth watching."
              value={bytes(disk.fragmented)}
              sub={
                disk.allocated > 0
                  ? percent(disk.fragmented / disk.allocated, 1)
                  : undefined
              }
            />
            <Stat
              label="Storage files"
              explain="How many append-only files the accounts data is spread across. Rises with the number of slots held and falls as shrink combines them."
              value={count(disk.storages)}
            />
          </div>
        </div>
      )}

      <div className="card-footnote">
        One minute of the accounts database's own counters. The read side is
        counted in accounts and the write side in bytes, because that is how the
        database counts them — there is no byte figure on the load path to build
        a read throughput from.
      </div>
    </Card>
  );
}
