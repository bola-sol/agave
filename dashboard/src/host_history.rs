//! Host readings kept in ten-second bins for two days, so a page can chart them over hours. Filled
//! as the meters read, and at startup from what the feed's recording holds.

use {
    crate::meters::{Host, Network},
    serde::{Deserialize, Serialize},
    std::collections::HashMap,
};

pub const BIN_MILLIS: u64 = 10_000;

/// Two days of bins, the recording's own reach, in about two megabytes.
pub const BINS: usize = 17_280;

/// The most points one reply draws per series.
pub const MAX_POINTS: usize = 240;

/// The most series one request reads.
pub const MAX_SERIES: usize = 3;

/// Two live readings of a counter further apart than this say nothing of the rate between them.
const MAX_COUNTER_GAP_MILLIS: u64 = 15_000;

/// The recording holds a counter's line only when it changed, so the time since the last line was
/// quiet. Past a segment's length and some slack it was a stop instead.
const MAX_RECORDED_QUIET_MILLIS: u64 = 660_000;

/// The meters read once a second, so a recorded change came within about that.
const READING_SECONDS: f64 = 1.0;

const SERIES: usize = 9;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeriesKey {
    /// Share of the cores busy, nought to one.
    Cpu,
    Load,
    /// Bytes in use, as the machine card counts them.
    Memory,
    /// The validator's resident bytes.
    ValidatorMemory,
    /// Bytes a second, the host's total.
    NetIn,
    NetOut,
    /// Share used of the fullest filesystem, nought to one.
    DiskUsed,
    /// Share of the time the busiest device was busy, nought to one.
    DiskBusy,
    /// Datagrams a second the kernel dropped on the validator's ports.
    UdpDrops,
}

impl SeriesKey {
    /// Summed over a bin and divided by its seconds, rather than averaged.
    fn counted(self) -> bool {
        self == Self::UdpDrops
    }
}

/// What one reading of the host adds; `None` where the reading lacks the figure.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct HostReading {
    pub cpu: Option<f64>,
    pub load: Option<f64>,
    pub memory: Option<f64>,
    pub validator_memory: Option<f64>,
    pub disk_used: Option<f64>,
    pub disk_busy: Option<f64>,
}

fn highest(values: impl Iterator<Item = f64>) -> Option<f64> {
    values.fold(None, |most, value| {
        Some(most.map_or(value, |held: f64| held.max(value)))
    })
}

fn used_share(total: u64, available: u64) -> Option<f64> {
    (total > 0).then(|| total.saturating_sub(available) as f64 / total as f64)
}

fn in_use(total: u64, free: u64, reclaimable: u64) -> f64 {
    total.saturating_sub(free).saturating_sub(reclaimable) as f64
}

impl From<&Host> for HostReading {
    fn from(host: &Host) -> Self {
        Self {
            cpu: host.cpu.map(|cpu| cpu.busy),
            load: Some(host.load_one),
            memory: Some(in_use(
                host.memory_total,
                host.memory_free,
                host.memory_reclaimable,
            )),
            validator_memory: host.process_resident.map(|bytes| bytes as f64),
            disk_used: highest(
                host.filesystems
                    .iter()
                    .filter_map(|filesystem| used_share(filesystem.total, filesystem.available)),
            ),
            disk_busy: highest(host.devices.iter().map(|device| device.busy)),
        }
    }
}

/// `summary.host` as the recording holds it, every field optional so an older shape still reads.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RecordedHost {
    load_one: Option<f64>,
    cpu: Option<RecordedCpu>,
    memory_total: Option<u64>,
    memory_free: Option<u64>,
    memory_reclaimable: Option<u64>,
    process_resident: Option<u64>,
    filesystems: Vec<RecordedFilesystem>,
    devices: Vec<RecordedDevice>,
}

#[derive(Debug, Deserialize)]
struct RecordedCpu {
    busy: f64,
}

#[derive(Debug, Deserialize)]
struct RecordedFilesystem {
    total: u64,
    available: u64,
}

#[derive(Debug, Deserialize)]
struct RecordedDevice {
    busy: f64,
}

impl From<RecordedHost> for HostReading {
    fn from(host: RecordedHost) -> Self {
        Self {
            cpu: host.cpu.map(|cpu| cpu.busy),
            load: host.load_one,
            memory: host
                .memory_total
                .zip(host.memory_free)
                .map(|(total, free)| {
                    in_use(total, free, host.memory_reclaimable.unwrap_or_default())
                }),
            validator_memory: host.process_resident.map(|bytes| bytes as f64),
            disk_used: highest(
                host.filesystems
                    .iter()
                    .filter_map(|filesystem| used_share(filesystem.total, filesystem.available)),
            ),
            disk_busy: highest(host.devices.iter().map(|device| device.busy)),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RecordedNetwork {
    received_per_second: u64,
    sent_per_second: u64,
}

#[derive(Debug, Deserialize)]
struct RecordedIngest {
    paths: Vec<RecordedPath>,
}

#[derive(Debug, Deserialize)]
struct RecordedPath {
    port: u16,
    drops_total: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Bin {
    sum: f32,
    max: f32,
    count: u8,
}

impl Bin {
    fn add(&mut self, value: f32) {
        self.add_counted(value, value);
    }

    /// A count over one reading's interval: the count joins the sum, its rate vies for the highest.
    fn add_counted(&mut self, count: f32, rate: f32) {
        self.max = if self.count == 0 {
            rate
        } else {
            self.max.max(rate)
        };
        self.sum += count;
        self.count = self.count.saturating_add(1);
    }
}

/// One span of `BIN_MILLIS`; `index` is its place in time, so a row from a lap ago cannot answer
/// for a current one.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct Row {
    index: u64,
    bins: [Bin; SERIES],
}

impl Row {
    fn is_empty(&self) -> bool {
        self.bins.iter().all(|bin| bin.count == 0)
    }
}

/// One series of a reply, a point per span: its mean and its highest reading, `None` where
/// nothing was read.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SeriesRange {
    pub key: SeriesKey,
    pub mean: Vec<Option<f32>>,
    pub max: Vec<Option<f32>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HostRange {
    /// The window answered, in unix milliseconds: the one asked, widened to whole bins and held
    /// to the two days before its end.
    pub from: u64,
    pub to: u64,
    /// Every step but the last, which is shorter where the steps do not divide the window.
    pub step_ms: u64,
    /// When the readings held begin, or `None` with none held.
    pub held_from: Option<u64>,
    pub series: Vec<SeriesRange>,
}

pub struct HostHistory {
    rows: Vec<Row>,
    /// The bin of the latest reading, which sets how far back a row still counts.
    newest: Option<u64>,
    /// Each port's drop total and when it was read, for the next reading's difference.
    drops: HashMap<u16, u64>,
    drops_at: Option<u64>,
}

impl Default for HostHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl HostHistory {
    pub fn new() -> Self {
        Self {
            rows: vec![Row::default(); BINS],
            newest: None,
            drops: HashMap::new(),
            drops_at: None,
        }
    }

    fn place(index: u64) -> usize {
        // The remainder is below `BINS`, which fits.
        index.checked_rem(BINS as u64).unwrap_or_default() as usize
    }

    /// The row for a moment, emptied of an older lap's readings; `None` for a moment a lap or more
    /// behind what the row already holds.
    fn row(&mut self, at_ms: u64) -> Option<&mut Row> {
        let index = at_ms.checked_div(BIN_MILLIS)?;
        let row = self.rows.get_mut(Self::place(index))?;
        if row.index > index && !row.is_empty() {
            return None;
        }
        if row.index != index {
            *row = Row {
                index,
                ..Row::default()
            };
        }
        self.newest = Some(self.newest.map_or(index, |newest| newest.max(index)));
        self.rows.get_mut(Self::place(index))
    }

    fn add(&mut self, at_ms: u64, key: SeriesKey, value: Option<f64>) {
        let Some(value) = value.filter(|value| value.is_finite()) else {
            return;
        };
        if let Some(bin) = self
            .row(at_ms)
            .and_then(|row| row.bins.get_mut(key as usize))
        {
            bin.add(value as f32);
        }
    }

    pub fn note_host(&mut self, at_ms: u64, reading: &HostReading) {
        self.add(at_ms, SeriesKey::Cpu, reading.cpu);
        self.add(at_ms, SeriesKey::Load, reading.load);
        self.add(at_ms, SeriesKey::Memory, reading.memory);
        self.add(at_ms, SeriesKey::ValidatorMemory, reading.validator_memory);
        self.add(at_ms, SeriesKey::DiskUsed, reading.disk_used);
        self.add(at_ms, SeriesKey::DiskBusy, reading.disk_busy);
    }

    pub fn note_network(&mut self, at_ms: u64, network: &Network) {
        self.add(
            at_ms,
            SeriesKey::NetIn,
            Some(network.received_per_second as f64),
        );
        self.add(
            at_ms,
            SeriesKey::NetOut,
            Some(network.sent_per_second as f64),
        );
    }

    fn add_drops(&mut self, at_ms: u64, count: u64, seconds: f64) {
        if let Some(bin) = self
            .row(at_ms)
            .and_then(|row| row.bins.get_mut(SeriesKey::UdpDrops as usize))
        {
            bin.add_counted(count as f32, (count as f64 / seconds) as f32);
        }
    }

    /// Holds `totals` for the next reading and gives what was dropped since the last on the ports
    /// both name. A port whose total went back counts nothing, as when its socket is bound again.
    fn take_drops(
        &mut self,
        at_ms: u64,
        totals: impl IntoIterator<Item = (u16, u64)>,
    ) -> Option<u64> {
        let mut dropped: Option<u64> = None;
        let mut next = HashMap::new();
        for (port, total) in totals {
            if let Some(since) = self
                .drops
                .get(&port)
                .and_then(|before| total.checked_sub(*before))
            {
                dropped = Some(dropped.unwrap_or_default().saturating_add(since));
            }
            next.insert(port, total);
        }
        self.drops = next;
        self.drops_at = Some(at_ms);
        dropped
    }

    /// Each port's running drop total, as the meters read it every second. What was dropped since
    /// the last reading is counted as a rate over the time between, but not across a long gap.
    pub fn note_drops(&mut self, at_ms: u64, totals: impl IntoIterator<Item = (u16, u64)>) {
        let gap = self
            .drops_at
            .and_then(|at| at_ms.checked_sub(at))
            .filter(|gap| (1..=MAX_COUNTER_GAP_MILLIS).contains(gap));
        let dropped = self.take_drops(at_ms, totals);
        if let (Some(gap), Some(count)) = (gap, dropped) {
            self.add_drops(at_ms, count, gap as f64 / 1000.0);
        }
    }

    /// A recorded line of drop totals. `opening` is a segment's own copy of the last line, which
    /// carries on from the line before only if it repeats it; anything else is another run of the
    /// process, whose totals count from elsewhere. Between two lines of one run nothing was
    /// dropped, so those bins read nought.
    pub fn note_recorded_drops(&mut self, at_ms: u64, totals: Vec<(u16, u64)>, opening: bool) {
        let before = self.drops_at.filter(|at| {
            at_ms
                .checked_sub(*at)
                .is_some_and(|gap| gap <= MAX_RECORDED_QUIET_MILLIS)
        });
        let repeats = totals.len() == self.drops.len()
            && totals
                .iter()
                .all(|(port, total)| self.drops.get(port) == Some(total));
        let dropped = self.take_drops(at_ms, totals);
        let Some(before) = before.filter(|_| !opening || repeats) else {
            return;
        };
        let quiet_from = before
            .checked_div(BIN_MILLIS)
            .unwrap_or_default()
            .saturating_add(1);
        let this = at_ms.checked_div(BIN_MILLIS).unwrap_or_default();
        for index in quiet_from..this {
            self.add_drops(index.saturating_mul(BIN_MILLIS), 0, READING_SECONDS);
        }
        if let Some(count) = dropped {
            self.add_drops(at_ms, count, READING_SECONDS);
        }
    }

    /// One recorded feed line's value, by its key. `None` for a key that is not a host reading,
    /// otherwise whether it was read. A segment's `opening` copy of the last host or network value
    /// is passed over, having been counted when it was first sent.
    pub fn note_recorded(
        &mut self,
        at_ms: u64,
        key: &str,
        value: &str,
        opening: bool,
    ) -> Option<bool> {
        Some(match key {
            "host" | "network" if opening => true,
            "host" => serde_json::from_str::<RecordedHost>(value)
                .map(|host| self.note_host(at_ms, &host.into()))
                .is_ok(),
            "network" => serde_json::from_str::<RecordedNetwork>(value)
                .map(|network| {
                    self.note_network(
                        at_ms,
                        &Network {
                            received_per_second: network.received_per_second,
                            sent_per_second: network.sent_per_second,
                        },
                    )
                })
                .is_ok(),
            "ingest_paths" => serde_json::from_str::<RecordedIngest>(value)
                .map(|ingest| {
                    let totals = ingest
                        .paths
                        .iter()
                        .map(|path| (path.port, path.drops_total))
                        .collect();
                    self.note_recorded_drops(at_ms, totals, opening)
                })
                .is_ok(),
            _ => return None,
        })
    }

    /// Whether a bin is within two days of the newest reading.
    fn in_reach(&self, index: u64) -> bool {
        self.newest
            .is_some_and(|newest| index.saturating_add(BINS as u64) > newest)
    }

    /// Takes every span `older` holds and this does not, so what was read back at startup fills in
    /// behind what has been read since.
    pub fn merge(&mut self, older: &HostHistory) {
        self.newest = self.newest.max(older.newest);
        for (place, theirs) in older.rows.iter().enumerate() {
            if theirs.is_empty() || !self.in_reach(theirs.index) {
                continue;
            }
            let Some(row) = self.rows.get_mut(place) else {
                continue;
            };
            if row.is_empty() || theirs.index > row.index {
                *row = *theirs;
            }
        }
    }

    fn held(&self, index: u64) -> Option<&Row> {
        self.rows
            .get(Self::place(index))
            .filter(|row| row.index == index && !row.is_empty() && self.in_reach(index))
    }

    /// When the oldest reading within reach of the newest was taken.
    fn held_from(&self) -> Option<u64> {
        self.rows
            .iter()
            .filter(|row| !row.is_empty() && self.in_reach(row.index))
            .map(|row| row.index)
            .min()
            .map(|index| index.saturating_mul(BIN_MILLIS))
    }

    /// `keys` over `from..to` in at most `points` steps of whole bins. A window longer than two
    /// days keeps its newest end, and one reaching past what is held answers with empty points
    /// there, not an error.
    pub fn range(&self, keys: &[SeriesKey], from: u64, to: u64, points: usize) -> HostRange {
        let last = to
            .saturating_sub(1)
            .checked_div(BIN_MILLIS)
            .unwrap_or_default();
        let first = from
            .checked_div(BIN_MILLIS)
            .unwrap_or_default()
            .max(last.saturating_sub((BINS as u64).saturating_sub(1)))
            .min(last);
        let end = last.saturating_add(1);
        let bins = end.saturating_sub(first);
        let points = (points.clamp(1, MAX_POINTS) as u64).min(bins);
        // Whole bins to a step, rounded up so the steps cover the window.
        let per_step = bins.div_ceil(points).max(1);
        let steps = bins.div_ceil(per_step);
        let series = keys
            .iter()
            .map(|key| {
                let (mean, max) = (0..steps)
                    .map(|step| {
                        let start = first.saturating_add(step.saturating_mul(per_step));
                        self.point(*key, start, per_step.min(end.saturating_sub(start)))
                    })
                    .unzip();
                SeriesRange {
                    key: *key,
                    mean,
                    max,
                }
            })
            .collect();
        HostRange {
            from: first.saturating_mul(BIN_MILLIS),
            to: end.saturating_mul(BIN_MILLIS),
            step_ms: per_step.saturating_mul(BIN_MILLIS),
            held_from: self.held_from(),
            series,
        }
    }

    /// One step's mean and highest reading over `count` bins from `start`.
    fn point(&self, key: SeriesKey, start: u64, count: u64) -> (Option<f32>, Option<f32>) {
        let mut sum = 0f64;
        let mut readings = 0u64;
        let mut spans = 0u64;
        let mut max: Option<f32> = None;
        for index in start..start.saturating_add(count) {
            let Some(bin) = self
                .held(index)
                .and_then(|row| row.bins.get(key as usize))
                .filter(|bin| bin.count > 0)
            else {
                continue;
            };
            sum += f64::from(bin.sum);
            readings = readings.saturating_add(u64::from(bin.count));
            spans = spans.saturating_add(1);
            max = Some(max.map_or(bin.max, |held| held.max(bin.max)));
        }
        if readings == 0 {
            return (None, None);
        }
        // A count is spread over the seconds of the bins that hold one; a level is averaged.
        let over = if key.counted() {
            spans.saturating_mul(BIN_MILLIS) as f64 / 1000.0
        } else {
            readings as f64
        };
        (Some((sum / over) as f32), max)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            host_stats::CpuUse,
            meters::{DeviceLoad, FilesystemUsage},
            metrics_tap::MemoryParts,
        },
    };

    const T: u64 = 1_764_000_000_000;

    fn level(history: &mut HostHistory, at_ms: u64, cpu: f64) {
        history.note_host(
            at_ms,
            &HostReading {
                cpu: Some(cpu),
                ..HostReading::default()
            },
        );
    }

    fn cpu(
        history: &HostHistory,
        from: u64,
        to: u64,
        points: usize,
    ) -> (Vec<Option<f32>>, Vec<Option<f32>>) {
        let range = history.range(&[SeriesKey::Cpu], from, to, points);
        let series = range.series.into_iter().next().unwrap();
        (series.mean, series.max)
    }

    fn host() -> Host {
        Host {
            cores: 64,
            load_one: 21.5,
            load_five: 20.0,
            load_fifteen: 19.0,
            threads: 900,
            running: 12,
            cpu: Some(CpuUse {
                busy: 0.38,
                user: 0.3,
                system: 0.07,
                iowait: 0.01,
                steal: 0.0,
            }),
            cpu_governor: Some("performance".to_owned()),
            memory_total: 512_000,
            memory_available: 300_000,
            memory_reclaimable: 60_000,
            memory_free: 200_000,
            swap: None,
            process_resident: Some(240_000),
            process_resident_hour_ago: None,
            process_kinds: None,
            memory_parts: MemoryParts::default(),
            snapshot_device: None,
            filesystems: vec![
                FilesystemUsage {
                    name: "ledger".to_owned(),
                    path: "/ledger".to_owned(),
                    total: 1000,
                    available: 390,
                    reserved: 0,
                    parts: Vec::new(),
                    leftovers: Vec::new(),
                },
                FilesystemUsage {
                    name: "accounts".to_owned(),
                    path: "/accounts".to_owned(),
                    total: 1000,
                    available: 560,
                    reserved: 0,
                    parts: Vec::new(),
                    leftovers: Vec::new(),
                },
            ],
            devices: vec![
                DeviceLoad {
                    device: "nvme0n1".to_owned(),
                    roles: Vec::new(),
                    busy: 0.22,
                    wait_ms: None,
                    operations_per_second: 0,
                    read_per_second: 0,
                    write_per_second: 0,
                },
                DeviceLoad {
                    device: "nvme1n1".to_owned(),
                    roles: Vec::new(),
                    busy: 0.71,
                    wait_ms: None,
                    operations_per_second: 0,
                    read_per_second: 0,
                    write_per_second: 0,
                },
            ],
        }
    }

    #[test]
    fn test_a_host_reading_takes_the_cards_figures() {
        assert_eq!(
            HostReading::from(&host()),
            HostReading {
                cpu: Some(0.38),
                load: Some(21.5),
                memory: Some(252_000.0),
                validator_memory: Some(240_000.0),
                disk_used: Some(0.61),
                disk_busy: Some(0.71),
            }
        );
    }

    #[test]
    fn test_the_recorded_host_reads_as_the_live_one() {
        let host = host();
        let recorded: RecordedHost =
            serde_json::from_str(&serde_json::to_string(&host).unwrap()).unwrap();
        assert_eq!(HostReading::from(recorded), HostReading::from(&host));
    }

    #[test]
    fn test_a_recorded_host_missing_fields_still_reads_the_rest() {
        let recorded: RecordedHost =
            serde_json::from_str(r#"{"load_one":3.5,"cpu":null}"#).unwrap();
        assert_eq!(
            HostReading::from(recorded),
            HostReading {
                load: Some(3.5),
                ..HostReading::default()
            }
        );
    }

    #[test]
    fn test_readings_in_one_bin_give_its_mean_and_its_highest() {
        let mut history = HostHistory::new();
        level(&mut history, T, 0.2);
        level(&mut history, T.saturating_add(1000), 0.4);
        level(&mut history, T.saturating_add(9999), 0.9);
        level(&mut history, T.saturating_add(10_000), 0.1);
        let (mean, max) = cpu(&history, T, T.saturating_add(20_000), 2);
        assert_eq!(mean, [Some(0.5), Some(0.1)]);
        assert_eq!(max, [Some(0.9), Some(0.1)]);
    }

    #[test]
    fn test_a_step_of_several_bins_weighs_each_reading_alike() {
        let mut history = HostHistory::new();
        level(&mut history, T, 1.0);
        level(&mut history, T.saturating_add(10_000), 0.0);
        level(&mut history, T.saturating_add(11_000), 0.0);
        level(&mut history, T.saturating_add(12_000), 0.0);
        let (mean, max) = cpu(&history, T, T.saturating_add(20_000), 1);
        assert_eq!(mean, [Some(0.25)]);
        assert_eq!(max, [Some(1.0)]);
    }

    #[test]
    fn test_a_span_with_no_reading_is_empty_not_nought() {
        let mut history = HostHistory::new();
        level(&mut history, T, 0.5);
        level(&mut history, T.saturating_add(30_000), 0.5);
        let (mean, max) = cpu(&history, T, T.saturating_add(40_000), 4);
        assert_eq!(mean, [Some(0.5), None, None, Some(0.5)]);
        assert_eq!(max, [Some(0.5), None, None, Some(0.5)]);
    }

    #[test]
    fn test_a_range_is_never_finer_than_a_bin_nor_longer_than_asked() {
        let history = HostHistory::new();
        let fine = history.range(&[SeriesKey::Cpu], T, T.saturating_add(60_000), 240);
        assert_eq!((fine.step_ms, fine.series[0].mean.len()), (10_000, 6));
        let hour = history.range(&[SeriesKey::Cpu], T, T.saturating_add(3_600_000), 120);
        assert_eq!((hour.step_ms, hour.series[0].mean.len()), (30_000, 120));
        assert_eq!((hour.from, hour.to), (T, T.saturating_add(3_600_000)));
        // 360 bins in whole bins to a step: two each, the most that stays within the points allowed.
        let many = history.range(&[SeriesKey::Cpu], T, T.saturating_add(3_600_000), 100_000);
        assert_eq!((many.step_ms, many.series[0].mean.len()), (20_000, 180));
        assert!(many.series[0].mean.len() <= MAX_POINTS);
        let none = history.range(&[SeriesKey::Cpu], T, T.saturating_add(3_600_000), 0);
        assert_eq!(none.series[0].mean.len(), 1);
    }

    #[test]
    fn test_a_window_past_what_is_held_is_held_to_two_days() {
        let history = HostHistory::new();
        let range = history.range(&[SeriesKey::Cpu], 0, u64::MAX, 240);
        assert_eq!(range.series[0].mean.len(), 240);
        assert_eq!(range.step_ms, 720_000);
        let backwards = history.range(&[SeriesKey::Cpu], T, T.saturating_sub(5), 10);
        assert_eq!(backwards.series[0].mean.len(), 1);
    }

    #[test]
    fn test_each_series_asked_for_is_answered_in_order() {
        let mut history = HostHistory::new();
        history.note_host(T, &HostReading::from(&host()));
        history.note_network(
            T,
            &Network {
                received_per_second: 6_000_000,
                sent_per_second: 4_000_000,
            },
        );
        let range = history.range(
            &[SeriesKey::NetOut, SeriesKey::DiskUsed, SeriesKey::Load],
            T,
            T.saturating_add(10_000),
            1,
        );
        let read: Vec<_> = range
            .series
            .iter()
            .map(|series| (series.key, series.mean[0]))
            .collect();
        assert_eq!(
            read,
            [
                (SeriesKey::NetOut, Some(4_000_000.0)),
                (SeriesKey::DiskUsed, Some(0.61)),
                (SeriesKey::Load, Some(21.5)),
            ]
        );
    }

    #[test]
    fn test_drops_are_counted_since_the_last_reading_as_a_rate() {
        let mut history = HostHistory::new();
        history.note_drops(T, [(8001, 100), (8002, 50)]);
        history.note_drops(T.saturating_add(1000), [(8001, 130), (8002, 50)]);
        history.note_drops(T.saturating_add(2000), [(8001, 130), (8002, 120)]);
        let range = history.range(&[SeriesKey::UdpDrops], T, T.saturating_add(10_000), 1);
        // A hundred dropped over a ten-second bin, seventy of them in one second.
        assert_eq!(range.series[0].mean, [Some(10.0)]);
        assert_eq!(range.series[0].max, [Some(70.0)]);
    }

    #[test]
    fn test_the_first_reading_of_a_port_counts_nothing() {
        let mut history = HostHistory::new();
        history.note_drops(T, [(8001, 5_000_000)]);
        let range = history.range(&[SeriesKey::UdpDrops], T, T.saturating_add(10_000), 1);
        assert_eq!(range.series[0].mean, [None]);
        history.note_drops(T.saturating_add(1000), [(8001, 5_000_000), (8002, 900)]);
        let range = history.range(&[SeriesKey::UdpDrops], T, T.saturating_add(10_000), 1);
        assert_eq!(range.series[0].mean, [Some(0.0)]);
    }

    #[test]
    fn test_a_total_that_went_back_or_a_long_gap_counts_nothing() {
        let mut history = HostHistory::new();
        history.note_drops(T, [(8001, 100), (8002, 100)]);
        // The socket was bound again, so its total restarts below the last.
        history.note_drops(T.saturating_add(1000), [(8001, 10), (8002, 104)]);
        let range = history.range(&[SeriesKey::UdpDrops], T, T.saturating_add(10_000), 1);
        assert_eq!(range.series[0].max, [Some(4.0)]);

        let later = T.saturating_add(60_000);
        history.note_drops(later, [(8001, 9_000), (8002, 9_000)]);
        let range = history.range(
            &[SeriesKey::UdpDrops],
            later,
            later.saturating_add(10_000),
            1,
        );
        assert_eq!(range.series[0].mean, [None]);
    }

    #[test]
    fn test_a_lap_later_replaces_the_row_and_a_lap_earlier_is_turned_away() {
        let mut history = HostHistory::new();
        let lap = (BINS as u64).saturating_mul(BIN_MILLIS);
        level(&mut history, T, 0.3);
        level(&mut history, T.saturating_add(lap), 0.8);
        assert_eq!(cpu(&history, T, T.saturating_add(10_000), 1).0, [None]);
        let later = T.saturating_add(lap);
        assert_eq!(
            cpu(&history, later, later.saturating_add(10_000), 1).0,
            [Some(0.8)]
        );
        // A clock stepped back two days adds nothing to the row that moment shares.
        level(&mut history, T, 0.1);
        assert_eq!(
            cpu(&history, later, later.saturating_add(10_000), 1).0,
            [Some(0.8)]
        );
    }

    #[test]
    fn test_held_from_is_the_oldest_reading_within_two_days_of_the_newest() {
        let mut history = HostHistory::new();
        assert_eq!(
            history
                .range(&[SeriesKey::Cpu], T, T.saturating_add(10_000), 1)
                .held_from,
            None
        );
        level(&mut history, T.saturating_add(50_000), 0.3);
        level(&mut history, T.saturating_add(20_000), 0.3);
        level(&mut history, T.saturating_add(90_000), 0.3);
        let range = history.range(&[SeriesKey::Cpu], T, T.saturating_add(10_000), 1);
        assert_eq!(range.held_from, Some(T.saturating_add(20_000)));
    }

    #[test]
    fn test_recorded_lines_are_read_by_key() {
        let mut history = HostHistory::new();
        let host = serde_json::to_string(&host()).unwrap();
        assert_eq!(history.note_recorded(T, "host", &host, false), Some(true));
        assert_eq!(
            history.note_recorded(
                T,
                "network",
                r#"{"received_per_second":600,"sent_per_second":400}"#,
                false
            ),
            Some(true)
        );
        let ingest = |total: u64| {
            format!(
                r#"{{"window_seconds":60.0,"paths":[{{"name":"gossip","port":8001,"drops_recent":0,"drops_total":{total},"queued_bytes":0}}]}}"#
            )
        };
        assert_eq!(
            history.note_recorded(T, "ingest_paths", &ingest(40), false),
            Some(true)
        );
        assert_eq!(
            history.note_recorded(T.saturating_add(1000), "ingest_paths", &ingest(60), false),
            Some(true)
        );
        assert_eq!(history.note_recorded(T, "host", "[1,2", false), Some(false));
        assert_eq!(history.note_recorded(T, "tps_sample", "{}", false), None);

        let range = history.range(
            &[SeriesKey::Cpu, SeriesKey::NetIn, SeriesKey::UdpDrops],
            T,
            T.saturating_add(10_000),
            1,
        );
        let means: Vec<_> = range.series.iter().map(|series| series.mean[0]).collect();
        assert_eq!(means, [Some(0.38), Some(600.0), Some(2.0)]);
    }

    #[test]
    fn test_a_merge_fills_in_behind_what_has_been_read_since() {
        let mut live = HostHistory::new();
        level(&mut live, T.saturating_add(20_000), 0.9);
        let mut older = HostHistory::new();
        level(&mut older, T, 0.2);
        level(&mut older, T.saturating_add(20_000), 0.1);
        live.merge(&older);
        let (mean, _) = cpu(&live, T, T.saturating_add(30_000), 3);
        assert_eq!(mean, [Some(0.2), None, Some(0.9)]);
    }

    #[test]
    fn test_a_window_the_steps_do_not_divide_ends_where_it_was_asked_to() {
        let mut history = HostHistory::new();
        for bin in 0..9u64 {
            level(
                &mut history,
                T.saturating_add(bin.saturating_mul(10_000)),
                0.5,
            );
        }
        let range = history.range(&[SeriesKey::Cpu], T, T.saturating_add(70_000), 3);
        assert_eq!(
            (range.from, range.to, range.step_ms),
            (T, T.saturating_add(70_000), 30_000)
        );
        // Three bins, three bins, then the one left; nothing from past the window.
        assert_eq!(range.series[0].mean, [Some(0.5), Some(0.5), Some(0.5)]);
        let unaligned = history.range(
            &[SeriesKey::Cpu],
            T.saturating_add(4000),
            T.saturating_add(16_000),
            9,
        );
        assert_eq!(
            (unaligned.from, unaligned.to),
            (T, T.saturating_add(20_000))
        );
        assert_eq!(unaligned.series[0].mean.len(), 2);
    }

    #[test]
    fn test_a_window_longer_than_two_days_keeps_its_newest_end() {
        let history = HostHistory::new();
        let two_days = (BINS as u64).saturating_mul(BIN_MILLIS);
        let range = history.range(
            &[SeriesKey::Cpu],
            T.saturating_sub(two_days.saturating_mul(2)),
            T,
            240,
        );
        assert_eq!((range.from, range.to), (T.saturating_sub(two_days), T));
        let all = history.range(&[SeriesKey::Cpu], 0, T, 240);
        assert_eq!((all.from, all.to), (T.saturating_sub(two_days), T));
    }

    #[test]
    fn test_a_row_more_than_two_days_behind_the_newest_no_longer_answers() {
        let mut history = HostHistory::new();
        let two_days = (BINS as u64).saturating_mul(BIN_MILLIS);
        level(&mut history, T, 0.3);
        // A place of its own, so the old row is still there.
        let later = T.saturating_add(two_days).saturating_add(50_000);
        level(&mut history, later, 0.8);
        assert_eq!(cpu(&history, T, T.saturating_add(10_000), 1).0, [None]);
        let range = history.range(&[SeriesKey::Cpu], later, later.saturating_add(10_000), 1);
        assert_eq!(range.held_from, Some(later));
        assert_eq!(range.series[0].mean, [Some(0.8)]);
    }

    #[test]
    fn test_a_merge_keeps_the_newer_lap_of_a_place_whichever_side_holds_it() {
        let two_days = (BINS as u64).saturating_mul(BIN_MILLIS);
        let later = T.saturating_add(two_days);

        let mut live = HostHistory::new();
        level(&mut live, later, 0.9);
        let mut older = HostHistory::new();
        level(&mut older, T, 0.2);
        live.merge(&older);
        assert_eq!(
            cpu(&live, later, later.saturating_add(10_000), 1).0,
            [Some(0.9)]
        );

        let mut stale = HostHistory::new();
        level(&mut stale, T, 0.2);
        let mut read_back = HostHistory::new();
        level(&mut read_back, later, 0.9);
        stale.merge(&read_back);
        assert_eq!(
            cpu(&stale, later, later.saturating_add(10_000), 1).0,
            [Some(0.9)]
        );
        assert_eq!(cpu(&stale, T, T.saturating_add(10_000), 1).0, [None]);
    }

    #[test]
    fn test_a_live_count_is_a_rate_over_the_time_since_the_last_reading() {
        let mut history = HostHistory::new();
        history.note_drops(T, [(8001, 0)]);
        history.note_drops(T.saturating_add(4000), [(8001, 1200)]);
        let range = history.range(&[SeriesKey::UdpDrops], T, T.saturating_add(10_000), 1);
        assert_eq!(range.series[0].mean, [Some(120.0)]);
        assert_eq!(range.series[0].max, [Some(300.0)]);
    }

    fn recorded_drops(history: &HostHistory, from: u64, bins: u64) -> Vec<Option<f32>> {
        history
            .range(
                &[SeriesKey::UdpDrops],
                from,
                from.saturating_add(bins.saturating_mul(BIN_MILLIS)),
                bins as usize,
            )
            .series
            .into_iter()
            .next()
            .unwrap()
            .mean
    }

    #[test]
    fn test_recorded_drops_count_across_a_quiet_stretch_which_reads_nought() {
        let mut history = HostHistory::new();
        history.note_recorded_drops(T, vec![(8001, 100)], false);
        // Nothing changed for forty seconds, so no line was written; then five hundred at once.
        history.note_recorded_drops(T.saturating_add(40_000), vec![(8001, 600)], false);
        assert_eq!(
            recorded_drops(&history, T, 5),
            [None, Some(0.0), Some(0.0), Some(0.0), Some(50.0)]
        );
        let peak = history.range(&[SeriesKey::UdpDrops], T, T.saturating_add(50_000), 1);
        assert_eq!(peak.series[0].max, [Some(500.0)]);
    }

    #[test]
    fn test_a_segments_opening_copy_carries_on_only_where_it_repeats_the_last_line() {
        let mut history = HostHistory::new();
        history.note_recorded_drops(T, vec![(8001, 100)], false);
        // The same process rolled over to a new segment: its copy repeats the last line.
        history.note_recorded_drops(T.saturating_add(30_000), vec![(8001, 100)], true);
        history.note_recorded_drops(T.saturating_add(31_000), vec![(8001, 130)], false);
        assert_eq!(
            recorded_drops(&history, T, 4),
            [None, Some(0.0), Some(0.0), Some(3.0)]
        );
    }

    #[test]
    fn test_another_run_of_the_process_counts_from_its_own_totals() {
        let mut history = HostHistory::new();
        history.note_recorded_drops(T, vec![(8001, 40)], false);
        // Started again ten seconds on, before its totals have a baseline taken off them.
        let restarted = T.saturating_add(10_000);
        history.note_recorded_drops(restarted, vec![(8001, 5_000_050)], true);
        history.note_recorded_drops(
            restarted.saturating_add(1000),
            vec![(8001, 5_000_060)],
            false,
        );
        assert_eq!(recorded_drops(&history, T, 2), [None, Some(1.0)]);
    }

    #[test]
    fn test_recorded_drops_after_a_long_silence_count_nothing_of_it() {
        let mut history = HostHistory::new();
        history.note_recorded_drops(T, vec![(8001, 40)], false);
        let later = T
            .saturating_add(MAX_RECORDED_QUIET_MILLIS)
            .saturating_add(10_000);
        history.note_recorded_drops(later, vec![(8001, 900)], false);
        let range = history.range(&[SeriesKey::UdpDrops], T, later.saturating_add(10_000), 240);
        assert!(range.series[0].mean.iter().all(Option::is_none));
    }

    #[test]
    fn test_a_segments_opening_copy_of_the_host_and_network_is_not_read_twice() {
        let mut history = HostHistory::new();
        let host = serde_json::to_string(&host()).unwrap();
        let network = r#"{"received_per_second":600,"sent_per_second":400}"#;
        assert_eq!(history.note_recorded(T, "host", &host, true), Some(true));
        assert_eq!(
            history.note_recorded(T, "network", network, true),
            Some(true)
        );
        let range = history.range(
            &[SeriesKey::Cpu, SeriesKey::NetIn],
            T,
            T.saturating_add(10_000),
            1,
        );
        assert_eq!(range.held_from, None);
        assert_eq!(
            history.note_recorded(T, "ingest_paths", "[1,2", true),
            Some(false)
        );
    }

    #[test]
    fn test_series_keys_are_named_in_snake_case_on_the_wire() {
        let keys: Vec<SeriesKey> =
            serde_json::from_str(r#"["cpu","validator_memory","net_in","udp_drops"]"#).unwrap();
        assert_eq!(
            keys,
            [
                SeriesKey::Cpu,
                SeriesKey::ValidatorMemory,
                SeriesKey::NetIn,
                SeriesKey::UdpDrops
            ]
        );
        assert!(serde_json::from_str::<Vec<SeriesKey>>(r#"["swap"]"#).is_err());
        assert_eq!(SeriesKey::UdpDrops as usize, SERIES.saturating_sub(1));
    }
}
