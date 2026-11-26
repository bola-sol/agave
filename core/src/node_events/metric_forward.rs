//! Hands the metrics datapoints the metric streams carry to their publisher, and keeps the points
//! sent once an hour, a run or an epoch to send again.

use {
    super::metric_types::carried,
    crossbeam_channel::Sender,
    solana_clock::Epoch,
    solana_metrics::datapoint::DataPoint,
    std::{
        sync::{
            Arc, Mutex, MutexGuard, OnceLock, PoisonError,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    },
};

const XDP_NETWORK_CONFIG: &str = "xdp-network-config";
const WFSM_GOSSIP: &str = "wfsm_gossip";
const VOTE_ARRIVAL: &str = "consensus_vote_metrics";

/// The newest point of each one-shot row, and every vote arrival point of the newest epoch.
#[derive(Debug, Default)]
struct Latched {
    once: Vec<(DataPoint, u64)>,
    arrival_epoch: Option<Epoch>,
    arrivals: Vec<(DataPoint, u64)>,
}

#[derive(Debug, Default)]
pub struct MetricForward {
    forward: OnceLock<Sender<(DataPoint, u64)>>,
    forwarding: AtomicBool,
    forward_drops: AtomicU64,
    latched: Mutex<Latched>,
}

impl MetricForward {
    /// The process's one forwarder.
    pub fn global() -> &'static Arc<Self> {
        static GLOBAL: OnceLock<Arc<MetricForward>> = OnceLock::new();
        GLOBAL.get_or_init(Arc::default)
    }

    /// Watches every datapoint from now on, where nothing else holds the process's one observer.
    pub fn install() {
        let installed = solana_metrics::set_datapoint_observer(Box::new(|point| {
            Self::global().observe(point);
        }));
        if !installed {
            log::debug!("node events: metrics points are already observed");
        }
    }

    /// Hands every point a metric stream carries to `sender` while forwarding is on. Set once; a
    /// second call is ignored.
    pub fn forward_to(&self, sender: Sender<(DataPoint, u64)>) {
        let _ = self.forward.set(sender);
    }

    pub fn set_forwarding(&self, on: bool) {
        self.forwarding.store(on, Ordering::Relaxed);
    }

    /// Points the forwarding queue had no room for.
    pub fn forward_drops(&self) -> u64 {
        self.forward_drops.load(Ordering::Relaxed)
    }

    /// Points sent once an hour, a run or an epoch, with when each was observed.
    pub fn latched_points(&self) -> Vec<(DataPoint, u64)> {
        let latched = locked(&self.latched);
        latched
            .once
            .iter()
            .chain(&latched.arrivals)
            .cloned()
            .collect()
    }

    pub fn observe(&self, point: &DataPoint) {
        let now_millis = unix_millis();
        // From install, since the XDP and supermajority wait points come before the event system.
        self.latch(point, now_millis);
        if !self.forwarding.load(Ordering::Relaxed) || !carried(point.name) {
            return;
        }
        let sent = self
            .forward
            .get()
            .is_some_and(|sender| sender.try_send((point.clone(), now_millis)).is_ok());
        if !sent {
            self.forward_drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn latch(&self, point: &DataPoint, now_millis: u64) {
        match point.name {
            XDP_NETWORK_CONFIG | WFSM_GOSSIP => {
                let mut latched = locked(&self.latched);
                latched.once.retain(|(held, _)| held.name != point.name);
                latched.once.push((point.clone(), now_millis));
            }
            VOTE_ARRIVAL => {
                let Some(epoch) = number_named(point, "epoch") else {
                    return;
                };
                let mut latched = locked(&self.latched);
                match latched.arrival_epoch {
                    Some(held) if epoch < held => return,
                    Some(held) if epoch == held => (),
                    _ => {
                        latched.arrival_epoch = Some(epoch);
                        latched.arrivals.clear();
                    }
                }
                latched.arrivals.push((point.clone(), now_millis));
            }
            _ => (),
        }
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn number_named(point: &DataPoint, wanted: &str) -> Option<u64> {
    let (_, value) = point.fields.iter().find(|(name, _)| *name == wanted)?;
    value.strip_suffix('i')?.parse().ok()
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(name: &'static str, fields: &[(&'static str, &str)]) -> DataPoint {
        let mut point = DataPoint::new(name);
        for (field, value) in fields {
            point.fields.push((field, value.to_string()));
        }
        point
    }

    #[test]
    fn test_latched_points_keep_the_newest_once_points_and_the_newest_epochs_arrivals() {
        let forward = MetricForward::default();
        let (sender, _receiver) = crossbeam_channel::unbounded();
        forward.forward_to(sender);
        forward.observe(&named(WFSM_GOSSIP, &[("online_stake", "5i")]));
        forward.observe(&named(WFSM_GOSSIP, &[("online_stake", "9i")]));
        forward.observe(&named(XDP_NETWORK_CONFIG, &[]));
        for epoch in ["6i", "7i", "7i", "6i"] {
            forward.observe(&named(VOTE_ARRIVAL, &[("epoch", epoch)]));
        }
        forward.observe(&named("vote-tracking", &[]));

        let latched = forward.latched_points();
        let names: Vec<_> = latched.iter().map(|(point, _)| point.name).collect();
        assert_eq!(
            names,
            [WFSM_GOSSIP, XDP_NETWORK_CONFIG, VOTE_ARRIVAL, VOTE_ARRIVAL]
        );
        assert_eq!(number_named(&latched[0].0, "online_stake"), Some(9));
        assert!(
            latched[2..]
                .iter()
                .all(|(point, _)| number_named(point, "epoch") == Some(7))
        );
    }

    #[test]
    fn test_points_before_the_event_system_are_latched() {
        let forward = MetricForward::default();
        forward.observe(&named(XDP_NETWORK_CONFIG, &[]));
        let (sender, receiver) = crossbeam_channel::unbounded();
        forward.forward_to(sender);
        let latched = forward.latched_points();
        assert_eq!(latched.len(), 1);
        assert_eq!(latched[0].0.name, XDP_NETWORK_CONFIG);
        assert!(receiver.is_empty());
    }
}
