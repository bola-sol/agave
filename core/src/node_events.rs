//! The node's state as event streams, for a monitor in its own process: banks, ledger, gossip,
//! validator info, the node's catch-up and the metrics datapoints.

pub mod bank_view;
pub mod blocks;
pub mod catch_up;
pub mod info;
pub mod metric_forward;
pub mod metric_types;
pub mod output;
pub mod reader;
pub mod types;
pub mod versions;

use {
    agave_event_system::EventSystem,
    output::{EventOutput, MetricOutput},
    reader::Reader,
    std::{
        io,
        sync::{Arc, atomic::AtomicBool},
        thread::{self, JoinHandle},
    },
};

pub struct NodeEvents {
    threads: Vec<JoinHandle<()>>,
}

impl NodeEvents {
    /// Every stream is created; the event filter decides which are published.
    pub fn new(
        event_system: &EventSystem,
        reader: Reader,
        exit: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        let events = EventOutput::new(event_system)?.spawn(Arc::new(reader), exit.clone())?;
        let metrics = MetricOutput::new(event_system)?.spawn(exit)?;
        Ok(Self {
            threads: std::iter::once(events).chain(metrics).collect(),
        })
    }

    pub fn join(self) -> thread::Result<()> {
        self.threads.into_iter().try_for_each(JoinHandle::join)
    }
}
