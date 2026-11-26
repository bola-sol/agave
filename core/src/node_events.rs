//! What is still read from the validator's handles for the dashboard: each full block's
//! transaction outcomes and our blocks' message versions.

pub mod bank_view;
pub mod blocks;
pub mod info;
pub mod output;
pub mod reader;
pub mod types;
pub mod versions;

use {
    agave_event_system::EventSystem,
    output::EventOutput,
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
        let metrics = crate::metric_events::MetricOutput::new(event_system)?.spawn(exit)?;
        Ok(Self {
            threads: std::iter::once(events).chain(metrics).collect(),
        })
    }

    pub fn join(self) -> thread::Result<()> {
        self.threads.into_iter().try_for_each(JoinHandle::join)
    }
}
