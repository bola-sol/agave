#![cfg(feature = "agave-unstable-api")]
//! A web dashboard for the Agave validator: a single-page app and a websocket
//! feed of its state on one port, read through handles the validator already
//! holds.

pub mod admin_socket;
pub mod bank_view;
pub mod blocks;
pub mod catch_up;
pub mod certs;
pub mod collect;
pub mod config;
pub mod context;
pub mod event_input;
#[cfg(test)]
pub(crate) mod fixture;
pub mod geoip;
pub mod history;
pub mod host_stats;
pub mod meters;
pub mod metrics_tap;
pub mod mirror;
pub mod net_ifaces;
pub mod net_stats;
pub mod produced;
pub mod proto;
pub mod refill;
pub mod rewind_log;
pub mod search;
pub mod server;
pub mod service;
pub mod slots;
pub mod snapshot;
pub mod source;
pub mod startup;
pub mod thread_stats;
pub mod tips;
pub mod turns;
pub mod udp_drops;
pub mod validator_info;
pub mod versions;

pub use {
    config::DashboardConfig,
    context::DashboardContext,
    service::DashboardService,
    solana_core::node_events::{metric_types as metric_events, types as events},
};
