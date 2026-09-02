//! What an operator can set. Everything else the dashboard needs to know is a
//! constant next to the code that reads it: sampling rates in `collect`, buffer
//! sizes in `server`. Only these two reach a command-line flag, so only these
//! two travel as configuration.

use {solana_pubkey::Pubkey, std::net::SocketAddr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DashboardConfig {
    pub listen_addr: SocketAddr,
    /// Host names this dashboard will answer to.
    ///
    /// A browser can be steered at a service on the machine it is running on by
    /// resolving a name the attacker controls to a loopback address — the page
    /// then counts as same-origin and the origin check below cannot tell the
    /// difference. Pinning the acceptable `Host` is what stops that.
    ///
    /// Address literals are always accepted and are not listed here — they
    /// cannot be rebound, so testing on `127.0.0.1:10999` or on a public IP
    /// needs no configuration. Only names need naming, and an operator serving
    /// the dashboard through a reverse proxy must add the public one, because
    /// the proxy forwards the name the visitor typed.
    pub allowed_hosts: Vec<String>,
    /// The jito tip payment program, where this validator runs one.
    ///
    /// The eight tip accounts are derived from it rather than written down,
    /// because the id differs between clusters. `None` on a validator with no
    /// tip programs at all, which is every plain agave one, and then no tips
    /// are read and no column is drawn.
    pub tip_payment_program_id: Option<Pubkey>,
    /// This validator's commission on tips, in basis points.
    ///
    /// Used for one figure on one page: what our own blocks earned us. Never
    /// applied to another validator's turn, whose commission is not ours to
    /// know.
    pub commission_bps: Option<u16>,
}

impl DashboardConfig {
    /// Answers to `localhost` and to any address literal; a domain has to be
    /// added by the caller.
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            allowed_hosts: vec!["localhost".to_string()],
            tip_payment_program_id: None,
            commission_bps: None,
        }
    }
}
