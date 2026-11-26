//! Validator display names from on-chain `ValidatorInfo` config accounts,
//! found by owner through the secondary index and kept current from each
//! slot's writes. Without the index the dashboard shows pubkeys.

use {
    serde::{Deserialize, Serialize},
    solana_account::ReadableAccount,
    solana_accounts_db::accounts_index::IndexKey,
    solana_clock::Slot,
    solana_config_interface::state::{ConfigKeys, get_config_data},
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    std::{
        collections::{HashMap, hash_map::Entry},
        sync::{Arc, OnceLock},
    },
};

pub const VALIDATOR_INFO_PROGRAM: Pubkey =
    Pubkey::from_str_const("Va1idator1nfo111111111111111111111111111111");

pub const MAX_VALIDATOR_INFO_LEN: usize = 576 + 1 + (32 + 1) * 2;

/// The account also carries a website, description and keybase name, which nothing displays.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ValidatorInfo {
    pub name: Option<String>,
    #[serde(rename = "iconUrl")]
    pub icon_url: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Displays {
    pub keys: Vec<String>,
    pub names: Vec<Option<String>>,
    pub icons: Vec<Option<String>>,
}

#[derive(Debug, Default)]
pub struct ValidatorInfoCache {
    /// Each entry with the slot it was read at.
    by_identity: HashMap<Pubkey, (Slot, ValidatorInfo)>,
    /// `displays` as JSON, made on the first request after an entry changes.
    displays_json: OnceLock<Arc<str>>,
}

impl ValidatorInfoCache {
    /// Every entry, with the newest slot any was read at.
    pub fn entries(&self) -> (Slot, Vec<(Pubkey, ValidatorInfo)>) {
        let slot = self
            .by_identity
            .values()
            .map(|(slot, _)| *slot)
            .max()
            .unwrap_or_default();
        let entries = self
            .by_identity
            .iter()
            .map(|(identity, (_, info))| (*identity, info.clone()))
            .collect();
        (slot, entries)
    }

    pub fn is_empty(&self) -> bool {
        self.by_identity.is_empty()
    }

    pub fn get(&self, identity: &Pubkey) -> Option<&ValidatorInfo> {
        self.by_identity.get(identity).map(|(_, info)| info)
    }

    pub fn displays(&self) -> Displays {
        let mut keys = Vec::with_capacity(self.by_identity.len());
        let mut names = Vec::with_capacity(self.by_identity.len());
        let mut icons = Vec::with_capacity(self.by_identity.len());
        for (identity, (_, info)) in &self.by_identity {
            if info.name.is_none() && info.icon_url.is_none() {
                continue;
            }
            keys.push(identity.to_string());
            names.push(info.name.clone());
            icons.push(info.icon_url.clone());
        }
        Displays { keys, names, icons }
    }

    /// The `summary.displays` value, encoded once and shared until an entry changes.
    pub fn displays_json(&self) -> Arc<str> {
        Arc::clone(self.displays_json.get_or_init(|| {
            match serde_json::to_string(&self.displays()) {
                Ok(json) => Arc::from(json),
                Err(err) => {
                    log::error!("dashboard: failed to encode the display table: {err}");
                    Arc::from("null")
                }
            }
        }))
    }

    /// A read from an older slot than the one held is ignored.
    pub fn insert(&mut self, identity: Pubkey, slot: Slot, info: ValidatorInfo) -> bool {
        match self.by_identity.entry(identity) {
            Entry::Vacant(entry) => {
                entry.insert((slot, info));
            }
            Entry::Occupied(mut entry) => {
                let (held_slot, held) = entry.get_mut();
                if slot < *held_slot {
                    return false;
                }
                *held_slot = slot;
                if *held == info {
                    return false;
                }
                *held = info;
            }
        }
        self.displays_json = OnceLock::new();
        true
    }

    /// Separate from the scan so the lock is held only for the merge. `slot` is the scanned bank's.
    pub fn merge(&mut self, slot: Slot, entries: Vec<(Pubkey, ValidatorInfo)>) -> usize {
        entries
            .into_iter()
            .map(|(identity, info)| self.insert(identity, slot, info))
            .filter(|changed| *changed)
            .count()
    }
}

pub fn scan_slot(bank: &Bank) -> Vec<(Pubkey, ValidatorInfo)> {
    bank.get_program_accounts_modified_since_parent(&solana_sdk_ids::config::id())
        .into_iter()
        .filter_map(|(_pubkey, account)| parse(account.data()))
        .collect()
}

/// Minutes on a real cluster: run off-thread with no lock held.
pub fn scan_all(bank: &Bank) -> Vec<(Pubkey, ValidatorInfo)> {
    let config_id = solana_sdk_ids::config::id();

    // Without the config program in the index, the indexed call reads every account on the
    // validator, which takes hours and cannot be interrupted.
    if !bank.account_indexes_include_key(&config_id) {
        log::info!(
            "dashboard: the config program is excluded from the account index, so validator names \
             are unavailable and the dashboard will show pubkeys"
        );
        return Vec::new();
    }

    let accounts = match bank.get_filtered_indexed_accounts(
        &IndexKey::ProgramId(config_id),
        |account| account.owner() == &config_id,
        None,
    ) {
        Ok(accounts) => accounts,
        Err(err) => {
            log::warn!("dashboard: could not read validator info accounts: {err}");
            return Vec::new();
        }
    };

    // An empty result on a live cluster means the index is not switched on: an
    // index never built answers none rather than failing.
    if accounts.is_empty() {
        log::info!(
            "dashboard: found no validator info accounts. Start the validator with \
             --account-index program-id --account-index-include-key {config_id} to show validator \
             names instead of pubkeys"
        );
    }

    accounts
        .into_iter()
        .filter_map(|(_pubkey, account)| parse(account.data()))
        .collect()
}

pub fn parse(data: &[u8]) -> Option<(Pubkey, ValidatorInfo)> {
    if data.len() > MAX_VALIDATOR_INFO_LEN {
        return None;
    }
    let keys = bincode::deserialize::<ConfigKeys>(data).ok()?;
    // The first key marks the account type; the second is the identity that
    // signed for it, which is what we key the cache on.
    if keys.keys.first()?.0 != VALIDATOR_INFO_PROGRAM {
        return None;
    }
    let identity = keys.keys.get(1)?.0;

    let json = bincode::deserialize::<String>(get_config_data(data).ok()?).ok()?;
    let info = serde_json::from_str::<ValidatorInfo>(&json).ok()?;
    Some((identity, info))
}
