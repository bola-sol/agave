pub use solana_core::node_events::info::*;
#[cfg(test)]
use {solana_config_interface::state::ConfigKeys, solana_pubkey::Pubkey};

#[cfg(test)]
fn encode(keys: Vec<(Pubkey, bool)>, json: &str) -> Vec<u8> {
    let mut data = bincode::serialize(&ConfigKeys { keys }).unwrap();
    data.extend(bincode::serialize(&json.to_string()).unwrap());
    data
}

/// A validator info account naming `identity`, for tests elsewhere too.
#[cfg(test)]
pub(crate) fn info_account(identity: Pubkey, json: &str) -> solana_account::AccountSharedData {
    solana_account::AccountSharedData::from(solana_account::Account {
        lamports: 1,
        data: encode(
            vec![(VALIDATOR_INFO_PROGRAM, false), (identity, true)],
            json,
        ),
        owner: solana_sdk_ids::config::id(),
        executable: false,
        rent_epoch: 0,
    })
}

#[cfg(test)]
mod tests {
    use {super::*, crate::fixture::fixture, std::sync::Arc};

    #[test]
    fn test_the_slot_sweep_finds_info_written_in_that_slot() {
        let harness = fixture();
        let identity = Pubkey::new_unique();
        let bank = harness.advance_with(
            1,
            &[(
                Pubkey::new_unique(),
                info_account(identity, r#"{"name":"Lantern"}"#),
            )],
        );

        let found = scan_slot(&bank);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, identity);
        assert_eq!(found[0].1.name.as_deref(), Some("Lantern"));
    }

    #[test]
    fn test_the_slot_sweep_ignores_slots_that_wrote_nothing() {
        let harness = fixture();
        harness.advance_with(
            1,
            &[(
                Pubkey::new_unique(),
                info_account(Pubkey::new_unique(), r#"{"name":"Lantern"}"#),
            )],
        );
        let later = harness.advance_to(2);

        assert!(
            scan_slot(&later).is_empty(),
            "a slot's sweep must cover its own writes only"
        );
    }

    #[test]
    fn test_the_full_scan_finds_info_from_an_earlier_slot() {
        let harness = fixture();
        let identity = Pubkey::new_unique();
        harness.advance_with(
            1,
            &[(
                Pubkey::new_unique(),
                info_account(identity, r#"{"name":"Lantern"}"#),
            )],
        );
        let later = harness.advance_to(2);

        let found = scan_all(&later);
        assert_eq!(found.len(), 1, "the scan should reach back past this slot");
        assert_eq!(found[0].0, identity);
    }

    #[test]
    fn test_merging_reports_only_what_changed() {
        let mut cache = ValidatorInfoCache::default();

        let identity = Pubkey::new_unique();
        let info = ValidatorInfo {
            name: Some("Lantern".into()),
            icon_url: None,
        };
        assert_eq!(cache.merge(1, vec![(identity, info.clone())]), 1);
        assert_eq!(cache.merge(1, vec![(identity, info)]), 0, "nothing changed");
        assert_eq!(
            cache.get(&identity).and_then(|info| info.name.as_deref()),
            Some("Lantern")
        );
    }

    #[test]
    fn test_entries_hand_back_every_name_at_the_newest_slot() {
        let mut cache = ValidatorInfoCache::default();
        assert_eq!(cache.entries(), (0, Vec::new()));
        let (first, second) = (Pubkey::new_unique(), Pubkey::new_unique());
        let named = |name: &str| ValidatorInfo {
            name: Some(name.into()),
            icon_url: None,
        };
        cache.merge(4, vec![(first, named("Lantern"))]);
        cache.merge(9, vec![(second, named("Other"))]);
        let (slot, mut entries) = cache.entries();
        entries.sort_by_key(|(identity, _)| *identity);
        let mut expected = vec![(first, named("Lantern")), (second, named("Other"))];
        expected.sort_by_key(|(identity, _)| *identity);
        assert_eq!(slot, 9);
        assert_eq!(entries, expected);
    }

    #[test]
    fn test_parses_a_validator_info_account() {
        let identity = Pubkey::new_unique();
        let data = encode(
            vec![(VALIDATOR_INFO_PROGRAM, false), (identity, true)],
            r#"{"name":"Lantern","iconUrl":"https://example.com/i.png"}"#,
        );
        let (parsed_identity, info) = parse(&data).unwrap();
        assert_eq!(parsed_identity, identity);
        assert_eq!(info.name.as_deref(), Some("Lantern"));
        assert_eq!(info.icon_url.as_deref(), Some("https://example.com/i.png"));
    }

    #[test]
    fn test_fields_the_dashboard_does_not_render_are_ignored() {
        // An unknown field must be skipped, or a validator publishing a website would
        // lose its name too.
        let identity = Pubkey::new_unique();
        let data = encode(
            vec![(VALIDATOR_INFO_PROGRAM, false), (identity, true)],
            r#"{"name":"Lantern","website":"https://example.com","details":"hi",
                "keybaseUsername":"lantern"}"#,
        );
        let (_, info) = parse(&data).unwrap();
        assert_eq!(info.name.as_deref(), Some("Lantern"));
    }

    #[test]
    fn test_ignores_other_config_accounts() {
        let data = encode(
            vec![(Pubkey::new_unique(), false), (Pubkey::new_unique(), true)],
            r#"{"name":"nope"}"#,
        );
        assert!(parse(&data).is_none());
    }

    #[test]
    fn test_ignores_malformed_data() {
        assert!(parse(&[]).is_none());
        assert!(parse(&[0xff; 32]).is_none());
        assert!(parse(&vec![0u8; MAX_VALIDATOR_INFO_LEN.saturating_add(1)]).is_none());
    }

    #[test]
    fn test_an_older_read_does_not_replace_a_newer_one() {
        let mut cache = ValidatorInfoCache::default();
        let identity = Pubkey::new_unique();
        let named = |name: &str| ValidatorInfo {
            name: Some(name.into()),
            icon_url: None,
        };
        let held =
            |cache: &ValidatorInfoCache| cache.get(&identity).and_then(|info| info.name.clone());

        assert_eq!(cache.merge(10, vec![(identity, named("New"))]), 1);
        assert_eq!(
            cache.merge(5, vec![(identity, named("Old"))]),
            0,
            "a scan of an older bank finishing late must not revert the name"
        );
        assert_eq!(held(&cache).as_deref(), Some("New"));

        assert_eq!(cache.merge(12, vec![(identity, named("Newer"))]), 1);
        assert_eq!(held(&cache).as_deref(), Some("Newer"));

        // An unchanged read still moves the entry's slot forward.
        assert_eq!(cache.merge(20, vec![(identity, named("Newer"))]), 0);
        assert_eq!(cache.merge(15, vec![(identity, named("Older"))]), 0);
        assert_eq!(held(&cache).as_deref(), Some("Newer"));
    }

    #[test]
    fn test_the_encoded_table_is_kept_until_an_entry_changes() {
        let mut cache = ValidatorInfoCache::default();
        let identity = Pubkey::new_unique();
        let named = |name: &str| ValidatorInfo {
            name: Some(name.into()),
            icon_url: None,
        };
        let fresh = |cache: &ValidatorInfoCache| serde_json::to_string(&cache.displays()).unwrap();

        assert_eq!(&*cache.displays_json(), fresh(&cache).as_str());
        cache.insert(identity, 10, named("Lantern"));
        let held = cache.displays_json();
        assert_eq!(&*held, fresh(&cache).as_str());

        // Reads that change nothing shown keep the encoding.
        cache.insert(identity, 12, named("Lantern"));
        cache.insert(identity, 5, named("Old"));
        assert!(Arc::ptr_eq(&held, &cache.displays_json()));

        cache.insert(identity, 14, named("Beacon"));
        assert_eq!(&*cache.displays_json(), fresh(&cache).as_str());
        assert!(cache.displays_json().contains("Beacon"));

        cache.insert(Pubkey::new_unique(), 14, named("Harbour"));
        assert_eq!(&*cache.displays_json(), fresh(&cache).as_str());
        assert!(cache.displays_json().contains("Harbour"));
    }
}
