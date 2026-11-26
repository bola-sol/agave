//! Fee payers whose transactions fail in our blocks, against the same payers in other leaders'
//! blocks, for this epoch and the last.

pub use solana_core::node_events::blocks::{TxOutcome, tx_outcomes};
use {
    crate::{
        produced::{BlockFees, TopFeePayer, TopFeeTransaction},
        source::{BankView, Source},
    },
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        cmp::Reverse,
        collections::{BTreeMap, HashMap},
        sync::Arc,
    },
};
#[cfg(test)]
use {solana_entry::entry::Entry, solana_runtime::bank::Bank};

/// Flagged from this many transactions in our blocks in an epoch.
const MIN_TRANSACTIONS: u64 = 100;
const MIN_FAILED_PERCENT: u64 = 10;
/// 0.05 SOL of priority fees on failed transactions.
const MIN_PRIORITY_FAILED: u64 = 50_000_000;
/// Escalated above this many times its rate in other leaders' blocks.
const ESCALATE_RATIO: u128 = 3;
/// The rate elsewhere counts from this many transactions there.
const MIN_AWAY_TRANSACTIONS: u64 = 100;

/// Payers followed per epoch; past this, new payers count only in the epoch's totals.
const MAX_PAYERS: usize = 100_000;
/// A frozen bank is held until its block is whole in the blockstore, at most this many at once.
const MAX_WAITING: usize = 64;
/// Read blocks wait for their root; under TowerBFT that is thirty two slots behind.
const MAX_UNSETTLED: usize = 1_024;
/// A block not whole this far under the root is given up.
const WAIT_SLOTS: u64 = 64;

const TOP_PAYERS: usize = 10;
const MAX_ALERTS: usize = 20;
/// Payers and transactions listed for each of our blocks.
const TOP_IN_BLOCK: usize = 5;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Tally {
    transactions: u64,
    failed: u64,
    /// Lamports.
    priority_failed: u64,
}

impl Tally {
    fn add(&mut self, other: &Tally) {
        self.transactions = self.transactions.saturating_add(other.transactions);
        self.failed = self.failed.saturating_add(other.failed);
        self.priority_failed = self.priority_failed.saturating_add(other.priority_failed);
    }

    fn note(&mut self, failed: bool, priority_fee: u64) {
        self.transactions = self.transactions.saturating_add(1);
        if failed {
            self.failed = self.failed.saturating_add(1);
            self.priority_failed = self.priority_failed.saturating_add(priority_fee);
        }
    }
}

#[derive(Debug, Default)]
struct BlockTally {
    epoch: Epoch,
    ours: bool,
    /// Every non-vote transaction of ours whose result was found.
    total: Tally,
    payers: HashMap<Pubkey, Tally>,
}

#[derive(Debug, Default)]
struct Payer {
    ours: Tally,
    away: Tally,
    flagged_slot: Option<Slot>,
}

#[derive(Debug, Default)]
struct EpochPayers {
    blocks: u64,
    ours: Tally,
    /// The followed payers' transactions in other leaders' blocks.
    away: Tally,
    payers: HashMap<Pubkey, Payer>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Alert {
    Flagged,
    Escalated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EpochFigures {
    pub epoch: Epoch,
    pub blocks: u64,
    pub transactions: u64,
    pub failed: u64,
    pub priority_failed: u64,
    pub away_transactions: u64,
    pub away_failed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PayerRow {
    pub epoch: Epoch,
    pub payer: String,
    pub transactions: u64,
    pub failed: u64,
    pub priority_failed: u64,
    pub away_transactions: u64,
    pub away_failed: u64,
    pub alert: Option<Alert>,
    /// Our slot in which it first met the flag.
    pub flagged_slot: Option<Slot>,
}

/// Newest epoch first; the payers are the newest epoch's, by priority fees on failed transactions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeePayerSummary {
    pub epochs: Vec<EpochFigures>,
    pub payers: Vec<PayerRow>,
    pub alerts: Vec<PayerRow>,
}

#[derive(Default)]
pub(super) struct FeePayers {
    waiting: BTreeMap<Slot, (Arc<dyn BankView>, bool)>,
    unsettled: BTreeMap<Slot, BlockTally>,
    epochs: BTreeMap<Epoch, EpochPayers>,
    /// The root last settled to; read from bank forks, the same banks come back every tick.
    settled_root: Option<Slot>,
}

impl FeePayers {
    pub(super) fn note_frozen(&mut self, bank: &Arc<dyn BankView>, ours: bool) {
        let slot = bank.slot();
        if self.settled_root.is_some_and(|root| slot <= root)
            || self.waiting.contains_key(&slot)
            || self.unsettled.contains_key(&slot)
        {
            return;
        }
        self.waiting.insert(slot, (bank.clone(), ours));
        crate::metrics_tap::keep_newest(&mut self.waiting, MAX_WAITING);
    }

    /// Reads each held block once the blockstore has all of it, and lets its bank go. Returns what
    /// our own blocks paid.
    pub(super) fn read(&mut self, src: &dyn Source) -> Vec<BlockFees> {
        let mut fees = Vec::new();
        let full: Vec<Slot> = self
            .waiting
            .keys()
            .copied()
            .filter(|slot| src.is_full(*slot))
            .collect();
        for slot in full {
            let Some((bank, ours)) = self.waiting.remove(&slot) else {
                continue;
            };
            let followed = self.epochs.get(&bank.epoch());
            let followed =
                |payer: &Pubkey| followed.is_some_and(|epoch| epoch.payers.contains_key(payer));
            let Some(outcomes) = src.tx_outcomes(slot, bank.as_ref(), ours, &followed) else {
                continue;
            };
            let (tally, paid) = tally_outcomes(bank.epoch(), slot, ours, &outcomes);
            self.unsettled.insert(slot, tally);
            fees.extend(paid);
        }
        crate::metrics_tap::keep_newest(&mut self.unsettled, MAX_UNSETTLED);
        fees
    }

    /// Counts the read blocks at or under the root that were rooted, and drops the rest, which
    /// were on abandoned forks.
    pub(super) fn settle(&mut self, root: Slot, root_epoch: Epoch, is_root: impl Fn(Slot) -> bool) {
        let later = self.unsettled.split_off(&root.saturating_add(1));
        for (slot, tally) in std::mem::replace(&mut self.unsettled, later) {
            if is_root(slot) {
                self.count(slot, tally);
            }
        }
        self.waiting = self.waiting.split_off(&root.saturating_sub(WAIT_SLOTS));
        self.epochs = self.epochs.split_off(&root_epoch.saturating_sub(1));
        self.settled_root = Some(root);
    }

    fn count(&mut self, slot: Slot, tally: BlockTally) {
        let epoch = self.epochs.entry(tally.epoch).or_default();
        if tally.ours {
            epoch.blocks = epoch.blocks.saturating_add(1);
            epoch.ours.add(&tally.total);
            for (key, block) in &tally.payers {
                if epoch.payers.len() >= MAX_PAYERS && !epoch.payers.contains_key(key) {
                    continue;
                }
                let payer = epoch.payers.entry(*key).or_default();
                payer.ours.add(block);
                if payer.flagged_slot.is_none() && is_flagged(&payer.ours) {
                    payer.flagged_slot = Some(slot);
                }
            }
        } else {
            for (key, block) in &tally.payers {
                if let Some(payer) = epoch.payers.get_mut(key) {
                    payer.away.add(block);
                    epoch.away.add(block);
                }
            }
        }
    }

    pub(super) fn summary(&self) -> Option<FeePayerSummary> {
        let (newest, _) = self.epochs.last_key_value()?;
        let epochs = self
            .epochs
            .iter()
            .rev()
            .map(|(epoch, held)| EpochFigures {
                epoch: *epoch,
                blocks: held.blocks,
                transactions: held.ours.transactions,
                failed: held.ours.failed,
                priority_failed: held.ours.priority_failed,
                away_transactions: held.away.transactions,
                away_failed: held.away.failed,
            })
            .collect();

        // Ranked before any row is built, since an epoch can hold many thousands of payers.
        let mut ranked: Vec<(&Pubkey, &Payer)> = self
            .epochs
            .get(newest)
            .into_iter()
            .flat_map(|held| held.payers.iter())
            .filter(|(_, payer)| payer.ours.failed > 0)
            .collect();
        ranked.sort_unstable_by_key(|(_, payer)| {
            Reverse((payer.ours.priority_failed, payer.ours.failed))
        });
        let payers = ranked
            .into_iter()
            .take(TOP_PAYERS)
            .map(|(key, payer)| row(*newest, key, payer))
            .collect();

        let mut alerts: Vec<PayerRow> = self
            .epochs
            .iter()
            .flat_map(|(epoch, held)| {
                held.payers
                    .iter()
                    .filter(|(_, payer)| is_flagged(&payer.ours))
                    .map(|(key, payer)| row(*epoch, key, payer))
            })
            .collect();
        alerts.sort_by_key(|row| {
            Reverse((
                row.epoch,
                row.alert == Some(Alert::Escalated),
                row.priority_failed,
            ))
        });
        alerts.truncate(MAX_ALERTS);

        Some(FeePayerSummary {
            epochs,
            payers,
            alerts,
        })
    }
}

fn row(epoch: Epoch, key: &Pubkey, payer: &Payer) -> PayerRow {
    PayerRow {
        epoch,
        payer: key.to_string(),
        transactions: payer.ours.transactions,
        failed: payer.ours.failed,
        priority_failed: payer.ours.priority_failed,
        away_transactions: payer.away.transactions,
        away_failed: payer.away.failed,
        alert: alert(&payer.ours, &payer.away),
        flagged_slot: payer.flagged_slot,
    }
}

fn is_flagged(ours: &Tally) -> bool {
    ours.transactions >= MIN_TRANSACTIONS
        && ours.failed.saturating_mul(100) >= ours.transactions.saturating_mul(MIN_FAILED_PERCENT)
        && ours.priority_failed >= MIN_PRIORITY_FAILED
}

fn alert(ours: &Tally, away: &Tally) -> Option<Alert> {
    if !is_flagged(ours) {
        return None;
    }
    // Our rate over three times theirs, cross multiplied.
    let here = u128::from(ours.failed).saturating_mul(u128::from(away.transactions));
    let there = u128::from(away.failed)
        .saturating_mul(u128::from(ours.transactions))
        .saturating_mul(ESCALATE_RATIO);
    if away.transactions >= MIN_AWAY_TRANSACTIONS && here > there {
        Some(Alert::Escalated)
    } else {
        Some(Alert::Flagged)
    }
}

#[derive(Debug)]
struct PaidTransaction {
    signature: Option<Signature>,
    payer: Pubkey,
    failed: Option<bool>,
    priority: u64,
    paid: u64,
}

/// What one of our blocks' non-vote transactions paid, as they are read.
#[derive(Debug, Default)]
struct BlockPaid {
    payers: HashMap<Pubkey, TopFeePayer>,
    transactions: Vec<PaidTransaction>,
}

impl BlockPaid {
    fn note(
        &mut self,
        signature: Option<Signature>,
        payer: Pubkey,
        failed: Option<bool>,
        priority: u64,
        base: u64,
    ) {
        let paid = base.saturating_add(priority);
        let fees = self.payers.entry(payer).or_insert(TopFeePayer {
            payer,
            transactions: 0,
            failed: 0,
            priority: 0,
            paid: 0,
        });
        fees.transactions = fees.transactions.saturating_add(1);
        fees.failed = fees
            .failed
            .saturating_add(u64::from(failed.unwrap_or(false)));
        fees.priority = fees.priority.saturating_add(priority);
        fees.paid = fees.paid.saturating_add(paid);
        self.transactions.push(PaidTransaction {
            signature,
            payer,
            failed,
            priority,
            paid,
        });
    }

    fn top(mut self, slot: Slot) -> BlockFees {
        let payer_count = u64::try_from(self.payers.len()).unwrap_or(u64::MAX);
        let mut payers: Vec<TopFeePayer> = self.payers.into_values().collect();
        payers.sort_unstable_by_key(|fees| Reverse(fees.paid));
        payers.truncate(TOP_IN_BLOCK);
        self.transactions
            .sort_unstable_by_key(|row| Reverse(row.paid));
        self.transactions.truncate(TOP_IN_BLOCK);
        BlockFees {
            slot,
            payer_count,
            payers,
            transactions: self
                .transactions
                .into_iter()
                .map(|row| TopFeeTransaction {
                    signature: row
                        .signature
                        .map_or_else(String::new, |signature| signature.to_string()),
                    payer: row.payer,
                    failed: row.failed,
                    priority: row.priority,
                    paid: row.paid,
                })
                .collect(),
        }
    }
}

/// Every non-vote transaction of our block, or only the followed payers' in another's. A result
/// not found in the status cache leaves the transaction out of the tally, though not out of what
/// the block paid.
#[cfg(test)]
fn tally_block(
    bank: &Bank,
    entries: &[Entry],
    ours: bool,
    followed: impl Fn(&Pubkey) -> bool,
) -> (BlockTally, Option<BlockFees>) {
    let outcomes = tx_outcomes(bank, entries, ours, &followed);
    tally_outcomes(bank.epoch(), bank.slot(), ours, &outcomes)
}

/// A result not found in the status cache leaves the transaction out of the tally, though not
/// out of what the block paid.
fn tally_outcomes(
    epoch: Epoch,
    slot: Slot,
    ours: bool,
    outcomes: &[TxOutcome],
) -> (BlockTally, Option<BlockFees>) {
    let mut tally = BlockTally {
        epoch,
        ours,
        ..BlockTally::default()
    };
    let mut paid = BlockPaid::default();
    for outcome in outcomes {
        if ours {
            paid.note(
                outcome.signature,
                outcome.payer,
                outcome.failed,
                outcome.priority,
                outcome.base,
            );
        }
        let Some(failed) = outcome.failed else {
            continue;
        };
        tally
            .payers
            .entry(outcome.payer)
            .or_default()
            .note(failed, outcome.priority);
        if ours {
            tally.total.note(failed, outcome.priority);
        }
    }
    (tally, ours.then(|| paid.top(slot)))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_core::node_events::blocks::base_fee,
        solana_keypair::Keypair,
        solana_message::{VersionedMessage, compiled_instruction::CompiledInstruction, legacy},
        solana_runtime::genesis_utils::{GenesisConfigInfo, create_genesis_config},
        solana_sdk_ids::{ed25519_program, secp256k1_program, secp256r1_program},
        solana_signature::Signature,
        solana_signer::Signer,
        solana_system_interface::instruction::transfer,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
    };

    fn tally(transactions: u64, failed: u64, priority_failed: u64) -> Tally {
        Tally {
            transactions,
            failed,
            priority_failed,
        }
    }

    fn block(epoch: Epoch, ours: bool, payers: &[(Pubkey, Tally)]) -> BlockTally {
        let mut total = Tally::default();
        for (_, payer) in payers {
            total.add(payer);
        }
        BlockTally {
            epoch,
            ours,
            total,
            payers: payers.iter().copied().collect(),
        }
    }

    #[test]
    fn flags_at_each_threshold() {
        assert!(is_flagged(&tally(100, 10, MIN_PRIORITY_FAILED)));
        assert!(!is_flagged(&tally(99, 99, MIN_PRIORITY_FAILED)));
        assert!(!is_flagged(&tally(100, 9, MIN_PRIORITY_FAILED)));
        assert!(!is_flagged(&tally(
            100,
            100,
            MIN_PRIORITY_FAILED.saturating_sub(1)
        )));
    }

    #[test]
    fn escalates_over_three_times_the_rate_elsewhere() {
        let ours = tally(100, 40, MIN_PRIORITY_FAILED);
        // 40% against 13% is over three times, against 14% not.
        assert_eq!(alert(&ours, &tally(100, 13, 0)), Some(Alert::Escalated));
        assert_eq!(alert(&ours, &tally(100, 14, 0)), Some(Alert::Flagged));
        assert_eq!(alert(&ours, &tally(100, 0, 0)), Some(Alert::Escalated));
        // Too few elsewhere to judge.
        assert_eq!(alert(&ours, &tally(99, 0, 0)), Some(Alert::Flagged));
        assert_eq!(alert(&tally(100, 5, 0), &tally(100, 0, 0)), None);
    }

    #[test]
    fn counts_rooted_blocks_and_follows_our_payers_elsewhere() {
        let (spammer, other) = (Pubkey::new_unique(), Pubkey::new_unique());
        let mut payers = FeePayers::default();
        payers.unsettled.insert(
            10,
            block(5, true, &[(spammer, tally(120, 60, MIN_PRIORITY_FAILED))]),
        );
        // Another leader's block, and one on a fork that was abandoned.
        payers.unsettled.insert(
            11,
            block(
                5,
                false,
                &[(spammer, tally(200, 10, 0)), (other, tally(50, 50, 0))],
            ),
        );
        payers
            .unsettled
            .insert(12, block(5, true, &[(other, tally(500, 500, u64::MAX))]));
        payers.unsettled.insert(13, block(5, true, &[]));
        payers.settle(12, 5, |slot| slot != 12);

        assert_eq!(payers.unsettled.keys().copied().collect::<Vec<_>>(), [13]);
        let summary = payers.summary().unwrap();
        assert_eq!(
            summary.epochs,
            [EpochFigures {
                epoch: 5,
                blocks: 1,
                transactions: 120,
                failed: 60,
                priority_failed: MIN_PRIORITY_FAILED,
                away_transactions: 200,
                away_failed: 10,
            }]
        );
        assert_eq!(summary.payers.len(), 1);
        let row = &summary.alerts[0];
        assert_eq!(row.payer, spammer.to_string());
        assert_eq!(row.alert, Some(Alert::Escalated));
        assert_eq!(row.flagged_slot, Some(10));
    }

    #[test]
    fn keeps_this_epoch_and_the_last() {
        let payer = Pubkey::new_unique();
        let mut payers = FeePayers::default();
        for (slot, epoch) in [(1, 3), (2, 4), (3, 5)] {
            payers
                .unsettled
                .insert(slot, block(epoch, true, &[(payer, tally(1, 1, 1))]));
        }
        payers.settle(3, 5, |_| true);
        let summary = payers.summary().unwrap();
        let epochs: Vec<Epoch> = summary.epochs.iter().map(|figures| figures.epoch).collect();
        assert_eq!(epochs, [5, 4]);
        assert!(summary.payers.iter().all(|row| row.epoch == 5));
    }

    #[test]
    fn orders_alerts_newest_epoch_and_escalated_first() {
        let (a, b, c) = (
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        );
        let flagged = tally(100, 100, MIN_PRIORITY_FAILED);
        let mut payers = FeePayers::default();
        payers.unsettled.insert(1, block(4, true, &[(a, flagged)]));
        payers
            .unsettled
            .insert(2, block(5, true, &[(b, flagged), (c, flagged)]));
        payers
            .unsettled
            .insert(3, block(5, false, &[(c, tally(100, 1, 0))]));
        payers.settle(3, 5, |_| true);
        let alerts: Vec<(String, Option<Alert>)> = payers
            .summary()
            .unwrap()
            .alerts
            .into_iter()
            .map(|row| (row.payer, row.alert))
            .collect();
        assert_eq!(
            alerts,
            [
                (c.to_string(), Some(Alert::Escalated)),
                (b.to_string(), Some(Alert::Flagged)),
                (a.to_string(), Some(Alert::Flagged)),
            ]
        );
    }

    #[test]
    fn reads_results_and_priority_fees_from_the_bank() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(1_000_000_000);
        let (bank, _forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let blockhash = bank.last_blockhash();
        let to = Pubkey::new_unique();
        let paid = |lamports: u64| {
            Transaction::new_signed_with_payer(
                &[
                    ComputeBudgetInstruction::set_compute_unit_limit(10_000),
                    ComputeBudgetInstruction::set_compute_unit_price(2_000_000),
                    transfer(&mint_keypair.pubkey(), &to, lamports),
                ],
                Some(&mint_keypair.pubkey()),
                &[&mint_keypair],
                blockhash,
            )
        };
        let landed = paid(1_000);
        let failed = paid(10_000_000_000);
        bank.process_transaction(&landed).unwrap();
        assert!(bank.process_transaction(&failed).is_err());
        let unprocessed = solana_system_transaction::transfer(&Keypair::new(), &to, 1, blockhash);
        let entries = [Entry {
            num_hashes: 1,
            hash: blockhash,
            transactions: [landed, failed, unprocessed]
                .into_iter()
                .map(VersionedTransaction::from)
                .collect(),
        }];

        let (ours, fees) = tally_block(&bank, &entries, true, |_| false);
        // 2,000,000 micro-lamports per unit over a 10,000 unit limit.
        assert_eq!(ours.total, tally(2, 1, 20_000));
        assert_eq!(ours.payers[&mint_keypair.pubkey()], tally(2, 1, 20_000));

        let per_signature = bank.get_lamports_per_signature();
        let fees = fees.expect("our block's fees");
        assert_eq!(fees.slot, bank.slot());
        // The unprocessed transaction has no result, yet is counted as paid.
        assert_eq!(fees.transactions.len(), 3);
        assert_eq!(fees.payer_count, 2);
        let top = &fees.payers[0];
        assert_eq!(top.payer, mint_keypair.pubkey());
        assert_eq!((top.transactions, top.failed), (2, 1));
        assert_eq!(top.priority, 40_000);
        assert_eq!(
            top.paid,
            per_signature.saturating_mul(2).saturating_add(40_000)
        );
        let results: Vec<Option<bool>> = fees.transactions.iter().map(|row| row.failed).collect();
        assert!(results.contains(&Some(false)) && results.contains(&Some(true)));
        assert_eq!(results[2], None);
        assert_eq!(fees.transactions[0].priority, 20_000);
        assert_eq!(
            fees.transactions[0].signature.len(),
            entries[0].transactions[0].signatures[0].to_string().len()
        );

        let (away, no_fees) = tally_block(&bank, &entries, false, |payer| {
            *payer == mint_keypair.pubkey()
        });
        assert_eq!(away.total, Tally::default());
        assert_eq!(away.payers[&mint_keypair.pubkey()], tally(2, 1, 0));
        assert!(no_fees.is_none());
        assert!(
            tally_block(&bank, &entries, false, |_| false)
                .0
                .payers
                .is_empty()
        );
    }

    #[test]
    fn lists_a_blocks_five_highest_payers_and_transactions() {
        let transaction = VersionedTransaction::default();
        let mut paid = BlockPaid::default();
        let payers: Vec<Pubkey> = (0..7).map(|_| Pubkey::new_unique()).collect();
        for (fee, payer) in (1u64..).zip(&payers) {
            paid.note(
                transaction.signatures.first().copied(),
                *payer,
                Some(false),
                fee,
                0,
            );
        }
        // Many small payments outrank the larger single ones, by payer but not by transaction.
        for _ in 0..30 {
            paid.note(
                transaction.signatures.first().copied(),
                payers[0],
                Some(true),
                1,
                0,
            );
        }
        let fees = paid.top(9);
        assert_eq!(fees.slot, 9);
        assert_eq!(fees.payer_count, 7);
        assert_eq!(fees.payers.len(), TOP_IN_BLOCK);
        assert_eq!(fees.payers[0].payer, payers[0]);
        assert_eq!(fees.payers[0].paid, 31);
        assert_eq!(fees.payers[0].failed, 30);
        assert_eq!(fees.transactions.len(), TOP_IN_BLOCK);
        let highest: Vec<u64> = fees.transactions.iter().map(|row| row.paid).collect();
        assert_eq!(highest, [7, 6, 5, 4, 3]);
        assert_eq!(fees.transactions[0].payer, payers[6]);
    }

    #[test]
    fn charges_each_signature_a_precompile_verifies() {
        let with_program = |program: Pubkey, data: Vec<u8>| VersionedTransaction {
            signatures: vec![Signature::default(); 2],
            message: VersionedMessage::Legacy(legacy::Message {
                account_keys: vec![Pubkey::new_unique(), Pubkey::new_unique(), program],
                instructions: vec![CompiledInstruction {
                    program_id_index: 2,
                    accounts: vec![],
                    data,
                }],
                ..legacy::Message::default()
            }),
        };
        let fee = |program, data| base_fee(&with_program(program, data), 5_000);
        assert_eq!(fee(ed25519_program::id(), vec![3, 9]), 25_000);
        assert_eq!(fee(secp256k1_program::id(), vec![1]), 15_000);
        assert_eq!(fee(secp256r1_program::id(), vec![2]), 20_000);
        assert_eq!(fee(ed25519_program::id(), vec![]), 10_000);
        // The first byte of any other program's data counts for nothing.
        assert_eq!(fee(Pubkey::new_unique(), vec![200]), 10_000);
    }
}
