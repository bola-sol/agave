//! Blocks as the node events read them: a block's footer, and its non-vote transactions'
//! results and message versions.

use {
    crate::versions::is_simple_vote,
    agave_feature_set::FeatureSet,
    solana_clock::Slot,
    solana_compute_budget_instruction::instructions_processor::process_compute_budget_instructions,
    solana_entry::{
        block_component::{
            BlockComponent, BlockFooterV1, VersionedBlockFooter, VersionedBlockMarker,
        },
        entry::Entry,
    },
    solana_ledger::{blockstore::Blockstore, shred::DATA_SHREDS_PER_FEC_BLOCK},
    solana_message::VersionedMessage,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_sdk_ids::{ed25519_program, secp256k1_program, secp256r1_program},
    solana_signature::Signature,
    solana_svm_transaction::instruction::SVMInstruction,
    solana_transaction::versioned::VersionedTransaction,
};

/// The footer sits in the second to last FEC set; the last one closes the
/// block. Reading from there costs two sets of shreds whatever the block holds.
const FOOTER_SPAN: u64 = 2 * DATA_SHREDS_PER_FEC_BLOCK as u64;

/// What a block footer carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FooterFacts {
    /// The leader's close time, unix nanoseconds.
    pub close_nanos: u64,
    pub notar_bitmap: Option<Vec<u8>>,
    pub skip_bitmap: Option<Vec<u8>>,
    /// Whether the finalization certificate it carries is the fast one.
    pub fast: Option<bool>,
}

impl From<&BlockFooterV1> for FooterFacts {
    fn from(footer: &BlockFooterV1) -> Self {
        Self {
            close_nanos: footer.block_producer_time_nanos,
            notar_bitmap: footer
                .notar_reward_cert
                .as_ref()
                .map(|cert| cert.bitmap().to_vec()),
            skip_bitmap: footer
                .skip_reward_cert
                .as_ref()
                .map(|cert| cert.to_bitmap().to_vec()),
            fast: carried_fast(footer),
        }
    }
}

pub enum Block {
    Footer(Box<FooterFacts>),
    Missing,
    Pending,
    Opaque,
}

/// A fast certificate is one notarize aggregate; a slow one adds the notarize aggregate to its
/// finalize votes.
pub fn carried_fast(footer: &BlockFooterV1) -> Option<bool> {
    footer
        .block_final_cert
        .as_ref()
        .map(|cert| cert.notar_aggregate.is_none())
}

pub fn read_block(blockstore: &Blockstore, slot: Slot, root: Slot) -> Block {
    if slot > root {
        return Block::Pending;
    }
    // Blockstore roots are written before bank forks' root moves, so a slot up to the root that
    // is not one was skipped.
    if !blockstore.is_root(slot) {
        return Block::Missing;
    }
    let meta = match blockstore.meta(slot) {
        Ok(Some(meta)) if meta.is_full() => meta,
        // Rooted but not held, as a downloaded snapshot's slot.
        _ => return Block::Opaque,
    };
    let Some(last) = meta.last_index else {
        return Block::Opaque;
    };
    let start = last.saturating_add(1).saturating_sub(FOOTER_SPAN);
    let Ok((components, _, _)) = blockstore.get_slot_components_with_shred_info(slot, start, false)
    else {
        return Block::Opaque;
    };
    components
        .into_iter()
        .rev()
        .find_map(|component| match component {
            BlockComponent::BlockMarker(VersionedBlockMarker::V1(marker)) => marker
                .as_block_footer()
                .map(|VersionedBlockFooter::V1(footer)| footer.clone()),
            BlockComponent::EntryBatch(_) => None,
        })
        .map_or(Block::Opaque, |footer| {
            Block::Footer(Box::new(FooterFacts::from(&footer)))
        })
}

/// One non-vote transaction of a block as the fee tallies read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxOutcome {
    pub signature: Option<Signature>,
    pub payer: Pubkey,
    /// `None` where the status cache held no result.
    pub failed: Option<bool>,
    /// Read for our own blocks only.
    pub priority: u64,
    pub base: u64,
}

/// Every non-vote transaction of our block, or only the followed payers' in another's.
pub fn tx_outcomes(
    bank: &Bank,
    entries: &[Entry],
    ours: bool,
    followed: &dyn Fn(&Pubkey) -> bool,
) -> Vec<TxOutcome> {
    let lamports_per_signature = bank.get_lamports_per_signature();
    entries
        .iter()
        .flat_map(|entry| &entry.transactions)
        .filter(|transaction| !is_simple_vote(transaction))
        .filter_map(|transaction| {
            let payer = *transaction.message.static_account_keys().first()?;
            if !ours && !followed(&payer) {
                return None;
            }
            let (priority, base) = if ours {
                (
                    priority_fee(transaction, &bank.feature_set),
                    base_fee(transaction, lamports_per_signature),
                )
            } else {
                (0, 0)
            };
            Some(TxOutcome {
                signature: transaction.signatures.first().copied(),
                payer,
                failed: failed_in(bank, transaction),
                priority,
                base,
            })
        })
        .collect()
}

/// Charged for each of the transaction's signatures and each a precompile verifies, whose count
/// leads its instruction data.
pub fn base_fee(transaction: &VersionedTransaction, lamports_per_signature: u64) -> u64 {
    let message = &transaction.message;
    let keys = message.static_account_keys();
    let signatures = u64::try_from(transaction.signatures.len()).unwrap_or(u64::MAX);
    message
        .instructions()
        .iter()
        .filter(|instruction| {
            keys.get(usize::from(instruction.program_id_index))
                .is_some_and(|program| {
                    ed25519_program::check_id(program)
                        || secp256k1_program::check_id(program)
                        || secp256r1_program::check_id(program)
                })
        })
        .map(|instruction| u64::from(instruction.data.first().copied().unwrap_or(0)))
        .fold(signatures, u64::saturating_add)
        .saturating_mul(lamports_per_signature)
}

/// By message hash, which the cache holds for all but durable nonce transactions; those are found
/// by signature where the node keeps signatures.
pub(crate) fn failed_in(bank: &Bank, transaction: &VersionedTransaction) -> Option<bool> {
    let message = &transaction.message;
    let by_hash = bank.status_cache.read().unwrap().get_status(
        message.hash(),
        message.recent_blockhash(),
        &bank.ancestors,
    );
    let (slot, status) = match by_hash {
        Some(found) => found,
        None => bank.get_signature_status_slot(transaction.signatures.first()?)?,
    };
    (slot == bank.slot()).then(|| status.is_err())
}

/// What the fee charged beyond the signatures: a v1 message states it, earlier ones set a price
/// per compute unit.
pub fn priority_fee(transaction: &VersionedTransaction, feature_set: &FeatureSet) -> u64 {
    let message = &transaction.message;
    if let VersionedMessage::V1(v1) = message {
        return v1.config.priority_fee.unwrap_or(0);
    }
    let keys = message.static_account_keys();
    let instructions = message.instructions().iter().filter_map(|instruction| {
        let program = keys.get(usize::from(instruction.program_id_index))?;
        Some((program, SVMInstruction::from(instruction)))
    });
    process_compute_budget_instructions(instructions, feature_set)
        .map_or(0, |limits| limits.get_prioritization_fee())
}
