//! This epoch's inflation reward to our vote account, split into voting and the producer bonus our
//! blocks earn for the votes their reward certificates carry.

pub(crate) use solana_core::node_events::output::VOTE_REWARD_ACCOUNT;
use {
    super::Collector,
    crate::{
        certs::{self, FooterFacts},
        source::{BankView, Ranks, Source},
    },
    serde::Serialize,
    solana_clock::{Epoch, Slot},
    solana_pubkey::Pubkey,
};

const BLOCKS_PER_TICK: usize = 64;

/// Lamports before commission. The maxima are for the whole epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EpochEarnings {
    pub epoch: Epoch,
    pub voting: u64,
    pub voting_lost: u64,
    pub voting_max: u64,
    pub bonus: u64,
    pub bonus_lost: u64,
    pub bonus_max: u64,
    pub commission_bps: u16,
    /// Our blocks the ledger no longer holds; their bonus counts as voting.
    pub unread: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Inflation {
    reward: u64,
    slots: u64,
    epoch: Epoch,
}

/// The account holds the current epoch's three words, then an optional previous epoch's.
fn read_inflation(data: &[u8], epoch: Epoch) -> Option<Inflation> {
    let word = |at: usize| {
        data.get(at..)?
            .first_chunk::<8>()
            .map(|bytes| u64::from_le_bytes(*bytes))
    };
    let state = |reward: usize, slots: usize, epoch: usize| {
        Some(Inflation {
            reward: word(reward)?,
            slots: word(slots)?,
            epoch: word(epoch)?,
        })
    };
    let current = state(0, 8, 16)?;
    if current.epoch == epoch {
        return Some(current);
    }
    let previous = (data.get(24) == Some(&1))
        .then(|| state(25, 33, 41))
        .flatten()?;
    (previous.epoch == epoch).then_some(previous)
}

/// The runtime's reward for one slot's vote by `stake`, before the voter and leader halves.
fn slot_reward(inflation: &Inflation, total: u64, stake: u64) -> u64 {
    let numerator = u128::from(inflation.reward).saturating_mul(u128::from(stake));
    let denominator = u128::from(inflation.slots).saturating_mul(u128::from(total));
    numerator
        .checked_div(denominator)
        .and_then(|reward| u64::try_from(reward).ok())
        .unwrap_or(0)
}

fn voter_half(reward: u64) -> u64 {
    reward / 2
}

fn leader_half(reward: u64) -> u64 {
    reward.saturating_sub(voter_half(reward))
}

/// What a certificate paid its leader, and what one carrying every rank would have.
fn block_bonus(
    stakes: impl Iterator<Item = (u64, bool)>,
    inflation: &Inflation,
    total: u64,
) -> (u64, u64) {
    stakes.fold((0u64, 0u64), |(got, full), (stake, paid)| {
        let half = leader_half(slot_reward(inflation, total, stake));
        let got = if paid { got.saturating_add(half) } else { got };
        (got, full.saturating_add(half))
    })
}

fn rank_stakes(map: &dyn Ranks) -> impl Iterator<Item = u64> + '_ {
    (0..map.len()).map(|rank| map.entry(rank).map_or(0, |entry| entry.stake))
}

/// Ranks in either reward certificate; `None` where one does not decode.
fn paid_ranks(footer: &FooterFacts, len: usize) -> Option<Vec<bool>> {
    let mut paid = vec![false; len];
    let mut mark = |ranks: Vec<bool>| {
        for (flag, set) in paid.iter_mut().zip(ranks) {
            *flag |= set;
        }
    };
    if let Some(bitmap) = &footer.notar_bitmap {
        mark(certs::decode_paid(bitmap, len)?);
    }
    if let Some(bitmap) = &footer.skip_bitmap {
        mark(certs::decode_paid(bitmap, len)?);
    }
    Some(paid)
}

enum BlockRead {
    Pending,
    Unread,
    Counted { got: u64, full: u64 },
}

/// A skipped block of ours earns nothing of what its certificate could have paid.
fn read_bonus(
    src: &dyn Source,
    bank: &dyn BankView,
    slot: Slot,
    inflation: &Inflation,
    held_from: Slot,
) -> BlockRead {
    // Under the ledger's first slot a block that is no root may only have been cleaned up.
    if slot < held_from {
        return BlockRead::Unread;
    }
    let block = src.block(slot, bank.slot());
    let footer = match block {
        certs::Block::Pending => return BlockRead::Pending,
        certs::Block::Opaque => return BlockRead::Unread,
        certs::Block::Missing => None,
        certs::Block::Footer(footer) => Some(footer),
    };
    // The cluster's first slots have no slot to reward.
    let Some(rewarded) = certs::rewarded_slot(slot) else {
        return BlockRead::Counted { got: 0, full: 0 };
    };
    let Some(stakes) = bank.epoch_stakes_from_slot(rewarded) else {
        return BlockRead::Unread;
    };
    let map = stakes.ranks.as_ref();
    let paid = match &footer {
        Some(footer) => match paid_ranks(footer, map.len()) {
            Some(paid) => paid,
            None => return BlockRead::Unread,
        },
        None => vec![false; map.len()],
    };
    let (got, full) = block_bonus(rank_stakes(map).zip(paid), inflation, stakes.total_stake);
    BlockRead::Counted { got, full }
}

/// One epoch's pay rates, fixed for the epoch.
#[derive(Debug, Clone, Copy)]
struct Rates {
    inflation: Inflation,
    /// Our voter half for one slot.
    voter_slot: u64,
    /// The bonus of a block whose certificate carries every rank.
    full_block: u64,
}

#[derive(Debug, Default)]
pub(super) struct EarningsWalk {
    /// The epoch and identity the walk is for; a swapped identity starts it again.
    key: Option<(Epoch, Pubkey)>,
    /// `None` when we hold no rank or the inflation is unknown, until the next epoch.
    rates: Option<Rates>,
    leader_slots: Vec<Slot>,
    next: usize,
    bonus: u64,
    bonus_lost: u64,
    unread: u32,
    /// The figures last published, kept while a block is still to arrive.
    shown: Option<EpochEarnings>,
}

impl EarningsWalk {
    /// Reads up to `BLOCKS_PER_TICK` more of our blocks up to `root`; whether all are read.
    fn advance(&mut self, root: Slot, mut read: impl FnMut(Slot) -> BlockRead) -> bool {
        let mut count = 0usize;
        while let Some(slot) = self.leader_slots.get(self.next).copied() {
            if slot > root || count == BLOCKS_PER_TICK {
                break;
            }
            match read(slot) {
                BlockRead::Pending => break,
                BlockRead::Unread => self.unread = self.unread.saturating_add(1),
                BlockRead::Counted { got, full } => {
                    self.bonus = self.bonus.saturating_add(got);
                    self.bonus_lost = self.bonus_lost.saturating_add(full.saturating_sub(got));
                }
            }
            self.next = self.next.saturating_add(1);
            count = count.saturating_add(1);
        }
        self.leader_slots
            .get(self.next)
            .is_none_or(|slot| *slot > root)
    }
}

impl Collector {
    pub(super) fn collect_earnings(&mut self, root_bank: &dyn BankView) {
        let earnings = self.read_earnings(root_bank);
        self.debounces
            .epoch_earnings
            .publish(&self.publisher, earnings);
    }

    /// `None` until every block of ours the root has passed is read, then the last figures while
    /// one is still to arrive.
    fn read_earnings(&mut self, bank: &dyn BankView) -> Option<EpochEarnings> {
        if !bank.is_alpenglow() {
            return None;
        }
        let epoch = bank.epoch();
        let me = self.src.identity();
        if self.earnings.key != Some((epoch, me)) {
            self.earnings = self.start_earnings(bank, epoch, me)?;
        }
        let rates = self.earnings.rates?;
        let inflation = rates.inflation;
        let root = bank.slot();
        let src = self.src.as_ref();
        // Another process's blocks were read before cleanup; a cleaned one comes unreadable.
        let held_from = if src.direct().is_some() {
            src.lowest_slot()
        } else {
            0
        };
        let walk = &mut self.earnings;
        if !walk.advance(root, |slot| {
            read_bonus(src, bank, slot, &inflation, held_from)
        }) {
            return walk.shown;
        }

        let row = bank.vote_row(&self.src.vote_account())?;
        let voting = row.credits_in(epoch).saturating_sub(walk.bonus);
        let start = bank.epoch_schedule().get_first_slot_in_epoch(epoch);
        let passed = root.saturating_sub(start).saturating_add(1);
        let blocks = u64::try_from(walk.leader_slots.len()).unwrap_or(u64::MAX);
        let earnings = EpochEarnings {
            epoch,
            voting,
            voting_lost: passed
                .saturating_mul(rates.voter_slot)
                .saturating_sub(voting),
            voting_max: inflation.slots.saturating_mul(rates.voter_slot),
            bonus: walk.bonus,
            bonus_lost: walk.bonus_lost,
            bonus_max: blocks.saturating_mul(rates.full_block),
            commission_bps: row.inflation_rewards_commission_bps,
            unread: walk.unread,
        };
        self.earnings.shown = Some(earnings);
        Some(earnings)
    }

    /// Latched once the schedule is in hand; reading it walks the whole epoch.
    fn start_earnings(
        &self,
        bank: &dyn BankView,
        epoch: Epoch,
        me: Pubkey,
    ) -> Option<EarningsWalk> {
        Some(EarningsWalk {
            key: Some((epoch, me)),
            rates: self.read_rates(bank, epoch),
            leader_slots: self.leader_slots_in_epoch(bank, epoch)?,
            ..EarningsWalk::default()
        })
    }

    fn read_rates(&self, bank: &dyn BankView, epoch: Epoch) -> Option<Rates> {
        let inflation = read_inflation(&bank.account_data(&VOTE_REWARD_ACCOUNT)?, epoch)?;
        let stakes = bank.epoch_stakes(epoch)?;
        let total = stakes.total_stake;
        let map = stakes.ranks.as_ref();
        let rank = map.rank_of(&self.src.vote_account())?;
        let ours = map.entry(usize::from(rank))?.stake;
        let all_paid = rank_stakes(map).map(|stake| (stake, true));
        let (_, full_block) = block_bonus(all_paid, &inflation, total);
        Some(Rates {
            inflation,
            voter_slot: voter_half(slot_reward(&inflation, total, ours)),
            full_block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The account as mainnet held it in epoch 1079.
    const MAINNET: &str = "a7c2e02f9c3800008097060000000000370400000000000001fbf0833fa138000080970600000000003604000000000000";

    fn bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&hex[at..at.saturating_add(2)], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_the_address_is_the_runtimes() {
        let (address, _) = Pubkey::find_program_address(
            &[b"vote_reward_account"],
            &agave_feature_set::alpenglow::id(),
        );
        assert_eq!(address, VOTE_REWARD_ACCOUNT);
    }

    #[test]
    fn test_a_block_under_the_ledgers_first_slot_is_unread_not_skipped() {
        let fixture = crate::fixture::fixture();
        let bank = fixture.advance_to(20);
        let inflation = read_inflation(&bytes(MAINNET), 1079).unwrap();
        let src = crate::source::Direct::new(fixture.ctx.clone(), None);
        let read = |held_from| read_bonus(&src, bank.as_ref(), 12, &inflation, held_from);
        assert!(matches!(read(0), BlockRead::Counted { got: 0, .. }));
        assert!(matches!(read(13), BlockRead::Unread));
    }

    #[test]
    fn test_the_walk_stops_at_a_pending_block_and_resumes_from_it() {
        let mut walk = EarningsWalk {
            leader_slots: vec![4, 5, 6, 40],
            ..EarningsWalk::default()
        };
        let first = |slot: Slot| match slot {
            4 => BlockRead::Counted { got: 3, full: 5 },
            5 => BlockRead::Pending,
            _ => BlockRead::Counted { got: 1, full: 1 },
        };
        assert!(!walk.advance(20, first));
        assert_eq!((walk.next, walk.bonus, walk.bonus_lost), (1, 3, 2));
        // Slot 40 is past the root.
        let then = |slot: Slot| match slot {
            5 => BlockRead::Unread,
            _ => BlockRead::Counted { got: 1, full: 1 },
        };
        assert!(walk.advance(20, then));
        assert_eq!((walk.next, walk.bonus, walk.unread), (3, 4, 1));
    }

    #[test]
    fn test_reads_either_epoch_and_no_other() {
        let data = bytes(MAINNET);
        let current = read_inflation(&data, 1079).unwrap();
        assert_eq!(current.reward, 62_243_469_312_679);
        assert_eq!(current.slots, 432_000);
        let previous = read_inflation(&data, 1078).unwrap();
        assert_eq!(previous.reward, 62_265_206_501_627);
        assert_eq!(read_inflation(&data, 1077), None);
        assert_eq!(read_inflation(&data[..25], 1078), None);
        assert_eq!(read_inflation(&data[..20], 1079), None);
    }

    #[test]
    fn test_the_leader_takes_the_odd_lamport() {
        assert_eq!(voter_half(7), 3);
        assert_eq!(leader_half(7), 4);
        assert_eq!(leader_half(0), 0);
    }

    #[test]
    fn test_slot_reward_matches_the_runtime_formula() {
        let inflation = Inflation {
            reward: 62_243_469_312_679,
            slots: 432_000,
            epoch: 1079,
        };
        let total = 434_917_723_208_692_500;
        // 280,042 SOL of the 434.9M staked, at epoch 1079's inflation.
        assert_eq!(slot_reward(&inflation, total, 280_041_995_582_784), 92_773);
        assert_eq!(slot_reward(&inflation, 0, 1), 0);
    }

    #[test]
    fn test_block_bonus_counts_only_paid_ranks() {
        let inflation = Inflation {
            reward: 1_000,
            slots: 10,
            epoch: 0,
        };
        // Each rank's slot reward is its stake share of 100: 50, 30 and 20, leader halves 25, 15, 10.
        let stakes = [(50, true), (30, false), (20, true)];
        assert_eq!(block_bonus(stakes.into_iter(), &inflation, 100), (35, 50));
        assert_eq!(block_bonus(std::iter::empty(), &inflation, 100), (0, 0));
    }
}
