//! Which shard carries which symbol. A new symbol goes to the first shard with room, within the
//! exchange's limit on websockets, and leaves its shard once two complete listings in a row leave
//! it out.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

use crate::exchange::Symbol;

/// Symbols by the shard that carries them.
pub type ByShard = BTreeMap<usize, Vec<Symbol>>;

/// Every shard's size, and the shard of every symbol.
pub struct Shards {
    per_shard: usize,
    max_shards: usize,
    /// How many symbols each shard carries.
    sizes: Vec<usize>,
    placed: BTreeMap<Symbol, Placed>,
    /// When the round of the latest complete listing started.
    last_round: Option<DateTime<Utc>>,
}

struct Placed {
    shard: usize,
    /// When a page of the listing last had the symbol.
    listed: DateTime<Utc>,
}

impl Shards {
    /// No shards yet. Each takes `per_shard` symbols, and once there are `max_shards`, more.
    pub fn new(per_shard: usize, max_shards: usize) -> Self {
        Self {
            per_shard,
            max_shards,
            sizes: Vec::new(),
            placed: BTreeMap::new(),
            last_round: None,
        }
    }

    /// A page of the listing, read at `listed`. Places the symbols no shard carries yet, and
    /// returns them.
    pub fn add<'a>(
        &mut self,
        page: impl IntoIterator<Item = &'a Symbol>,
        listed: DateTime<Utc>,
    ) -> ByShard {
        let mut added = ByShard::new();
        for symbol in page {
            if let Some(placed) = self.placed.get_mut(symbol) {
                placed.listed = listed;
                continue;
            }
            let shard = self.shard_with_room();
            self.sizes[shard] += 1;
            let placed = Placed { shard, listed };
            self.placed.insert(symbol.clone(), placed);
            added.entry(shard).or_default().push(symbol.clone());
        }
        added
    }

    /// A complete listing of `live`, whose round started at `started`. Removes the symbols that
    /// no page has listed since the previous complete listing's round started, and returns them.
    /// So a listing whose pages end early removes nothing the one before it had, and a symbol
    /// newer than a round's first page outlives that round.
    pub fn remove_unlisted(&mut self, live: &BTreeSet<Symbol>, started: DateTime<Utc>) -> ByShard {
        let mut removed = ByShard::new();
        let Some(previous) = self.last_round.replace(started) else {
            return removed;
        };
        self.placed.retain(|symbol, placed| {
            let listed = placed.listed >= previous || live.contains(symbol);
            if !listed {
                self.sizes[placed.shard] -= 1;
                removed
                    .entry(placed.shard)
                    .or_default()
                    .push(symbol.clone());
            }
            listed
        });
        removed
    }

    /// The first shard with room, or else a new one while the limit allows it, or else the
    /// shard with the fewest symbols.
    fn shard_with_room(&mut self) -> usize {
        if let Some(shard) = self.sizes.iter().position(|&size| size < self.per_shard) {
            return shard;
        }
        if self.sizes.len() < self.max_shards {
            self.sizes.push(0);
            return self.sizes.len() - 1;
        }
        (0..self.sizes.len())
            .min_by_key(|&shard| self.sizes[shard])
            .expect("the limit allows a shard")
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    /// `n` seconds after the epoch.
    fn seconds(n: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::seconds(n)
    }

    fn symbols(names: &[&str]) -> BTreeSet<Symbol> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn by_shard(shards: &[(usize, &[&str])]) -> ByShard {
        let names = |names: &[&str]| names.iter().map(|name| name.to_string()).collect();
        shards
            .iter()
            .map(|&(shard, symbols)| (shard, names(symbols)))
            .collect()
    }

    /// A complete listing of `live` whose round ran from `started` to `ended`, applied the way
    /// the collector does: what it removed, then what it added.
    fn complete(shards: &mut Shards, live: &[&str], started: i64, ended: i64) -> [ByShard; 2] {
        let live = symbols(live);
        let removed = shards.remove_unlisted(&live, seconds(started));
        [removed, shards.add(&live, seconds(ended))]
    }

    fn unchanged() -> [ByShard; 2] {
        [ByShard::new(), ByShard::new()]
    }

    #[test]
    fn new_symbols_fill_each_shard_before_the_next_and_are_added_once() {
        let mut shards = Shards::new(2, usize::MAX);
        let added = shards.add(&symbols(&["a", "b", "c"]), seconds(0));
        assert_eq!(added, by_shard(&[(0, &["a", "b"]), (1, &["c"])]));
        let added = shards.add(&symbols(&["b", "c", "d"]), seconds(1));
        assert_eq!(added, by_shard(&[(1, &["d"])]));
    }

    #[test]
    fn once_the_shards_reach_their_limit_a_new_symbol_goes_to_the_one_with_the_fewest() {
        let mut shards = Shards::new(2, 2);
        let added = shards.add(&symbols(&["a", "b", "c", "d", "e"]), seconds(0));
        assert_eq!(added, by_shard(&[(0, &["a", "b", "e"]), (1, &["c", "d"])]));
        let added = shards.add(&symbols(&["f"]), seconds(1));
        assert_eq!(added, by_shard(&[(1, &["f"])]));
    }

    #[test]
    fn symbols_two_complete_listings_in_a_row_leave_out_are_removed_and_their_room_reused() {
        let mut shards = Shards::new(2, usize::MAX);
        complete(&mut shards, &["a", "b", "c"], 0, 0);
        assert_eq!(complete(&mut shards, &["b", "c"], 10, 20), unchanged());
        assert_eq!(
            complete(&mut shards, &["b", "c", "d"], 30, 40),
            [by_shard(&[(0, &["a"])]), by_shard(&[(0, &["d"])])]
        );
    }

    #[test]
    fn a_listing_whose_pages_end_early_removes_nothing_the_listing_before_it_had() {
        let mut shards = Shards::new(2, usize::MAX);
        complete(&mut shards, &["a", "b", "c"], 0, 0);
        // The next listing ends after its first page, and the one after that is whole again.
        assert_eq!(complete(&mut shards, &["a"], 10, 20), unchanged());
        assert_eq!(complete(&mut shards, &["a", "b", "c"], 30, 40), unchanged());
    }

    #[test]
    fn a_symbol_a_page_listed_after_the_round_started_outlives_that_round() {
        let mut shards = Shards::new(2, usize::MAX);
        complete(&mut shards, &["a", "b"], 0, 0);
        // A round starts at 10 s and reads its first page. A market created after that shows up
        // on the first page read again at 20 s; the round ends at 30 s without it.
        shards.add(&symbols(&["new"]), seconds(20));
        assert_eq!(complete(&mut shards, &["a"], 10, 30), unchanged());

        let [removed, _] = complete(&mut shards, &["a"], 40, 70);
        assert_eq!(
            removed,
            by_shard(&[(0, &["b"])]),
            "b was last listed before the previous round started, new after"
        );
        let [removed, _] = complete(&mut shards, &["a"], 80, 110);
        assert_eq!(removed, by_shard(&[(1, &["new"])]));
    }
}
