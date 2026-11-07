//! The order book of one symbol: the absolute quantity resting at each price, per side.

use std::collections::BTreeMap;

use anyhow::Context;

use crate::Side;

/// One price level. Both numbers use the output's fixed point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

/// How an exchange states a change to one price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Change {
    /// The level's new absolute quantity, as Polymarket sends it.
    Set(i64),
    /// A signed difference to the level's quantity, as Kalshi sends it.
    Add(i64),
}

/// The best price of each side, `None` for an empty side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Top {
    pub bid: Option<i64>,
    pub ask: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Book {
    /// Builds a book from its levels. A price listed twice keeps the quantity listed last, and
    /// levels left empty are dropped.
    pub fn new(
        bids: impl IntoIterator<Item = Level>,
        asks: impl IntoIterator<Item = Level>,
    ) -> Self {
        Self {
            bids: levels(bids),
            asks: levels(asks),
        }
    }

    /// Applies `change` to one level and returns the level's new absolute quantity, where zero
    /// means the level is gone. A quantity below zero, or past what the fixed point holds, means
    /// missed updates or a bad message, so it is an error and leaves the book untouched.
    pub fn apply(&mut self, side: Side, price: i64, change: Change) -> anyhow::Result<i64> {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let quantity = match change {
            Change::Set(quantity) => Some(quantity),
            Change::Add(delta) => levels.get(&price).copied().unwrap_or(0).checked_add(delta),
        };
        let quantity = quantity
            .filter(|quantity| *quantity >= 0)
            .with_context(|| format!("{side} level {price} can't take {change:?}"))?;
        if quantity == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, quantity);
        }
        Ok(quantity)
    }

    /// Replaces this book with `new` and returns every level that changed, with its new quantity
    /// (zero for a level that is gone).
    pub fn replace(&mut self, new: Book) -> Vec<(Side, Level)> {
        let mut changed = Vec::new();
        diff(Side::Buy, &self.bids, &new.bids, &mut changed);
        diff(Side::Sell, &self.asks, &new.asks, &mut changed);
        *self = new;
        changed
    }

    /// Bids from the best (highest) price down.
    pub fn bids(&self) -> impl Iterator<Item = Level> {
        self.bids.iter().rev().map(level)
    }

    /// Asks from the best (lowest) price up.
    pub fn asks(&self) -> impl Iterator<Item = Level> {
        self.asks.iter().map(level)
    }

    /// The best bid and the best ask.
    pub fn top(&self) -> Top {
        Top {
            bid: self.bids.keys().next_back().copied(),
            ask: self.asks.keys().next().copied(),
        }
    }
}

fn levels(levels: impl IntoIterator<Item = Level>) -> BTreeMap<i64, i64> {
    let mut levels: BTreeMap<i64, i64> = levels
        .into_iter()
        .map(|level| (level.price, level.quantity))
        .collect();
    levels.retain(|_, quantity| *quantity > 0);
    levels
}

fn diff(
    side: Side,
    old: &BTreeMap<i64, i64>,
    new: &BTreeMap<i64, i64>,
    changed: &mut Vec<(Side, Level)>,
) {
    let gone = old
        .keys()
        .filter(|price| !new.contains_key(price))
        .map(|&price| Level { price, quantity: 0 });
    let moved = new
        .iter()
        .filter(|(price, quantity)| old.get(price) != Some(quantity))
        .map(level);
    changed.extend(gone.chain(moved).map(|level| (side, level)));
}

fn level((&price, &quantity): (&i64, &i64)) -> Level {
    Level { price, quantity }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// `[(price, quantity)]` pairs as levels, to keep tests short.
    pub(crate) fn at(levels: &[(i64, i64)]) -> Vec<Level> {
        levels
            .iter()
            .map(|&(price, quantity)| Level { price, quantity })
            .collect()
    }

    #[test]
    fn lists_bids_and_asks_best_first_without_empty_levels() {
        let book = Book::new(at(&[(40, 1), (60, 2), (50, 0)]), at(&[(90, 3), (70, 4)]));
        assert_eq!(book.bids().collect::<Vec<_>>(), at(&[(60, 2), (40, 1)]));
        assert_eq!(book.asks().collect::<Vec<_>>(), at(&[(70, 4), (90, 3)]));
    }

    #[test]
    fn the_top_is_the_best_price_of_each_side_and_none_for_an_empty_side() {
        let book = Book::new(at(&[(40, 1), (60, 2)]), at(&[(90, 3), (70, 4)]));
        assert_eq!(
            book.top(),
            Top {
                bid: Some(60),
                ask: Some(70)
            }
        );
        assert_eq!(
            Book::new([], at(&[(70, 4)])).top(),
            Top {
                bid: None,
                ask: Some(70)
            }
        );
    }

    #[test]
    fn set_replaces_a_level_and_zero_removes_it() {
        let mut book = Book::new(at(&[(40, 1)]), []);
        assert_eq!(book.apply(Side::Buy, 40, Change::Set(7)).unwrap(), 7);
        assert_eq!(book.apply(Side::Sell, 60, Change::Set(2)).unwrap(), 2);
        assert_eq!(book, Book::new(at(&[(40, 7)]), at(&[(60, 2)])));
        assert_eq!(book.apply(Side::Buy, 40, Change::Set(0)).unwrap(), 0);
        assert_eq!(book, Book::new([], at(&[(60, 2)])));
    }

    #[test]
    fn add_returns_the_absolute_quantity_and_removes_a_level_that_reaches_zero() {
        let mut book = Book::new(at(&[(40, 5)]), []);
        assert_eq!(book.apply(Side::Buy, 40, Change::Add(3)).unwrap(), 8);
        assert_eq!(book.apply(Side::Buy, 30, Change::Add(2)).unwrap(), 2);
        assert_eq!(book.apply(Side::Buy, 40, Change::Add(-8)).unwrap(), 0);
        assert_eq!(book, Book::new(at(&[(30, 2)]), []));
    }

    #[test]
    fn a_price_listed_twice_keeps_the_quantity_listed_last() {
        let book = Book::new(at(&[(40, 1), (40, 3), (50, 2), (50, 0)]), []);
        assert_eq!(book, Book::new(at(&[(40, 3)]), []));
    }

    #[test]
    fn a_change_that_would_go_negative_or_overflow_is_an_error_and_changes_nothing() {
        let mut book = Book::new(at(&[(40, 5)]), []);
        assert!(book.apply(Side::Buy, 40, Change::Add(-6)).is_err());
        assert!(book.apply(Side::Sell, 60, Change::Set(-1)).is_err());
        assert!(book.apply(Side::Buy, 40, Change::Add(i64::MAX)).is_err());
        assert_eq!(book, Book::new(at(&[(40, 5)]), []));
    }

    #[test]
    fn replace_reports_exactly_the_levels_that_changed() {
        let mut book = Book::new(at(&[(40, 1), (50, 2)]), at(&[(70, 3)]));
        let new = Book::new(at(&[(40, 1), (50, 9), (45, 4)]), []);
        let mut changed = book.replace(new.clone());
        changed.sort_by_key(|(side, level)| (*side as u8, level.price));
        assert_eq!(
            changed,
            vec![
                (
                    Side::Buy,
                    Level {
                        price: 45,
                        quantity: 4
                    }
                ),
                (
                    Side::Buy,
                    Level {
                        price: 50,
                        quantity: 9
                    }
                ),
                (
                    Side::Sell,
                    Level {
                        price: 70,
                        quantity: 0
                    }
                ),
            ]
        );
        assert_eq!(book, new);
    }
}
