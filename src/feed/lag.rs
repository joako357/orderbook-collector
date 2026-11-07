//! How far one replica trails the others (extension 1).
//!
//! A replica trails while another replica has streamed a change its book doesn't cover yet, by
//! the age of the oldest such change, counted from when the other replica read it. So a replica
//! that reads every change 50 ms after its partner trails by 50 ms, however busy the market.
//! Updates are streamed, and so is a whole book that changed levels. A book that changes nothing
//! only says when it was taken: it covers what came before it, and no other replica misses it.
//! When a replica gets its book, or another replica loses its own, what it misses starts over
//! from the newest change the other books streamed, as if read then.
//!
//! A book keeps up to `MAX_MISSING` of the changes it misses apart. Past that, the newest ones
//! count as one, as old as the oldest of them, so a replica that far behind can only look further
//! behind than it is.

use std::collections::VecDeque;

use chrono::{DateTime, TimeDelta, Utc};

use crate::output::Stamp;

/// The most changes a book keeps apart of those it misses. That covers the first 30-second stuck
/// wait of a symbol that changes over 100 times a second, and holds a trailing replica's memory
/// for one symbol to about 100 KB until a renewal drops its book.
const MAX_MISSING: usize = 4_096;

/// What one replica's book covers of the changes the replicas streamed.
pub(super) struct Lag {
    /// Exchange time up to which the book is complete: its snapshot plus every message since.
    covered: DateTime<Utc>,
    /// Exchange time of the newest change this replica streamed.
    newest: Option<DateTime<Utc>>,
    /// The changes other replicas streamed that this book doesn't cover yet, oldest first. The
    /// last one also stands for any newer ones past `MAX_MISSING`, with its own receive time.
    missing: VecDeque<Stamp>,
}

impl Lag {
    /// The lag of a book complete up to `covered`, which misses nothing yet.
    pub(super) fn new(covered: DateTime<Utc>) -> Self {
        Self {
            covered,
            newest: None,
            missing: VecDeque::new(),
        }
    }

    /// Exchange time up to which the book is complete.
    pub(super) fn covered(&self) -> DateTime<Utc> {
        self.covered
    }

    /// Exchange time of the newest change this replica streamed.
    pub(super) fn newest(&self) -> Option<DateTime<Utc>> {
        self.newest
    }

    /// Whether another replica streamed a change this book doesn't cover yet.
    pub(super) fn trails(&self) -> bool {
        !self.missing.is_empty()
    }

    /// How far this replica trails at `now`: the age of the oldest change it misses.
    pub(super) fn lag(&self, now: DateTime<Utc>) -> TimeDelta {
        self.missing
            .front()
            .map_or(TimeDelta::zero(), |oldest| now - oldest.received)
    }

    /// The book is complete up to `exchange_time`, so it no longer misses anything older.
    pub(super) fn cover(&mut self, exchange_time: DateTime<Utc>) {
        self.covered = self.covered.max(exchange_time);
        while let Some(missed) = self.missing.front()
            && missed.exchange <= self.covered
        {
            self.missing.pop_front();
        }
    }

    /// This replica streamed a change at `exchange_time`, which its book now covers.
    pub(super) fn stream(&mut self, exchange_time: DateTime<Utc>) {
        self.cover(exchange_time);
        self.newest = self.newest.max(Some(exchange_time));
    }

    /// Another replica streamed `change`. This book misses it, unless it covers it or already
    /// misses one at least as new. Once it misses `MAX_MISSING`, the newest one also stands for
    /// `change`, and keeps its own, older receive time.
    pub(super) fn miss(&mut self, change: Stamp) {
        let newest_missed = self
            .missing
            .back()
            .map_or(self.covered, |missed| missed.exchange);
        if change.exchange <= newest_missed {
            return;
        }
        if self.missing.len() < MAX_MISSING {
            self.missing.push_back(change);
        } else if let Some(newest) = self.missing.back_mut() {
            newest.exchange = change.exchange;
        }
    }

    /// Starts over what this book misses: just `elsewhere`, the newest change the other replicas
    /// streamed, if the book doesn't cover it, as if read at `since`.
    pub(super) fn restart(&mut self, elsewhere: Option<DateTime<Utc>>, since: DateTime<Utc>) {
        self.missing.clear();
        if let Some(exchange) = elsewhere.filter(|newest| *newest > self.covered) {
            self.missing.push_back(Stamp {
                exchange,
                received: since,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `n` milliseconds after the epoch.
    fn ms(n: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::milliseconds(n)
    }

    /// Exchange time `exchange` and receive time `received`, in milliseconds.
    fn at_ms(exchange: i64, received: i64) -> Stamp {
        Stamp {
            exchange: ms(exchange),
            received: ms(received),
        }
    }

    #[test]
    fn a_change_another_replica_streamed_is_missed_until_the_book_covers_it() {
        let mut lag = Lag::new(ms(10));
        lag.miss(at_ms(20, 100));
        assert!(lag.trails());
        assert_eq!(lag.lag(ms(150)), TimeDelta::milliseconds(50));
        lag.cover(ms(20));
        assert!(!lag.trails());
        assert_eq!(lag.lag(ms(150)), TimeDelta::zero());
    }

    #[test]
    fn a_replica_trails_by_the_age_of_the_oldest_change_it_misses() {
        let mut lag = Lag::new(ms(10));
        for (exchange, received) in [(20, 100), (30, 110), (40, 120)] {
            lag.miss(at_ms(exchange, received));
        }
        assert_eq!(lag.lag(ms(200)), TimeDelta::milliseconds(100));
        lag.cover(ms(35));
        assert_eq!(
            lag.lag(ms(200)),
            TimeDelta::milliseconds(80),
            "only 40 is left"
        );
    }

    #[test]
    fn a_change_the_book_covers_or_one_no_newer_than_a_missed_one_adds_nothing() {
        let mut lag = Lag::new(ms(10));
        lag.miss(at_ms(10, 100));
        assert!(!lag.trails(), "the book covers 10");
        lag.miss(at_ms(30, 110));
        // The same change, read later by a third replica, and an older one.
        lag.miss(at_ms(30, 300));
        lag.miss(at_ms(20, 300));
        assert_eq!(lag.lag(ms(400)), TimeDelta::milliseconds(290));
        lag.cover(ms(30));
        assert!(!lag.trails());
    }

    #[test]
    fn past_max_missing_the_newest_changes_count_as_one_as_old_as_the_oldest_of_them() {
        let mut lag = Lag::new(ms(0));
        let first_merged = MAX_MISSING as i64;
        // Change n happens at n ms and is read 1 s later.
        for n in 1..=first_merged + 10 {
            lag.miss(at_ms(n, 1_000 + n));
        }
        assert_eq!(lag.missing.len(), MAX_MISSING);
        let now = ms(100_000);
        lag.cover(ms(first_merged - 1));
        assert_eq!(
            lag.lag(now),
            now - ms(1_000 + first_merged),
            "exact while the oldest change missed is the first of those that count as one"
        );
        lag.cover(ms(first_merged + 5));
        assert_eq!(
            lag.lag(now),
            now - ms(1_000 + first_merged),
            "6 ms longer than it is: the oldest change missed now was read 6 ms after the first"
        );
        lag.cover(ms(first_merged + 10));
        assert!(!lag.trails());
    }

    #[test]
    fn a_change_this_replica_streamed_is_covered_and_its_newest() {
        let mut lag = Lag::new(ms(10));
        lag.miss(at_ms(30, 100));
        lag.stream(ms(40));
        assert!(!lag.trails());
        assert_eq!((lag.covered(), lag.newest()), (ms(40), Some(ms(40))));
        lag.stream(ms(35));
        assert_eq!(lag.newest(), Some(ms(40)), "an older change is no newer");
    }

    #[test]
    fn a_book_that_only_covers_more_streams_nothing() {
        let mut lag = Lag::new(ms(10));
        lag.cover(ms(50));
        lag.cover(ms(40));
        assert_eq!((lag.covered(), lag.newest()), (ms(50), None));
    }

    #[test]
    fn a_restart_keeps_only_the_newest_change_elsewhere_that_the_book_lacks() {
        let mut lag = Lag::new(ms(10));
        lag.miss(at_ms(20, 100));
        lag.miss(at_ms(30, 110));
        lag.restart(Some(ms(50)), ms(500));
        assert_eq!(
            lag.lag(ms(600)),
            TimeDelta::milliseconds(100),
            "50, as read at 500"
        );
        lag.cover(ms(50));
        assert!(!lag.trails());

        lag.restart(Some(ms(40)), ms(700));
        assert!(!lag.trails(), "the book covers 40");
        lag.miss(at_ms(60, 800));
        lag.restart(None, ms(900));
        assert!(!lag.trails(), "nothing elsewhere to miss");
    }
}
