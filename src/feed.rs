//! One symbol across its replicas, and which replica the output follows (extension 1).
//!
//! Each replica keeps its own copy of the book. The output follows one of them, the source: it
//! starts with the source's snapshot and then carries the source's changes, so replaying it gives
//! exactly the source's book. When the source loses its book or trails another replica by
//! `MAX_LAG`, the output switches to the most up-to-date replica, with a snapshot of its book.
//! `lag` says how far a replica trails.
//!
//! Everything here counts in receive times, never the clock. Every method gets `Times`, the
//! receive times the collector has processed, whose `now` stands in for now, so an event waiting
//! in the queue never makes a replica look late. That includes the renewals of extension 2, of a
//! stuck replica (`stuck`) and of a symbol gone silent (`silence`). `top_check` drops a book that
//! missed a change.

mod lag;
mod silence;
mod stuck;
mod top_check;

use std::cmp::Reverse;

use chrono::{DateTime, TimeDelta, Utc};

use crate::Side;
use crate::book::{Book, Level};
use crate::exchange::Update;
use crate::output::{Output, Sink, Stamp};
use lag::Lag;
use silence::Silence;
use stuck::Stuck;
use top_check::TopCheck;

/// Connections that carry the same symbols, so that one can fail without losing data.
pub const REPLICAS: usize = 2;

/// How far the source may trail another replica before the output switches to that replica.
const MAX_LAG: TimeDelta = TimeDelta::seconds(2);

/// The receive times a feed goes by, as far as the collector has processed them.
#[derive(Debug, Clone, Copy)]
pub struct Times {
    /// The newest receive time processed from any websocket, which stands in for now.
    pub now: DateTime<Utc>,
    /// The newest receive time processed from each replica's websocket. An event a feed takes is
    /// the newest its websocket read, so this is also that event's receive time.
    pub replicas: [DateTime<Utc>; REPLICAS],
}

/// One symbol across its replicas.
pub struct Feed {
    messages: Messages,
    replicas: [Replica; REPLICAS],
    /// The replica the output follows. It always has a book.
    source: Option<usize>,
    silence: Silence,
}

/// The messages the output gets about one symbol. An update goes out with its own receive time,
/// and a snapshot with the newest receive time processed from the websocket whose book it holds:
/// each socket stamps and queues its frames in order, so the changes that book still lacks come
/// later. Right after a switch of source, the new source's snapshot and any change it read before
/// the old source's last message go out with that message's time instead, so per symbol, receive
/// times in the output never go back. Across symbols the order is close but not exact, since
/// each websocket stamps its own frames, and a quiet one's snapshots carry an older time.
struct Messages {
    symbol: String,
    /// The receive time of the latest one.
    latest: Option<DateTime<Utc>>,
}

/// One replica's copy of the symbol, and how long it may stay stuck.
struct Replica {
    state: State,
    stuck: Stuck,
}

enum State {
    /// Subscribed at this time, and still waiting for its first book.
    Waiting(DateTime<Utc>),
    Synced(Synced),
}

/// A replica that has its book, and how far that book trails the other replicas'.
struct Synced {
    book: Book,
    lag: Lag,
    top_check: TopCheck,
}

impl Feed {
    /// A symbol that every replica subscribed to at `now`.
    pub fn new(symbol: String, now: DateTime<Utc>) -> Self {
        Self {
            messages: Messages {
                symbol,
                latest: None,
            },
            replicas: std::array::from_fn(|_| Replica {
                state: State::Waiting(now),
                stuck: Stuck::default(),
            }),
            source: None,
            silence: Silence::default(),
        }
    }

    /// A whole book from `replica`, as of `exchange_time`. The first one syncs the replica. Later
    /// ones replace its book, and when the replica is the source, the output gets the levels that
    /// changed.
    pub fn book(
        &mut self,
        replica: usize,
        book: Book,
        exchange_time: DateTime<Utc>,
        times: Times,
        out: &mut impl Sink,
    ) {
        let stamp = Stamp {
            exchange: exchange_time,
            received: times.replicas[replica],
        };
        match &mut self.replicas[replica].state {
            State::Synced(synced) => {
                let changed = synced.book.replace(book);
                synced.top_check.clear();
                if changed.is_empty() {
                    // Like a first book, it only says when it was taken.
                    synced.lag.cover(exchange_time);
                } else {
                    self.streamed(replica, stamp);
                    if self.source == Some(replica) {
                        for (side, level) in changed {
                            out.put(self.messages.update(side, level, stamp));
                        }
                    }
                }
            }
            // A first book starts the silence over, but not the stuck wait: see `Stuck`.
            waiting => {
                *waiting = State::Synced(Synced::new(book, exchange_time));
                self.restart_missing(replica, stamp.received);
                self.silence.restart(stamp.received);
            }
        }
        self.settle(times, out);
    }

    /// A change to one level from `replica`, with the top of book the exchange reported after
    /// it, if any. Returns false when it shows that the replica missed messages: the change
    /// doesn't fit its book, or the book fails the top check of an earlier change. That book is
    /// dropped, to be refreshed.
    pub fn update(
        &mut self,
        replica: usize,
        update: &Update,
        times: Times,
        out: &mut impl Sink,
    ) -> bool {
        let State::Synced(synced) = &mut self.replicas[replica].state else {
            // The first book, once it comes, already has this change.
            return true;
        };
        let Ok(quantity) = synced.apply(update) else {
            self.reset(replica, times, out);
            return false;
        };
        let stamp = Stamp {
            exchange: update.exchange_time,
            received: times.replicas[replica],
        };
        self.streamed(replica, stamp);
        if self.source == Some(replica) {
            let level = Level {
                price: update.price,
                quantity,
            };
            out.put(self.messages.update(update.side, level, stamp));
        }
        self.settle(times, out);
        true
    }

    /// Drops `replica`'s book, which then waits for a new one from now.
    pub fn reset(&mut self, replica: usize, times: Times, out: &mut impl Sink) {
        self.replicas[replica].state = State::Waiting(times.now);
        if self.source == Some(replica) {
            self.source = None;
        }
        for other in (0..REPLICAS).filter(|&other| other != replica) {
            self.restart_missing(other, times.now);
        }
        self.settle(times, out);
    }

    /// The checks that depend on time alone. Switches away from a source that trails by
    /// `MAX_LAG`, and returns the replicas to renew, whose books are dropped: the stuck ones, and
    /// the one that isn't the source when the symbol has been silent too long.
    pub fn on_tick(&mut self, times: Times, out: &mut impl Sink) -> Vec<usize> {
        self.settle(times, out);
        let now = times.now;
        let mut renew: Vec<usize> = (0..REPLICAS)
            .filter(|&replica| self.stuck(replica, now))
            .collect();
        for &replica in &renew {
            self.replicas[replica].stuck.renewed();
        }
        if let Some(source) = self.source
            && self.silence.is_due(now)
        {
            self.silence.renewed(now);
            let others: Vec<usize> = (0..REPLICAS)
                .filter(|replica| *replica != source && !renew.contains(replica))
                .collect();
            renew.extend(others);
        }
        for &replica in &renew {
            self.reset(replica, times, out);
        }
        renew
    }

    /// Writes the source's whole book: every minute, and whenever the source changes.
    pub fn snapshot(&mut self, times: Times, out: &mut impl Sink) {
        if let Some(source) = self.source
            && let State::Synced(synced) = &self.replicas[source].state
        {
            let stamp = Stamp {
                exchange: synced.lag.covered(),
                received: times.replicas[source],
            };
            out.put(self.messages.snapshot(&synced.book, stamp));
        }
    }

    /// Makes sure the source is a replica with a book that doesn't trail by `MAX_LAG` now. A new
    /// source is announced with its snapshot.
    fn settle(&mut self, times: Times, out: &mut impl Sink) {
        let keep = self
            .source
            .and_then(|source| self.synced(source))
            .is_some_and(|synced| synced.lag.lag(times.now) < MAX_LAG);
        if keep {
            return;
        }
        if let Some(best) = self.best() {
            self.source = Some(best);
            self.snapshot(times, out);
        }
    }

    /// The most up-to-date replica with a book: one that does not trail, then the one covering
    /// the latest exchange time. Ties go to the lowest index.
    fn best(&self) -> Option<usize> {
        (0..REPLICAS)
            .filter_map(|replica| Some((replica, self.synced(replica)?)))
            .max_by_key(|&(replica, synced)| {
                (!synced.lag.trails(), synced.lag.covered(), Reverse(replica))
            })
            .map(|(replica, _)| replica)
    }

    /// `replica` streamed a change at `stamp`: its own book covers it, every other book misses
    /// it unless it covers it already, its stuck wait starts over, and the symbol's silence ends.
    fn streamed(&mut self, replica: usize, stamp: Stamp) {
        for (index, other) in self.replicas.iter_mut().enumerate() {
            if let State::Synced(synced) = &mut other.state {
                if index == replica {
                    synced.lag.stream(stamp.exchange);
                } else {
                    synced.lag.miss(stamp);
                }
            }
        }
        self.replicas[replica].stuck.streamed();
        self.silence.changed(stamp);
    }

    /// Starts over what `replica`'s book misses, from the newest change the other books streamed,
    /// as if read at `since`.
    fn restart_missing(&mut self, replica: usize, since: DateTime<Utc>) {
        let elsewhere = (0..REPLICAS)
            .filter(|&other| other != replica)
            .filter_map(|other| self.synced(other)?.lag.newest())
            .max();
        if let State::Synced(synced) = &mut self.replicas[replica].state {
            synced.lag.restart(elsewhere, since);
        }
    }

    /// Whether `replica` trails by its current stuck wait at `now`, or has waited that long for a
    /// book.
    fn stuck(&self, replica: usize, now: DateTime<Utc>) -> bool {
        let Replica { state, stuck } = &self.replicas[replica];
        let stuck_for = match state {
            State::Waiting(since) => now - *since,
            State::Synced(synced) => synced.lag.lag(now),
        };
        stuck.is_due(stuck_for)
    }

    fn synced(&self, replica: usize) -> Option<&Synced> {
        match &self.replicas[replica].state {
            State::Synced(synced) => Some(synced),
            State::Waiting(_) => None,
        }
    }
}

impl Messages {
    /// One level of the symbol, with its new quantity.
    fn update(&mut self, side: Side, level: Level, stamp: Stamp) -> Output {
        let stamp = self.no_earlier(stamp);
        Output::update(&self.symbol, side, level, stamp)
    }

    /// The symbol's whole book.
    fn snapshot(&mut self, book: &Book, stamp: Stamp) -> Output {
        let stamp = self.no_earlier(stamp);
        Output::snapshot(&self.symbol, book, stamp)
    }

    /// `stamp`, or the latest receive time in its place when that's later.
    fn no_earlier(&mut self, stamp: Stamp) -> Stamp {
        let received = self
            .latest
            .map_or(stamp.received, |latest| latest.max(stamp.received));
        self.latest = Some(received);
        Stamp { received, ..stamp }
    }
}

impl Synced {
    fn new(book: Book, covered: DateTime<Utc>) -> Self {
        Self {
            book,
            lag: Lag::new(covered),
            top_check: TopCheck::default(),
        }
    }

    /// Applies `update` to the book, after the top check it makes due, and keeps the top it
    /// reports for a later change to check. Returns the level's new quantity, or an error when
    /// the book missed messages.
    fn apply(&mut self, update: &Update) -> anyhow::Result<i64> {
        self.top_check.check(&self.book, update.exchange_time)?;
        let quantity = self.book.apply(update.side, update.price, update.change)?;
        if let Some(top) = update.top {
            self.top_check.report(top, update.exchange_time);
        }
        Ok(quantity)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ops::Deref;

    use proptest::prelude::*;

    use super::*;
    use crate::Side;
    use crate::book::tests::at;
    use crate::book::{Change, Top};
    use silence::MIN_SYMBOL_SILENCE;

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

    fn book(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Book {
        Book::new(at(bids), at(asks))
    }

    /// A feed of the symbol "S", driven the way the collector drives one: an event or a reset
    /// moves its replica's newest receive time, and now, along. A tick moves now to where other
    /// symbols' events would have taken it.
    struct Fed {
        feed: Feed,
        times: Times,
    }

    impl Deref for Fed {
        type Target = Feed;

        fn deref(&self) -> &Feed {
            &self.feed
        }
    }

    impl Fed {
        fn new() -> Self {
            Self {
                feed: Feed::new("S".into(), ms(0)),
                times: Times {
                    now: ms(0),
                    replicas: [ms(0); REPLICAS],
                },
            }
        }

        /// `replica`'s websocket was read at `received`.
        fn read(&mut self, replica: usize, received: DateTime<Utc>) {
            let read = &mut self.times.replicas[replica];
            *read = (*read).max(received);
            self.times.now = self.times.now.max(received);
        }

        fn book(&mut self, replica: usize, book: Book, stamp: Stamp, out: &mut Vec<Output>) {
            let Stamp { exchange, received } = stamp;
            self.read(replica, received);
            self.feed.book(replica, book, exchange, self.times, out);
        }

        fn update(
            &mut self,
            replica: usize,
            update: &Update,
            received: DateTime<Utc>,
            out: &mut Vec<Output>,
        ) -> bool {
            self.read(replica, received);
            self.feed.update(replica, update, self.times, out)
        }

        /// `replica`'s session ended at `at`.
        fn reset(&mut self, replica: usize, at: DateTime<Utc>, out: &mut Vec<Output>) {
            self.read(replica, at);
            self.feed.reset(replica, self.times, out);
        }

        fn on_tick(&mut self, now: DateTime<Utc>, out: &mut Vec<Output>) -> Vec<usize> {
            self.times.now = self.times.now.max(now);
            self.feed.on_tick(self.times, out)
        }

        fn snapshot(&mut self, out: &mut Vec<Output>) {
            self.feed.snapshot(self.times, out);
        }
    }

    /// A feed whose two replicas both hold `bids` as of exchange time 10, read at 20 and 21 ms,
    /// with replica 0 the source; the output so far is discarded.
    fn synced_feed(bids: &[(i64, i64)]) -> Fed {
        let mut feed = Fed::new();
        let mut out = Vec::new();
        feed.book(0, book(bids, &[]), at_ms(10, 20), &mut out);
        feed.book(1, book(bids, &[]), at_ms(10, 21), &mut out);
        assert_eq!(feed.source, Some(0));
        feed
    }

    /// `change` to bid `price` at `exchange_time`, after which the exchange reported `top`.
    fn bid(price: i64, change: Change, exchange_time: DateTime<Utc>, top: Option<Top>) -> Update {
        Update {
            symbol: "S".into(),
            exchange_time,
            side: Side::Buy,
            price,
            change,
            top,
        }
    }

    /// Sets bid `price` to `quantity` on `replica`; returns the output and whether it applied.
    fn set(
        feed: &mut Fed,
        replica: usize,
        price: i64,
        quantity: i64,
        stamp: Stamp,
    ) -> (Vec<Output>, bool) {
        let mut out = Vec::new();
        let update = bid(price, Change::Set(quantity), stamp.exchange, None);
        let applied = feed.update(replica, &update, stamp.received, &mut out);
        (out, applied)
    }

    fn bid_update(price: i64, quantity: i64, stamp: Stamp) -> Output {
        let level = Level { price, quantity };
        Output::update("S", Side::Buy, level, stamp)
    }

    fn snapshot(bids: &[(i64, i64)], stamp: Stamp) -> Output {
        Output::snapshot("S", &book(bids, &[]), stamp)
    }

    fn top(bid: i64) -> Top {
        Top {
            bid: Some(bid),
            ask: None,
        }
    }

    #[test]
    fn the_first_replica_with_a_book_becomes_the_source_and_starts_the_output_with_a_snapshot() {
        let mut feed = Fed::new();
        let mut out = Vec::new();
        feed.book(1, book(&[(40, 1)], &[(60, 2)]), at_ms(10, 11), &mut out);
        assert_eq!(
            out,
            vec![Output::snapshot(
                "S",
                &book(&[(40, 1)], &[(60, 2)]),
                at_ms(10, 11)
            )]
        );
        assert_eq!(feed.source, Some(1));

        out.clear();
        feed.book(0, book(&[(40, 1)], &[(60, 2)]), at_ms(12, 13), &mut out);
        assert!(out.is_empty(), "a second replica syncing writes nothing");
    }

    #[test]
    fn only_the_source_reaches_the_output() {
        let mut feed = synced_feed(&[(40, 1)]);
        assert!(set(&mut feed, 1, 40, 5, at_ms(30, 31)).0.is_empty());
        assert_eq!(
            set(&mut feed, 0, 40, 5, at_ms(30, 32)).0,
            vec![bid_update(40, 5, at_ms(30, 32))]
        );
    }

    #[test]
    fn a_relative_change_is_written_as_the_new_absolute_quantity() {
        let mut feed = synced_feed(&[(40, 5)]);
        let mut out = Vec::new();
        assert!(feed.update(0, &bid(40, Change::Add(-2), ms(30), None), ms(31), &mut out));
        assert_eq!(out, vec![bid_update(40, 3, at_ms(30, 31))]);
    }

    #[test]
    fn a_new_book_on_the_source_is_written_as_the_levels_that_changed() {
        let mut feed = synced_feed(&[(40, 1), (30, 2)]);
        let mut out = Vec::new();
        feed.book(0, book(&[(40, 1), (35, 4)], &[]), at_ms(30, 31), &mut out);
        assert_eq!(
            out,
            vec![
                bid_update(30, 0, at_ms(30, 31)),
                bid_update(35, 4, at_ms(30, 31))
            ]
        );
    }

    #[test]
    fn when_the_source_resets_the_output_switches_to_the_other_replica_with_a_snapshot() {
        let mut feed = synced_feed(&[(40, 1)]);
        set(&mut feed, 1, 40, 7, at_ms(30, 31));
        let mut out = Vec::new();
        feed.reset(0, ms(40), &mut out);
        assert_eq!(
            out,
            vec![snapshot(&[(40, 7)], at_ms(30, 31))],
            "as of when the other replica's websocket last read"
        );
        assert_eq!(feed.source, Some(1));
    }

    #[test]
    fn with_no_replica_left_the_output_waits_and_resumes_with_a_snapshot() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        feed.reset(0, ms(40), &mut out);
        feed.reset(1, ms(41), &mut out);
        out.clear();
        assert_eq!(feed.source, None);
        assert!(set(&mut feed, 0, 40, 9, at_ms(50, 51)).0.is_empty());

        feed.book(0, book(&[(40, 9)], &[]), at_ms(60, 61), &mut out);
        assert_eq!(out, vec![snapshot(&[(40, 9)], at_ms(60, 61))]);
    }

    #[test]
    fn a_source_trailing_by_max_lag_is_replaced_by_the_replica_it_trails() {
        let mut feed = synced_feed(&[(40, 1)]);
        set(&mut feed, 1, 40, 8, at_ms(100, 1_000));

        let mut out = Vec::new();
        feed.on_tick(ms(2_999), &mut out);
        assert!(
            out.is_empty(),
            "not yet: the source trails by less than MAX_LAG"
        );

        feed.on_tick(ms(3_000), &mut out);
        assert_eq!(out, vec![snapshot(&[(40, 8)], at_ms(100, 1_000))]);
        assert_eq!(feed.source, Some(1));
    }

    /// What a stream of changes did: when the output switched source, and which replicas were
    /// renewed when.
    #[derive(Debug, Default, PartialEq)]
    struct Played {
        switched: Vec<DateTime<Utc>>,
        renewed: Vec<(DateTime<Utc>, usize)>,
    }

    /// Plays a stream on `feed`: bid 40 changes every `every` ms from exchange time 1 s on,
    /// `count` times, and change `n` reaches replica `r` `delay(r, n)` ms after its exchange
    /// time, or never. A tick comes every second of receive time, and everything is applied in
    /// the order it was received.
    fn play(
        feed: &mut Fed,
        every: i64,
        count: i64,
        delay: impl Fn(usize, i64) -> Option<i64>,
    ) -> Played {
        let mut steps = Vec::new();
        for n in 1..=count {
            let exchange = 1_000 + n * every;
            for replica in 0..REPLICAS {
                if let Some(delay) = delay(replica, n) {
                    steps.push((exchange + delay, Some((replica, n, exchange))));
                }
            }
        }
        let last = steps
            .iter()
            .map(|(received, _)| *received)
            .max()
            .unwrap_or(0);
        steps.extend((1..=last / 1_000 + 1).map(|second| (second * 1_000, None)));
        steps.sort_by_key(|(received, step)| (*received, step.is_none()));

        let mut played = Played::default();
        for (received, step) in steps {
            let out = match step {
                Some((replica, n, exchange)) => {
                    set(feed, replica, 40, n, at_ms(exchange, received)).0
                }
                None => {
                    let mut out = Vec::new();
                    for replica in feed.on_tick(ms(received), &mut out) {
                        played.renewed.push((ms(received), replica));
                    }
                    out
                }
            };
            let switches = out.iter().filter(|o| matches!(o, Output::Snapshot(_)));
            played.switched.extend(switches.map(|_| ms(received)));
        }
        played
    }

    #[test]
    fn ordinary_jitter_between_replicas_does_not_switch_the_source() {
        let mut feed = synced_feed(&[(40, 1)]);
        // A change every 10 ms. The source reads each 50 to 70 ms after it happened, the other
        // replica 5 to 25 ms after, so the source never has the newest change.
        let delays = [[50, 60, 70, 65, 55], [5, 15, 25, 20, 10]];
        let played = play(&mut feed, 10, 2_000, |replica, n| {
            Some(delays[replica][n as usize % 5])
        });
        assert_eq!(played, Played::default());
        assert_eq!(feed.source, Some(0));
    }

    #[test]
    fn a_busy_symbol_with_a_steady_lag_well_below_two_seconds_switches_and_renews_nothing() {
        let mut feed = synced_feed(&[(40, 1)]);
        // For two minutes, a change every 10 ms reaches the source 45 ms after the other replica.
        let played = play(&mut feed, 10, 12_000, |replica, _| {
            Some(if replica == 0 { 50 } else { 5 })
        });
        assert_eq!(played, Played::default());
        assert_eq!(feed.source, Some(0));
    }

    #[test]
    fn a_source_whose_changes_arrive_a_steady_three_seconds_late_loses_the_output_after_two() {
        let mut feed = synced_feed(&[(40, 1)]);
        // For 40 seconds, a change every 100 ms reaches the source 3 s after the other replica.
        let played = play(&mut feed, 100, 400, |replica, _| {
            Some(if replica == 0 { 3_005 } else { 5 })
        });
        // The first change the source lacks reached the other replica at 1,105 ms.
        let expected = Played {
            switched: vec![ms(3_105)],
            renewed: Vec::new(),
        };
        assert_eq!(played, expected, "3 s late isn't stuck");
        assert_eq!(feed.source, Some(1));
    }

    #[test]
    fn a_replica_that_stalls_in_a_busy_stream_is_renewed_after_thirty_seconds() {
        let mut feed = synced_feed(&[(40, 1)]);
        // A change every 100 ms for 40 seconds. The source stops reading after the 10th.
        let played = play(&mut feed, 100, 400, |replica, n| {
            (replica == 1 || n <= 10).then_some(5)
        });
        // The 11th change reached the other replica at 2,105 ms.
        let expected = Played {
            switched: vec![ms(4_105)],
            renewed: vec![(ms(33_000), 0)],
        };
        assert_eq!(played, expected);
    }

    #[test]
    fn a_replica_that_gets_its_book_after_the_other_streamed_past_it_trails_from_then() {
        let mut feed = Fed::new();
        let mut out = Vec::new();
        feed.book(0, book(&[(40, 1)], &[]), at_ms(10, 20), &mut out);
        set(&mut feed, 0, 40, 2, at_ms(100, 1_000));
        // Its snapshot is older than the change the source streamed at 1 s.
        feed.book(1, book(&[(40, 1)], &[]), at_ms(50, 5_000), &mut out);
        assert!(feed.on_tick(ms(34_999), &mut out).is_empty());
        assert_eq!(feed.on_tick(ms(35_000), &mut out), vec![1]);
    }

    #[test]
    fn a_replica_trails_no_more_once_the_book_it_trailed_is_dropped() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        set(&mut feed, 0, 40, 2, at_ms(100, 1_000));
        feed.reset(0, ms(20_000), &mut out);
        assert_eq!(
            out,
            vec![snapshot(&[(40, 1)], at_ms(10, 1_000))],
            "no earlier than the source's last update"
        );
        // Only the replica waiting for its book since 20 s is stuck.
        assert!(feed.on_tick(ms(49_999), &mut out).is_empty());
        assert_eq!(feed.on_tick(ms(50_000), &mut out), vec![0]);
    }

    #[test]
    fn a_snapshot_taken_later_does_not_make_the_other_replica_trail() {
        let mut feed = Fed::new();
        let mut out = Vec::new();
        feed.book(0, book(&[(40, 1)], &[]), at_ms(50, 51), &mut out);
        feed.book(1, book(&[(40, 1)], &[]), at_ms(5_000, 5_001), &mut out);
        // Nor does a second one that changes nothing, like the answer to a renewal that crossed
        // the first book on its way.
        feed.book(1, book(&[(40, 1)], &[]), at_ms(9_000, 9_001), &mut out);
        out.clear();
        assert!(feed.on_tick(ms(120_000), &mut out).is_empty());
        assert!(out.is_empty());
        assert_eq!(feed.source, Some(0));
    }

    #[test]
    fn a_second_book_that_changes_levels_is_a_change_the_other_replica_has_to_cover() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        feed.book(1, book(&[(40, 2)], &[]), at_ms(9_000, 9_001), &mut out);
        feed.on_tick(ms(11_000), &mut out);
        assert!(out.is_empty(), "the source trails by less than MAX_LAG");
        feed.on_tick(ms(11_001), &mut out);
        assert_eq!(out, vec![snapshot(&[(40, 2)], at_ms(9_000, 9_001))]);
    }

    #[test]
    fn a_replica_trailing_by_thirty_seconds_has_its_subscription_renewed() {
        let mut feed = synced_feed(&[(40, 1)]);
        set(&mut feed, 0, 40, 8, at_ms(100, 1_000));
        let mut out = Vec::new();
        assert!(feed.on_tick(ms(30_999), &mut out).is_empty());
        assert_eq!(feed.on_tick(ms(31_000), &mut out), vec![1]);
        assert!(out.is_empty(), "the source did not change");
        assert!(
            feed.synced(1).is_none(),
            "the renewed replica waits for a new book"
        );
    }

    #[test]
    fn a_replica_that_never_gets_a_book_is_renewed_each_time_its_stuck_wait_runs_out() {
        let mut feed = Fed::new();
        let mut out = Vec::new();
        feed.book(0, book(&[(40, 1)], &[]), at_ms(10, 11), &mut out);
        // At 30 s, then a minute later, then two minutes after that.
        for due in [30_000, 90_000, 210_000] {
            assert!(feed.on_tick(ms(due - 1), &mut out).is_empty(), "{due}");
            assert_eq!(feed.on_tick(ms(due), &mut out), vec![1], "{due}");
        }
    }

    #[test]
    fn a_renewed_replica_goes_back_to_the_first_wait_once_it_streams_not_when_its_book_arrives() {
        let mut feed = synced_feed(&[(40, 1)]);
        set(&mut feed, 0, 40, 8, at_ms(100, 1_000));
        let mut out = Vec::new();
        assert_eq!(feed.on_tick(ms(31_000), &mut out), vec![1]);
        let mut renewed_once = Stuck::default();
        renewed_once.renewed();
        assert_eq!(feed.replicas[1].stuck, renewed_once);

        feed.book(1, book(&[(40, 8)], &[]), at_ms(31_100, 31_200), &mut out);
        assert_eq!(feed.replicas[1].stuck, renewed_once, "a book is no stream");
        set(&mut feed, 1, 40, 9, at_ms(31_300, 31_400));
        assert_eq!(feed.replicas[1].stuck, Stuck::default());
    }

    #[test]
    fn a_change_that_proves_missed_messages_drops_the_replica_and_moves_the_source() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        assert!(!feed.update(0, &bid(40, Change::Add(-5), ms(30), None), ms(31), &mut out));
        assert_eq!(out, vec![snapshot(&[(40, 1)], at_ms(10, 21))]);
        assert_eq!(feed.source, Some(1));
        assert!(feed.synced(0).is_none());
    }

    #[test]
    fn changes_before_the_first_book_are_ignored() {
        let mut feed = Fed::new();
        assert!(set(&mut feed, 0, 40, 5, at_ms(10, 11)).0.is_empty());
        assert!(feed.synced(0).is_none());
    }

    /// The change to bid 50 at 30 ms, after which the exchange reports a best bid of 60.
    fn bid_50_with_top_60() -> Update {
        bid(50, Change::Set(2), ms(30), Some(top(60)))
    }

    #[test]
    fn a_book_that_fails_the_top_check_of_its_last_change_is_dropped_and_the_source_moves() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        assert!(feed.update(0, &bid_50_with_top_60(), ms(31), &mut out));
        let (out, applied) = set(&mut feed, 0, 45, 1, at_ms(31, 32));
        assert!(!applied, "the book's best bid is 50, the exchange's 60");
        assert_eq!(
            out,
            vec![snapshot(&[(40, 1)], at_ms(10, 31))],
            "no earlier than the update the book took at 31 ms"
        );
        assert_eq!(feed.source, Some(1));
    }

    #[test]
    fn a_change_whose_top_the_book_reaches_passes_the_check() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        let reaches = bid(60, Change::Set(2), ms(30), Some(top(60)));
        assert!(feed.update(0, &reaches, ms(31), &mut out));
        assert!(set(&mut feed, 0, 45, 1, at_ms(31, 32)).1);
    }

    #[test]
    fn a_whole_book_replaces_a_pending_top_check() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        feed.update(0, &bid_50_with_top_60(), ms(31), &mut out);
        feed.book(0, book(&[(40, 1)], &[]), at_ms(31, 32), &mut out);
        assert!(set(&mut feed, 0, 45, 1, at_ms(32, 33)).1);
    }

    #[test]
    fn a_top_reported_to_a_replica_without_a_book_is_ignored() {
        let mut feed = Fed::new();
        let mut out = Vec::new();
        feed.update(0, &bid_50_with_top_60(), ms(31), &mut out);
        feed.book(0, book(&[(40, 1)], &[]), at_ms(31, 32), &mut out);
        assert!(set(&mut feed, 0, 45, 1, at_ms(32, 33)).1);
    }

    /// A synced feed that changed every second from exchange time 1 s to `last` s. Each change
    /// reaches the source 5 ms later and the other replica 600 ms later.
    fn active_feed(last: i64) -> Fed {
        let mut feed = synced_feed(&[(40, 1)]);
        for second in 1..=last {
            let ms = second * 1_000;
            set(&mut feed, 0, 40, second, at_ms(ms, ms + 5));
            set(&mut feed, 1, 40, second, at_ms(ms, ms + 600));
        }
        feed
    }

    #[test]
    fn a_symbol_silent_for_twenty_usual_gaps_renews_the_replica_that_is_not_the_source() {
        let mut feed = active_feed(10);
        let mut out = Vec::new();
        // Gaps of 1 s make 20 s, which the floor raises to a minute.
        let silent = 10_005 + MIN_SYMBOL_SILENCE.num_milliseconds();
        assert!(feed.on_tick(ms(silent), &mut out).is_empty());
        assert_eq!(feed.on_tick(ms(silent + 1), &mut out), vec![1]);
        assert!(out.is_empty(), "the source keeps its book");
        assert!(feed.synced(0).is_some() && feed.synced(1).is_none());
    }

    #[test]
    fn replicas_that_get_their_books_back_after_a_long_outage_start_the_silence_over() {
        let mut feed = active_feed(10);
        let mut out = Vec::new();
        // Both sessions are down for two minutes, longer than the minute this symbol may be
        // silent, and come back with fresh books.
        feed.reset(0, ms(11_000), &mut out);
        feed.reset(1, ms(11_000), &mut out);
        feed.book(0, book(&[(40, 10)], &[]), at_ms(131_000, 131_000), &mut out);
        feed.book(1, book(&[(40, 10)], &[]), at_ms(131_000, 131_100), &mut out);
        assert!(feed.on_tick(ms(131_200), &mut out).is_empty());

        let silent = 131_100 + MIN_SYMBOL_SILENCE.num_milliseconds();
        assert!(feed.on_tick(ms(silent), &mut out).is_empty());
        assert_eq!(feed.on_tick(ms(silent + 1), &mut out), vec![1]);
    }

    #[test]
    fn a_second_book_that_changes_nothing_does_not_end_a_silence() {
        let mut feed = active_feed(10);
        let mut out = Vec::new();
        feed.book(1, book(&[(40, 10)], &[]), at_ms(30_000, 30_001), &mut out);
        let silent = 10_005 + MIN_SYMBOL_SILENCE.num_milliseconds();
        assert!(feed.on_tick(ms(silent), &mut out).is_empty());
        assert_eq!(feed.on_tick(ms(silent + 1), &mut out), vec![1]);
    }

    #[test]
    fn the_minute_snapshot_is_the_source_book_as_of_its_latest_exchange_time() {
        let mut feed = synced_feed(&[(40, 1)]);
        set(&mut feed, 0, 30, 2, at_ms(70, 71));
        let mut out = Vec::new();
        feed.snapshot(&mut out);
        assert_eq!(out, vec![snapshot(&[(40, 1), (30, 2)], at_ms(70, 71))]);

        out.clear();
        Fed::new().snapshot(&mut out);
        assert!(out.is_empty(), "nothing to write before any book");
    }

    #[test]
    fn a_minute_snapshot_carries_its_source_websocket_newest_receive_time_not_now() {
        let mut feed = synced_feed(&[(40, 1)]);
        let mut out = Vec::new();
        // Other symbols' events took now to a minute.
        assert!(feed.on_tick(ms(60_000), &mut out).is_empty());
        feed.snapshot(&mut out);
        assert_eq!(out, vec![snapshot(&[(40, 1)], at_ms(10, 20))]);
    }

    #[test]
    fn right_after_a_switch_nothing_goes_out_earlier_than_the_old_source_last_message() {
        let mut feed = synced_feed(&[(40, 1)]);
        let (out, _) = set(&mut feed, 0, 40, 2, at_ms(100, 1_000));
        assert_eq!(out, vec![bid_update(40, 2, at_ms(100, 1_000))]);
        let mut out = Vec::new();
        feed.reset(0, ms(1_100), &mut out);
        // The new source's websocket had only been read up to 21 ms, and still had a change
        // read at 500 ms on its way.
        let (later, _) = set(&mut feed, 1, 40, 2, at_ms(100, 500));
        out.extend(later);
        let (latest, _) = set(&mut feed, 1, 40, 3, at_ms(200, 1_200));
        out.extend(latest);
        assert_eq!(
            out,
            vec![
                snapshot(&[(40, 1)], at_ms(10, 1_000)),
                bid_update(40, 2, at_ms(100, 1_000)),
                bid_update(40, 3, at_ms(200, 1_200)),
            ]
        );
    }

    /// One step of a random session against both replicas.
    #[derive(Debug, Clone)]
    enum Step {
        Book(usize, Vec<(i64, i64)>, Vec<(i64, i64)>),
        /// A change on the bid side or not, at a price, and the top reported after it, if any.
        Update(usize, bool, i64, Change, Option<Top>),
        Reset(usize),
        Tick,
    }

    fn step() -> impl Strategy<Value = Step> {
        let levels = || prop::collection::vec((1..6_i64, 1..4_i64), 0..4);
        let change = prop_oneof![
            (0..4_i64).prop_map(Change::Set),
            (-3..4_i64).prop_map(Change::Add)
        ];
        let price = || prop::option::of(1..6_i64);
        // A random top rarely matches the book, so only a quarter of the changes report one, and
        // books still live through a few changes.
        let top = prop::option::weighted(
            0.25,
            (price(), price()).prop_map(|(bid, ask)| Top { bid, ask }),
        );
        prop_oneof![
            1 => (0..REPLICAS, levels(), levels())
                .prop_map(|(replica, bids, asks)| Step::Book(replica, bids, asks)),
            4 => (0..REPLICAS, any::<bool>(), 1..6_i64, change, top).prop_map(
                |(replica, bid, price, change, top)| Step::Update(replica, bid, price, change, top)
            ),
            1 => (0..REPLICAS).prop_map(Step::Reset),
            1 => Just(Step::Tick),
        ]
    }

    /// The receive time of a message of the output.
    fn received(output: &Output) -> DateTime<Utc> {
        match output {
            Output::Snapshot(snapshot) => snapshot.received_at,
            Output::Update(update) => update.backtest_timestamp,
        }
    }

    proptest! {
        /// The output's promise: replaying it gives exactly the source's book, whatever the
        /// replicas did and however often the source changed.
        #[test]
        fn replaying_the_output_always_gives_the_source_book(
            steps in prop::collection::vec((step(), 0..1_500_i64), 1..80),
        ) {
            let mut feed = Fed::new();
            let mut held = BTreeMap::new();
            let mut now = 0;
            for (step, elapsed) in steps {
                now += elapsed;
                let stamp = at_ms(now, now);
                let mut out = Vec::new();
                match step {
                    Step::Book(replica, bids, asks) => {
                        feed.book(replica, book(&bids, &asks), stamp, &mut out);
                    }
                    Step::Update(replica, bid, price, change, top) => {
                        let side = if bid { Side::Buy } else { Side::Sell };
                        let update = Update {
                            symbol: "S".into(),
                            exchange_time: ms(now),
                            side,
                            price,
                            change,
                            top,
                        };
                        feed.update(replica, &update, ms(now), &mut out);
                    }
                    Step::Reset(replica) => feed.reset(replica, ms(now), &mut out),
                    Step::Tick => {
                        feed.on_tick(ms(now), &mut out);
                    }
                }
                for output in &out {
                    output.replay(&mut held).expect("the output starts the symbol with a snapshot");
                }
                if let Some(source) = feed.source {
                    let synced = feed.synced(source).expect("the source always has a book");
                    prop_assert_eq!(held.get("S"), Some(&synced.book));
                }
            }
        }

        /// The output's times: the symbol's receive times never go back, whatever the replicas
        /// did, however often the source changed, and however far apart the replicas' websockets
        /// read.
        #[test]
        fn receive_times_in_the_output_never_go_back(
            steps in prop::collection::vec((step(), 0..1_500_i64), 1..80),
        ) {
            let mut feed = Fed::new();
            // Each replica's websocket reads at a pace of its own.
            let mut read = [0; REPLICAS];
            let mut latest = None;
            for (step, elapsed) in steps {
                let mut out = Vec::new();
                match step {
                    Step::Book(replica, bids, asks) => {
                        read[replica] += elapsed;
                        let stamp = at_ms(read[replica], read[replica]);
                        feed.book(replica, book(&bids, &asks), stamp, &mut out);
                    }
                    Step::Update(replica, bid, price, change, top) => {
                        read[replica] += elapsed;
                        let side = if bid { Side::Buy } else { Side::Sell };
                        let update = Update {
                            symbol: "S".into(),
                            exchange_time: ms(read[replica]),
                            side,
                            price,
                            change,
                            top,
                        };
                        feed.update(replica, &update, ms(read[replica]), &mut out);
                    }
                    Step::Reset(replica) => {
                        read[replica] += elapsed;
                        feed.reset(replica, ms(read[replica]), &mut out);
                    }
                    // A tick at the minute, after other symbols' events took now further.
                    Step::Tick => {
                        let now = read.iter().max().unwrap() + elapsed;
                        feed.on_tick(ms(now), &mut out);
                        feed.snapshot(&mut out);
                    }
                }
                for output in &out {
                    prop_assert!(latest <= Some(received(output)), "{output:?} after {latest:?}");
                    latest = Some(received(output));
                }
            }
        }
    }
}
