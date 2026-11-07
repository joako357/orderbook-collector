//! The exchange-agnostic core: every symbol's feed, what the sockets subscribe, and the minute
//! snapshots. It never waits on a socket and never reads the clock: every input brings its own
//! time, so tests drive it with plain values.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, DurationRound, TimeDelta, Utc};

use crate::command::{Command, Conn};
use crate::exchange::{Event, Symbol};
use crate::feed::{Feed, REPLICAS, Times};
use crate::output::Sink;
use crate::shards::{ByShard, Shards};

/// How often every book is written in full, on the minute.
const SNAPSHOT_EVERY: TimeDelta = TimeDelta::minutes(1);

/// Every symbol of one exchange, across all of its sockets.
pub struct Collector {
    shards: Shards,
    /// The feeds of each shard's symbols.
    feeds: Vec<BTreeMap<Symbol, Feed>>,
    /// How far each shard's websockets have been read, by replica: the newest receive time
    /// processed from each. What a feed writes from a replica's book goes out with that one.
    read_up_to: Vec<[DateTime<Utc>; REPLICAS]>,
    /// The newest receive time processed from any websocket, which stands in for now in the
    /// feeds' checks.
    now: DateTime<Utc>,
    next_snapshot: DateTime<Utc>,
}

impl Collector {
    /// A collector with no symbols yet, `per_socket` symbols per shard for as long as the
    /// exchange's `max_sockets` allow, started when the clock read `clock`.
    pub fn new(per_socket: usize, max_sockets: usize, clock: DateTime<Utc>) -> Self {
        Self {
            shards: Shards::new(per_socket, max_sockets / REPLICAS),
            feeds: Vec::new(),
            read_up_to: Vec::new(),
            now: clock,
            next_snapshot: next_minute(clock),
        }
    }

    /// A page of the listing, read at `listed`: subscribes the symbols that no shard carries yet,
    /// on every replica of their shard.
    pub fn add_live<'a>(
        &mut self,
        page: impl IntoIterator<Item = &'a Symbol>,
        listed: DateTime<Utc>,
    ) -> Vec<(Conn, Command)> {
        let added = self.shards.add(page, listed);
        for (&shard, symbols) in &added {
            if self.feeds.len() <= shard {
                self.feeds.resize_with(shard + 1, BTreeMap::new);
                self.read_up_to.resize(shard + 1, [self.now; REPLICAS]);
            }
            for symbol in symbols {
                let feed = Feed::new(symbol.clone(), self.now);
                self.feeds[shard].insert(symbol.clone(), feed);
            }
        }
        each_replica(added, Command::Subscribe)
    }

    /// A complete listing, whose round started at `started` and ended at `listed`: unsubscribes
    /// the symbols that stopped trading, and then subscribes any that are new.
    pub fn set_live(
        &mut self,
        live: &BTreeSet<Symbol>,
        started: DateTime<Utc>,
        listed: DateTime<Utc>,
    ) -> Vec<(Conn, Command)> {
        let removed = self.shards.remove_unlisted(live, started);
        for (&shard, symbols) in &removed {
            for symbol in symbols {
                self.feeds[shard].remove(symbol);
            }
        }
        let mut commands = each_replica(removed, Command::Unsubscribe);
        commands.extend(self.add_live(live, listed));
        commands
    }

    /// Applies one event from `conn`, read at `received`. Returns the refresh it calls for, if
    /// any.
    pub fn on_event(
        &mut self,
        conn: Conn,
        received: DateTime<Utc>,
        event: Event,
        out: &mut impl Sink,
    ) -> Vec<(Conn, Command)> {
        let times = self.read(conn, received);
        match event {
            Event::Book {
                symbol,
                exchange_time,
                book,
            } => {
                if let Some(feed) = self.feed(conn, &symbol) {
                    feed.book(conn.replica, book, exchange_time, times, out);
                }
                Vec::new()
            }
            Event::Update(update) => {
                let Some(feed) = self.feed(conn, &update.symbol) else {
                    return Vec::new();
                };
                if feed.update(conn.replica, &update, times, out) {
                    Vec::new()
                } else {
                    vec![(conn, Command::Refresh(vec![update.symbol]))]
                }
            }
            // A refresh right away would loop on a value that never reads, so the stuck check
            // renews this book later, like any subscription that has none.
            Event::Unreadable { symbol } => {
                if let Some(feed) = self.feed(conn, &symbol) {
                    feed.reset(conn.replica, times, out);
                }
                Vec::new()
            }
        }
    }

    /// The session of `conn` ended at `at`, so every book it built is dropped. The socket
    /// subscribes them all again in its next session.
    pub fn on_reset(&mut self, conn: Conn, at: DateTime<Utc>, out: &mut impl Sink) {
        let times = self.read(conn, at);
        for feed in self.feeds[conn.shard].values_mut() {
            feed.reset(conn.replica, times, out);
        }
    }

    /// Runs every second, when the clock reads `clock`: renews the subscriptions of stuck
    /// replicas and silent symbols, and writes the minute snapshots. The clock only says when the
    /// snapshots are due, so they come even while nothing arrives.
    pub fn on_tick(&mut self, clock: DateTime<Utc>, out: &mut impl Sink) -> Vec<(Conn, Command)> {
        let commands = self.renew_stuck_and_silent(out);
        self.write_minute_snapshots(clock, out);
        commands
    }

    /// Runs every feed's checks, and renews the subscriptions they ask for, one command per
    /// socket.
    fn renew_stuck_and_silent(&mut self, out: &mut impl Sink) -> Vec<(Conn, Command)> {
        let mut renew: BTreeMap<Conn, Vec<Symbol>> = BTreeMap::new();
        for (shard, feeds) in self.feeds.iter_mut().enumerate() {
            let times = Times {
                now: self.now,
                replicas: self.read_up_to[shard],
            };
            for (symbol, feed) in feeds {
                for replica in feed.on_tick(times, out) {
                    let conn = Conn { shard, replica };
                    renew.entry(conn).or_default().push(symbol.clone());
                }
            }
        }
        renew
            .into_iter()
            .map(|(conn, symbols)| (conn, Command::Renew(symbols)))
            .collect()
    }

    /// Writes every book in full once the clock, at `clock`, reaches the next minute.
    fn write_minute_snapshots(&mut self, clock: DateTime<Utc>, out: &mut impl Sink) {
        if clock < self.next_snapshot {
            return;
        }
        for (feeds, read_up_to) in self.feeds.iter_mut().zip(&self.read_up_to) {
            let times = Times {
                now: self.now,
                replicas: *read_up_to,
            };
            for feed in feeds.values_mut() {
                feed.snapshot(times, out);
            }
        }
        self.next_snapshot = next_minute(clock);
    }

    /// `conn`'s websocket was read at `received`. Returns the times the feeds of its shard go by
    /// now. Sockets only start for the shards this collector made, so `conn`'s shard is one.
    fn read(&mut self, conn: Conn, received: DateTime<Utc>) -> Times {
        self.now = self.now.max(received);
        let read_up_to = &mut self.read_up_to[conn.shard];
        let replica = &mut read_up_to[conn.replica];
        *replica = (*replica).max(received);
        Times {
            now: self.now,
            replicas: *read_up_to,
        }
    }

    /// The feed of `symbol`, if `conn` carries it. Messages for a symbol can still arrive just
    /// after it was unsubscribed.
    fn feed(&mut self, conn: Conn, symbol: &str) -> Option<&mut Feed> {
        self.feeds[conn.shard].get_mut(symbol)
    }
}

/// `command` for every replica of each shard, with that shard's symbols.
fn each_replica(by_shard: ByShard, command: fn(Vec<Symbol>) -> Command) -> Vec<(Conn, Command)> {
    let mut commands = Vec::new();
    for (shard, symbols) in by_shard {
        for replica in 0..REPLICAS {
            commands.push((Conn { shard, replica }, command(symbols.clone())));
        }
    }
    commands
}

fn next_minute(clock: DateTime<Utc>) -> DateTime<Utc> {
    let minute = clock
        .duration_trunc(SNAPSHOT_EVERY)
        .expect("a minute fits any timestamp");
    minute + SNAPSHOT_EVERY
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::book::tests::at;
    use crate::book::{Book, Change, Top};
    use crate::exchange::Update;
    use crate::output::{Output, Stamp};

    /// `n` seconds after the epoch.
    fn seconds(n: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::seconds(n)
    }

    /// Exchange time `exchange` and receive time `received`, in seconds.
    fn at_seconds(exchange: i64, received: i64) -> Stamp {
        Stamp {
            exchange: seconds(exchange),
            received: seconds(received),
        }
    }

    fn symbols(names: &[&str]) -> BTreeSet<Symbol> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn names(names: &[&str]) -> Vec<Symbol> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn conn(shard: usize, replica: usize) -> Conn {
        Conn { shard, replica }
    }

    fn book_event(symbol: &str, bids: &[(i64, i64)], exchange_seconds: i64) -> Event {
        Event::Book {
            symbol: symbol.into(),
            exchange_time: seconds(exchange_seconds),
            book: Book::new(at(bids), []),
        }
    }

    fn update_event(
        symbol: &str,
        change: Change,
        top: Option<Top>,
        exchange_seconds: i64,
    ) -> Event {
        Event::Update(Update {
            symbol: symbol.into(),
            exchange_time: seconds(exchange_seconds),
            side: Side::Buy,
            price: 40,
            change,
            top,
        })
    }

    fn snapshot(symbol: &str, stamp: Stamp) -> Output {
        Output::snapshot(symbol, &Book::new(at(&[(40, 1)]), []), stamp)
    }

    /// A collector carrying `live`, two symbols per socket, where both replicas of every symbol
    /// got the book `[(40, 1)]`. The output so far is discarded.
    fn synced(live: &[&str]) -> Collector {
        let mut collector = Collector::new(2, usize::MAX, seconds(0));
        collector.set_live(&symbols(live), seconds(0), seconds(0));
        let mut out = Vec::new();
        for (index, symbol) in live.iter().enumerate() {
            for replica in 0..REPLICAS {
                let event = book_event(symbol, &[(40, 1)], 1);
                collector.on_event(conn(index / 2, replica), seconds(1), event, &mut out);
            }
        }
        collector
    }

    /// Processes an event that `conn` read at `received` seconds about a symbol no socket
    /// carries: the receive times processed move on, and no feed changes.
    fn receive_until(collector: &mut Collector, conn: Conn, received: i64) {
        let mut out = Vec::new();
        let elsewhere = book_event("elsewhere", &[(40, 1)], received);
        assert!(
            collector
                .on_event(conn, seconds(received), elsewhere, &mut out)
                .is_empty()
        );
        assert!(out.is_empty());
    }

    /// The receive times of `symbol`'s messages in `out`, in order.
    fn receive_times(out: &[Output], symbol: &str) -> Vec<DateTime<Utc>> {
        let received = |output: &Output| match output {
            Output::Snapshot(snapshot) => {
                (snapshot.symbol == symbol).then_some(snapshot.received_at)
            }
            Output::Update(update) => {
                (update.symbol == symbol).then_some(update.backtest_timestamp)
            }
        };
        out.iter().filter_map(received).collect()
    }

    #[test]
    fn listings_subscribe_and_unsubscribe_symbols_on_both_replicas_of_their_shard() {
        let mut collector = Collector::new(2, usize::MAX, seconds(0));
        let commands = collector.set_live(&symbols(&["a", "b", "c"]), seconds(0), seconds(0));
        assert_eq!(
            commands,
            vec![
                (conn(0, 0), Command::Subscribe(names(&["a", "b"]))),
                (conn(0, 1), Command::Subscribe(names(&["a", "b"]))),
                (conn(1, 0), Command::Subscribe(names(&["c"]))),
                (conn(1, 1), Command::Subscribe(names(&["c"]))),
            ]
        );
        collector.set_live(&symbols(&["b", "c"]), seconds(10), seconds(20));
        let commands = collector.set_live(&symbols(&["b", "c", "d"]), seconds(30), seconds(40));
        assert_eq!(
            commands,
            vec![
                (conn(0, 0), Command::Unsubscribe(names(&["a"]))),
                (conn(0, 1), Command::Unsubscribe(names(&["a"]))),
                (conn(0, 0), Command::Subscribe(names(&["d"]))),
                (conn(0, 1), Command::Subscribe(names(&["d"]))),
            ]
        );
    }

    #[test]
    fn the_limit_on_websockets_counts_both_replicas_of_each_shard() {
        // Four websockets are two shards of two replicas.
        let mut collector = Collector::new(2, 4, seconds(0));
        let commands =
            collector.set_live(&symbols(&["a", "b", "c", "d", "e"]), seconds(0), seconds(0));
        let shards: BTreeSet<usize> = commands.iter().map(|(conn, _)| conn.shard).collect();
        assert_eq!(shards, BTreeSet::from([0, 1]));
    }

    #[test]
    fn an_unsubscribed_symbol_has_no_feed_left() {
        let mut collector = synced(&["a", "b"]);
        collector.set_live(&symbols(&["b"]), seconds(10), seconds(10));
        collector.set_live(&symbols(&["b"]), seconds(20), seconds(20));
        let mut out = Vec::new();
        let update = update_event("a", Change::Set(2), None, 21);
        assert!(
            collector
                .on_event(conn(0, 0), seconds(21), update, &mut out)
                .is_empty()
        );
        collector.on_tick(seconds(60), &mut out);
        assert_eq!(
            out,
            vec![snapshot("b", at_seconds(1, 21))],
            "only b is left"
        );
    }

    #[test]
    fn a_book_reaches_the_feed_of_its_symbol_and_starts_its_output() {
        let mut collector = Collector::new(2, usize::MAX, seconds(0));
        collector.set_live(&symbols(&["a"]), seconds(0), seconds(0));
        let mut out = Vec::new();
        let commands = collector.on_event(
            conn(0, 1),
            seconds(2),
            book_event("a", &[(40, 1)], 1),
            &mut out,
        );
        assert!(commands.is_empty());
        assert_eq!(out, vec![snapshot("a", at_seconds(1, 2))]);
    }

    #[test]
    fn events_for_a_symbol_the_socket_does_not_carry_are_ignored() {
        let mut collector = Collector::new(1, usize::MAX, seconds(0));
        collector.set_live(&symbols(&["a", "b"]), seconds(0), seconds(0));
        let mut out = Vec::new();
        collector.on_event(
            conn(1, 0),
            seconds(2),
            book_event("a", &[(40, 1)], 1),
            &mut out,
        );
        collector.on_event(
            conn(0, 0),
            seconds(2),
            book_event("zzz", &[(40, 1)], 1),
            &mut out,
        );
        let unreadable = Event::Unreadable {
            symbol: "zzz".into(),
        };
        collector.on_event(conn(0, 0), seconds(2), unreadable, &mut out);
        let update = update_event("zzz", Change::Set(1), None, 1);
        assert!(
            collector
                .on_event(conn(0, 0), seconds(2), update, &mut out)
                .is_empty()
        );
        assert!(out.is_empty());
    }

    #[test]
    fn a_reset_moves_every_symbol_of_that_socket_to_the_other_replica() {
        let mut collector = synced(&["a", "b", "c"]);
        let mut out = Vec::new();
        collector.on_reset(conn(0, 0), seconds(5), &mut out);
        assert_eq!(
            out,
            vec![
                snapshot("a", at_seconds(1, 1)),
                snapshot("b", at_seconds(1, 1))
            ]
        );
    }

    #[test]
    fn a_takeover_is_stamped_with_the_newest_receive_time_of_the_websocket_taking_over() {
        let mut collector = synced(&["a"]);
        receive_until(&mut collector, conn(0, 1), 7);
        receive_until(&mut collector, conn(0, 0), 10);
        let mut out = Vec::new();
        collector.on_reset(conn(0, 0), seconds(8), &mut out);
        assert_eq!(out, vec![snapshot("a", at_seconds(1, 7))]);
    }

    #[test]
    fn an_update_that_proves_missed_messages_refreshes_that_book() {
        let mut collector = synced(&["a"]);
        let event = update_event("a", Change::Add(-9), None, 2);
        let mut out = Vec::new();
        assert_eq!(
            collector.on_event(conn(0, 1), seconds(3), event, &mut out),
            vec![(conn(0, 1), Command::Refresh(names(&["a"])))]
        );
    }

    #[test]
    fn a_top_of_book_that_turns_out_wrong_refreshes_that_book_right_away() {
        let mut collector = synced(&["a"]);
        let mut out = Vec::new();
        let wrong = Top {
            bid: Some(99),
            ask: None,
        };
        let reported = update_event("a", Change::Set(2), Some(wrong), 2);
        assert!(
            collector
                .on_event(conn(0, 1), seconds(2), reported, &mut out)
                .is_empty()
        );
        let later = update_event("a", Change::Set(3), None, 3);
        assert_eq!(
            collector.on_event(conn(0, 1), seconds(3), later, &mut out),
            vec![(conn(0, 1), Command::Refresh(names(&["a"])))]
        );
    }

    #[test]
    fn an_unreadable_message_drops_that_book_and_the_stuck_check_renews_it() {
        let mut collector = synced(&["a"]);
        let mut out = Vec::new();
        let unreadable = Event::Unreadable { symbol: "a".into() };
        assert!(
            collector
                .on_event(conn(0, 0), seconds(2), unreadable, &mut out)
                .is_empty()
        );
        assert_eq!(
            out,
            vec![snapshot("a", at_seconds(1, 1))],
            "replica 1 takes over"
        );

        // Long past the stuck check's 30 seconds.
        receive_until(&mut collector, conn(0, 0), 100);
        assert_eq!(
            collector.on_tick(seconds(100), &mut out),
            vec![(conn(0, 0), Command::Renew(names(&["a"])))]
        );
    }

    #[test]
    fn every_book_is_written_in_full_on_each_minute() {
        let mut collector = synced(&["a", "b"]);
        let mut out = Vec::new();
        collector.on_tick(seconds(59), &mut out);
        assert!(out.is_empty());

        // Nothing arrived since 1 s: the clock alone says the minute is due.
        collector.on_tick(seconds(60), &mut out);
        assert_eq!(
            out,
            vec![
                snapshot("a", at_seconds(1, 1)),
                snapshot("b", at_seconds(1, 1))
            ]
        );

        out.clear();
        collector.on_tick(seconds(61), &mut out);
        assert!(out.is_empty(), "the next one is due at 120 s");
    }

    #[test]
    fn minute_snapshots_are_stamped_with_their_websocket_newest_receive_time_not_the_clock() {
        let mut collector = synced(&["a"]);
        let mut out = Vec::new();
        collector.on_event(
            conn(0, 0),
            seconds(58),
            book_event("a", &[(40, 1)], 57),
            &mut out,
        );
        collector.on_reset(conn(0, 1), seconds(59), &mut out);
        out.clear();
        collector.on_tick(seconds(60), &mut out);
        assert_eq!(out, vec![snapshot("a", at_seconds(57, 58))]);
    }

    #[test]
    fn a_minute_snapshot_claims_no_later_receive_time_than_a_change_its_book_still_lacks() {
        // a on shard 0 and b on shard 1, every book read at 1 s.
        let mut collector = Collector::new(1, usize::MAX, seconds(0));
        collector.set_live(&symbols(&["a", "b"]), seconds(0), seconds(0));
        let mut out = Vec::new();
        for (shard, symbol) in ["a", "b"].into_iter().enumerate() {
            for replica in 0..REPLICAS {
                let book = book_event(symbol, &[(40, 1)], 1);
                collector.on_event(conn(shard, replica), seconds(1), book, &mut out);
            }
        }
        out.clear();
        // b's change, read at 10 s, is processed before the minute tick, and a's, read at 9 s,
        // after it.
        let b = update_event("b", Change::Set(2), None, 10);
        collector.on_event(conn(1, 0), seconds(10), b, &mut out);
        collector.on_tick(seconds(60), &mut out);
        let a = update_event("a", Change::Set(3), None, 9);
        collector.on_event(conn(0, 0), seconds(9), a, &mut out);
        assert_eq!(receive_times(&out, "a"), [seconds(1), seconds(9)]);
        assert_eq!(receive_times(&out, "b"), [seconds(10), seconds(10)]);
    }

    #[test]
    fn lag_counts_in_receive_time_so_a_backlog_in_the_queue_switches_nothing() {
        let mut collector = synced(&["a"]);
        let mut out = Vec::new();
        let newer = update_event("a", Change::Set(1), None, 2);
        collector.on_event(conn(0, 1), seconds(2), newer, &mut out);
        // The clock says 50 s, but nothing read after 2 s has been processed yet.
        assert!(collector.on_tick(seconds(50), &mut out).is_empty());
        assert!(out.is_empty(), "the source keeps up as far as anyone knows");

        receive_until(&mut collector, conn(0, 0), 4);
        collector.on_tick(seconds(51), &mut out);
        assert_eq!(out, vec![snapshot("a", at_seconds(2, 2))]);
    }

    #[test]
    fn subscriptions_that_never_delivered_a_book_are_renewed_per_socket() {
        let mut collector = Collector::new(2, usize::MAX, seconds(0));
        collector.set_live(&symbols(&["a", "b"]), seconds(0), seconds(0));
        let mut out = Vec::new();
        collector.on_event(
            conn(0, 0),
            seconds(1),
            book_event("a", &[(40, 1)], 1),
            &mut out,
        );
        collector.on_event(
            conn(0, 0),
            seconds(1),
            book_event("b", &[(40, 1)], 1),
            &mut out,
        );

        receive_until(&mut collector, conn(0, 0), 100);
        assert_eq!(
            collector.on_tick(seconds(100), &mut out),
            vec![(conn(0, 1), Command::Renew(names(&["a", "b"])))]
        );
    }

    #[test]
    fn a_long_silence_renews_the_replica_that_is_not_the_source() {
        let mut collector = synced(&["a"]);
        let mut out = Vec::new();
        for second in 2..=4 {
            for replica in 0..REPLICAS {
                let update = update_event("a", Change::Set(second), None, second);
                collector.on_event(conn(0, replica), seconds(second), update, &mut out);
            }
        }
        // Long past the minute this symbol may be silent.
        receive_until(&mut collector, conn(0, 0), 100);
        assert_eq!(
            collector.on_tick(seconds(100), &mut out),
            vec![(conn(0, 1), Command::Renew(names(&["a"])))]
        );
    }
}
