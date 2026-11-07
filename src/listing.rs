//! The listing of the live symbols: complete rounds a minute apart, each page retried, and a
//! re-read of the newest pages every 10 seconds. Both run until `serve` aborts them, and both
//! keep to one `Pace`, which spaces their requests out while the exchange answers 429.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use reqwest::StatusCode;
use tokio::sync::oneshot;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};

use crate::exchange::{Exchange, Symbol};

/// The pause between two complete listings of the live symbols.
const LIST_EVERY: Duration = Duration::from_secs(60);

/// How often the newest pages of the listing, where new symbols show up, are read again between
/// complete listings.
const NEWEST_EVERY: Duration = Duration::from_secs(10);

/// How many times a page of the listing is retried before its round fails, and the pause before
/// the first retry, which doubles with each one.
const PAGE_RETRIES: u32 = 3;
const RETRY_AFTER: Duration = Duration::from_secs(1);

/// The gap between the starts of two listing requests after the exchange's first 429 Too Many
/// Requests: short enough that a lone 429 costs little, and seven doublings reach `MAX_GAP`.
const FIRST_GAP: Duration = Duration::from_millis(100);

/// The longest the gap grows: the listing still asks for a page every 10 seconds, the re-read's
/// own period, so it sees soon enough when the exchange takes requests again.
const MAX_GAP: Duration = Duration::from_secs(10);

/// Each request that goes through takes `1 / GAP_SHRINK` of the gap off, so 11 in a row halve
/// it: the gap creeps back towards the exchange's limit instead of jumping past it.
const GAP_SHRINK: u32 = 16;

/// Lists the live symbols over and over, `LIST_EVERY` apart. Every page goes to `pages` as soon
/// as it arrives, and every complete listing to `listings`, with the time its round started. A
/// listing that fails halfway is never sent whole, so only a complete listing can unsubscribe a
/// symbol.
pub async fn rounds<E: Exchange>(
    exchange: Arc<E>,
    pace: Arc<Pace>,
    pages: Sender<Vec<Symbol>>,
    listings: Sender<(DateTime<Utc>, BTreeSet<Symbol>)>,
) {
    loop {
        let started = Utc::now();
        match list_once(&*exchange, &pace, &pages).await {
            Ok(live) => {
                let _ = listings.send((started, live));
            }
            Err(error) => eprintln!("listing live symbols: {error:#}"),
        }
        sleep(LIST_EVERY).await;
    }
}

async fn list_once<E: Exchange>(
    exchange: &E,
    pace: &Pace,
    pages: &Sender<Vec<Symbol>>,
) -> anyhow::Result<BTreeSet<Symbol>> {
    let mut live = BTreeSet::new();
    let mut cursor = None;
    loop {
        let (page, next) = live_page_with_retries(exchange, pace, cursor).await?;
        live.extend(page.iter().cloned());
        let _ = pages.send(page);
        match next {
            Some(next) => cursor = Some(next),
            None => return Ok(live),
        }
    }
}

/// One page of the listing, retried up to `PAGE_RETRIES` times.
async fn live_page_with_retries<E: Exchange>(
    exchange: &E,
    pace: &Pace,
    cursor: Option<String>,
) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
    let mut pause = RETRY_AFTER;
    for _ in 0..PAGE_RETRIES {
        match pace.page(exchange, cursor.clone()).await {
            Ok(page) => return Ok(page),
            Err(error) => eprintln!("a page of the listing failed, retrying: {error:#}"),
        }
        sleep(pause).await;
        pause *= 2;
    }
    pace.page(exchange, cursor).await
}

/// Reads the listing again from its first page every `NEWEST_EVERY`, since new symbols show up
/// there. Each page goes to the collector with a channel to answer whether the page had symbols
/// it didn't, and only a yes reads on, so more new symbols than a page holds still come within
/// seconds.
pub async fn newest<E: Exchange>(
    exchange: Arc<E>,
    pace: Arc<Pace>,
    pages: Sender<(Vec<Symbol>, oneshot::Sender<bool>)>,
) {
    let mut every = interval(NEWEST_EVERY);
    every.set_missed_tick_behavior(MissedTickBehavior::Delay);
    every.tick().await;
    loop {
        every.tick().await;
        let mut cursor = None;
        loop {
            let (page, next) = match pace.page(&*exchange, cursor).await {
                Ok(page) => page,
                Err(error) => {
                    eprintln!("reading the newest symbols: {error:#}");
                    break;
                }
            };
            let (had_new_tx, had_new) = oneshot::channel();
            let _ = pages.send((page, had_new_tx));
            match (had_new.await, next) {
                (Ok(true), Some(next)) => cursor = Some(next),
                _ => break,
            }
        }
    }
}

/// How far apart the listing's requests to one exchange start, shared by the rounds and the
/// re-read. The gap stays at zero until the exchange answers 429 Too Many Requests. Each 429
/// doubles it, from `FIRST_GAP` up to `MAX_GAP`, and each request that goes through takes a
/// sixteenth off, so it settles near the exchange's limit without knowing what that is.
#[derive(Default)]
pub struct Pace(Mutex<Gap>);

#[derive(Default)]
struct Gap {
    length: Duration,
    /// When the latest request started, or is due to start.
    last: Option<Instant>,
}

impl Pace {
    /// One page of `exchange`'s listing, asked for once the gap since the latest request has
    /// passed. On a 429 the gap grows, and the same page is asked for again after it, as many
    /// times as it takes, so a 429 never counts as a failure.
    async fn page<E: Exchange>(
        &self,
        exchange: &E,
        cursor: Option<String>,
    ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
        loop {
            let wait = self.0.lock().unwrap().turn();
            if !wait.is_zero() {
                sleep(wait).await;
            }
            match exchange.live_page(cursor.clone()).await {
                Ok(page) => {
                    self.0.lock().unwrap().went_through();
                    return Ok(page);
                }
                Err(error) if too_many_requests(&error) => {
                    let gap = self.0.lock().unwrap().too_many();
                    eprintln!("the listing's requests now start {gap:?} apart: {error:#}");
                }
                Err(error) => return Err(error),
            }
        }
    }
}

impl Gap {
    /// Takes the next request's turn, one gap after the latest, and returns how long it waits for
    /// it.
    fn turn(&mut self) -> Duration {
        let now = Instant::now();
        let start = self.last.map_or(now, |last| (last + self.length).max(now));
        self.last = Some(start);
        start - now
    }

    /// The exchange answered 429: doubles the gap, from `FIRST_GAP` up to `MAX_GAP`, and returns
    /// it.
    fn too_many(&mut self) -> Duration {
        self.length = (self.length * 2).clamp(FIRST_GAP, MAX_GAP);
        self.length
    }

    /// A request went through: takes a sixteenth off the gap.
    fn went_through(&mut self) {
        self.length -= self.length / GAP_SHRINK;
    }
}

/// Whether the exchange answered 429 Too Many Requests, which the listing's HTTP request turns
/// into an error.
fn too_many_requests(error: &anyhow::Error) -> bool {
    let status = error
        .downcast_ref::<reqwest::Error>()
        .and_then(reqwest::Error::status);
    status == Some(StatusCode::TOO_MANY_REQUESTS)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use anyhow::Context as _;
    use crossbeam::channel::{Receiver, TryRecvError, unbounded};
    use tokio_tungstenite::tungstenite::handshake::client::Request;

    use super::*;
    use crate::exchange::polymarket::PolymarketCodec;
    use crate::exchange::tests::http_server;

    /// A listing of two pages, "a b" then "c", whose calls fail as `fails` says, in order; any
    /// call past the list succeeds.
    struct Pages {
        fails: Mutex<Vec<bool>>,
        calls: Mutex<Vec<Option<String>>>,
        /// When each call was made.
        called_at: Mutex<Vec<DateTime<Utc>>>,
    }

    impl Pages {
        fn failing(fails: &[bool]) -> Self {
            Self {
                fails: Mutex::new(fails.iter().rev().copied().collect()),
                calls: Mutex::new(Vec::new()),
                called_at: Mutex::new(Vec::new()),
            }
        }
    }

    impl Exchange for Pages {
        type Codec = PolymarketCodec;

        const SYMBOLS_PER_SOCKET: usize = 10;

        async fn live_page(
            &self,
            cursor: Option<String>,
        ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
            self.calls.lock().unwrap().push(cursor.clone());
            self.called_at.lock().unwrap().push(Utc::now());
            if self.fails.lock().unwrap().pop().unwrap_or(false) {
                anyhow::bail!("HTTP 502");
            }
            Ok(match cursor {
                None => (vec!["a".into(), "b".into()], Some("2".into())),
                Some(_) => (vec!["c".into()], None),
            })
        }

        fn request(&self) -> anyhow::Result<Request> {
            anyhow::bail!("no websocket")
        }

        fn codec(&self) -> PolymarketCodec {
            PolymarketCodec::default()
        }
    }

    fn page(symbols: &[&str]) -> Vec<Symbol> {
        symbols.iter().map(|symbol| symbol.to_string()).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_page_is_retried_before_the_listing_gives_up_on_it() {
        let exchange = Pages::failing(&[false, true, true, true]);
        let (pages_tx, pages) = unbounded();
        let live = list_once(&exchange, &Pace::default(), &pages_tx)
            .await
            .unwrap();
        assert_eq!(live, BTreeSet::from(["a".into(), "b".into(), "c".into()]));
        assert_eq!(
            pages.try_iter().collect::<Vec<_>>(),
            [page(&["a", "b"]), page(&["c"])]
        );
        assert_eq!(exchange.calls.lock().unwrap().len(), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn a_listing_whose_second_page_fails_sends_the_first_page_and_no_complete_listing() {
        let exchange = Arc::new(Pages::failing(&[false, true, true, true, true]));
        let (pages_tx, pages) = unbounded();
        let (listings_tx, listings) = unbounded();
        let round = rounds(exchange.clone(), Arc::default(), pages_tx, listings_tx);
        let _ = tokio::time::timeout(LIST_EVERY / 2, round).await;
        assert_eq!(pages.try_iter().collect::<Vec<_>>(), [page(&["a", "b"])]);
        assert!(listings.try_recv().is_err(), "nothing gets unsubscribed");
        let calls = exchange.calls.lock().unwrap();
        assert_eq!(calls.len(), 1 + 1 + PAGE_RETRIES as usize);
    }

    #[tokio::test(start_paused = true)]
    async fn a_complete_listing_carries_the_time_its_round_started_before_its_first_page() {
        let exchange = Arc::new(Pages::failing(&[]));
        let (pages_tx, _pages) = unbounded();
        let (listings_tx, listings) = unbounded();
        let before = Utc::now();
        let round = rounds(exchange.clone(), Arc::default(), pages_tx, listings_tx);
        let _ = tokio::time::timeout(LIST_EVERY / 2, round).await;
        let (started, live) = listings.try_recv().unwrap();
        assert_eq!(live, BTreeSet::from(["a".into(), "b".into(), "c".into()]));
        let first_page = exchange.called_at.lock().unwrap()[0];
        assert!(before <= started && started <= first_page);
    }

    /// A listing of two pages served over HTTP by `http_server`, whose bodies list the page's
    /// symbols: "/1", which has a next page, and "/2".
    struct Served {
        http: reqwest::Client,
        address: SocketAddr,
        /// When each request started.
        started: Mutex<Vec<Instant>>,
    }

    impl Served {
        fn at(address: SocketAddr) -> Self {
            Self {
                http: reqwest::Client::new(),
                address,
                started: Mutex::new(Vec::new()),
            }
        }
    }

    impl Exchange for Served {
        type Codec = PolymarketCodec;

        const SYMBOLS_PER_SOCKET: usize = 10;

        async fn live_page(
            &self,
            cursor: Option<String>,
        ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
            self.started.lock().unwrap().push(Instant::now());
            let next = cursor.is_none().then(|| "2".to_owned());
            let page = cursor.unwrap_or_else(|| "1".to_owned());
            let url = format!("http://{}/{page}", self.address);
            let response = self.http.get(url).send().await;
            let response = response.and_then(reqwest::Response::error_for_status);
            let body = response.context("listing over HTTP")?.text().await?;
            Ok((body.split(' ').map(str::to_owned).collect(), next))
        }

        fn request(&self) -> anyhow::Result<Request> {
            anyhow::bail!("no websocket")
        }

        fn codec(&self) -> PolymarketCodec {
            PolymarketCodec::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_429_slows_the_listing_down_and_asks_again_without_spending_a_retry() {
        // More 429s on the first page than it has retries, then both pages go through.
        let mut responses = vec![("429 Too Many Requests", String::new()); 4];
        responses.extend([("200 OK", "a b".to_owned()), ("200 OK", "c".to_owned())]);
        let (address, served) = http_server(responses).await;
        let exchange = Served::at(address);
        let (pages_tx, pages) = unbounded();
        let before = Instant::now();
        let live = list_once(&exchange, &Pace::default(), &pages_tx)
            .await
            .unwrap();

        assert_eq!(live, BTreeSet::from(["a".into(), "b".into(), "c".into()]));
        assert_eq!(
            pages.try_iter().collect::<Vec<_>>(),
            [page(&["a", "b"]), page(&["c"])]
        );
        assert_eq!(served.await.unwrap(), ["/1", "/1", "/1", "/1", "/1", "/2"]);
        let started = exchange.started.lock().unwrap();
        assert_eq!(started[0], before, "no gap before the first 429");
        let gaps: Vec<u128> = started
            .windows(2)
            .map(|pair| (pair[1] - pair[0]).as_millis())
            .collect();
        assert_eq!(
            gaps,
            [100, 200, 400, 800, 750],
            "each 429 doubles the gap, and the page that went through took a sixteenth off"
        );
    }

    #[test]
    fn the_gap_doubles_with_each_429_up_to_ten_seconds_and_eleven_requests_through_halve_it() {
        let mut gap = Gap::default();
        let doubled: Vec<u128> = (0..9).map(|_| gap.too_many().as_millis()).collect();
        assert_eq!(
            doubled,
            [100, 200, 400, 800, 1_600, 3_200, 6_400, 10_000, 10_000]
        );
        for _ in 0..10 {
            gap.went_through();
        }
        assert!(gap.length > MAX_GAP / 2, "{:?}", gap.length);
        gap.went_through();
        assert!(gap.length < MAX_GAP / 2, "{:?}", gap.length);
    }

    #[tokio::test(start_paused = true)]
    async fn requests_start_one_gap_apart_whichever_task_asks() {
        let mut gap = Gap::default();
        assert_eq!(
            gap.turn(),
            Duration::ZERO,
            "an exchange that never answers 429 never waits"
        );
        gap.too_many();
        // A round and the re-read asking at the same moment.
        assert_eq!(gap.turn(), FIRST_GAP);
        assert_eq!(gap.turn(), FIRST_GAP * 2);
    }

    /// Plays the collector for `newest`: takes each page it sends, answers whether the page had
    /// new symbols with the next of `had_new`, and returns the pages. Fails once `newest` has
    /// stopped without sending the page it waits for.
    async fn answer(
        pages: Receiver<(Vec<Symbol>, oneshot::Sender<bool>)>,
        had_new: &[bool],
    ) -> Vec<Vec<Symbol>> {
        let mut read = Vec::new();
        for &answer in had_new {
            let (page, had_new) = loop {
                match pages.try_recv() {
                    Ok(page) => break page,
                    Err(TryRecvError::Empty) => sleep(Duration::from_millis(100)).await,
                    Err(TryRecvError::Disconnected) => {
                        panic!("the re-read stopped after sending {} pages", read.len())
                    }
                }
            };
            read.push(page);
            had_new.send(answer).unwrap();
        }
        read
    }

    #[tokio::test(start_paused = true)]
    async fn every_ten_seconds_the_newest_pages_are_read_for_as_long_as_they_bring_new_symbols() {
        let exchange = Arc::new(Pages::failing(&[true]));
        let (pages_tx, pages) = unbounded();
        let rereads = newest(exchange.clone(), Arc::default(), pages_tx);
        let rereads = tokio::time::timeout(NEWEST_EVERY * 3 + NEWEST_EVERY / 2, rereads);
        let (_, read) = tokio::join!(rereads, answer(pages, &[true, true, false]));
        assert_eq!(read, [page(&["a", "b"]), page(&["c"]), page(&["a", "b"])]);
        assert_eq!(
            *exchange.calls.lock().unwrap(),
            [None, None, Some("2".into()), None],
            "the first read failed, the second went on to the last page, and the third stopped at \
             a page without new symbols"
        );
    }
}
