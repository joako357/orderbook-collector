//! The whole service against websockets on localhost, from the listing to the output file.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::Duration;

use chrono::{DateTime, TimeDelta};
use engineering_challenge::Side;
use engineering_challenge::book::{Book, Change};
use engineering_challenge::exchange::kalshi::KalshiCodec;
use engineering_challenge::exchange::polymarket::PolymarketCodec;
use engineering_challenge::exchange::{Codec, Event, Exchange, Symbol, Update};
use engineering_challenge::output::Output;
use engineering_challenge::service::serve;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tempfile::NamedTempFile;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

/// Few, so that the eight Polymarket tokens need two shards.
const PER_SOCKET: usize = 5;

/// An exchange on localhost that lists `pages` and speaks a new `codec()` in every session.
struct Local<C> {
    url: String,
    pages: Vec<Vec<Symbol>>,
    codec: fn() -> C,
}

impl<C: Codec> Exchange for Local<C> {
    type Codec = C;

    const SYMBOLS_PER_SOCKET: usize = PER_SOCKET;

    async fn live_page(
        &self,
        cursor: Option<String>,
    ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
        let page = cursor.map_or(0, |cursor| cursor.parse().unwrap());
        let next = (page + 1 < self.pages.len()).then(|| (page + 1).to_string());
        Ok((self.pages[page].clone(), next))
    }

    fn request(&self) -> anyhow::Result<Request> {
        Ok(self.url.as_str().into_client_request()?)
    }

    fn codec(&self) -> C {
        (self.codec)()
    }
}

/// Subscribes in Polymarket's form and reads frames about the one symbol "a": "book" is an empty
/// book, and a number `n` is `n` new bid levels, so that one frame queues far more events than
/// the collector applies in a moment. "ancient" is one more level from the last millisecond of
/// 1969, which the template's encoder can't write.
struct Scripted;

impl Codec for Scripted {
    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        vec![serde_json::json!({ "assets_ids": symbols }).to_string()]
    }

    fn unsubscribe(&mut self, _symbols: &[Symbol]) -> Vec<String> {
        Vec::new()
    }

    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Event>> {
        let level = |price, exchange_time| {
            Event::Update(Update {
                symbol: "a".into(),
                exchange_time,
                side: Side::Buy,
                price,
                change: Change::Set(1),
                top: None,
            })
        };
        let epoch = DateTime::UNIX_EPOCH;
        Ok(match frame {
            "book" => vec![Event::Book {
                symbol: "a".into(),
                exchange_time: epoch,
                book: Book::default(),
            }],
            "ancient" => vec![level(0, epoch - TimeDelta::milliseconds(1))],
            levels => (1..=levels.parse()?)
                .map(|price| level(price, epoch))
                .collect(),
        })
    }
}

/// The listing of the one symbol the scripted sessions are about.
fn only_a() -> Vec<Vec<Symbol>> {
    vec![vec!["a".into()]]
}

/// 112 frames recorded from Polymarket's market channel: the initial dump of eight tokens, then
/// their price changes, two books sent after a trade, and a trade.
const SESSION: &str = include_str!("fixtures/polymarket/session.jsonl");

/// The books the Polymarket session leaves.
fn session_books() -> BTreeMap<Symbol, Book> {
    books(PolymarketCodec::default(), SESSION.lines())
}

/// The books `frames` leave, by decoding them with `codec` and applying their events directly.
fn books<S: AsRef<str>>(
    mut codec: impl Codec,
    frames: impl IntoIterator<Item = S>,
) -> BTreeMap<Symbol, Book> {
    let mut books = BTreeMap::new();
    for frame in frames {
        for event in codec.decode(frame.as_ref()).unwrap() {
            match event {
                Event::Book { symbol, book, .. } => {
                    books.insert(symbol, book);
                }
                Event::Update(Update {
                    symbol,
                    side,
                    price,
                    change,
                    ..
                }) => {
                    let book: &mut Book = books.get_mut(&symbol).unwrap();
                    book.apply(side, price, change).unwrap();
                }
                Event::Unreadable { symbol } => panic!("every frame reads cleanly, {symbol} too"),
            }
        }
    }
    books
}

/// The symbols in two pages, so that the listing goes through its paging.
fn two_pages(symbols: &BTreeSet<Symbol>) -> Vec<Vec<Symbol>> {
    let symbols: Vec<Symbol> = symbols.iter().cloned().collect();
    let (first, second) = symbols.split_at(symbols.len() / 2);
    vec![first.to_vec(), second.to_vec()]
}

/// The symbols of each shard. The collector fills shards in the order the listing gives the
/// symbols, `PER_SOCKET` each.
fn shards(symbols: &BTreeSet<Symbol>) -> Vec<BTreeSet<Symbol>> {
    let listed: Vec<&Symbol> = symbols.iter().collect();
    listed
        .chunks(PER_SOCKET)
        .map(|shard| shard.iter().map(|symbol| symbol.to_string()).collect())
        .collect()
}

/// The session's frames that concern `assets`, with the initial dump cut down to them.
fn frames_for(assets: &BTreeSet<Symbol>) -> Vec<String> {
    let concerns = |value: &Value| value.as_str().is_some_and(|asset| assets.contains(asset));
    SESSION
        .lines()
        .filter_map(|frame| match serde_json::from_str(frame).unwrap() {
            Value::Array(books) => {
                let dump: Vec<Value> = books
                    .into_iter()
                    .filter(|book| concerns(&book["asset_id"]))
                    .collect();
                (!dump.is_empty()).then(|| Value::Array(dump).to_string())
            }
            event => {
                let changes = event["price_changes"].as_array();
                let mine = concerns(&event["asset_id"])
                    || changes
                        .is_some_and(|changes| changes.iter().any(|c| concerns(&c["asset_id"])));
                mine.then(|| frame.to_owned())
            }
        })
        .collect()
}

/// One of the service's websockets as the exchange sees it, with the symbols it subscribed.
struct Replica {
    symbols: BTreeSet<Symbol>,
    socket: WebSocketStream<TcpStream>,
    /// Text frames the service sent while `send` waited for a pong, oldest first.
    heard: VecDeque<String>,
}

impl Replica {
    /// Takes the service's next websocket and reads its subscriptions, in Polymarket's form or
    /// Kalshi's, until they add up to one of `shards`. The service subscribes each page of its
    /// listing as it arrives, so a shard can take more than one.
    async fn accept(listener: &TcpListener, shards: &[BTreeSet<Symbol>]) -> Self {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let mut symbols = BTreeSet::new();
        while !shards.contains(&symbols) {
            let Some(Ok(Message::Text(subscription))) = socket.next().await else {
                panic!("the service should subscribe");
            };
            let subscription: Value = serde_json::from_str(&subscription).unwrap();
            let ids = subscription["assets_ids"].as_array();
            let ids = ids.or(subscription["params"]["market_tickers"].as_array());
            symbols.extend(
                ids.unwrap()
                    .iter()
                    .map(|id| id.as_str().unwrap().to_owned()),
            );
        }
        Self {
            symbols,
            socket,
            heard: VecDeque::new(),
        }
    }

    /// Sends the Polymarket session's frames for this replica's symbols, and waits until the
    /// collector has their events.
    async fn replay(&mut self) {
        self.send(frames_for(&self.symbols)).await;
    }

    /// Sends `frames`, then a ping, and waits for the pong. The service's socket answers it after
    /// reading every frame before it, so by then the collector has all of their events.
    async fn send(&mut self, frames: impl IntoIterator<Item = String>) {
        for frame in frames {
            self.socket.send(Message::text(frame)).await.unwrap();
        }
        self.socket
            .send(Message::Ping(Default::default()))
            .await
            .unwrap();
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Pong(_))) => return,
                Some(Ok(Message::Text(text))) if text != "PING" => {
                    self.heard.push_back(text.to_string());
                }
                Some(Ok(_)) => {}
                other => panic!("expected a pong, got {other:?}"),
            }
        }
    }

    /// The next text frame the service sends, Polymarket's keepalive aside.
    async fn next_heard(&mut self) -> String {
        if let Some(text) = self.heard.pop_front() {
            return text;
        }
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Text(text))) if text != "PING" => return text.to_string(),
                Some(Ok(_)) => {}
                other => panic!("expected a text frame, got {other:?}"),
            }
        }
    }

    /// Reads until the service hangs up, and returns whether it sent a Close frame.
    async fn closed(mut self) -> bool {
        let mut closed = false;
        while let Some(Ok(message)) = self.socket.next().await {
            closed |= matches!(message, Message::Close(_));
        }
        closed
    }
}

/// Runs the service on a `Local` exchange that lists `pages` and speaks `codec()`, against the
/// websockets `play` plays. Once `play` says on its channel that it played everything, interrupts
/// the service. Returns what the service returned, what `play` returned once every websocket
/// ended, and the output file. A `play` that panics fails the test with its own message, once the
/// service has stopped.
fn run<C: Codec, T, F>(
    pages: Vec<Vec<Symbol>>,
    codec: fn() -> C,
    play: impl FnOnce(TcpListener, mpsc::Sender<()>) -> F,
) -> (anyhow::Result<()>, T, NamedTempFile)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let runtime = Runtime::new().unwrap();
    let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (played_tx, played) = mpsc::channel();
    let played_out = runtime.spawn(play(listener, played_tx));
    let output = NamedTempFile::new().unwrap();
    let exchange = Local { url, pages, codec };
    let (interrupt, interrupted) = crossbeam::channel::bounded(1);
    let (served, played) = std::thread::scope(|scope| {
        let service = scope.spawn(|| serve(exchange, &runtime, output.path(), interrupted));
        let played = played.recv_timeout(Duration::from_secs(30));
        // Whatever `play` did, the service has to stop, or the scope waits for it forever. It may
        // have stopped on its own already.
        let _ = interrupt.send(());
        (service.join().unwrap(), played)
    });
    assert_ne!(
        played,
        Err(RecvTimeoutError::Timeout),
        "the exchange didn't play everything within 30 seconds"
    );
    // A `play` that panicked, before saying it played or after, ended its task with the panic.
    let ended = runtime
        .block_on(played_out)
        .unwrap_or_else(|ended| panic::resume_unwind(ended.into_panic()));
    (served, ended, output)
}

/// The books the output file rebuilds when replayed.
fn replayed(output: &NamedTempFile) -> BTreeMap<Symbol, Book> {
    let mut books = BTreeMap::new();
    Output::read_each(output.path(), |message| message.replay(&mut books)).unwrap();
    books
}

/// Asserts that once each of `took_over` and `stayed` has had its first snapshot, the file has
/// one snapshot more of each of `took_over` than of `stayed`, whose source didn't change. From
/// then on minute snapshots reach every one of them alike, so that one is the takeover's. Each of
/// them has to be the symbol's book in `books`.
fn assert_took_over(
    output: &NamedTempFile,
    took_over: &BTreeSet<Symbol>,
    stayed: &Symbol,
    books: &BTreeMap<Symbol, Book>,
) {
    let mut snapshots: Vec<(Symbol, Book)> = Vec::new();
    Output::read_each(output.path(), |message| {
        if let Output::Snapshot(_) = &message {
            let mut book = BTreeMap::new();
            message.replay(&mut book)?;
            snapshots.extend(book);
        }
        Ok(())
    })
    .unwrap();
    let mut seen = BTreeSet::new();
    let all_seen = snapshots
        .iter()
        .position(|(symbol, _)| {
            if took_over.contains(symbol) || symbol == stayed {
                seen.insert(symbol);
            }
            seen.len() == took_over.len() + 1
        })
        .expect("each symbol has a snapshot");
    let mut since: BTreeMap<&Symbol, Vec<&Book>> = BTreeMap::new();
    for (symbol, book) in &snapshots[all_seen + 1..] {
        since.entry(symbol).or_default().push(book);
    }
    let minutes = since.get(stayed).map_or(0, Vec::len);
    for symbol in took_over {
        let expected = vec![&books[symbol]; minutes + 1];
        assert_eq!(since.get(symbol), Some(&expected), "{symbol}");
    }
}

#[test]
fn a_replayed_session_comes_out_as_the_same_books_and_an_interrupt_closes_everything() {
    let books = session_books();
    let symbols: BTreeSet<Symbol> = books.keys().cloned().collect();
    assert_eq!(symbols.len(), 8);
    let shards = shards(&symbols);

    let pages = two_pages(&symbols);
    let (served, ended, output) = run(
        pages,
        PolymarketCodec::default,
        |listener, played| async move {
            let mut replicas = Vec::new();
            for _ in 0..2 * shards.len() {
                replicas.push(Replica::accept(&listener, &shards).await);
            }
            for replica in &mut replicas {
                replica.replay().await;
            }
            played.send(()).unwrap();
            let mut ended = Vec::new();
            for replica in replicas {
                ended.push((replica.symbols.clone(), replica.closed().await));
            }
            ended
        },
    );

    served.unwrap();
    assert!(
        ended.iter().all(|(_, closed)| *closed),
        "every websocket got a Close frame"
    );
    let mut subscribed: Vec<BTreeSet<Symbol>> =
        ended.into_iter().map(|(assets, _)| assets).collect();
    subscribed.sort();
    subscribed.dedup();
    assert_eq!(
        subscribed.len(),
        2,
        "both replicas of a shard carry the same symbols"
    );
    assert_eq!(replayed(&output), books);
}

#[test]
fn when_a_replica_drops_its_partner_takes_over_with_a_snapshot_and_the_books_come_out_the_same() {
    let books = session_books();
    let symbols: BTreeSet<Symbol> = books.keys().cloned().collect();
    let shards = shards(&symbols);
    let dropping = shards[0].clone();
    let stayed = shards[1].first().unwrap().clone();

    let shard = dropping.clone();
    let pages = two_pages(&symbols);
    let (served, ended, output) = run(pages, PolymarketCodec::default, |listener, played| {
        async move {
            let mut replicas = Vec::new();
            for _ in 0..2 * shards.len() {
                replicas.push(Replica::accept(&listener, &shards).await);
            }
            // The replicas of the shard that drops go first, the one that drops before its
            // partner, so that it is the source of their symbols.
            replicas.sort_by_key(|replica| replica.symbols != shard);
            for replica in &mut replicas {
                replica.replay().await;
            }
            drop(replicas.remove(0));

            let mut back = Replica::accept(&listener, &shards).await;
            assert_eq!(
                back.symbols, shard,
                "the socket comes back with its symbols"
            );
            back.replay().await;
            replicas.push(back);
            played.send(()).unwrap();
            let mut ended = Vec::new();
            for replica in replicas {
                ended.push(replica.closed().await);
            }
            ended
        }
    });

    served.unwrap();
    assert_eq!(ended, [true; 4], "every websocket left got a Close frame");
    assert_eq!(replayed(&output), books);
    // After the drop, the file has the partner's book of every symbol of the shard.
    assert_took_over(&output, &dropping, &stayed, &books);
}

const POLYMARKET_BOOK: &str = include_str!("fixtures/polymarket/book.json");
const POLYMARKET_PRICE_CHANGE: &str = include_str!("fixtures/polymarket/price_change.json");

/// The token of the fixture's book.
fn polymarket_token() -> Symbol {
    fixture(POLYMARKET_BOOK)["asset_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The fixture's book, moved to `timestamp` with these levels, as `price` and `size`, and sent
/// the way Polymarket answers a subscription.
fn polymarket_book(bids: &[(&str, &str)], asks: &[(&str, &str)], timestamp: i64) -> String {
    let levels = |levels: &[(&str, &str)]| {
        let levels = levels
            .iter()
            .map(|(price, size)| json!({ "price": price, "size": size }));
        Value::Array(levels.collect())
    };
    let mut book = fixture(POLYMARKET_BOOK);
    book["bids"] = levels(bids);
    book["asks"] = levels(asks);
    book["timestamp"] = timestamp.to_string().into();
    json!([book]).to_string()
}

/// The fixture's price change, moved to the fixture book's token at `timestamp`: `size` on
/// `side` at `price`, after which the exchange reports the best bid and ask `top`.
fn polymarket_change(
    side: &str,
    price: &str,
    size: &str,
    top: [&str; 2],
    timestamp: i64,
) -> String {
    let mut message = fixture(POLYMARKET_PRICE_CHANGE);
    let mut change = message["price_changes"][0].clone();
    change["asset_id"] = polymarket_token().into();
    change["side"] = side.into();
    change["price"] = price.into();
    change["size"] = size.into();
    change["best_bid"] = top[0].into();
    change["best_ask"] = top[1].into();
    message["price_changes"] = json!([change]);
    message["timestamp"] = timestamp.to_string().into();
    message.to_string()
}

#[test]
fn a_replica_that_misses_a_change_fails_the_top_check_and_a_refresh_brings_its_book_back() {
    let token = polymarket_token();
    let at = fixture(POLYMARKET_BOOK)["timestamp"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();
    let dump = polymarket_book(&[("0.40", "100")], &[("0.60", "100")], at);
    // The first change moves the best bid. A book that misses it lacks the top the second one
    // reports, which the third one checks.
    let changes = [
        polymarket_change("BUY", "0.45", "50", ["0.45", "0.60"], at + 1),
        polymarket_change("SELL", "0.55", "50", ["0.45", "0.55"], at + 2),
        polymarket_change("BUY", "0.41", "10", ["0.45", "0.55"], at + 3),
    ];
    let later = polymarket_change("BUY", "0.42", "5", ["0.45", "0.55"], at + 5);
    let session: Vec<String> = [dump.clone()]
        .into_iter()
        .chain(changes.iter().cloned())
        .chain([later.clone()])
        .collect();
    let books = books(PolymarketCodec::default(), &session);
    // What Polymarket would answer a new subscription with after the third change.
    let refreshed = polymarket_book(
        &[("0.45", "50"), ("0.41", "10"), ("0.40", "100")],
        &[("0.55", "50"), ("0.60", "100")],
        at + 4,
    );

    let shards = [BTreeSet::from([token.clone()])];
    let pages = vec![vec![token.clone()]];
    let (served, (heard, closed), output) = run(
        pages,
        PolymarketCodec::default,
        |listener, played| async move {
            // This replica never gets the change that moves the bid, and its partner hears
            // nothing at all, so only the refresh can bring the book back.
            let mut replica = Replica::accept(&listener, &shards).await;
            let partner = Replica::accept(&listener, &shards).await;
            let [_, second, third] = changes;
            replica.send([dump, second, third]).await;
            let heard = [replica.next_heard().await, replica.next_heard().await];
            replica.send([refreshed, later]).await;
            played.send(()).unwrap();
            (heard, [replica.closed().await, partner.closed().await])
        },
    );

    served.unwrap();
    let assets = json!([token]);
    assert_eq!(
        heard.map(|frame| serde_json::from_str::<Value>(&frame).unwrap()),
        [
            json!({ "operation": "unsubscribe", "assets_ids": assets }),
            json!({ "operation": "subscribe", "assets_ids": assets }),
        ],
        "the refresh unsubscribes the token and subscribes it again"
    );
    assert_eq!(closed, [true, true]);
    assert_eq!(replayed(&output), books);
}

// Kalshi's messages and a page of its markets, as its docs and its API give them.
const KALSHI_SUBSCRIBED: &str = include_str!("fixtures/kalshi/subscribed.json");
const KALSHI_SNAPSHOT: &str = include_str!("fixtures/kalshi/orderbook_snapshot.json");
const KALSHI_DELTA: &str = include_str!("fixtures/kalshi/orderbook_delta.json");
const KALSHI_OK: &str = include_str!("fixtures/kalshi/ok.json");
const KALSHI_MARKETS: &str = include_str!("fixtures/kalshi/markets_page.json");

fn fixture(json: &str) -> Value {
    serde_json::from_str(json).unwrap()
}

/// The tickers on the fixture's page of markets.
fn kalshi_markets() -> Vec<Symbol> {
    let page = fixture(KALSHI_MARKETS);
    let markets = page["markets"].as_array().unwrap();
    let ticker = |market: &Value| market["ticker"].as_str().unwrap().to_owned();
    markets.iter().map(ticker).collect()
}

/// The fixture's delta, moved to `market`: `delta` contracts on `side` at `price`, `after_ms`
/// after the fixture's snapshot was sent.
fn kalshi_delta(market: &str, side: &str, price: &str, delta: &str, after_ms: i64) -> Value {
    let sent = fixture(KALSHI_SNAPSHOT)["sending_ts_ms"].as_i64().unwrap();
    let mut message = fixture(KALSHI_DELTA);
    let msg = &mut message["msg"];
    msg["market_ticker"] = market.into();
    msg["side"] = side.into();
    msg["price_dollars"] = price.into();
    msg["delta_fp"] = delta.into();
    msg["ts_ms"] = (sent + after_ms).into();
    message
}

/// `message` as number `seq` of the fake exchange's one subscription.
fn in_sequence(mut message: Value, seq: usize) -> String {
    message["sid"] = 1.into();
    message["seq"] = seq.into();
    message.to_string()
}

/// A Kalshi session from the fixtures: the subscription's confirmation, the snapshot of each
/// market, and three rounds of deltas with an `ok` reply after the first, all in sequence.
fn kalshi_session(markets: &[Symbol]) -> Vec<String> {
    let mut subscribed = fixture(KALSHI_SUBSCRIBED);
    subscribed["msg"]["sid"] = 1.into();
    let mut messages = Vec::new();
    for market in markets {
        let mut snapshot = fixture(KALSHI_SNAPSHOT);
        snapshot["msg"]["market_ticker"] = market.as_str().into();
        messages.push(snapshot);
    }
    for (n, market) in markets.iter().enumerate() {
        let more = format!("{}.00", n + 1);
        messages.push(kalshi_delta(market, "yes", "0.0800", &more, 100));
    }
    messages.push(fixture(KALSHI_OK));
    for market in markets {
        messages.push(kalshi_delta(market, "no", "0.5400", "-20.00", 200));
        messages.push(kalshi_delta(market, "yes", "0.1500", "5.00", 300));
    }
    let sequenced = (1..)
        .zip(messages)
        .map(|(seq, message)| in_sequence(message, seq));
    std::iter::once(subscribed.to_string())
        .chain(sequenced)
        .collect()
}

#[test]
fn a_kalshi_session_comes_out_as_its_books_and_a_gap_in_its_sequence_hands_over_the_output() {
    let markets = kalshi_markets();
    let session = kalshi_session(&markets);
    let books = books(KalshiCodec::default(), &session);
    assert_eq!(books.len(), 3);
    let shard: BTreeSet<Symbol> = books.keys().cloned().collect();
    // The first replica's session leaves out the last market, so the partner is its source.
    let (first_markets, last_market) = markets.split_at(2);
    let first_session = kalshi_session(first_markets);
    let first_markets: BTreeSet<Symbol> = first_markets.iter().cloned().collect();
    let last_market = last_market[0].clone();
    // A delta that skips a number of the sequence.
    let skipping = kalshi_delta(&markets[0], "yes", "0.2200", "1.00", 400);
    let skipping = in_sequence(skipping, first_session.len() + 1);

    let shards = [shard.clone()];
    let pages = vec![markets];
    let (served, closed, output) =
        run(pages, KalshiCodec::default, |listener, played| async move {
            let mut first = Replica::accept(&listener, &shards).await;
            let mut second = Replica::accept(&listener, &shards).await;
            // The first replica gets its books before the second, so it is their source.
            first.send(first_session).await;
            second.send(session.clone()).await;

            first.socket.send(Message::text(skipping)).await.unwrap();
            let gap_closed = first.closed().await;
            // The socket comes back, and its new session gets the same messages.
            let mut back = Replica::accept(&listener, &shards).await;
            back.send(session).await;
            played.send(()).unwrap();
            let interrupt_closed = [second.closed().await, back.closed().await];
            [gap_closed, interrupt_closed[0], interrupt_closed[1]]
        });

    served.unwrap();
    assert_eq!(
        closed,
        [false, true, true],
        "the gap ended its session without a Close frame, and the interrupt closed the others"
    );
    assert_eq!(replayed(&output), books);
    // After the gap, the file has the partner's book of every market the first replica had.
    assert_took_over(&output, &first_markets, &last_market, &books);
}

#[test]
fn an_interrupt_still_writes_every_event_the_websockets_read_before_it() {
    let levels = 10_000;
    let (served, closed, output) = run(
        only_a(),
        || Scripted,
        |listener, played| async move {
            let shards = [BTreeSet::from(["a".to_owned()])];
            let mut first = Replica::accept(&listener, &shards).await;
            let second = Replica::accept(&listener, &shards).await;
            // One frame that the socket turns into 10,000 events at once, most of them still on
            // their way to the collector when the interrupt comes.
            first.send(["book".to_owned(), levels.to_string()]).await;
            played.send(()).unwrap();
            [first.closed().await, second.closed().await]
        },
    );
    served.unwrap();
    assert_eq!(closed, [true, true]);
    assert_eq!(replayed(&output)["a"].bids().count(), levels as usize);
}

#[test]
fn a_write_that_fails_stops_the_service_with_its_error_and_still_closes_every_websocket() {
    let (served, closed, output) = run(
        only_a(),
        || Scripted,
        |listener, played| async move {
            let shards = [BTreeSet::from(["a".to_owned()])];
            let mut first = Replica::accept(&listener, &shards).await;
            let second = Replica::accept(&listener, &shards).await;
            for frame in ["book", "ancient"] {
                first.socket.send(Message::text(frame)).await.unwrap();
            }
            // The service stops on its own, before any interrupt.
            let closed = [first.closed().await, second.closed().await];
            played.send(()).unwrap();
            closed
        },
    );
    let error = format!("{:#}", served.unwrap_err());
    assert!(error.contains("exchange_timestamp"), "{error}");
    assert_eq!(closed, [true, true], "every websocket got a Close frame");
    assert_eq!(
        replayed(&output)["a"],
        Book::default(),
        "the book's snapshot stays"
    );
}

#[test]
fn a_write_that_fails_after_the_interrupt_leaves_what_was_written_before_it() {
    let levels = 100_000;
    let (served, closed, output) = run(
        only_a(),
        || Scripted,
        |listener, played| async move {
            let shards = [BTreeSet::from(["a".to_owned()])];
            let mut first = Replica::accept(&listener, &shards).await;
            let second = Replica::accept(&listener, &shards).await;
            // Most of the updates still wait for the collector when the interrupt comes, so the
            // write that fails after them comes as the service drains them.
            let frames = ["book".to_owned(), levels.to_string(), "ancient".to_owned()];
            first.send(frames).await;
            played.send(()).unwrap();
            [first.closed().await, second.closed().await]
        },
    );
    let error = format!("{:#}", served.unwrap_err());
    assert!(error.contains("exchange_timestamp"), "{error}");
    assert_eq!(closed, [true, true]);
    assert_eq!(replayed(&output)["a"].bids().count(), levels);
}
