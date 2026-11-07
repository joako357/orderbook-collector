//! Kalshi: its open markets, the signed websocket handshake, and the `orderbook_delta` channel.

use anyhow::{Context, anyhow, ensure};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use chrono::Utc;
use ed25519_dalek::Signer as _;
use ed25519_dalek::pkcs8::DecodePrivateKey as _;
use rand_core::OsRng;
use reqwest::Url;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey as _;
use rsa::pss::BlindedSigningKey;
use rsa::signature::{RandomizedSigner as _, SignatureEncoding as _};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;

use crate::Side;
use crate::book::{Book, Change};
use crate::exchange::{Codec, Event, Exchange, Symbol, Update, get, http_client, levels, time};
use crate::fixed;

const REST_PATH: &str = "/trade-api/v2";

/// The websocket path, which is also what every handshake signs, whatever the host.
const WEBSOCKET_PATH: &str = "/trade-api/ws/v2";

const CHANNEL: &str = "orderbook_delta";

/// The error code Kalshi sends when a subscription's outbound buffer overflows.
const BUFFER_OVERFLOW: u64 = 25;

/// One Kalshi environment, with the API key that signs every websocket handshake.
pub struct Kalshi {
    rest: String,
    websocket: String,
    key_id: String,
    key: Key,
    http: reqwest::Client,
}

impl Kalshi {
    /// Production. The key is the private key Kalshi generated, in PEM.
    pub fn production(key_id: String, private_key_pem: &str) -> anyhow::Result<Self> {
        Self::new(
            "external-api.kalshi.com",
            "external-api-ws.kalshi.com",
            key_id,
            private_key_pem,
        )
    }

    /// The demo environment, whose keys are separate from production's.
    pub fn demo(key_id: String, private_key_pem: &str) -> anyhow::Result<Self> {
        Self::new(
            "external-api.demo.kalshi.co",
            "external-api-ws.demo.kalshi.co",
            key_id,
            private_key_pem,
        )
    }

    fn new(
        rest_host: &str,
        websocket_host: &str,
        key_id: String,
        private_key_pem: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            rest: format!("https://{rest_host}{REST_PATH}"),
            websocket: format!("wss://{websocket_host}{WEBSOCKET_PATH}"),
            key_id,
            key: Key::from_pem(private_key_pem)?,
            http: http_client()?,
        })
    }

    /// The request for a page of open markets that are not multivariate (MVE), a thousand per
    /// page: the first page for `None`, otherwise the page at `cursor`.
    fn page_url(&self, cursor: Option<&str>) -> anyhow::Result<Url> {
        let mut url = Url::parse(&format!("{}/markets", self.rest))?;
        url.query_pairs_mut()
            .append_pair("status", "open")
            .append_pair("mve_filter", "exclude")
            .append_pair("limit", "1000");
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        Ok(url)
    }
}

impl Exchange for Kalshi {
    type Codec = KalshiCodec;

    /// With 2 replicas, 2,000 per websocket fit 200,000 markets into the 200 websockets Kalshi
    /// allows. Past that, shards grow beyond 2,000, toward Kalshi's unpublished cap on markets
    /// per subscription (error 26).
    const SYMBOLS_PER_SOCKET: usize = 2000;

    /// Kalshi allows 200 websockets per user below its Premier tier.
    const MAX_SOCKETS: usize = 200;

    /// Open markets that are not multivariate (MVE), a thousand per page, newest first.
    async fn live_page(
        &self,
        cursor: Option<String>,
    ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
        let url = self.page_url(cursor.as_deref())?;
        let body = get(&self.http, url)
            .await
            .context("listing Kalshi markets")?;
        page_symbols(&body)
    }

    fn request(&self) -> anyhow::Result<Request> {
        let timestamp_ms = Utc::now().timestamp_millis();
        let mut request = self.websocket.as_str().into_client_request()?;
        let headers = request.headers_mut();
        headers.insert("kalshi-access-key", HeaderValue::from_str(&self.key_id)?);
        headers.insert("kalshi-access-timestamp", HeaderValue::from(timestamp_ms));
        let signature = signature(&self.key, timestamp_ms, "GET", WEBSOCKET_PATH);
        headers.insert(
            "kalshi-access-signature",
            HeaderValue::from_str(&signature)?,
        );
        Ok(request)
    }

    fn codec(&self) -> KalshiCodec {
        KalshiCodec::default()
    }
}

#[derive(Deserialize)]
struct MarketPage {
    markets: Vec<Market>,
    /// Empty on the last page.
    cursor: String,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
}

/// The tickers on one page of `GET /markets`, and the cursor of the next page if there is one.
fn page_symbols(body: &str) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
    let page: MarketPage = serde_json::from_str(body).context("unreadable Kalshi market page")?;
    let symbols = page
        .markets
        .into_iter()
        .map(|market| market.ticker)
        .collect();
    let next = Some(page.cursor).filter(|cursor| !cursor.is_empty());
    Ok((symbols, next))
}

/// The private key of a Kalshi API key. Kalshi's API Keys page generates Ed25519 keys by default
/// and RSA keys on request.
enum Key {
    Ed25519(ed25519_dalek::SigningKey),
    Rsa(BlindedSigningKey<Sha256>),
}

impl Key {
    /// Reads an Ed25519 key in PKCS#8 PEM, or an RSA key in PKCS#1 or PKCS#8 PEM.
    fn from_pem(pem: &str) -> anyhow::Result<Self> {
        if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(pem) {
            return Ok(Self::Ed25519(key));
        }
        let rsa = RsaPrivateKey::from_pkcs1_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(pem))
            .map_err(|_| {
                anyhow!("the Kalshi key must be an Ed25519 or an RSA private key in PEM")
            })?;
        Ok(Self::Rsa(BlindedSigningKey::new(rsa)))
    }

    /// Signs `message`: Ed25519, or RSA-PSS with SHA-256 and a salt as long as the digest.
    fn sign(&self, message: &[u8]) -> Vec<u8> {
        match self {
            Self::Ed25519(key) => key.sign(message).to_bytes().to_vec(),
            Self::Rsa(key) => key.sign_with_rng(&mut OsRng, message).to_vec(),
        }
    }
}

/// Standard base64 of the signature over `{timestamp_ms}{method}{path}`.
fn signature(key: &Key, timestamp_ms: i64, method: &str, path: &str) -> String {
    let text = format!("{timestamp_ms}{method}{path}");
    STANDARD.encode(key.sign(text.as_bytes()))
}

/// One Kalshi session. All of its markets share one `orderbook_delta` subscription, whose `seq`
/// must have no gaps.
#[derive(Default)]
pub struct KalshiCodec {
    last_id: u64,
    /// The subscription's id, known once Kalshi confirms it.
    sid: Option<u64>,
    /// The `seq` of the subscription's last message.
    seq: Option<u64>,
}

impl KalshiCodec {
    fn command(&mut self, cmd: &str, params: Value) -> String {
        self.last_id += 1;
        json!({ "id": self.last_id, "cmd": cmd, "params": params }).to_string()
    }

    /// An `update_subscription` with `action` on these markets. Before Kalshi confirms the
    /// subscription there is no id to name, so it sends nothing.
    fn update_subscription(&mut self, action: &str, symbols: &[Symbol]) -> Vec<String> {
        let Some(sid) = self.sid else {
            return Vec::new();
        };
        if symbols.is_empty() {
            return Vec::new();
        }
        let params = json!({
            "sids": [sid],
            "market_tickers": symbols,
            "action": action,
        });
        vec![self.command("update_subscription", params)]
    }

    /// Checks that a message of the subscription follows the previous one. The first message
    /// may have any `seq`.
    fn follow(&mut self, sid: u64, seq: u64) -> anyhow::Result<()> {
        if let Some(known) = self.sid {
            ensure!(
                sid == known,
                "message for subscription {sid}, but this session's is {known}"
            );
        }
        self.sid = Some(sid);
        if let Some(last) = self.seq {
            ensure!(
                seq == last + 1,
                "sequence gap on subscription {sid}: {last} then {seq}"
            );
        }
        self.seq = Some(seq);
        Ok(())
    }
}

impl Codec for KalshiCodec {
    /// A repeated subscribe adds the markets to the session's one subscription. `use_yes_price`
    /// prices NO levels as the YES asks they are, the only pricing Kalshi will keep supporting.
    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        let params = json!({
            "channels": [CHANNEL],
            "market_tickers": symbols,
            "use_yes_price": true,
        });
        vec![self.command("subscribe", params)]
    }

    /// Before Kalshi confirms the subscription this sends nothing: those markets keep streaming
    /// until the session restarts, and the collector ignores symbols it no longer carries.
    fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        self.update_subscription("delete_markets", symbols)
    }

    /// Asks for a fresh snapshot of each market without changing the subscription
    /// (`get_snapshot`). Before Kalshi confirms the subscription this sends nothing: the
    /// session's own subscribe is already bringing the books, and Kalshi sends no second
    /// snapshot of a market already subscribed.
    fn refresh(&mut self, symbols: &[Symbol]) -> Vec<String> {
        self.update_subscription("get_snapshot", symbols)
    }

    /// Every message of the subscription is checked against its sequence before its values are
    /// read, so that a value that can't be read never hides a gap.
    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Event>> {
        let message: Message = serde_json::from_str(frame)
            .with_context(|| format!("unreadable Kalshi frame {frame:?}"))?;
        match message {
            Message::Subscribed { msg } => {
                if msg.channel == CHANNEL && self.sid != Some(msg.sid) {
                    self.sid = Some(msg.sid);
                    self.seq = None;
                }
                Ok(Vec::new())
            }
            Message::OrderbookSnapshot {
                sid,
                seq,
                sending_ts_ms,
                msg,
            } => {
                self.follow(sid, seq)?;
                let symbol = ticker(&msg)?;
                let book = snapshot(msg, sending_ts_ms);
                Ok(vec![book.unwrap_or(Event::Unreadable { symbol })])
            }
            Message::OrderbookDelta {
                sid,
                seq,
                sending_ts_ms,
                msg,
            } => {
                self.follow(sid, seq)?;
                let symbol = ticker(&msg)?;
                let update = delta(msg, sending_ts_ms);
                Ok(vec![update.unwrap_or(Event::Unreadable { symbol })])
            }
            // Kalshi's docs give `ok` and `unsubscribed` replies the subscription's `seq`, and its
            // demo environment showed `ok` replies taking the next number in it. So they count in
            // the sequence like any other message of the subscription.
            Message::Ok {
                sid: Some(sid),
                seq: Some(seq),
            }
            | Message::Unsubscribed {
                sid,
                seq: Some(seq),
            } => {
                self.follow(sid, seq)?;
                Ok(Vec::new())
            }
            // The subscription's buffer on Kalshi's side overflowed, so messages were lost and
            // no book of this session can be trusted.
            Message::Error { msg } if msg["code"] == BUFFER_OVERFLOW => {
                anyhow::bail!("Kalshi error {msg}")
            }
            // Any other error rejects one command. A rejected subscription means that market
            // never gets a book, and the core renews subscriptions that deliver no book within
            // 30 s. Failing the session here would only restart it with the same command.
            Message::Error { msg } => {
                eprintln!("Kalshi error {msg}");
                Ok(Vec::new())
            }
            Message::Ok { .. } | Message::Unsubscribed { .. } | Message::Other => Ok(Vec::new()),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Message {
    Subscribed {
        msg: Subscribed,
    },
    OrderbookSnapshot {
        sid: u64,
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: Value,
    },
    OrderbookDelta {
        sid: u64,
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: Value,
    },
    Ok {
        sid: Option<u64>,
        seq: Option<u64>,
    },
    Unsubscribed {
        sid: u64,
        seq: Option<u64>,
    },
    Error {
        #[serde(default)]
        msg: Value,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct Subscribed {
    channel: String,
    sid: u64,
}

/// Kalshi books only hold bids: YES bids, and NO bids, which `use_yes_price` prices as the YES
/// asks they are. Each level is `[price, count]`, and a side with no levels is left out.
#[derive(Deserialize)]
struct Snapshot {
    market_ticker: String,
    #[serde(default)]
    yes_dollars_fp: Vec<(String, String)>,
    #[serde(default)]
    no_dollars_fp: Vec<(String, String)>,
}

#[derive(Deserialize)]
struct Delta {
    market_ticker: String,
    price_dollars: String,
    delta_fp: String,
    side: MarketSide,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum MarketSide {
    Yes,
    No,
}

/// The market a snapshot or a delta is about.
fn ticker(msg: &Value) -> anyhow::Result<Symbol> {
    msg["market_ticker"]
        .as_str()
        .map(str::to_owned)
        .context("a Kalshi book message without market_ticker")
}

/// The book of an `orderbook_snapshot`, as of when Kalshi sent it.
fn snapshot(msg: Value, sending_ts_ms: Option<i64>) -> anyhow::Result<Event> {
    let snapshot: Snapshot = serde_json::from_value(msg)?;
    let sent = sending_ts_ms.context("orderbook_snapshot without sending_ts_ms")?;
    Ok(Event::Book {
        symbol: snapshot.market_ticker,
        exchange_time: time(sent)?,
        book: Book::new(
            levels(snapshot.yes_dollars_fp)?,
            levels(snapshot.no_dollars_fp)?,
        ),
    })
}

/// The level change of an `orderbook_delta`, as of the change itself when Kalshi says when.
fn delta(msg: Value, sending_ts_ms: Option<i64>) -> anyhow::Result<Event> {
    let delta: Delta = serde_json::from_value(msg)?;
    let at = delta
        .ts_ms
        .or(sending_ts_ms)
        .context("orderbook_delta without ts_ms or sending_ts_ms")?;
    Ok(Event::Update(Update {
        exchange_time: time(at)?,
        side: match delta.side {
            MarketSide::Yes => Side::Buy,
            MarketSide::No => Side::Sell,
        },
        price: fixed::parse(&delta.price_dollars)?,
        change: Change::Add(fixed::parse(&delta.delta_fp)?),
        top: None,
        symbol: delta.market_ticker,
    }))
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ed25519_dalek::Verifier as _;
    use rsa::pkcs1::{EncodeRsaPrivateKey as _, LineEnding};

    use super::*;
    use crate::book::Level;
    use crate::exchange::tests::http_server;

    const SNAPSHOT: &str = include_str!("../../tests/fixtures/kalshi/orderbook_snapshot.json");
    const DELTA: &str = include_str!("../../tests/fixtures/kalshi/orderbook_delta.json");
    const SUBSCRIBED: &str = include_str!("../../tests/fixtures/kalshi/subscribed.json");
    const ERROR: &str = include_str!("../../tests/fixtures/kalshi/error.json");
    const OK: &str = include_str!("../../tests/fixtures/kalshi/ok.json");
    const MARKETS_PAGE: &str = include_str!("../../tests/fixtures/kalshi/markets_page.json");
    const MARKETS_LAST_PAGE: &str =
        include_str!("../../tests/fixtures/kalshi/markets_last_page.json");

    /// A throwaway Ed25519 key from a fixed seed, in the PKCS#8 PEM Kalshi hands out.
    fn ed25519_pem() -> String {
        use ed25519_dalek::pkcs8::EncodePrivateKey as _;
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let pem = key.to_pkcs8_pem(Default::default());
        pem.unwrap().to_string()
    }

    /// A throwaway RSA key in PKCS#8 PEM, made once for the tests that need one.
    fn rsa_pem() -> &'static str {
        use rsa::pkcs8::EncodePrivateKey as _;
        static PEM: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        PEM.get_or_init(|| {
            let key = RsaPrivateKey::new(&mut OsRng, 1024).unwrap();
            key.to_pkcs8_pem(LineEnding::LF).unwrap().to_string()
        })
    }

    const TICKER: &str = "FED-23DEC-T3.00";
    /// The query of every listing request, before its cursor.
    const QUERY: &str = "status=open&mve_filter=exclude&limit=1000";
    const SIGNED: &[u8] = b"1700000000000GET/trade-api/ws/v2";

    /// `n` milliseconds after the epoch, as Kalshi's timestamps count.
    fn ms(n: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(n).unwrap()
    }

    fn level(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    fn symbols(tickers: &[&str]) -> Vec<Symbol> {
        tickers.iter().map(|ticker| ticker.to_string()).collect()
    }

    /// `fixture` with `edit` applied to its JSON.
    fn edited(fixture: &str, edit: impl FnOnce(&mut Value)) -> String {
        let mut value: Value = serde_json::from_str(fixture).unwrap();
        edit(&mut value);
        value.to_string()
    }

    fn json(frame: &str) -> Value {
        serde_json::from_str(frame).unwrap()
    }

    fn update(side: Side, price: i64, delta: i64, exchange_ms: i64) -> Event {
        Event::Update(Update {
            symbol: TICKER.into(),
            exchange_time: ms(exchange_ms),
            side,
            price,
            change: Change::Add(delta),
            top: None,
        })
    }

    fn unreadable() -> Event {
        Event::Unreadable {
            symbol: TICKER.into(),
        }
    }

    /// The signature `key` makes over the fixed handshake text, decoded from base64.
    fn signed_by(key: &str) -> Vec<u8> {
        let key = Key::from_pem(key).unwrap();
        STANDARD
            .decode(signature(
                &key,
                1_700_000_000_000,
                "GET",
                "/trade-api/ws/v2",
            ))
            .unwrap()
    }

    #[test]
    fn subscribe_adds_markets_to_the_orderbook_channel_with_increasing_ids() {
        let mut codec = KalshiCodec::default();
        let first = codec.subscribe(&symbols(&["A", "B"]));
        let second = codec.subscribe(&symbols(&["C"]));
        assert_eq!(first.len(), 1);
        assert_eq!(
            json(&first[0]),
            json!({
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": ["A", "B"],
                    "use_yes_price": true,
                },
            })
        );
        assert_eq!(
            json(&second[0]),
            json!({
                "id": 2,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": ["C"],
                    "use_yes_price": true,
                },
            })
        );
    }

    #[test]
    fn unsubscribe_deletes_markets_from_the_confirmed_subscription() {
        let mut codec = KalshiCodec::default();
        codec.subscribe(&symbols(&["A", "B"]));
        assert_eq!(codec.decode(SUBSCRIBED).unwrap(), vec![]);
        let frames = codec.unsubscribe(&symbols(&["B"]));
        assert_eq!(frames.len(), 1);
        assert_eq!(
            json(&frames[0]),
            json!({
                "id": 2,
                "cmd": "update_subscription",
                "params": { "sids": [1], "market_tickers": ["B"], "action": "delete_markets" },
            })
        );
    }

    #[test]
    fn unsubscribe_before_the_subscription_is_confirmed_sends_nothing() {
        let mut codec = KalshiCodec::default();
        codec.subscribe(&symbols(&["A"]));
        assert!(codec.unsubscribe(&symbols(&["A"])).is_empty());
    }

    #[test]
    fn a_refresh_asks_for_fresh_snapshots_without_changing_the_subscription() {
        let mut codec = KalshiCodec::default();
        codec.subscribe(&symbols(&["A", "B"]));
        codec.decode(SUBSCRIBED).unwrap();
        let frames = codec.refresh(&symbols(&["B"]));
        assert_eq!(frames.len(), 1);
        assert_eq!(
            json(&frames[0]),
            json!({
                "id": 2,
                "cmd": "update_subscription",
                "params": { "sids": [1], "market_tickers": ["B"], "action": "get_snapshot" },
            })
        );
        assert!(codec.refresh(&[]).is_empty());
    }

    #[test]
    fn a_refresh_before_the_subscription_is_confirmed_sends_nothing() {
        let mut codec = KalshiCodec::default();
        codec.subscribe(&symbols(&["A"]));
        assert!(
            codec.refresh(&symbols(&["A"])).is_empty(),
            "the subscribe is already bringing the book"
        );
    }

    #[test]
    fn a_renewal_deletes_the_markets_from_the_subscription_and_subscribes_them_again() {
        let mut codec = KalshiCodec::default();
        codec.subscribe(&symbols(&["A", "B"]));
        codec.decode(SUBSCRIBED).unwrap();
        let frames: Vec<Value> = codec
            .renew(&symbols(&["B"]))
            .iter()
            .map(|f| json(f))
            .collect();
        assert_eq!(
            frames,
            [
                json!({
                    "id": 2,
                    "cmd": "update_subscription",
                    "params": { "sids": [1], "market_tickers": ["B"], "action": "delete_markets" },
                }),
                json!({
                    "id": 3,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": ["B"],
                        "use_yes_price": true,
                    },
                }),
            ]
        );
    }

    #[test]
    fn subscribing_or_unsubscribing_no_markets_sends_nothing() {
        let mut codec = KalshiCodec::default();
        assert!(codec.subscribe(&[]).is_empty());
        codec.decode(SUBSCRIBED).unwrap();
        assert!(codec.unsubscribe(&[]).is_empty());
        let first = codec.subscribe(&symbols(&["A"]));
        assert_eq!(json(&first[0])["id"], 1, "nothing took an id before");
    }

    #[test]
    fn each_session_gets_a_codec_whose_ids_and_sequence_start_over() {
        let kalshi = Kalshi::demo("key-id".into(), &ed25519_pem()).unwrap();
        let mut first = kalshi.codec();
        first.subscribe(&symbols(&["A"]));
        first.decode(SNAPSHOT).unwrap();
        let mut second = kalshi.codec();
        assert_eq!(json(&second.subscribe(&symbols(&["A"]))[0])["id"], 1);
        let later = edited(SNAPSHOT, |value| value["seq"] = json!(9));
        assert_eq!(
            second.decode(&later).unwrap().len(),
            1,
            "any seq can come first"
        );
    }

    #[test]
    fn kalshi_needs_no_keepalive_frame() {
        assert_eq!(KalshiCodec::default().keepalive(), None);
    }

    #[test]
    fn a_snapshot_is_a_book_with_yes_bids_as_bids_and_no_bids_as_asks() {
        let mut codec = KalshiCodec::default();
        assert_eq!(
            codec.decode(SNAPSHOT).unwrap(),
            vec![Event::Book {
                symbol: TICKER.into(),
                exchange_time: ms(1_669_149_841_234),
                book: Book::new(
                    [level(80_000, 300_000_000), level(220_000, 333_000_000)],
                    [level(540_000, 20_000_000), level(560_000, 146_000_000)],
                ),
            }]
        );
    }

    #[test]
    fn a_snapshot_without_a_side_has_that_side_empty() {
        let no_asks = edited(SNAPSHOT, |value| {
            value["msg"]
                .as_object_mut()
                .unwrap()
                .remove("no_dollars_fp");
        });
        assert_eq!(
            KalshiCodec::default().decode(&no_asks).unwrap(),
            vec![Event::Book {
                symbol: TICKER.into(),
                exchange_time: ms(1_669_149_841_234),
                book: Book::new(
                    [level(80_000, 300_000_000), level(220_000, 333_000_000)],
                    []
                ),
            }]
        );
        let empty = edited(SNAPSHOT, |value| {
            let msg = value["msg"].as_object_mut().unwrap();
            msg.remove("yes_dollars_fp");
            msg.remove("no_dollars_fp");
        });
        assert_eq!(
            KalshiCodec::default().decode(&empty).unwrap(),
            vec![Event::Book {
                symbol: TICKER.into(),
                exchange_time: ms(1_669_149_841_234),
                book: Book::default(),
            }]
        );
    }

    #[test]
    fn a_snapshot_with_an_unreadable_value_is_unreadable_for_its_market() {
        let untimed = edited(SNAPSHOT, |value| {
            value.as_object_mut().unwrap().remove("sending_ts_ms");
        });
        let too_precise = edited(SNAPSHOT, |value| {
            value["msg"]["yes_dollars_fp"][0][0] = json!("0.0800001");
        });
        let negative = edited(SNAPSHOT, |value| {
            value["msg"]["yes_dollars_fp"][0][1] = json!("-3.00");
        });
        for frame in [untimed, too_precise, negative] {
            assert_eq!(
                KalshiCodec::default().decode(&frame).unwrap(),
                vec![unreadable()]
            );
        }
    }

    #[test]
    fn a_time_before_1970_is_unreadable_for_its_market_since_the_output_cannot_hold_it() {
        let old_snapshot = edited(SNAPSHOT, |value| value["sending_ts_ms"] = json!(-1));
        assert_eq!(
            KalshiCodec::default().decode(&old_snapshot).unwrap(),
            vec![unreadable()]
        );
        let mut codec = KalshiCodec::default();
        codec.decode(SNAPSHOT).unwrap();
        let old_delta = edited(DELTA, |value| value["msg"]["ts_ms"] = json!(-1));
        assert_eq!(codec.decode(&old_delta).unwrap(), vec![unreadable()]);
    }

    #[test]
    fn a_yes_delta_changes_a_bid_at_its_change_time() {
        let mut codec = KalshiCodec::default();
        codec.decode(SNAPSHOT).unwrap();
        assert_eq!(
            codec.decode(DELTA).unwrap(),
            vec![update(Side::Buy, 960_000, -54_000_000, 1_669_149_841_000)]
        );
    }

    #[test]
    fn a_no_delta_changes_the_ask_at_its_yes_price() {
        let no = edited(DELTA, |value| value["msg"]["side"] = json!("no"));
        assert_eq!(
            KalshiCodec::default().decode(&no).unwrap(),
            vec![update(Side::Sell, 960_000, -54_000_000, 1_669_149_841_000)]
        );
    }

    #[test]
    fn a_delta_without_ts_ms_takes_the_sending_time() {
        let untimed = edited(DELTA, |value| {
            value["msg"].as_object_mut().unwrap().remove("ts_ms");
        });
        assert_eq!(
            KalshiCodec::default().decode(&untimed).unwrap(),
            vec![update(Side::Buy, 960_000, -54_000_000, 1_669_149_841_123)]
        );
    }

    #[test]
    fn a_delta_with_an_unreadable_value_is_unreadable_for_its_market_and_keeps_the_sequence() {
        let breakages: [fn(&mut Value); 5] = [
            |value| {
                value["msg"]
                    .as_object_mut()
                    .unwrap()
                    .remove("price_dollars");
            },
            |value| value["msg"]["price_dollars"] = json!(0.96),
            |value| value["msg"]["price_dollars"] = json!("0.9600001"),
            |value| value["msg"]["side"] = json!("maybe"),
            |value| {
                value["msg"].as_object_mut().unwrap().remove("ts_ms");
                value.as_object_mut().unwrap().remove("sending_ts_ms");
            },
        ];
        for breakage in breakages {
            let mut codec = KalshiCodec::default();
            codec.decode(SNAPSHOT).unwrap();
            assert_eq!(
                codec.decode(&edited(DELTA, breakage)).unwrap(),
                vec![unreadable()]
            );
            let next = edited(DELTA, |value| value["seq"] = json!(4));
            assert_eq!(codec.decode(&next).unwrap().len(), 1, "seq 3 was counted");
        }
    }

    #[test]
    fn an_unreadable_value_does_not_hide_a_sequence_gap() {
        let mut codec = KalshiCodec::default();
        codec.decode(SNAPSHOT).unwrap();
        let skipped = edited(DELTA, |value| {
            value["seq"] = json!(4);
            value["msg"]["side"] = json!("maybe");
        });
        assert!(codec.decode(&skipped).is_err());
    }

    #[test]
    fn a_frame_that_names_no_market_or_is_not_json_is_an_error() {
        let anonymous = edited(DELTA, |value| {
            value["msg"]
                .as_object_mut()
                .unwrap()
                .remove("market_ticker");
        });
        let unsequenced = edited(DELTA, |value| {
            value.as_object_mut().unwrap().remove("seq");
        });
        for frame in [anonymous, unsequenced, "not json".to_owned()] {
            assert!(
                KalshiCodec::default().decode(&frame).is_err(),
                "{frame} was accepted"
            );
        }
    }

    #[test]
    fn a_sequence_gap_is_an_error_that_names_it() {
        let mut codec = KalshiCodec::default();
        codec.decode(SNAPSHOT).unwrap();
        let skipped = edited(DELTA, |value| value["seq"] = json!(4));
        let error = codec.decode(&skipped).unwrap_err();
        assert!(format!("{error:#}").contains("2 then 4"), "{error:#}");
    }

    #[test]
    fn a_message_for_another_subscription_is_an_error() {
        let mut codec = KalshiCodec::default();
        codec.decode(SUBSCRIBED).unwrap();
        assert!(codec.decode(SNAPSHOT).is_err());
    }

    #[test]
    fn a_new_subscription_id_restarts_the_sequence() {
        let mut codec = KalshiCodec::default();
        codec.decode(SUBSCRIBED).unwrap();
        let first = edited(DELTA, |value| value["sid"] = json!(1));
        codec.decode(&first).unwrap();
        let renewed = edited(SUBSCRIBED, |value| value["msg"]["sid"] = json!(2));
        codec.decode(&renewed).unwrap();
        let later = edited(DELTA, |value| value["seq"] = json!(9));
        assert_eq!(codec.decode(&later).unwrap().len(), 1);
    }

    #[test]
    fn ok_and_unsubscribed_replies_on_the_subscription_count_in_its_sequence() {
        let mut codec = KalshiCodec::default();
        codec.decode(SNAPSHOT).unwrap();
        let ok = edited(OK, |value| {
            value["sid"] = json!(2);
            value["seq"] = json!(3);
        });
        assert_eq!(codec.decode(&ok).unwrap(), vec![]);
        let unsubscribed = json!({ "type": "unsubscribed", "sid": 2, "seq": 4 });
        assert_eq!(codec.decode(&unsubscribed.to_string()).unwrap(), vec![]);
        let after_both = edited(DELTA, |value| value["seq"] = json!(5));
        assert_eq!(codec.decode(&after_both).unwrap().len(), 1);
    }

    #[test]
    fn a_buffer_overflow_ends_the_session_since_it_lost_messages() {
        let overflow = edited(ERROR, |value| {
            value["msg"] = json!({ "code": 25, "msg": "Subscription buffer overflow" });
        });
        let error = KalshiCodec::default().decode(&overflow).unwrap_err();
        assert!(
            format!("{error:#}").contains("buffer overflow"),
            "{error:#}"
        );
    }

    #[test]
    fn error_ok_and_other_replies_decode_to_no_events() {
        let mut codec = KalshiCodec::default();
        assert_eq!(codec.decode(ERROR).unwrap(), vec![]);
        assert_eq!(codec.decode(OK).unwrap(), vec![]);
        assert_eq!(
            codec
                .decode(r#"{"type":"ticker","sid":9,"msg":{}}"#)
                .unwrap(),
            vec![]
        );
    }

    #[test]
    fn an_ed25519_key_signs_timestamp_method_and_path_in_base64() {
        let key = ed25519_dalek::SigningKey::from_pkcs8_pem(&ed25519_pem()).unwrap();
        let bytes: [u8; 64] = signed_by(&ed25519_pem()).try_into().unwrap();
        let signature = ed25519_dalek::Signature::from_bytes(&bytes);
        key.verifying_key().verify(SIGNED, &signature).unwrap();
    }

    #[test]
    fn an_rsa_key_signs_with_pss_sha256_and_a_digest_long_salt() {
        let private = RsaPrivateKey::from_pkcs8_pem(rsa_pem()).unwrap();
        let verifying = rsa::pss::VerifyingKey::<Sha256>::new(private.to_public_key());
        let signature = rsa::pss::Signature::try_from(signed_by(rsa_pem()).as_slice()).unwrap();
        verifying.verify(SIGNED, &signature).unwrap();
    }

    #[test]
    fn an_rsa_key_reads_from_pkcs1_pem_as_well() {
        let private = RsaPrivateKey::from_pkcs8_pem(rsa_pem()).unwrap();
        let pkcs1 = private.to_pkcs1_pem(LineEnding::LF).unwrap();
        assert!(pkcs1.starts_with("-----BEGIN RSA PRIVATE KEY-----"));
        assert!(matches!(Key::from_pem(&pkcs1), Ok(Key::Rsa(_))));
    }

    #[test]
    fn a_key_that_is_neither_ed25519_nor_rsa_is_rejected() {
        let Err(error) = Kalshi::demo("key-id".into(), "not a key") else {
            panic!("a bad key should be rejected");
        };
        let error = format!("{error:#}");
        assert!(
            error.contains("Ed25519") && error.contains("RSA"),
            "{error}"
        );
    }

    #[test]
    fn the_handshake_carries_the_key_id_a_fresh_timestamp_and_its_signature() {
        let kalshi = Kalshi::demo("key-id".into(), &ed25519_pem()).unwrap();
        let before = Utc::now().timestamp_millis();
        let request = kalshi.request().unwrap();
        let after = Utc::now().timestamp_millis();
        assert_eq!(
            request.uri(),
            "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
        );
        let header = |name: &str| request.headers()[name].to_str().unwrap().to_string();
        assert_eq!(header("KALSHI-ACCESS-KEY"), "key-id");
        let timestamp: i64 = header("KALSHI-ACCESS-TIMESTAMP").parse().unwrap();
        assert!((before..=after).contains(&timestamp));
        assert_eq!(
            header("KALSHI-ACCESS-SIGNATURE"),
            signature(&kalshi.key, timestamp, "GET", "/trade-api/ws/v2")
        );
    }

    #[test]
    fn production_connects_to_the_production_hosts() {
        let kalshi = Kalshi::production("key-id".into(), &ed25519_pem()).unwrap();
        assert_eq!(kalshi.rest, "https://external-api.kalshi.com/trade-api/v2");
        assert_eq!(
            kalshi.request().unwrap().uri(),
            "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
        );
    }

    #[test]
    fn the_listing_asks_for_open_markets_and_leaves_out_mve_ones_as_the_challenge_requires() {
        let kalshi = Kalshi::production("key-id".into(), &ed25519_pem()).unwrap();
        let markets = "https://external-api.kalshi.com/trade-api/v2/markets";
        assert_eq!(
            kalshi.page_url(None).unwrap().as_str(),
            format!("{markets}?{QUERY}")
        );
        assert_eq!(
            kalshi.page_url(Some("CgwIubSz1wYQ4I")).unwrap().as_str(),
            format!("{markets}?{QUERY}&cursor=CgwIubSz1wYQ4I")
        );
    }

    /// Kalshi, with its listing served by the HTTP server at `address`.
    fn listing_at(address: std::net::SocketAddr) -> Kalshi {
        let mut kalshi = Kalshi::demo("key-id".into(), &ed25519_pem()).unwrap();
        kalshi.rest = format!("http://{address}{REST_PATH}");
        kalshi
    }

    #[tokio::test]
    async fn the_listing_reads_a_page_with_a_next_cursor_and_the_last_page_over_http() {
        let pages = vec![
            ("200 OK", MARKETS_PAGE.to_owned()),
            ("200 OK", MARKETS_LAST_PAGE.to_owned()),
        ];
        let (address, served) = http_server(pages).await;
        let kalshi = listing_at(address);

        let (symbols, next) = kalshi.live_page(None).await.unwrap();
        assert_eq!(symbols.len(), 3);
        let next = next.expect("the first page has a next one");
        let (symbols, last) = kalshi.live_page(Some(next.clone())).await.unwrap();
        assert_eq!((symbols.len(), last), (6, None));
        assert_eq!(
            served.await.unwrap(),
            [
                format!("{REST_PATH}/markets?{QUERY}"),
                format!("{REST_PATH}/markets?{QUERY}&cursor={next}"),
            ]
        );
    }

    #[tokio::test]
    async fn a_listing_request_that_fails_with_an_http_error_is_an_error() {
        let (address, _) = http_server(vec![("429 Too Many Requests", String::new())]).await;
        let error = listing_at(address).live_page(None).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("429 Too Many Requests"),
            "{error:#}"
        );
    }

    #[test]
    fn a_market_page_lists_its_tickers_and_the_next_cursor() {
        let (symbols, next) = page_symbols(MARKETS_PAGE).unwrap();
        assert_eq!(
            symbols,
            [
                "KXUELTEAMTOTAL-26NOV05SPABOU-BOU5",
                "KXUECLSPREAD-26NOV05FCMAJA-AJA6",
                "KXUECLTEAMTOTAL-26NOV05FCMAJA-AJA7",
            ]
        );
        assert_eq!(
            next.as_deref(),
            Some("CgwIubSz1wYQ4I_bjgISIktYVUVDTFRFQU1UT1RBTC0yNk5PVjA1RkNNQUpBLUFKQTc")
        );
    }

    #[test]
    fn the_last_market_page_has_no_next_cursor() {
        let (symbols, next) = page_symbols(MARKETS_LAST_PAGE).unwrap();
        assert_eq!(symbols.len(), 6);
        assert_eq!(next, None);
    }

    #[test]
    fn a_market_page_without_a_cursor_is_an_error() {
        let cursorless = edited(MARKETS_PAGE, |value| {
            value.as_object_mut().unwrap().remove("cursor");
        });
        assert!(page_symbols(&cursorless).is_err());
        assert!(page_symbols("<html>rate limited</html>").is_err());
    }
}
