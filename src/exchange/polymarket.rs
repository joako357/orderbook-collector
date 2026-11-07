//! Polymarket's market channel: every open market's tokens, their books and their level changes.

use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::Deserialize;
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use super::{Codec, Event, Exchange, Symbol, Update, get, http_client, levels, time};
use crate::Side;
use crate::book::{Book, Change, Top};
use crate::fixed;

const MARKETS: &str = "https://gamma-api.polymarket.com/markets/keyset";
/// The most markets the Gamma API returns per page: it answers a larger limit with 100.
const PAGE_SIZE: &str = "100";
const WEBSOCKET: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Polymarket, where every outcome token of a market has its own order book.
pub struct Polymarket {
    http: reqwest::Client,
    /// The Gamma API's listing of markets.
    markets: String,
}

impl Polymarket {
    /// Polymarket's public endpoints, which need no API key.
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self {
            http: http_client()?,
            markets: MARKETS.to_owned(),
        })
    }

    /// The request for a page of open markets, newest first: the first page for `None`,
    /// otherwise the page at `cursor`.
    fn page_url(&self, cursor: Option<&str>) -> anyhow::Result<Url> {
        let mut url = Url::parse(&self.markets)?;
        // The Gamma spec doesn't list `version`, but without it the API lists only v1 markets.
        url.query_pairs_mut()
            .append_pair("closed", "false")
            .append_pair("version", "v1")
            .append_pair("version", "v2")
            .append_pair("order", "id")
            .append_pair("ascending", "false")
            .append_pair("limit", PAGE_SIZE);
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("after_cursor", cursor);
        }
        Ok(url)
    }
}

impl Exchange for Polymarket {
    type Codec = PolymarketCodec;

    /// Polymarket's docs give no limit on the tokens one websocket carries or on how many
    /// websockets a client opens, so I don't know where either one is. It does cut off a
    /// websocket whose client doesn't keep up ("slow consumer"), which argues for a modest share
    /// of the stream on each, and every websocket is one more connection to keep alive. 2,000 is
    /// my pick between the two, and I haven't measured a better one.
    const SYMBOLS_PER_SOCKET: usize = 2000;

    /// The tokens of open markets that have an order book, from the Gamma API, newest market
    /// first. A market whose token ids can't be read is skipped rather than failing its page.
    async fn live_page(
        &self,
        cursor: Option<String>,
    ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
        let url = self.page_url(cursor.as_deref())?;
        let body = get(&self.http, url)
            .await
            .context("listing Polymarket markets")?;
        let (symbols, next, skipped) = page_symbols(&body)?;
        if skipped > 0 {
            eprintln!("Polymarket listing: skipped {skipped} markets with unreadable token ids");
        }
        Ok((symbols, next))
    }

    fn request(&self) -> anyhow::Result<Request> {
        Ok(WEBSOCKET.into_client_request()?)
    }

    fn codec(&self) -> PolymarketCodec {
        PolymarketCodec::default()
    }
}

#[derive(Deserialize)]
struct Page {
    markets: Vec<Value>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Market {
    version: Option<String>,
    enable_order_book: Option<bool>,
    clob_token_ids: Option<String>,
    position_ids: Option<Vec<String>>,
}

impl Market {
    /// The market's order book tokens. Markets that were listed but never deployed have none yet.
    fn tokens(self) -> anyhow::Result<Vec<Symbol>> {
        if self.enable_order_book != Some(true) {
            return Ok(Vec::new());
        }
        if self.version.as_deref() == Some("v2") {
            return Ok(self.position_ids.unwrap_or_default());
        }
        match self.clob_token_ids {
            Some(ids) => Ok(serde_json::from_str(&ids)?),
            None => Ok(Vec::new()),
        }
    }
}

/// The order book tokens on one page of markets, the cursor of the next page if there is one, and
/// how many markets were skipped because their token ids couldn't be read.
fn page_symbols(body: &str) -> anyhow::Result<(Vec<Symbol>, Option<String>, usize)> {
    let page: Page = serde_json::from_str(body).context("unreadable page of Polymarket markets")?;
    let mut symbols = Vec::new();
    let mut skipped = 0;
    for market in page.markets {
        let tokens = serde_json::from_value::<Market>(market)
            .map_err(anyhow::Error::from)
            .and_then(Market::tokens);
        match tokens {
            Ok(tokens) => symbols.extend(tokens),
            Err(_) => skipped += 1,
        }
    }
    let next = page.next_cursor.filter(|cursor| !cursor.is_empty());
    Ok((symbols, next, skipped))
}

/// The wire format of one market-channel session. Only the session's first subscription may use
/// the initial form; later ones must be subscription updates.
#[derive(Default)]
pub struct PolymarketCodec {
    subscribed: bool,
}

impl Codec for PolymarketCodec {
    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        let ids = Value::from(symbols);
        let frame = if self.subscribed {
            format!(r#"{{"operation":"subscribe","assets_ids":{ids}}}"#)
        } else {
            format!(r#"{{"assets_ids":{ids},"type":"market"}}"#)
        };
        self.subscribed = true;
        vec![frame]
    }

    fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        let ids = Value::from(symbols);
        vec![format!(
            r#"{{"operation":"unsubscribe","assets_ids":{ids}}}"#
        )]
    }

    /// Polymarket asks for a `PING` text frame every 10 seconds, and answers each with `PONG`.
    fn keepalive(&self) -> Option<String> {
        Some("PING".to_owned())
    }

    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Event>> {
        if frame == "PONG" {
            return Ok(Vec::new());
        }
        let messages = match serde_json::from_str(frame).context("unreadable Polymarket frame")? {
            Value::Array(messages) => messages,
            message => vec![message],
        };
        let mut events = Vec::new();
        for message in messages {
            decode_message(message, &mut events)?;
        }
        Ok(events)
    }
}

/// Decodes one message of a frame. Each book, and each entry of a price change, is read on its
/// own: one that can't be read becomes `Event::Unreadable` for its token.
fn decode_message(message: Value, events: &mut Vec<Event>) -> anyhow::Result<()> {
    match message["event_type"].as_str() {
        Some("book") => {
            let symbol = asset_id(&message)?;
            let book = serde_json::from_value::<BookMessage>(message)
                .map_err(anyhow::Error::from)
                .and_then(BookMessage::event);
            events.push(book.unwrap_or(Event::Unreadable { symbol }));
        }
        Some("price_change") => {
            let changes = message["price_changes"]
                .as_array()
                .context("a price_change without price_changes")?;
            let exchange_time = message["timestamp"]
                .as_str()
                .and_then(|millis| timestamp(millis).ok());
            for change in changes {
                let symbol = asset_id(change)?;
                let update = exchange_time.and_then(|exchange_time| {
                    PriceChange::deserialize(change)
                        .map_err(anyhow::Error::from)
                        .and_then(|change| change.event(exchange_time))
                        .ok()
                });
                events.push(update.unwrap_or(Event::Unreadable { symbol }));
            }
        }
        // Trades, tick size changes and market lifecycle events, none of which changes a book.
        Some(_) => {}
        None => anyhow::bail!("a Polymarket message without event_type"),
    }
    Ok(())
}

/// The token a message, or an entry of a price change, is about.
fn asset_id(message: &Value) -> anyhow::Result<Symbol> {
    message["asset_id"]
        .as_str()
        .map(str::to_owned)
        .context("a Polymarket message without asset_id")
}

#[derive(Deserialize)]
struct BookMessage {
    asset_id: Symbol,
    timestamp: String,
    bids: Vec<WireLevel>,
    asks: Vec<WireLevel>,
}

#[derive(Deserialize)]
struct WireLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct PriceChange {
    asset_id: Symbol,
    price: String,
    size: String,
    side: WireSide,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum WireSide {
    Buy,
    Sell,
}

impl BookMessage {
    fn event(self) -> anyhow::Result<Event> {
        let sent = |side: Vec<WireLevel>| side.into_iter().map(|level| (level.price, level.size));
        Ok(Event::Book {
            exchange_time: timestamp(&self.timestamp)?,
            book: Book::new(levels(sent(self.bids))?, levels(sent(self.asks))?),
            symbol: self.asset_id,
        })
    }
}

impl PriceChange {
    fn event(self, exchange_time: DateTime<Utc>) -> anyhow::Result<Event> {
        let top = match (&self.best_bid, &self.best_ask) {
            (Some(bid), Some(ask)) => Some(top(bid, ask)?),
            _ => None,
        };
        Ok(Event::Update(Update {
            exchange_time,
            side: match self.side {
                WireSide::Buy => Side::Buy,
                WireSide::Sell => Side::Sell,
            },
            price: fixed::parse(&self.price)?,
            change: Change::Set(fixed::parse(&self.size)?),
            top,
            symbol: self.asset_id,
        }))
    }
}

/// The top of book a price change reports for its token. Polymarket writes an empty bid side as
/// "0" and an empty ask side as "1".
fn top(best_bid: &str, best_ask: &str) -> anyhow::Result<Top> {
    let bid = fixed::parse(best_bid)?;
    let ask = fixed::parse(best_ask)?;
    Ok(Top {
        bid: (bid != 0).then_some(bid),
        ask: (ask != fixed::ONE).then_some(ask),
    })
}

/// A timestamp in milliseconds, which Polymarket sends as a string.
fn timestamp(millis: &str) -> anyhow::Result<DateTime<Utc>> {
    let millis = millis
        .parse()
        .with_context(|| format!("not a timestamp: {millis:?}"))?;
    time(millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::tests::at;
    use crate::exchange::tests::http_server;
    use crate::fixed::ONE;

    const INITIAL_DUMP: &str = include_str!("../../tests/fixtures/polymarket/initial_dump.json");
    const BOOK: &str = include_str!("../../tests/fixtures/polymarket/book.json");
    const PRICE_CHANGE: &str = include_str!("../../tests/fixtures/polymarket/price_change.json");
    const LAST_TRADE_PRICE: &str =
        include_str!("../../tests/fixtures/polymarket/last_trade_price.json");
    const MARKET_PAGE: &str = include_str!("../../tests/fixtures/polymarket/market_page.json");
    const LAST_MARKET_PAGE: &str =
        include_str!("../../tests/fixtures/polymarket/last_market_page.json");

    const NO: &str =
        "42506148661683943541979337517222758216686649928064404489628407344344901012834";
    const YES: &str =
        "100131727415826928119174243102601134161538322812855699360532027178816288290234";

    /// The query of every listing request, before its cursor.
    const QUERY: &str = "closed=false&version=v1&version=v2&order=id&ascending=false&limit=100";

    /// `n` milliseconds after the epoch, as Polymarket's timestamps count.
    fn ms(n: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(n).unwrap()
    }

    /// `units` thousandths of the fixed point, to write exchange decimals exactly.
    fn milli(units: i64) -> i64 {
        units * ONE / 1000
    }

    fn decode(frame: &str) -> anyhow::Result<Vec<Event>> {
        PolymarketCodec::default().decode(frame)
    }

    /// `fixture` with `edit` applied to its JSON.
    fn edited(fixture: &str, edit: impl FnOnce(&mut Value)) -> String {
        let mut value: Value = serde_json::from_str(fixture).unwrap();
        edit(&mut value);
        value.to_string()
    }

    fn unreadable(symbol: &str) -> Event {
        Event::Unreadable {
            symbol: symbol.into(),
        }
    }

    /// The fixture's price change of 77 at 0.5 for `symbol`, with the top it reports.
    fn price_change(symbol: &str, side: Side, top: Option<Top>) -> Event {
        Event::Update(Update {
            symbol: symbol.into(),
            exchange_time: ms(1793901640046),
            side,
            price: milli(500),
            change: Change::Set(77 * ONE),
            top,
        })
    }

    fn top_at(bid: i64, ask: i64) -> Option<Top> {
        Some(Top {
            bid: Some(milli(bid)),
            ask: Some(milli(ask)),
        })
    }

    #[test]
    fn the_initial_dump_is_one_book_per_token() {
        assert_eq!(
            decode(INITIAL_DUMP).unwrap(),
            vec![
                Event::Book {
                    symbol: NO.into(),
                    exchange_time: ms(1793901639881),
                    book: Book::new(
                        at(&[(milli(460), 8_766_350_000), (milli(470), 6_557_950_000)]),
                        at(&[(milli(490), 10 * ONE), (milli(480), 81_920_000)]),
                    ),
                },
                Event::Book {
                    symbol: YES.into(),
                    exchange_time: ms(1793901639881),
                    book: Book::new(
                        at(&[(milli(510), 10 * ONE), (milli(520), 81_920_000)]),
                        at(&[(milli(540), 8_766_350_000), (milli(530), 6_557_950_000)]),
                    ),
                },
            ]
        );
    }

    #[test]
    fn a_book_sent_after_a_trade_replaces_the_whole_book() {
        assert_eq!(
            decode(BOOK).unwrap(),
            vec![Event::Book {
                symbol:
                    "33339798372916037220786136133406478491116150628220441951753426683441228279665"
                        .into(),
                exchange_time: ms(1793901640841),
                book: Book::new(
                    at(&[(milli(1), 57_683_690_000)]),
                    at(&[(milli(3), 854_060_000), (milli(2), 61_149 * ONE)]),
                ),
            }]
        );
    }

    #[test]
    fn a_price_change_is_one_absolute_update_per_entry_with_the_top_it_reports() {
        assert_eq!(
            decode(PRICE_CHANGE).unwrap(),
            vec![
                price_change(YES, Side::Buy, top_at(520, 530)),
                price_change(NO, Side::Sell, top_at(470, 480)),
            ]
        );
    }

    #[test]
    fn a_reported_top_of_zero_and_one_means_both_sides_are_empty() {
        let emptied = edited(PRICE_CHANGE, |value| {
            value["price_changes"][0]["best_bid"] = Value::from("0");
            value["price_changes"][0]["best_ask"] = Value::from("1");
        });
        let empty = Some(Top {
            bid: None,
            ask: None,
        });
        assert_eq!(
            decode(&emptied).unwrap()[0],
            price_change(YES, Side::Buy, empty)
        );
    }

    #[test]
    fn a_price_change_without_a_top_carries_none() {
        let untopped = edited(PRICE_CHANGE, |value| {
            value["price_changes"][0]
                .as_object_mut()
                .unwrap()
                .remove("best_bid");
        });
        assert_eq!(
            decode(&untopped).unwrap()[0],
            price_change(YES, Side::Buy, None)
        );
    }

    #[test]
    fn trades_and_pongs_carry_no_book_data() {
        assert_eq!(decode(LAST_TRADE_PRICE).unwrap(), vec![]);
        assert_eq!(decode("PONG").unwrap(), vec![]);
    }

    #[test]
    fn an_unreadable_entry_is_unreadable_for_its_token_and_the_other_entries_still_decode() {
        let breakages: [(&str, Value); 4] = [
            ("side", Value::from("HOLD")),
            ("price", Value::from("0.5000001")),
            ("size", Value::from(77)),
            ("best_ask", Value::from("high")),
        ];
        for (field, value) in breakages {
            let frame = edited(PRICE_CHANGE, |frame| {
                frame["price_changes"][1][field] = value
            });
            assert_eq!(
                decode(&frame).unwrap(),
                vec![
                    price_change(YES, Side::Buy, top_at(520, 530)),
                    unreadable(NO)
                ],
                "bad {field}"
            );
        }
    }

    #[test]
    fn a_price_change_without_a_readable_timestamp_is_unreadable_for_every_token() {
        for timestamp in [Value::Null, Value::from("soon")] {
            let frame = edited(PRICE_CHANGE, |frame| frame["timestamp"] = timestamp);
            assert_eq!(
                decode(&frame).unwrap(),
                vec![unreadable(YES), unreadable(NO)]
            );
        }
    }

    #[test]
    fn a_timestamp_before_1970_is_unreadable_since_the_output_cannot_hold_it() {
        let old_book = edited(BOOK, |frame| frame["timestamp"] = Value::from("-1"));
        let Event::Book { symbol, .. } = &decode(BOOK).unwrap()[0] else {
            panic!("the fixture is a book");
        };
        assert_eq!(decode(&old_book).unwrap(), vec![unreadable(symbol)]);
        let old_change = edited(PRICE_CHANGE, |frame| frame["timestamp"] = Value::from("-1"));
        assert_eq!(
            decode(&old_change).unwrap(),
            vec![unreadable(YES), unreadable(NO)]
        );
    }

    #[test]
    fn an_unreadable_book_is_unreadable_for_its_token_and_the_rest_of_the_dump_still_decodes() {
        for (field, value) in [("price", "0.4.6"), ("size", "-3")] {
            let frame = edited(INITIAL_DUMP, |dump| {
                dump[0]["bids"][0][field] = Value::from(value);
            });
            let events = decode(&frame).unwrap();
            assert_eq!(events[0], unreadable(NO), "bad {field}");
            assert!(matches!(&events[1], Event::Book { symbol, .. } if symbol == YES));
        }
    }

    #[test]
    fn a_message_that_names_no_token_or_a_frame_that_is_not_json_is_an_error() {
        let frames = [
            edited(PRICE_CHANGE, |frame| {
                frame["price_changes"][1]["asset_id"] = Value::Null;
            }),
            edited(PRICE_CHANGE, |frame| {
                frame.as_object_mut().unwrap().remove("price_changes");
            }),
            edited(BOOK, |frame| {
                frame.as_object_mut().unwrap().remove("asset_id");
            }),
            edited(BOOK, |frame| {
                frame.as_object_mut().unwrap().remove("event_type");
            }),
            "not json".to_owned(),
        ];
        for frame in frames {
            assert!(decode(&frame).is_err(), "{frame} was accepted");
        }
    }

    #[test]
    fn only_the_first_subscription_of_a_session_uses_the_initial_form() {
        let mut codec = PolymarketCodec::default();
        let symbols = [YES.to_owned(), NO.to_owned()];
        assert_eq!(
            codec.subscribe(&symbols),
            vec![format!(
                r#"{{"assets_ids":["{YES}","{NO}"],"type":"market"}}"#
            )]
        );
        assert_eq!(
            codec.subscribe(&symbols[..1]),
            vec![format!(
                r#"{{"operation":"subscribe","assets_ids":["{YES}"]}}"#
            )]
        );
        assert_eq!(
            codec.unsubscribe(&symbols[1..]),
            vec![format!(
                r#"{{"operation":"unsubscribe","assets_ids":["{NO}"]}}"#
            )]
        );
        assert_eq!(codec.keepalive(), Some("PING".to_owned()));
    }

    #[test]
    fn a_refresh_and_a_renewal_both_unsubscribe_and_subscribe_again() {
        let mut codec = PolymarketCodec::default();
        codec.subscribe(&[YES.to_owned()]);
        let again = vec![
            format!(r#"{{"operation":"unsubscribe","assets_ids":["{YES}"]}}"#),
            format!(r#"{{"operation":"subscribe","assets_ids":["{YES}"]}}"#),
        ];
        assert_eq!(codec.refresh(&[YES.to_owned()]), again);
        assert_eq!(codec.renew(&[YES.to_owned()]), again);
    }

    #[test]
    fn subscribing_or_unsubscribing_no_tokens_sends_nothing() {
        let mut codec = PolymarketCodec::default();
        assert!(codec.subscribe(&[]).is_empty());
        assert!(codec.unsubscribe(&[]).is_empty());
        assert_eq!(
            codec.subscribe(&[YES.to_owned()]),
            vec![format!(r#"{{"assets_ids":["{YES}"],"type":"market"}}"#)],
            "the first subscription that sends a frame uses the initial form"
        );
    }

    #[test]
    fn a_new_session_starts_with_the_initial_form_again() {
        let polymarket = Polymarket::new().unwrap();
        let mut first = polymarket.codec();
        first.subscribe(&[YES.to_owned()]);
        let mut second = polymarket.codec();
        assert_eq!(
            second.subscribe(&[YES.to_owned()]),
            vec![format!(r#"{{"assets_ids":["{YES}"],"type":"market"}}"#)]
        );
    }

    #[test]
    fn the_listing_asks_for_open_v1_and_v2_markets_newest_first_a_hundred_at_a_time() {
        let polymarket = Polymarket::new().unwrap();
        assert_eq!(
            polymarket.page_url(None).unwrap().as_str(),
            format!("{MARKETS}?{QUERY}")
        );
        assert_eq!(
            polymarket
                .page_url(Some("AwamGprB6dqvOL7QQ"))
                .unwrap()
                .as_str(),
            format!("{MARKETS}?{QUERY}&after_cursor=AwamGprB6dqvOL7QQ")
        );
    }

    /// Polymarket, with its listing served by the HTTP server at `address`.
    fn listing_at(address: std::net::SocketAddr) -> Polymarket {
        let mut polymarket = Polymarket::new().unwrap();
        polymarket.markets = format!("http://{address}/markets/keyset");
        polymarket
    }

    #[tokio::test]
    async fn the_listing_reads_a_page_with_a_next_cursor_and_the_last_page_over_http() {
        // One market's token ids can't be read, and the page still lists the other's.
        let first = edited(MARKET_PAGE, |page| {
            let mut broken = page["markets"][0].clone();
            broken["clobTokenIds"] = Value::from("[\"32338220190071351");
            page["markets"].as_array_mut().unwrap().push(broken);
        });
        let pages = vec![("200 OK", first), ("200 OK", LAST_MARKET_PAGE.to_owned())];
        let (address, served) = http_server(pages).await;
        let polymarket = listing_at(address);

        let (symbols, next) = polymarket.live_page(None).await.unwrap();
        assert_eq!(symbols.len(), 2);
        let next = next.expect("the first page has a next one");
        let (symbols, last) = polymarket.live_page(Some(next.clone())).await.unwrap();
        assert_eq!((symbols.len(), last), (2, None));
        assert_eq!(
            served.await.unwrap(),
            [
                format!("/markets/keyset?{QUERY}"),
                format!("/markets/keyset?{QUERY}&after_cursor={next}"),
            ]
        );
    }

    #[tokio::test]
    async fn a_listing_request_that_fails_with_an_http_error_is_an_error() {
        let (address, _) = http_server(vec![("502 Bad Gateway", String::new())]).await;
        let error = listing_at(address).live_page(None).await.unwrap_err();
        assert!(
            format!("{error:#}").contains("502 Bad Gateway"),
            "{error:#}"
        );
    }

    #[test]
    fn a_market_page_lists_the_tokens_of_markets_with_an_order_book_and_the_next_cursor() {
        let (symbols, next, skipped) = page_symbols(MARKET_PAGE).unwrap();
        // The v1 market's own positionIds are not its book tokens; the undeployed market has none.
        assert_eq!(
            symbols,
            [
                "32338220190071351435772801779725302244575775216413325951443816017994629993401",
                "25659310674993675562345759665114759892400026242514633218387667107987341231962",
            ]
        );
        assert_eq!(
            next.as_deref(),
            Some(
                "AwamGprB6dqvOL7QQdPpM3xWvMIF7A6kKRDolJ09o617InYiOjEsImsiOiJtYXJrZXRzIiwib2giOiI0ZjUzY2RhMThjMmJhYTBjMDM1NGJiNWY5YTNlY2JlNWVkMTJhYjRkOGUxMWJhODczYzJmMTExNjEyMDJiOTQ1Iiwia2V5cyI6W3sidCI6InN0cmluZyIsInYiOiIyMjkwMDc4In1dfQ"
            )
        );
        assert_eq!(skipped, 0);
    }

    #[test]
    fn the_last_market_page_has_no_cursor_and_v2_markets_list_their_position_ids() {
        let (symbols, next, _) = page_symbols(LAST_MARKET_PAGE).unwrap();
        assert_eq!(
            symbols,
            [
                "944412068274729391445398613530435636018762177453597551650732970930727813120",
                "944412068274729391445398613530435636018762177453597551650732970930727813121",
            ]
        );
        assert_eq!(next, None);
    }

    #[test]
    fn markets_with_unreadable_token_ids_are_skipped_and_the_rest_of_the_page_kept() {
        let page = edited(MARKET_PAGE, |page| {
            let mut broken_v1 = page["markets"][0].clone();
            broken_v1["clobTokenIds"] = Value::from("[\"32338220190071351");
            let mut broken_v2 = page["markets"][0].clone();
            broken_v2["version"] = Value::from("v2");
            broken_v2["positionIds"] = serde_json::json!([1, 2]);
            let markets = page["markets"].as_array_mut().unwrap();
            markets.push(broken_v1);
            markets.push(broken_v2);
        });
        let (symbols, _, skipped) = page_symbols(&page).unwrap();
        assert_eq!(symbols.len(), 2, "the readable market keeps its two tokens");
        assert_eq!(skipped, 2);
    }

    #[test]
    fn a_market_page_that_cannot_be_read_is_an_error() {
        assert!(page_symbols(r#"{"type":"validation error","error":"invalid cursor"}"#).is_err());
        assert!(page_symbols("<html>rate limited</html>").is_err());
    }
}
