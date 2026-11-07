//! The line between the exchange-agnostic service and one exchange's wire format. Supporting
//! another exchange means implementing these two traits and naming it in `main.rs`; nothing else
//! changes.

pub mod kalshi;
pub mod polymarket;

use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use reqwest::Url;
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::Side;
use crate::book::{Book, Change, Level, Top};
use crate::fixed;

/// An exchange's name for one order book: a Kalshi market ticker, a Polymarket token id.
pub type Symbol = String;

/// How long a listing request may take before it fails, and is retried.
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// What arrived on a connection, in exchange-agnostic terms.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The whole book of a symbol, replacing whatever was known.
    Book {
        symbol: Symbol,
        exchange_time: DateTime<Utc>,
        book: Book,
    },
    /// A change to one price level of a symbol.
    Update(Update),
    /// A message about this symbol that can't be read, so the connection's book of it can't be
    /// trusted any more.
    Unreadable { symbol: Symbol },
}

/// A change to one price level of a symbol. Exchanges that report their top of book with every
/// change pass the top after it as `top`, so that a missed change shows.
#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub symbol: Symbol,
    pub exchange_time: DateTime<Utc>,
    pub side: Side,
    pub price: i64,
    pub change: Change,
    pub top: Option<Top>,
}

/// Everything specific to one exchange that all of its connections share.
pub trait Exchange: Send + Sync + 'static {
    type Codec: Codec;

    /// The most symbols one websocket should carry.
    const SYMBOLS_PER_SOCKET: usize;

    /// The most websockets the exchange lets one user open: no limit unless it sets one.
    const MAX_SOCKETS: usize = usize::MAX;

    /// One page of the symbols trading now, newest first, since that's where the service looks
    /// for new ones: the first page for `None`, otherwise the page at `cursor`, with the next
    /// page's cursor (`None` after the last page).
    fn live_page(
        &self,
        cursor: Option<String>,
    ) -> impl Future<Output = anyhow::Result<(Vec<Symbol>, Option<String>)>> + Send;

    /// The websocket handshake: the URL plus any authentication headers. Called on every connect.
    fn request(&self) -> anyhow::Result<Request>;

    /// The wire format of one new websocket session.
    fn codec(&self) -> Self::Codec;
}

/// The wire format of one websocket session. It may keep session state, such as a subscription
/// id or the last sequence number.
pub trait Codec: Send + 'static {
    /// Text frames that subscribe to `symbols`. The exchange answers with each symbol's book and
    /// then its changes.
    fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String>;

    /// Text frames that unsubscribe from `symbols`.
    fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String>;

    /// Text frames that bring a fresh book of each of `symbols`, whose books in this session
    /// stopped matching the exchange's. Renewing the subscription works on any exchange.
    fn refresh(&mut self, symbols: &[Symbol]) -> Vec<String> {
        self.renew(symbols)
    }

    /// Text frames that unsubscribe from `symbols` and subscribe to them again, for a
    /// subscription that stopped delivering.
    fn renew(&mut self, symbols: &[Symbol]) -> Vec<String> {
        let mut frames = self.unsubscribe(symbols);
        frames.extend(self.subscribe(symbols));
        frames
    }

    /// A text frame to send every few seconds, for exchanges that drop silent clients.
    fn keepalive(&self) -> Option<String> {
        None
    }

    /// Reads one text frame. A frame without book data decodes to no events, and a message about
    /// one symbol that can't be read decodes to `Event::Unreadable` while the rest of the frame
    /// still decodes. An error means the session can't be trusted any more (a frame can't be
    /// read, a message can't be tied to a symbol, a sequence gap, or the exchange saying it lost
    /// messages), and the session restarts.
    fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Event>>;
}

/// The HTTP client for the listing, with a timeout so that a stalled request fails.
fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?)
}

/// The body `url` answers with. An HTTP error status is an error.
async fn get(http: &reqwest::Client, url: Url) -> anyhow::Result<String> {
    let response = http.get(url).send().await?.error_for_status()?;
    Ok(response.text().await?)
}

/// Book levels as an exchange sends them: a decimal price and a decimal quantity each. A
/// negative quantity can't be read, the same as in a change.
fn levels<S: AsRef<str>>(sent: impl IntoIterator<Item = (S, S)>) -> anyhow::Result<Vec<Level>> {
    sent.into_iter()
        .map(|(price, quantity)| {
            let level = Level {
                price: fixed::parse(price.as_ref())?,
                quantity: fixed::parse(quantity.as_ref())?,
            };
            anyhow::ensure!(
                level.quantity >= 0,
                "negative quantity {}",
                quantity.as_ref()
            );
            Ok(level)
        })
        .collect()
}

/// A timestamp in milliseconds. The output can't hold one before 1970.
fn time(ms: i64) -> anyhow::Result<DateTime<Utc>> {
    anyhow::ensure!(ms >= 0, "timestamp {ms} is before 1970");
    DateTime::from_timestamp_millis(ms).with_context(|| format!("timestamp {ms} out of range"))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::net::SocketAddr;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    /// An HTTP server on localhost that answers each request with the next of `responses`, a
    /// status and a body, and hangs up. Returns its address, and the targets of the requests it
    /// got, once it has answered them all.
    pub(crate) async fn http_server(
        responses: Vec<(&'static str, String)>,
    ) -> (SocketAddr, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let answered = tokio::spawn(async move {
            let mut targets = Vec::new();
            for (status, body) in responses {
                let (stream, _) = listener.accept().await.unwrap();
                let mut stream = BufReader::new(stream);
                let mut request = String::new();
                stream.read_line(&mut request).await.unwrap();
                targets.push(request.split(' ').nth(1).unwrap().to_owned());
                loop {
                    let mut header = String::new();
                    let read = stream.read_line(&mut header).await.unwrap();
                    if read == 0 || header == "\r\n" {
                        break;
                    }
                }
                let head = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\n", body.len());
                let response = format!("{head}connection: close\r\n\r\n{body}");
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            targets
        });
        (address, answered)
    }
}
