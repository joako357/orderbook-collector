//! Extension 4: how quickly the same Polymarket updates reach several websockets, and whether
//! replacing the slow ones finds faster ones.
//!
//! It opens websockets to the tokens of the busiest markets and matches every price change across
//! them by token, exchange timestamp, price, size, side and hash. After each round it prints a
//! Markdown table: per connection, how far behind the fastest one it got the updates, how old
//! they were on arrival (on a clock corrected with SNTP before and after the round), and what it
//! never got. Then it replaces the slower half with new connections. What I found is in
//! LATENCY.md.
//!
//! Usage: `cargo run --release --example latency -- [connections] [seconds per round] [rounds]`

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{SocketAddr, UdpSocket};
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Instant, interval, timeout_at};
use tokio_tungstenite::tungstenite::Message;

use engineering_challenge::exchange::Codec;
use engineering_challenge::exchange::polymarket::PolymarketCodec;

const WEBSOCKET: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// The 100 busiest open markets by 24-hour volume. This endpoint lists v1 markets, whose order
/// book tokens are in `clobTokenIds`.
const BUSIEST: &str = "https://gamma-api.polymarket.com/markets?closed=false&order=volume24hr&ascending=false&limit=100";

const NTP_SERVERS: [&str; 3] = [
    "time.google.com:123",
    "time.cloudflare.com:123",
    "pool.ntp.org:123",
];

/// Queries per server. The reply with the shortest round trip gives the offset.
const NTP_QUERIES: usize = 8;

/// Seconds from the NTP epoch, 1900, to the Unix epoch.
const NTP_TO_UNIX: i64 = 2_208_988_800;

/// How long new connections get to subscribe and receive their books before a round starts.
const WARM_UP: Duration = Duration::from_secs(15);

/// How long after a round the slower connections still get to deliver the round's updates. An
/// update that takes longer counts as missed.
const GRACE: Duration = Duration::from_secs(30);

fn main() -> anyhow::Result<()> {
    let usage = "usage: latency [connections] [seconds per round] [rounds]";
    let numbers = std::env::args()
        .skip(1)
        .map(|arg| arg.parse::<u64>().context(usage))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let connections = numbers.first().map_or(8, |&n| n as usize);
    let round = Duration::from_secs(numbers.get(1).copied().unwrap_or(180));
    let rounds = numbers.get(2).map_or(4, |&n| n as usize);
    ensure!(
        connections >= 2,
        "{usage}: it takes 2 connections to compare"
    );

    println!("The local clock against SNTP:\n");
    print_offsets(&sntp_offsets());
    tokio::runtime::Runtime::new()?.block_on(experiment(connections, round, rounds))?;
    println!("\nThe local clock against SNTP, after the run:\n");
    print_offsets(&sntp_offsets());
    Ok(())
}

async fn experiment(connections: usize, round: Duration, rounds: usize) -> anyhow::Result<()> {
    let tokens = busiest_tokens().await?;
    println!(
        "\n{} tokens of the 100 busiest markets, {connections} connections, {rounds} rounds of {} s",
        tokens.len(),
        round.as_secs()
    );
    let mut lab = Lab::new(tokens);
    for _ in 0..connections {
        lab.open(1);
    }
    for number in 1..=rounds {
        lab.listen(WARM_UP, Record::Nothing).await;
        let from = clock_now().await?;
        let (start, offset) = from;
        lab.listen(round, Record::From(start + offset)).await;
        lab.listen(GRACE, Record::KnownOnly).await;
        let clock = Clock {
            from,
            to: clock_now().await?,
        };
        let slowest_first = lab.report(number, &clock);
        if number < rounds {
            let ended: Vec<usize> = lab.ended().collect();
            let slower_half = slowest_first.into_iter().take(connections / 2);
            for id in ended.into_iter().chain(slower_half) {
                lab.replace(id, number + 1);
            }
        }
    }
    lab.close().await;
    Ok(())
}

/// The connections, and the price changes of the round being measured.
struct Lab {
    tokens: Vec<String>,
    notes_tx: mpsc::UnboundedSender<Note>,
    notes: mpsc::UnboundedReceiver<Note>,
    next_id: usize,
    open: BTreeMap<usize, Open>,
    closing: Vec<JoinHandle<()>>,
    arrivals: HashMap<Update, Arrival>,
}

struct Open {
    /// The first round it takes part in.
    round: usize,
    info: Option<Info>,
    ended: bool,
    stop: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

/// What `listen` does with the price changes it reads.
#[derive(Clone, Copy, PartialEq)]
enum Record {
    Nothing,
    /// Every update the exchange stamped at this true time or later. An earlier one that a slow
    /// connection delivers now belongs to the time before the round, when nothing was recorded.
    From(DateTime<Utc>),
    /// Only more arrivals of updates already recorded.
    KnownOnly,
}

impl Lab {
    fn new(tokens: Vec<String>) -> Self {
        let (notes_tx, notes) = mpsc::unbounded_channel();
        Self {
            tokens,
            notes_tx,
            notes,
            next_id: 0,
            open: BTreeMap::new(),
            closing: Vec::new(),
            arrivals: HashMap::new(),
        }
    }

    /// Opens a new connection, which takes part from round `round` on.
    fn open(&mut self, round: usize) {
        let id = self.next_id;
        self.next_id += 1;
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(connection(
            id,
            self.tokens.clone(),
            self.notes_tx.clone(),
            stopped,
        ));
        let open = Open {
            round,
            info: None,
            ended: false,
            stop,
            task,
        };
        self.open.insert(id, open);
    }

    /// Closes connection `id` and opens a new one in its place.
    fn replace(&mut self, id: usize, round: usize) {
        if let Some(open) = self.open.remove(&id) {
            let _ = open.stop.send(());
            self.closing.push(open.task);
        }
        self.open(round);
    }

    /// The connections that failed or were closed by Polymarket.
    fn ended(&self) -> impl Iterator<Item = usize> + '_ {
        self.open
            .iter()
            .filter(|(_, open)| open.ended)
            .map(|(id, _)| *id)
    }

    /// Handles what the connections send for `duration`, recording price changes as `record`
    /// says.
    async fn listen(&mut self, duration: Duration, record: Record) {
        let until = Instant::now() + duration;
        while let Ok(Some(note)) = timeout_at(until, self.notes.recv()).await {
            match note {
                Note::Connected(id, info) => {
                    if let Some(open) = self.open.get_mut(&id) {
                        open.info = Some(info);
                    }
                }
                Note::Ended(id, why) => {
                    eprintln!("connection {id} ended: {why}");
                    if let Some(open) = self.open.get_mut(&id) {
                        open.ended = true;
                    }
                }
                Note::Frame(id, read, frame) => {
                    keep(&mut self.arrivals, record, id, read, &frame);
                }
            }
        }
    }

    /// Prints the round's table, starts the next round's record, and returns the connections
    /// that took part from the slowest to the fastest.
    fn report(&mut self, number: usize, clock: &Clock) -> Vec<usize> {
        let arrivals = std::mem::take(&mut self.arrivals);
        let conns: Vec<usize> = self
            .open
            .iter()
            .filter(|(_, open)| open.info.is_some() && !open.ended)
            .map(|(id, _)| *id)
            .collect();
        let (lags, fastest) = lags(&arrivals, &conns, clock);
        let everywhere = lags.values().next().map_or(0, |lag| lag.behind.len());
        println!(
            "\nRound {number}, from {} UTC: {} price changes, {everywhere} of them on all {} connections. The local clock was {} at the start and {} at the end.\n",
            clock.true_time(clock.from.0).format("%H:%M:%S"),
            arrivals.len(),
            conns.len(),
            offset_text(clock.from.1),
            offset_text(clock.to.1),
        );
        row([
            "conn",
            "from round",
            "pop",
            "edge",
            "handshake",
            "first",
            "behind the fastest, ms (p50 / p90 / p99)",
            "age on arrival, ms (p50 / p90 / p99)",
            "missed",
        ]);
        row([
            "---:", "---:", "---", "---", "---:", "---:", "---:", "---:", "---:",
        ]);
        for (id, lag) in &lags {
            let open = &self.open[id];
            let info = open.info.as_ref().expect("only connected ones take part");
            let missed = match lag.missed_tokens.len() {
                0 => "0".to_owned(),
                tokens => format!("{} in {tokens} tokens", lag.missed),
            };
            row([
                &id.to_string(),
                &open.round.to_string(),
                &info.pop,
                &info.edge.ip().to_string(),
                &format!("{} ms", info.handshake.as_millis()),
                &format!("{}%", 100 * lag.first / fastest.len().max(1)),
                &summary(&lag.behind),
                &summary(&lag.age),
                &missed,
            ]);
        }
        let all = "whichever got it first";
        row([all, "", "", "", "", "", "", &summary(&fastest), ""]);

        let p50_behind: BTreeMap<usize, TimeDelta> = lags
            .iter()
            .map(|(id, lag)| {
                let p50 = percentiles(&lag.behind).map_or(TimeDelta::MAX, |[p50, ..]| p50);
                (*id, p50)
            })
            .collect();
        if number > 1 {
            let median = |kept: bool| {
                let p50s: Vec<TimeDelta> = p50_behind
                    .iter()
                    .filter(|(id, _)| (self.open[*id].round < number) == kept)
                    .map(|(_, p50)| *p50)
                    .collect();
                percentiles(&p50s).map_or("-".into(), |[median, ..]| ms(median))
            };
            println!(
                "\nThe median p50 behind the fastest: {} ms on the connections kept from round {}, {} ms on the new ones.",
                median(true),
                number - 1,
                median(false)
            );
        }
        let mut slowest_first: Vec<usize> = p50_behind.keys().copied().collect();
        slowest_first.sort_by_key(|id| std::cmp::Reverse(p50_behind[id]));
        slowest_first
    }

    /// Closes every connection, waiting a few seconds for their Close frames.
    async fn close(self) {
        let mut tasks = self.closing;
        for open in self.open.into_values() {
            let _ = open.stop.send(());
            tasks.push(open.task);
        }
        let all = futures_util::future::join_all(tasks);
        let _ = tokio::time::timeout(Duration::from_secs(5), all).await;
    }
}

/// What a connection tells the experiment.
enum Note {
    Connected(usize, Info),
    /// A text frame, and when it was read.
    Frame(usize, DateTime<Utc>, String),
    Ended(usize, String),
}

/// Where a connection landed.
struct Info {
    /// The CloudFront point of presence that answered the handshake.
    pop: String,
    edge: SocketAddr,
    /// DNS, TCP, TLS and the websocket upgrade, from inside the container.
    handshake: Duration,
}

/// One websocket subscribed to `tokens`. It forwards every text frame with the time it was read,
/// until `stop` fires or the connection fails.
async fn connection(
    id: usize,
    tokens: Vec<String>,
    notes: mpsc::UnboundedSender<Note>,
    stop: oneshot::Receiver<()>,
) {
    if let Err(error) = stream(id, &tokens, &notes, stop).await {
        let _ = notes.send(Note::Ended(id, format!("{error:#}")));
    }
}

async fn stream(
    id: usize,
    tokens: &[String],
    notes: &mpsc::UnboundedSender<Note>,
    mut stop: oneshot::Receiver<()>,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let (mut socket, response) = tokio_tungstenite::connect_async(WEBSOCKET).await?;
    let info = Info {
        handshake: started.elapsed(),
        edge: socket.get_ref().get_ref().peer_addr()?,
        pop: response
            .headers()
            .get("x-amz-cf-pop")
            .and_then(|pop| pop.to_str().ok())
            .unwrap_or("?")
            .to_owned(),
    };
    notes.send(Note::Connected(id, info))?;
    let mut codec = PolymarketCodec::default();
    for frame in codec.subscribe(tokens) {
        socket.send(Message::text(frame)).await?;
    }
    let mut keepalive = interval(Duration::from_secs(10));
    loop {
        tokio::select! {
            frame = socket.next() => {
                let read = Utc::now();
                match frame.context("the connection dropped")?? {
                    Message::Text(text) => notes.send(Note::Frame(id, read, text.to_string()))?,
                    Message::Close(close) => bail!("closed by Polymarket: {close:?}"),
                    _ => {}
                }
            }
            _ = keepalive.tick() => {
                if let Some(ping) = codec.keepalive() {
                    socket.send(Message::text(ping)).await?;
                }
            }
            _ = &mut stop => {
                socket.close(None).await?;
                return Ok(());
            }
        }
    }
}

/// The order book tokens of the busiest markets.
async fn busiest_tokens() -> anyhow::Result<Vec<String>> {
    let response = reqwest::get(BUSIEST).await?.error_for_status()?;
    tokens(&response.text().await?)
}

/// The order book tokens in a list of Gamma API markets, which carry them as a JSON string.
fn tokens(markets: &str) -> anyhow::Result<Vec<String>> {
    let markets: Vec<Value> = serde_json::from_str(markets).context("unreadable markets")?;
    let mut tokens = Vec::new();
    for market in markets {
        if market["enableOrderBook"] != true {
            continue;
        }
        if let Some(ids) = market["clobTokenIds"].as_str() {
            tokens.extend(serde_json::from_str::<Vec<String>>(ids)?);
        }
    }
    Ok(tokens)
}

/// One price level change as Polymarket sends it. The same change comes with the same strings on
/// every connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Update {
    token: String,
    timestamp: String,
    price: String,
    size: String,
    side: String,
    hash: String,
}

/// The price changes in one frame, each with its exchange time. Books, trades and PONGs have
/// none.
fn price_changes(frame: &str) -> Vec<(Update, DateTime<Utc>)> {
    let messages = match serde_json::from_str(frame) {
        Ok(Value::Array(messages)) => messages,
        Ok(message) => vec![message],
        Err(_) => Vec::new(),
    };
    let mut changes = Vec::new();
    for message in messages {
        if message["event_type"] != "price_change" {
            continue;
        }
        let Some(timestamp) = message["timestamp"].as_str() else {
            continue;
        };
        let Some(exchange) = timestamp
            .parse()
            .ok()
            .and_then(DateTime::from_timestamp_millis)
        else {
            continue;
        };
        for change in message["price_changes"].as_array().into_iter().flatten() {
            let text = |field: &str| change[field].as_str().unwrap_or_default().to_owned();
            let update = Update {
                token: text("asset_id"),
                timestamp: timestamp.to_owned(),
                price: text("price"),
                size: text("size"),
                side: text("side"),
                hash: text("hash"),
            };
            changes.push((update, exchange));
        }
    }
    changes
}

/// When one update first reached each connection.
struct Arrival {
    exchange: DateTime<Utc>,
    at: BTreeMap<usize, DateTime<Utc>>,
}

/// Adds the price changes in a frame that connection `id` read at `read` to `arrivals`, as
/// `record` says.
fn keep(
    arrivals: &mut HashMap<Update, Arrival>,
    record: Record,
    id: usize,
    read: DateTime<Utc>,
    frame: &str,
) {
    if record == Record::Nothing {
        return;
    }
    for (update, exchange) in price_changes(frame) {
        if let Some(arrival) = arrivals.get_mut(&update) {
            arrival.at.entry(id).or_insert(read);
        } else if let Record::From(start) = record
            && exchange >= start
        {
            let at = BTreeMap::from([(id, read)]);
            arrivals.insert(update, Arrival { exchange, at });
        }
    }
}

/// One connection over one round.
#[derive(Debug, Default, PartialEq)]
struct Lags {
    /// How long after the fastest connection it got each update that reached every connection.
    behind: Vec<TimeDelta>,
    /// How long after its exchange timestamp each update it got arrived, in true time.
    age: Vec<TimeDelta>,
    /// How many updates it got first, ties included.
    first: usize,
    /// Updates that reached another connection but never this one, and their tokens.
    missed: usize,
    missed_tokens: BTreeSet<String>,
}

/// The lags of each of `conns` over `arrivals`, and the age of every update on whichever of them
/// got it first.
fn lags(
    arrivals: &HashMap<Update, Arrival>,
    conns: &[usize],
    clock: &Clock,
) -> (BTreeMap<usize, Lags>, Vec<TimeDelta>) {
    let mut lags: BTreeMap<usize, Lags> = conns.iter().map(|&id| (id, Lags::default())).collect();
    let mut fastest_ages = Vec::new();
    for (update, arrival) in arrivals {
        let times: Vec<DateTime<Utc>> = conns
            .iter()
            .filter_map(|id| arrival.at.get(id).copied())
            .collect();
        let Some(&fastest) = times.iter().min() else {
            continue;
        };
        let age = |at| clock.true_time(at) - arrival.exchange;
        fastest_ages.push(age(fastest));
        for (id, lag) in &mut lags {
            let Some(&at) = arrival.at.get(id) else {
                lag.missed += 1;
                lag.missed_tokens.insert(update.token.clone());
                continue;
            };
            lag.age.push(age(at));
            if times.len() == conns.len() {
                lag.behind.push(at - fastest);
            }
            if at == fastest {
                lag.first += 1;
            }
        }
    }
    (lags, fastest_ages)
}

/// The 50th, 90th and 99th percentiles of `values` by nearest rank, none for no values.
fn percentiles(values: &[TimeDelta]) -> Option<[TimeDelta; 3]> {
    let mut sorted = values.to_vec();
    sorted.sort();
    let rank = |p: usize| sorted[(p * sorted.len()).div_ceil(100) - 1];
    (!sorted.is_empty()).then(|| [rank(50), rank(90), rank(99)])
}

/// One row of a Markdown table.
fn row<const N: usize>(cells: [&str; N]) {
    println!("| {} |", cells.join(" | "));
}

/// "p50 / p90 / p99" in milliseconds.
fn summary(values: &[TimeDelta]) -> String {
    percentiles(values).map_or("-".into(), |[p50, p90, p99]| {
        format!("{} / {} / {}", ms(p50), ms(p90), ms(p99))
    })
}

fn ms(delta: TimeDelta) -> String {
    let micros = delta.num_microseconds().unwrap_or(i64::MAX);
    format!("{:.1}", micros as f64 / 1000.0)
}

/// How a clock offset reads: how far the local clock is behind or ahead of true time.
fn offset_text(offset: TimeDelta) -> String {
    let direction = if offset < TimeDelta::zero() {
        "ahead"
    } else {
        "behind"
    };
    format!("{} ms {direction}", ms(offset.abs()))
}

/// The local clock against true time: its offset at two moments, measured with SNTP, and the
/// drift between them spread evenly.
struct Clock {
    from: (DateTime<Utc>, TimeDelta),
    to: (DateTime<Utc>, TimeDelta),
}

impl Clock {
    /// True time at local time `local`.
    fn true_time(&self, local: DateTime<Utc>) -> DateTime<Utc> {
        let ((start, first), (end, last)) = (self.from, self.to);
        let micros = |delta: TimeDelta| delta.num_microseconds().unwrap_or(0);
        let share = match micros(end - start) {
            0 => 0.0,
            span => micros(local - start) as f64 / span as f64,
        };
        let drift = (micros(last - first) as f64 * share).round() as i64;
        local + first + TimeDelta::microseconds(drift)
    }
}

/// The clock offset now, from the SNTP reply with the shortest round trip.
async fn clock_now() -> anyhow::Result<(DateTime<Utc>, TimeDelta)> {
    let offsets = tokio::task::spawn_blocking(sntp_offsets).await?;
    let (_, offset, _) = offsets
        .into_iter()
        .min_by_key(|&(_, _, round_trip)| round_trip)
        .context("no SNTP server replied")?;
    Ok((Utc::now(), offset))
}

/// Each server's reply with the shortest round trip: how much to add to the local clock to get
/// true time, and that round trip.
fn sntp_offsets() -> Vec<(&'static str, TimeDelta, TimeDelta)> {
    NTP_SERVERS
        .into_iter()
        .filter_map(|server| {
            let replies = (0..NTP_QUERIES).filter_map(|_| sntp(server).ok());
            let (offset, round_trip) = replies.min_by_key(|&(_, round_trip)| round_trip)?;
            Some((server, offset, round_trip))
        })
        .collect()
}

fn print_offsets(offsets: &[(&str, TimeDelta, TimeDelta)]) {
    for (server, offset, round_trip) in offsets {
        let offset = offset_text(*offset);
        println!("- {server}: {offset}, round trip {} ms", ms(*round_trip));
    }
}

/// One SNTP query: the local clock's offset and the round trip.
fn sntp(server: &str) -> anyhow::Result<(TimeDelta, TimeDelta)> {
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.connect(server)?;
    let sent = Utc::now();
    let request = sntp_request(sent);
    socket.send(&request)?;
    let mut reply = [0; 64];
    let length = socket.recv(&mut reply)?;
    let received = Utc::now();
    let (server_received, server_sent) = server_times(&request, &reply[..length])?;
    Ok(offset_and_round_trip(
        sent,
        server_received,
        server_sent,
        received,
    ))
}

/// An SNTP client request that carries `sent` as its transmit time. The server echoes that back,
/// which ties its reply to this request.
fn sntp_request(sent: DateTime<Utc>) -> [u8; 48] {
    let mut request = [0; 48];
    // Leap indicator 0, version 4, mode 3 (client).
    request[0] = 0x23;
    request[40..].copy_from_slice(&ntp_timestamp(sent));
    request
}

/// When the server received `request` and when it sent `reply`.
fn server_times(
    request: &[u8; 48],
    reply: &[u8],
) -> anyhow::Result<(DateTime<Utc>, DateTime<Utc>)> {
    ensure!(reply.len() >= 48, "an SNTP reply of {} bytes", reply.len());
    ensure!(reply[0] & 0b111 == 4, "not a server reply");
    ensure!(
        (1..=15).contains(&reply[1]),
        "an unsynchronized server (stratum {})",
        reply[1]
    );
    ensure!(
        reply[24..32] == request[40..48],
        "a reply to another request"
    );
    let received = ntp_time(reply[32..40].try_into()?);
    let sent = ntp_time(reply[40..48].try_into()?);
    Ok((received, sent))
}

/// `time` as NTP writes it: 32 bits of seconds since 1900 and 32 bits of fraction. The seconds
/// wrap in 2036, long after this experiment.
fn ntp_timestamp(time: DateTime<Utc>) -> [u8; 8] {
    let seconds = (time.timestamp() + NTP_TO_UNIX) as u32;
    let fraction = ((u64::from(time.timestamp_subsec_nanos()) << 32) / 1_000_000_000) as u32;
    let mut bytes = [0; 8];
    bytes[..4].copy_from_slice(&seconds.to_be_bytes());
    bytes[4..].copy_from_slice(&fraction.to_be_bytes());
    bytes
}

fn ntp_time(bytes: [u8; 8]) -> DateTime<Utc> {
    let [s0, s1, s2, s3, f0, f1, f2, f3] = bytes;
    let seconds = i64::from(u32::from_be_bytes([s0, s1, s2, s3])) - NTP_TO_UNIX;
    let nanos = (u64::from(u32::from_be_bytes([f0, f1, f2, f3])) * 1_000_000_000) >> 32;
    DateTime::from_timestamp(seconds, nanos as u32).expect("32 bits of seconds fit")
}

/// The clock offset and the network round trip, from when the request left, reached the server,
/// left the server and came back. The offset is exact when the two legs take equally long.
fn offset_and_round_trip(
    sent: DateTime<Utc>,
    server_received: DateTime<Utc>,
    server_sent: DateTime<Utc>,
    received: DateTime<Utc>,
) -> (TimeDelta, TimeDelta) {
    let offset = ((server_received - sent) + (server_sent - received)) / 2;
    let round_trip = (received - sent) - (server_sent - server_received);
    (offset, round_trip)
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRICE_CHANGE: &str = include_str!("../tests/fixtures/polymarket/price_change.json");
    const BOOK: &str = include_str!("../tests/fixtures/polymarket/book.json");
    const TRADE: &str = include_str!("../tests/fixtures/polymarket/last_trade_price.json");

    /// `micros` microseconds after the epoch.
    fn t(micros: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(micros).unwrap()
    }

    fn us(micros: i64) -> TimeDelta {
        TimeDelta::microseconds(micros)
    }

    /// A clock that needs no correction.
    fn exact() -> Clock {
        let none = (t(0), TimeDelta::zero());
        Clock {
            from: none,
            to: none,
        }
    }

    /// Updates from the exchange at `exchange` µs, the n-th read at each `(connection, µs)` of
    /// the n-th list, all of them on one token.
    fn arrivals(exchange: i64, reads: &[&[(usize, i64)]]) -> HashMap<Update, Arrival> {
        reads
            .iter()
            .enumerate()
            .map(|(n, reads)| {
                let update = Update {
                    token: "T".into(),
                    timestamp: exchange.to_string(),
                    price: "0.5".into(),
                    size: n.to_string(),
                    side: "BUY".into(),
                    hash: n.to_string(),
                };
                let at = reads.iter().map(|&(id, micros)| (id, t(micros))).collect();
                let arrival = Arrival {
                    exchange: t(exchange),
                    at,
                };
                (update, arrival)
            })
            .collect()
    }

    fn sorted(mut values: Vec<TimeDelta>) -> Vec<TimeDelta> {
        values.sort();
        values
    }

    #[test]
    fn every_entry_of_a_price_change_is_an_update_at_the_frame_timestamp() {
        let changes = price_changes(PRICE_CHANGE);
        assert_eq!(changes.len(), 2);
        let (update, exchange) = &changes[1];
        assert_eq!(
            *update,
            Update {
                token:
                    "42506148661683943541979337517222758216686649928064404489628407344344901012834"
                        .into(),
                timestamp: "1793901640046".into(),
                price: "0.5".into(),
                size: "77".into(),
                side: "SELL".into(),
                hash: "babd63b23d84292a025a143b7c8f0a5be55ca354".into(),
            }
        );
        assert_eq!(*exchange, t(1_793_901_640_046_000));
        assert_ne!(changes[0].0, changes[1].0);
    }

    #[test]
    fn books_trades_pongs_and_unreadable_frames_hold_no_price_changes() {
        let untimed = r#"{"event_type":"price_change","price_changes":[]}"#;
        for frame in [BOOK, TRADE, "PONG", "[]", untimed] {
            assert!(price_changes(frame).is_empty(), "{frame}");
        }
    }

    #[test]
    fn a_round_takes_the_updates_stamped_since_it_started_and_every_later_arrival_of_them() {
        // The fixture's two updates are stamped 1793901640046 ms.
        let stamped = t(1_793_901_640_046_000);
        let mut arrivals = HashMap::new();
        keep(&mut arrivals, Record::Nothing, 0, t(1), PRICE_CHANGE);
        let late = Record::From(stamped + us(1_000));
        keep(&mut arrivals, late, 0, t(2), PRICE_CHANGE);
        assert!(arrivals.is_empty(), "stamped before the round");

        keep(&mut arrivals, Record::From(stamped), 3, t(3), PRICE_CHANGE);
        keep(&mut arrivals, Record::From(stamped), 3, t(4), PRICE_CHANGE);
        keep(&mut arrivals, Record::KnownOnly, 5, t(5), PRICE_CHANGE);
        keep(&mut arrivals, Record::KnownOnly, 6, t(6), BOOK);
        assert_eq!(arrivals.len(), 2);
        for arrival in arrivals.values() {
            assert_eq!(arrival.exchange, stamped);
            let reads: Vec<(usize, DateTime<Utc>)> =
                arrival.at.iter().map(|(&id, &at)| (id, at)).collect();
            assert_eq!(reads, [(3, t(3)), (5, t(5))], "first reads only");
        }

        let mut unknown = HashMap::new();
        keep(&mut unknown, Record::KnownOnly, 0, t(7), PRICE_CHANGE);
        assert!(
            unknown.is_empty(),
            "the grace after a round takes no new updates"
        );
    }

    #[test]
    fn each_connection_trails_the_fastest_by_the_time_between_their_reads() {
        let arrivals = arrivals(
            0,
            &[
                &[(0, 10_000), (1, 10_400), (2, 13_000)],
                &[(0, 20_900), (1, 20_000), (2, 20_000)],
            ],
        );
        let (lags, _) = lags(&arrivals, &[0, 1, 2], &exact());
        assert_eq!(sorted(lags[&0].behind.clone()), [us(0), us(900)]);
        assert_eq!(sorted(lags[&1].behind.clone()), [us(0), us(400)]);
        assert_eq!(sorted(lags[&2].behind.clone()), [us(0), us(3_000)]);
        let firsts = [lags[&0].first, lags[&1].first, lags[&2].first];
        assert_eq!(firsts, [1, 1, 1], "a tie counts for both");
    }

    #[test]
    fn an_update_one_connection_missed_counts_against_it_and_stays_out_of_the_comparison() {
        let arrivals = arrivals(0, &[&[(0, 10_000), (1, 10_500)]]);
        let (lags, fastest) = lags(&arrivals, &[0, 1, 2], &exact());
        assert_eq!(lags[&2].missed, 1);
        assert_eq!(lags[&2].missed_tokens, BTreeSet::from(["T".to_owned()]));
        assert!(lags.values().all(|lag| lag.behind.is_empty()));
        assert_eq!(lags[&1].age, [us(10_500)]);
        assert_eq!(fastest, [us(10_000)]);
    }

    #[test]
    fn connections_outside_the_comparison_are_ignored() {
        let arrivals = arrivals(0, &[&[(7, 1_000), (0, 10_000), (1, 10_500)], &[(7, 1_000)]]);
        let (lags, fastest) = lags(&arrivals, &[0, 1], &exact());
        assert_eq!(lags[&1].behind, [us(500)]);
        assert_eq!(lags[&0].missed + lags[&1].missed, 0);
        assert_eq!(fastest, [us(10_000)], "only connections 0 and 1 count");
    }

    #[test]
    fn ages_count_from_the_exchange_timestamp_in_true_time() {
        // The local clock is 2.8 s behind, so a read at local 1 s happened at 3.8 s.
        let behind = (t(0), TimeDelta::milliseconds(2_800));
        let clock = Clock {
            from: behind,
            to: behind,
        };
        let arrivals = arrivals(3_750_000, &[&[(0, 1_000_000), (1, 1_020_000)]]);
        let (lags, fastest) = lags(&arrivals, &[0, 1], &clock);
        assert_eq!(lags[&0].age, [us(50_000)]);
        assert_eq!(lags[&1].age, [us(70_000)]);
        assert_eq!(fastest, [us(50_000)]);
    }

    #[test]
    fn the_clock_spreads_its_drift_evenly_between_its_two_measurements() {
        let clock = Clock {
            from: (t(0), TimeDelta::milliseconds(100)),
            to: (t(100_000_000), TimeDelta::milliseconds(110)),
        };
        let true_time = |local: i64| clock.true_time(t(local)) - t(local);
        assert_eq!(true_time(0), TimeDelta::milliseconds(100));
        assert_eq!(true_time(50_000_000), TimeDelta::milliseconds(105));
        assert_eq!(true_time(100_000_000), TimeDelta::milliseconds(110));
        assert_eq!(
            exact().true_time(t(5)),
            t(5),
            "one measurement has no drift"
        );
    }

    #[test]
    fn percentiles_go_by_nearest_rank() {
        let hundred: Vec<TimeDelta> = (1..=100).rev().map(us).collect();
        assert_eq!(percentiles(&hundred), Some([us(50), us(90), us(99)]));
        let ten: Vec<TimeDelta> = (1..=10).map(us).collect();
        assert_eq!(percentiles(&ten), Some([us(5), us(9), us(10)]));
        assert_eq!(percentiles(&[us(7)]), Some([us(7); 3]));
        assert_eq!(percentiles(&[]), None);
    }

    #[test]
    fn a_summary_prints_the_percentiles_in_milliseconds() {
        let values: Vec<TimeDelta> = (1..=100).map(|n| us(n * 100)).collect();
        assert_eq!(summary(&values), "5.0 / 9.0 / 9.9");
        assert_eq!(summary(&[]), "-");
    }

    #[test]
    fn an_offset_reads_as_how_far_the_local_clock_is_behind_or_ahead() {
        assert_eq!(
            offset_text(TimeDelta::milliseconds(3_345)),
            "3345.0 ms behind"
        );
        assert_eq!(offset_text(us(-1_500)), "1.5 ms ahead");
    }

    #[test]
    fn ntp_timestamps_count_seconds_from_1900_and_a_binary_fraction() {
        let unix_epoch = ntp_timestamp(DateTime::UNIX_EPOCH);
        assert_eq!(unix_epoch, [0x83, 0xAA, 0x7E, 0x80, 0, 0, 0, 0]);
        let half_second = DateTime::UNIX_EPOCH + TimeDelta::milliseconds(500);
        assert_eq!(ntp_timestamp(half_second)[4..], [0x80, 0, 0, 0]);
        let time = t(1_793_901_640_046_123);
        let back = ntp_time(ntp_timestamp(time));
        assert!((back - time).abs() <= TimeDelta::nanoseconds(1), "{back}");
    }

    #[test]
    fn the_offset_splits_the_two_legs_and_the_round_trip_leaves_out_the_server_time() {
        // The local clock is 100 ms behind; each leg takes 10 ms; the server holds it 1 ms.
        let (offset, round_trip) = offset_and_round_trip(t(0), t(110_000), t(111_000), t(21_000));
        assert_eq!(offset, TimeDelta::milliseconds(100));
        assert_eq!(round_trip, TimeDelta::milliseconds(20));
    }

    #[test]
    fn a_reply_must_answer_this_request_from_a_synchronized_server() {
        let request = sntp_request(t(1_000_000));
        assert_eq!(request[0], 0x23, "version 4, client");
        let mut reply = [0; 48];
        reply[0] = 0x24;
        reply[1] = 2;
        reply[24..32].copy_from_slice(&request[40..48]);
        reply[32..40].copy_from_slice(&ntp_timestamp(t(2_000_000)));
        reply[40..48].copy_from_slice(&ntp_timestamp(t(2_000_500)));
        let (received, sent) = server_times(&request, &reply).unwrap();
        assert!((received - t(2_000_000)).abs() < us(1));
        assert!((sent - t(2_000_500)).abs() < us(1));

        let other_request = sntp_request(t(1_000_001));
        let mut unsynchronized = reply;
        unsynchronized[1] = 0;
        let mut from_a_client = reply;
        from_a_client[0] = 0x23;
        assert!(server_times(&other_request, &reply).is_err());
        assert!(server_times(&request, &unsynchronized).is_err());
        assert!(server_times(&request, &from_a_client).is_err());
        assert!(server_times(&request, &reply[..47]).is_err());
    }

    #[test]
    fn the_busiest_markets_give_the_tokens_of_those_with_an_order_book() {
        let markets = r#"[
            {"enableOrderBook": true, "clobTokenIds": "[\"11\", \"12\"]"},
            {"enableOrderBook": false, "clobTokenIds": "[\"21\", \"22\"]"},
            {"enableOrderBook": true}
        ]"#;
        assert_eq!(tokens(markets).unwrap(), ["11", "12"]);
        assert!(tokens("<html>rate limited</html>").is_err());
    }
}
