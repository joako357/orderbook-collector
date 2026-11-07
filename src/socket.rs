//! One websocket connection: subscribes its symbols, stamps and decodes every frame, keeps the
//! session alive, and starts a new session whenever the current one ends.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use futures_util::{SinkExt, StreamExt};
use rand_core::{OsRng, RngCore};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time::{Instant, interval_at, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::command::{Command, Conn};
use crate::exchange::{Codec, Event, Exchange, Symbol};

/// What a socket tells the collector, in the order it happened.
#[derive(Debug, Clone, PartialEq)]
pub enum Received {
    /// An event, and when its frame was read.
    Event(Conn, DateTime<Utc>, Event),
    /// The session ended at this time, so nothing it delivered can be built on.
    Reset(Conn, DateTime<Utc>),
}

/// Kalshi pings every 10 s and Polymarket answers the `PING` sent every 10 s, so a session with
/// no frame for 30 s is dead. So is one whose frame can't go out in 30 s, since the exchange
/// stopped reading: the kernel would only drop that connection after about 15 minutes.
const DEAD_SOCKET_AFTER: Duration = Duration::from_secs(30);
const KEEPALIVE_EVERY: Duration = Duration::from_secs(10);
/// How long the websocket handshake may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// The pause before reconnecting doubles from the first to the longest while sessions keep
/// failing.
const FIRST_PAUSE: Duration = Duration::from_secs(1);
const LONGEST_PAUSE: Duration = Duration::from_secs(60);
/// A session that lived this long after its handshake counts as healthy, and the next pause
/// starts over.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);

/// Runs one socket until its command channel closes. Every session that ends is followed by a
/// `Reset`, and one that failed by a new session with the same symbols, after a pause.
pub async fn run<E: Exchange>(
    exchange: Arc<E>,
    conn: Conn,
    mut commands: UnboundedReceiver<Command>,
    events: Sender<Received>,
) {
    let mut symbols = BTreeSet::new();
    let mut failures = 0;
    loop {
        let ended = match connect(&*exchange).await {
            Ok(socket) => {
                let started = Instant::now();
                let codec = exchange.codec();
                let ended =
                    session(socket, codec, conn, &mut symbols, &mut commands, &events).await;
                if started.elapsed() >= HEALTHY_SESSION {
                    failures = 0;
                }
                ended
            }
            Err(error) => Err(error),
        };
        let _ = events.send(Received::Reset(conn, Utc::now()));
        match ended {
            Ok(()) => return,
            Err(error) => eprintln!("socket {}/{}: {error:#}", conn.shard, conn.replica),
        }
        if !wait(pause(failures), &mut commands, &mut symbols).await {
            return;
        }
        failures += 1;
    }
}

async fn connect<E: Exchange>(
    exchange: &E,
) -> anyhow::Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let handshake = tokio_tungstenite::connect_async(exchange.request()?);
    let (socket, _response) = timeout(CONNECT_TIMEOUT, handshake)
        .await
        .map_err(|_| anyhow!("no handshake within {CONNECT_TIMEOUT:?}"))??;
    Ok(socket)
}

/// The pause before the next session after `failures` short sessions in a row: doubling from
/// `FIRST_PAUSE` up to `LONGEST_PAUSE`, less a random share of up to half, so that sockets
/// dropped together don't all come back at the same moment.
fn pause(failures: u32) -> Duration {
    let doubled = FIRST_PAUSE.saturating_mul(2_u32.saturating_pow(failures));
    let full = doubled.min(LONGEST_PAUSE);
    full.mul_f64(1.0 - random_fraction() / 2.0)
}

/// A random number in `[0, 1)`, from the operating system.
fn random_fraction() -> f64 {
    let bits = OsRng.next_u64();
    (bits >> 11) as f64 / (1_u64 << 53) as f64
}

/// Sits out `pause` between sessions while applying the commands that arrive. Returns false once
/// the command channel closes.
async fn wait(
    pause: Duration,
    commands: &mut UnboundedReceiver<Command>,
    symbols: &mut BTreeSet<Symbol>,
) -> bool {
    let over = sleep(pause);
    tokio::pin!(over);
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(command) => apply_between_sessions(command, symbols),
                None => return false,
            },
            () = &mut over => return true,
        }
    }
}

/// Applies a command that arrived while no session was subscribed: subscriptions change the
/// symbols the next session subscribes, and refreshes and renewals are dropped, since that
/// session subscribes everything fresh anyway.
fn apply_between_sessions(command: Command, symbols: &mut BTreeSet<Symbol>) {
    match command {
        Command::Subscribe(new) => symbols.extend(new),
        Command::Unsubscribe(old) => {
            for symbol in &old {
                symbols.remove(symbol);
            }
        }
        Command::Refresh(_) | Command::Renew(_) => {}
    }
}

/// One session: subscribes `symbols`, then forwards events and applies commands until it fails,
/// or until the command channel closes and it closes the websocket. The commands that arrived
/// during the handshake are applied before the subscribe, as between sessions, and a channel that
/// closed meanwhile closes the websocket without subscribing.
async fn session<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: WebSocketStream<S>,
    mut codec: impl Codec,
    conn: Conn,
    symbols: &mut BTreeSet<Symbol>,
    commands: &mut UnboundedReceiver<Command>,
    events: &Sender<Received>,
) -> anyhow::Result<()> {
    loop {
        match commands.try_recv() {
            Ok(command) => apply_between_sessions(command, symbols),
            Err(TryRecvError::Empty) => break,
            Err(TryRecvError::Disconnected) => return close(&mut socket).await,
        }
    }
    if !symbols.is_empty() {
        let all: Vec<Symbol> = symbols.iter().cloned().collect();
        send(&mut socket, codec.subscribe(&all)).await?;
    }
    let mut keepalive = interval_at(Instant::now() + KEEPALIVE_EVERY, KEEPALIVE_EVERY);
    let dead = sleep(DEAD_SOCKET_AFTER);
    tokio::pin!(dead);
    loop {
        tokio::select! {
            frame = socket.next() => {
                let received_at = Utc::now();
                dead.as_mut().reset(Instant::now() + DEAD_SOCKET_AFTER);
                match frame.context("the connection dropped")?? {
                    Message::Text(text) => {
                        for event in codec.decode(&text)? {
                            events.send(Received::Event(conn, received_at, event))?;
                        }
                    }
                    Message::Close(frame) => anyhow::bail!("closed by the exchange: {frame:?}"),
                    // Tungstenite answers pings itself; no other frame carries book data.
                    _ => {}
                }
            }
            () = &mut dead => anyhow::bail!("no frame for {DEAD_SOCKET_AFTER:?}"),
            command = commands.recv() => match command {
                Some(Command::Subscribe(new)) => {
                    symbols.extend(new.iter().cloned());
                    send(&mut socket, codec.subscribe(&new)).await?;
                }
                Some(Command::Unsubscribe(old)) => {
                    for symbol in &old {
                        symbols.remove(symbol);
                    }
                    send(&mut socket, codec.unsubscribe(&old)).await?;
                }
                Some(Command::Refresh(stale)) => {
                    send(&mut socket, codec.refresh(&stale)).await?;
                }
                Some(Command::Renew(again)) => {
                    send(&mut socket, codec.renew(&again)).await?;
                }
                None => return close(&mut socket).await,
            },
            _ = keepalive.tick() => send(&mut socket, codec.keepalive()).await?,
        }
    }
}

/// Sends text `frames` in order. Each one has `DEAD_SOCKET_AFTER` to go out.
async fn send<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
    frames: impl IntoIterator<Item = String>,
) -> anyhow::Result<()> {
    for frame in frames {
        within_dead_socket_after(socket.send(Message::text(frame))).await?;
    }
    Ok(())
}

/// Sends a Close frame, which also has `DEAD_SOCKET_AFTER` to go out.
async fn close<S: AsyncRead + AsyncWrite + Unpin>(
    socket: &mut WebSocketStream<S>,
) -> anyhow::Result<()> {
    within_dead_socket_after(socket.close(None)).await
}

/// A write to the websocket, which fails when it hasn't finished within `DEAD_SOCKET_AFTER`.
async fn within_dead_socket_after(
    write: impl Future<Output = Result<(), tokio_tungstenite::tungstenite::Error>>,
) -> anyhow::Result<()> {
    timeout(DEAD_SOCKET_AFTER, write)
        .await
        .map_err(|_| anyhow!("a frame took over {DEAD_SOCKET_AFTER:?} to go out"))??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tokio::io::DuplexStream;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
    use tokio::task::JoinHandle;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::handshake::client::Request;
    use tokio_tungstenite::tungstenite::protocol::Role;

    use super::*;
    use crate::book::Book;

    const CONN: Conn = Conn {
        shard: 3,
        replica: 1,
    };

    /// A codec whose frames spell out what was asked ("sub a b", "unsub a", "refresh a",
    /// "ping"), and renews with the default unsubscribe and subscribe. It decodes "book SYMBOL"
    /// into an empty book, "bad SYMBOL" into an unreadable message, and fails on anything else.
    struct Plain;

    impl Codec for Plain {
        fn subscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
            vec![format!("sub {}", symbols.join(" "))]
        }

        fn unsubscribe(&mut self, symbols: &[Symbol]) -> Vec<String> {
            vec![format!("unsub {}", symbols.join(" "))]
        }

        fn refresh(&mut self, symbols: &[Symbol]) -> Vec<String> {
            vec![format!("refresh {}", symbols.join(" "))]
        }

        fn keepalive(&self) -> Option<String> {
            Some("ping".into())
        }

        fn decode(&mut self, frame: &str) -> anyhow::Result<Vec<Event>> {
            if let Some(symbol) = frame.strip_prefix("book ") {
                return Ok(vec![Event::Book {
                    symbol: symbol.into(),
                    exchange_time: DateTime::UNIX_EPOCH,
                    book: Book::default(),
                }]);
            }
            if let Some(symbol) = frame.strip_prefix("bad ") {
                return Ok(vec![Event::Unreadable {
                    symbol: symbol.into(),
                }]);
            }
            anyhow::bail!("unreadable frame {frame:?}");
        }
    }

    /// An exchange on localhost that speaks `Plain`.
    struct Local {
        url: String,
    }

    impl Exchange for Local {
        type Codec = Plain;

        const SYMBOLS_PER_SOCKET: usize = 10;

        async fn live_page(
            &self,
            _cursor: Option<String>,
        ) -> anyhow::Result<(Vec<Symbol>, Option<String>)> {
            Ok((Vec::new(), None))
        }

        fn request(&self) -> anyhow::Result<Request> {
            Ok(self.url.as_str().into_client_request()?)
        }

        fn codec(&self) -> Plain {
            Plain
        }
    }

    struct Session {
        /// The exchange's end of the websocket.
        exchange: WebSocketStream<DuplexStream>,
        commands: UnboundedSender<Command>,
        events: crossbeam::channel::Receiver<Received>,
        /// How the session ended, and the symbols it carried by then.
        ended: JoinHandle<(anyhow::Result<()>, BTreeSet<Symbol>)>,
    }

    async fn start(symbols: &[&str]) -> Session {
        start_buffered(symbols, 1 << 16).await
    }

    /// A session whose connection holds `buffer` bytes in each direction that the other end
    /// hasn't read yet.
    async fn start_buffered(symbols: &[&str], buffer: usize) -> Session {
        let (client, server) = tokio::io::duplex(buffer);
        let client = WebSocketStream::from_raw_socket(client, Role::Client, None).await;
        let exchange = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let (commands_tx, mut commands) = unbounded_channel();
        let (events_tx, events) = crossbeam::channel::unbounded();
        let mut symbols: BTreeSet<Symbol> = symbols.iter().map(|s| s.to_string()).collect();
        let ended = tokio::spawn(async move {
            let ended = session(client, Plain, CONN, &mut symbols, &mut commands, &events_tx).await;
            (ended, symbols)
        });
        Session {
            exchange,
            commands: commands_tx,
            events,
            ended,
        }
    }

    /// Waits for the socket task to forward its next event.
    async fn next_received(events: &crossbeam::channel::Receiver<Received>) -> Received {
        for _ in 0..1_000 {
            if let Ok(received) = events.try_recv() {
                return received;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("nothing arrived");
    }

    impl Session {
        async fn next_text(&mut self) -> String {
            next_text(&mut self.exchange).await
        }

        async fn say(&mut self, text: &str) {
            self.exchange.send(Message::text(text)).await.unwrap();
        }

        async fn error(self) -> String {
            let (ended, _) = self.ended.await.unwrap();
            format!("{:#}", ended.expect_err("the session should have failed"))
        }
    }

    async fn next_text<S: AsyncRead + AsyncWrite + Unpin>(
        socket: &mut WebSocketStream<S>,
    ) -> String {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => text.to_string(),
            other => panic!("expected a text frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn subscribes_its_symbols_when_the_session_starts() {
        let mut session = start(&["a", "b"]).await;
        assert_eq!(session.next_text().await, "sub a b");
    }

    #[tokio::test]
    async fn forwards_decoded_events_tagged_with_the_socket_and_read_time() {
        let mut session = start(&["a"]).await;
        session.next_text().await;
        let sent = Utc::now();
        session.say("book a").await;
        let Received::Event(conn, received_at, event) = next_received(&session.events).await else {
            panic!("expected an event");
        };
        let forwarded = Utc::now();
        assert_eq!(conn, CONN);
        assert!(
            sent <= received_at && received_at <= forwarded,
            "read after the frame went out and before its event arrived"
        );
        assert!(matches!(event, Event::Book { symbol, .. } if symbol == "a"));
    }

    #[tokio::test]
    async fn an_unreadable_message_about_one_symbol_keeps_the_session_going() {
        let mut session = start(&["a", "b"]).await;
        session.next_text().await;
        session.say("bad a").await;
        session.say("book b").await;
        let symbols: Vec<Event> = [
            next_received(&session.events).await,
            next_received(&session.events).await,
        ]
        .into_iter()
        .map(|received| match received {
            Received::Event(_, _, event) => event,
            Received::Reset(..) => panic!("the session should go on"),
        })
        .collect();
        assert!(matches!(&symbols[0], Event::Unreadable { symbol } if symbol == "a"));
        assert!(matches!(&symbols[1], Event::Book { symbol, .. } if symbol == "b"));
    }

    #[tokio::test]
    async fn applies_subscription_commands_and_closes_cleanly_when_they_stop() {
        let mut session = start(&["a", "b"]).await;
        session.next_text().await;
        let send = |command| session.commands.send(command).unwrap();
        send(Command::Subscribe(vec!["c".into()]));
        send(Command::Unsubscribe(vec!["a".into()]));
        send(Command::Refresh(vec!["b".into()]));
        send(Command::Renew(vec!["b".into()]));
        for expected in ["sub c", "unsub a", "refresh b", "unsub b", "sub b"] {
            assert_eq!(session.next_text().await, expected);
        }

        drop(session.commands);
        assert!(matches!(
            session.exchange.next().await,
            Some(Ok(Message::Close(_)))
        ));
        let (ended, symbols) = session.ended.await.unwrap();
        ended.unwrap();
        assert_eq!(symbols, BTreeSet::from(["b".into(), "c".into()]));
    }

    #[tokio::test(start_paused = true)]
    async fn sends_a_keepalive_every_ten_seconds() {
        let mut session = start(&["a"]).await;
        session.next_text().await;
        let start = Instant::now();
        for keepalives in 1..=3 {
            assert_eq!(session.next_text().await, "ping");
            assert_eq!(start.elapsed(), KEEPALIVE_EVERY * keepalives);
            // The exchange answers each one, so the session stays alive.
            session.say("book a").await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_session_without_frames_for_thirty_seconds_ends() {
        let mut session = start(&["a"]).await;
        session.next_text().await;
        while let Some(Ok(Message::Text(_))) = session.exchange.next().await {}
        assert!(session.error().await.contains("no frame for 30s"));
    }

    /// Enough symbols that a subscribe frame of them doesn't fit in `BUFFER`.
    fn many_symbols() -> Vec<String> {
        (0..2_000).map(|n| format!("symbol-{n:05}")).collect()
    }

    /// The bytes the tests' connections hold unread, as a socket buffer would.
    const BUFFER: usize = 16 << 10;

    /// Waits for the session to fail, which has to happen within `DEAD_SOCKET_AFTER` of `since`
    /// (with a second to spare), and returns its error.
    async fn ended_within_dead_socket_after(session: Session, since: Instant) -> String {
        let ended = timeout(DEAD_SOCKET_AFTER * 20, session.ended).await;
        let (ended, _) = ended.expect("the session should end").unwrap();
        assert!(since.elapsed() <= DEAD_SOCKET_AFTER + Duration::from_secs(1));
        format!("{:#}", ended.expect_err("the session should have failed"))
    }

    #[tokio::test(start_paused = true)]
    async fn an_exchange_that_never_reads_ends_the_session_in_thirty_seconds() {
        let symbols = many_symbols();
        let symbols: Vec<&str> = symbols.iter().map(String::as_str).collect();
        let started = Instant::now();
        let session = start_buffered(&symbols, BUFFER).await;
        let error = ended_within_dead_socket_after(session, started).await;
        assert!(error.contains("a frame took over 30s to go out"), "{error}");
    }

    #[tokio::test(start_paused = true)]
    async fn an_exchange_that_stops_reading_mid_session_ends_the_session_in_thirty_seconds() {
        let mut session = start_buffered(&["a"], BUFFER).await;
        assert_eq!(session.next_text().await, "sub a");
        // The exchange reads the first keepalive and sends a frame, then stops reading, and a
        // subscribe too big for what the connection holds unread can't go out.
        assert_eq!(session.next_text().await, "ping");
        session.say("book a").await;
        next_received(&session.events).await;
        let stopped = Instant::now();
        let subscribe = Command::Subscribe(many_symbols());
        session.commands.send(subscribe).unwrap();
        let error = ended_within_dead_socket_after(session, stopped).await;
        assert!(error.contains("a frame took over 30s to go out"), "{error}");
    }

    #[tokio::test]
    async fn an_unreadable_frame_ends_the_session() {
        let mut session = start(&["a"]).await;
        session.say("garbage").await;
        assert!(session.error().await.contains("unreadable frame"));
    }

    #[tokio::test]
    async fn a_close_from_the_exchange_ends_the_session() {
        let mut session = start(&["a"]).await;
        session.exchange.close(None).await.unwrap();
        assert!(session.error().await.contains("closed by the exchange"));
    }

    #[tokio::test]
    async fn a_connection_that_drops_without_a_close_frame_ends_the_session() {
        let mut session = start(&["a"]).await;
        session.next_text().await;
        let Session {
            exchange, ended, ..
        } = session;
        drop(exchange);
        let (ended, _) = ended.await.unwrap();
        let error = format!("{:#}", ended.unwrap_err());
        assert!(error.contains("without closing handshake"), "{error}");
    }

    /// A socket running against an exchange on localhost, which takes its connections from
    /// `listener`.
    struct Running {
        listener: TcpListener,
        commands: UnboundedSender<Command>,
        events: crossbeam::channel::Receiver<Received>,
        socket: JoinHandle<()>,
    }

    async fn run_locally() -> Running {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let exchange = Arc::new(Local {
            url: format!("ws://{}", listener.local_addr().unwrap()),
        });
        let (commands, commands_rx) = unbounded_channel();
        let (events_tx, events) = crossbeam::channel::unbounded();
        Running {
            listener,
            commands,
            events,
            socket: tokio::spawn(run(exchange, CONN, commands_rx, events_tx)),
        }
    }

    /// How long a test waits for the socket to connect. None of them waits more than a couple
    /// of seconds, the pause before a reconnect.
    const CONNECTS_WITHIN: Duration = Duration::from_secs(10);

    impl Running {
        /// Takes the socket's next connection, before its websocket handshake.
        async fn connection(&self) -> TcpStream {
            let accepted = timeout(CONNECTS_WITHIN, self.listener.accept()).await;
            let accepted = accepted.expect("the socket should connect within 10 seconds");
            accepted.unwrap().0
        }

        /// Takes the socket's next connection, handshake included.
        async fn accept(&self) -> WebSocketStream<TcpStream> {
            let handshake = tokio_tungstenite::accept_async(self.connection().await);
            let shaken = timeout(CONNECTS_WITHIN, handshake).await;
            let shaken = shaken.expect("the socket should shake hands within 10 seconds");
            shaken.unwrap()
        }

        /// Sends every command to the socket, in order.
        fn send(&self, commands: impl IntoIterator<Item = Command>) {
            for command in commands {
                self.commands.send(command).unwrap();
            }
        }
    }

    /// Commands that change the symbols, and the two that the next session makes pointless.
    fn changes_and_pointless_commands() -> [Command; 4] {
        [
            Command::Subscribe(vec!["c".into()]),
            Command::Unsubscribe(vec!["a".into()]),
            Command::Refresh(vec!["b".into()]),
            Command::Renew(vec!["b".into()]),
        ]
    }

    #[tokio::test]
    async fn a_dropped_session_resets_and_the_next_subscribes_every_symbol_with_queued_changes() {
        let running = run_locally().await;
        running.send([Command::Subscribe(vec!["a".into(), "b".into()])]);
        let mut first = running.accept().await;
        assert_eq!(next_text(&mut first).await, "sub a b");
        first.close(None).await.unwrap();
        assert!(matches!(
            next_received(&running.events).await,
            Received::Reset(CONN, _)
        ));

        // Sent while no session is up, during the pause or the next handshake.
        running.send(changes_and_pointless_commands());
        let mut second = running.accept().await;
        assert_eq!(next_text(&mut second).await, "sub b c");

        drop(running.commands);
        assert!(matches!(second.next().await, Some(Ok(Message::Close(_)))));
        running.socket.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_handshake_resets_and_each_retry_waits_twice_as_long() {
        let running = run_locally().await;
        let mut attempts = Vec::new();
        for _ in 0..3 {
            // The exchange hangs up before it answers the handshake.
            let stream = running.connection().await;
            attempts.push(Instant::now());
            drop(stream);
            assert!(matches!(
                next_received(&running.events).await,
                Received::Reset(CONN, _)
            ));
        }
        let (first, second) = (attempts[1] - attempts[0], attempts[2] - attempts[1]);
        assert!(
            FIRST_PAUSE / 2 <= first && first <= FIRST_PAUSE,
            "{first:?}"
        );
        assert!(
            FIRST_PAUSE <= second && second <= FIRST_PAUSE * 2,
            "{second:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_socket_whose_commands_close_while_it_waits_to_reconnect_stops() {
        let running = run_locally().await;
        drop(running.connection().await);
        assert!(matches!(
            next_received(&running.events).await,
            Received::Reset(CONN, _)
        ));
        drop(running.commands);
        running.socket.await.unwrap();
    }

    #[tokio::test]
    async fn commands_sent_during_the_handshake_are_applied_before_the_session_subscribes() {
        let running = run_locally().await;
        running.send([Command::Subscribe(vec!["a".into(), "b".into()])]);
        let stream = running.connection().await;
        running.send(changes_and_pointless_commands());
        let mut exchange = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert_eq!(next_text(&mut exchange).await, "sub b c");

        drop(running.commands);
        assert!(
            matches!(exchange.next().await, Some(Ok(Message::Close(_)))),
            "the refresh and the renewal were dropped"
        );
    }

    #[tokio::test]
    async fn a_socket_whose_commands_close_during_the_handshake_closes_without_subscribing() {
        let running = run_locally().await;
        running.send([Command::Subscribe(vec!["a".into()])]);
        let stream = running.connection().await;
        drop(running.commands);
        let mut exchange = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert!(matches!(exchange.next().await, Some(Ok(Message::Close(_)))));
        running.socket.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_session_that_lived_a_minute_starts_the_pauses_over() {
        let running = run_locally().await;
        // A session that fails at once makes the next pause twice as long.
        running.accept().await.close(None).await.unwrap();
        // A frame every 25 s keeps the next one alive for 75 s.
        let mut healthy = running.accept().await;
        for _ in 0..3 {
            sleep(Duration::from_secs(25)).await;
            healthy.send(Message::text("book a")).await.unwrap();
        }
        healthy.close(None).await.unwrap();
        let closed = Instant::now();
        running.accept().await;
        assert!(closed.elapsed() <= FIRST_PAUSE, "{:?}", closed.elapsed());
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_command_channel_ends_the_pause_between_sessions() {
        let (commands, mut commands_rx) = unbounded_channel::<Command>();
        drop(commands);
        let started = Instant::now();
        assert!(!wait(LONGEST_PAUSE, &mut commands_rx, &mut BTreeSet::new()).await);
        assert!(started.elapsed() < LONGEST_PAUSE);
    }

    #[test]
    fn the_pause_doubles_with_every_failure_up_to_a_minute_less_up_to_half_at_random() {
        for (failures, full) in [(0, 1), (1, 2), (2, 4), (5, 32), (6, 60), (40, 60)] {
            let full = Duration::from_secs(full);
            for _ in 0..20 {
                let pause = pause(failures);
                assert!(full / 2 <= pause && pause <= full, "{failures}: {pause:?}");
            }
        }
        let pauses: BTreeSet<Duration> = (0..20).map(|_| pause(3)).collect();
        assert!(pauses.len() > 1, "the pauses vary");
    }
}
