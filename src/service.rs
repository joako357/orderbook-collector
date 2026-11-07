//! The running service: the listing and the exchange's sockets on the async runtime, the
//! collector on the calling thread, and the output file, until an interrupt arrives.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, tick, unbounded};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;

use crate::collector::Collector;
use crate::command::{Command, Conn};
use crate::exchange::{Exchange, Symbol};
use crate::listing::{self, Pace};
use crate::output::OutputFile;
use crate::socket::{self, Received};

/// How long the sockets get, once interrupted, to close their websockets and hand over what they
/// read before that.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Collects `exchange` into the SBE file at `output` until `interrupted` receives, then closes
/// every websocket, writes what they read until then, and closes the file. Every way out closes
/// the websockets and the file, which keeps everything written before a write that failed.
pub fn serve<E: Exchange>(
    exchange: E,
    runtime: &Runtime,
    output: impl AsRef<Path>,
    interrupted: Receiver<()>,
) -> anyhow::Result<()> {
    let exchange = Arc::new(exchange);
    let mut file = OutputFile::create(output)?;
    let mut collector = Collector::new(E::SYMBOLS_PER_SOCKET, E::MAX_SOCKETS, Utc::now());
    let (events_tx, events) = unbounded::<Received>();
    let (pages_tx, pages) = unbounded();
    let (listings_tx, listings) = unbounded();
    let (newest_tx, newest) = unbounded();
    let pace = Arc::new(Pace::default());
    let listing = [
        runtime.spawn(listing::rounds(
            exchange.clone(),
            pace.clone(),
            pages_tx,
            listings_tx,
        )),
        runtime.spawn(listing::newest(exchange.clone(), pace, newest_tx)),
    ];
    let mut sockets: HashMap<Conn, UnboundedSender<Command>> = HashMap::new();

    // The loop runs in a closure so that every way out of it reaches the shutdown below.
    let collected = (|| -> anyhow::Result<()> {
        let ticks = tick(Duration::from_secs(1));
        loop {
            let commands = crossbeam::select! {
                recv(events) -> received => match received? {
                    Received::Event(conn, at, event) => {
                        collector.on_event(conn, at, event, &mut file)
                    }
                    Received::Reset(conn, at) => {
                        collector.on_reset(conn, at, &mut file);
                        Vec::new()
                    }
                },
                recv(pages) -> page => collector.add_live(&page?, Utc::now()),
                recv(newest) -> page => {
                    let (page, had_new) = page?;
                    newest_page(&mut collector, &page, had_new, Utc::now())
                }
                recv(listings) -> listing => {
                    let (started, live) = listing?;
                    collector.set_live(&live, started, Utc::now())
                }
                recv(ticks) -> _ => collector.on_tick(Utc::now(), &mut file),
                recv(interrupted) -> _ => return Ok(()),
            };
            file.check()?;
            for (conn, command) in commands {
                let sender = sockets.entry(conn).or_insert_with(|| {
                    let (sender, commands) = unbounded_channel();
                    let socket = socket::run(exchange.clone(), conn, commands, events_tx.clone());
                    runtime.spawn(socket);
                    sender
                });
                let _ = sender.send(command);
            }
        }
    })();

    for task in &listing {
        task.abort();
    }
    // Dropping the command senders makes every socket close its websocket and end. Until the last
    // one has, or `CLOSE_TIMEOUT` passes, the events they read before that are still applied
    // after an interrupt. Their resets are not: the books end as the sockets left them.
    drop(sockets);
    drop(events_tx);
    let deadline = Instant::now() + CLOSE_TIMEOUT;
    while let Ok(received) = events.recv_deadline(deadline) {
        if let Received::Event(conn, at, event) = received
            && collected.is_ok()
        {
            collector.on_event(conn, at, event, &mut file);
        }
    }
    let closed = file.close();
    collected.and(closed)
}

/// Subscribes the new symbols of a page that `listing::newest` sent, read at `listed`, and answers
/// on `had_new` whether there were any, which is what decides whether it reads on.
fn newest_page(
    collector: &mut Collector,
    page: &[Symbol],
    had_new: oneshot::Sender<bool>,
    listed: DateTime<Utc>,
) -> Vec<(Conn, Command)> {
    let commands = collector.add_live(page, listed);
    let _ = had_new.send(!commands.is_empty());
    commands
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(symbols: &[&str]) -> Vec<Symbol> {
        symbols.iter().map(|symbol| symbol.to_string()).collect()
    }

    #[test]
    fn a_newest_page_answers_whether_the_collector_subscribed_a_new_symbol_from_it() {
        let mut collector = Collector::new(10, usize::MAX, Utc::now());
        let read = |collector: &mut Collector, symbols: &[&str]| {
            let (had_new_tx, mut had_new) = oneshot::channel();
            let commands = newest_page(collector, &page(symbols), had_new_tx, Utc::now());
            (commands, had_new.try_recv().unwrap())
        };
        let (commands, had_new) = read(&mut collector, &["a", "b"]);
        assert!(had_new);
        assert_eq!(commands.len(), 2, "one subscribe per replica");
        assert_eq!(read(&mut collector, &["b", "a"]), (Vec::new(), false));
        let (commands, had_new) = read(&mut collector, &["c", "a"]);
        assert!(had_new);
        assert!(
            commands
                .iter()
                .all(|(_, command)| *command == Command::Subscribe(page(&["c"])))
        );
    }
}
