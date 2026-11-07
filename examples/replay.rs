//! Replays an output file the way a backtest would, and checks it: every symbol must start with a
//! snapshot, and every later snapshot is compared with the book rebuilt from the messages before
//! it. Minute snapshots always match; a snapshot that differs marks a switch of source websocket.
//!
//! Usage: `cargo run --example replay -- books.sbe`

use std::collections::BTreeMap;

use anyhow::Context;
use engineering_challenge::output::Output;

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: replay <output file>")?;
    let mut books = BTreeMap::new();
    let (mut updates, mut snapshots, mut matching, mut differing) = (0, 0, 0, 0);
    Output::read_each(&path, |message| {
        match &message {
            Output::Update(_) => updates += 1,
            Output::Snapshot(snapshot) => {
                snapshots += 1;
                if let Some(held) = books.get(&snapshot.symbol) {
                    let mut carried = BTreeMap::new();
                    message.replay(&mut carried)?;
                    if carried[&snapshot.symbol] == *held {
                        matching += 1;
                    } else {
                        differing += 1;
                    }
                }
            }
        }
        message.replay(&mut books)
    })?;
    println!("symbols: {}", books.len());
    println!("updates: {updates}, every one after a snapshot of its symbol");
    println!(
        "snapshots: {snapshots}; repeats matching the replayed book: {matching}, differing: {differing}"
    );
    Ok(())
}
