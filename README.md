# Order book collector

My solution to the Voluntas engineering challenge. It connects to Kalshi (or Polymarket), subscribes to every live order book across many websockets, and writes every price level change, plus a full snapshot every minute, to stdout with the template's SBE helpers. SIGINT (or SIGTERM) closes every websocket, writes what they read up to then, and closes the output file.

I ran it live on Kalshi's production environment for 15.5 minutes and on Polymarket for 16. The numbers are under [Live runs](#live-runs).

## Running it

The template's reader uses `std::os::unix`, so this builds on Linux and macOS.

Kalshi asks for an API key even for public data. The service reads the key's id from `KALSHI_KEY_ID` and its private key, Ed25519 (Kalshi's default) or RSA in PEM, from the file `KALSHI_KEY_FILE` points to.

```sh
KALSHI_KEY_ID=... KALSHI_KEY_FILE=kalshi.key cargo run --release -- kalshi > books.sbe   # Ctrl+C to stop
cargo run --release --example replay -- books.sbe   # replays the file like a backtest and checks it
```

Polymarket needs no key, and `kalshi-demo` runs against Kalshi's demo environment, whose keys are separate from production's.

```sh
cargo run --release -- polymarket > books.sbe
KALSHI_KEY_ID=... KALSHI_KEY_FILE=kalshi-demo.key cargo run --release -- kalshi-demo > books.sbe
```

The latency experiment of extension 4 is an example of its own, and [LATENCY.md](LATENCY.md) has what it found.

```sh
cargo run --release --example latency   # 8 websockets to Polymarket, 4 rounds of 3 minutes
```

## How it works

```
 listing pages (REST) ──► Collector ──Command──► websockets: shard k, replicas 0 and 1
                          one thread ◄─Received─ Codec::decode(frame)
                              │
                              ▼ Snapshot, PriceLevelUpdate
               AlignedSbeWriter("/dev/stdout")
```

| File | What's in it |
|---|---|
| `src/exchange.rs` | The only two exchange-facing traits, and what both exchanges share: the level and timestamp parsing, and the listing's HTTP request. `Exchange`: listing pages, the handshake, symbols per socket, and how many websockets the exchange allows. `Codec`: one session's subscribe, unsubscribe, refresh and renew frames, keepalive, and decoding into `Event`s. |
| `src/exchange/kalshi.rs`, `src/exchange/polymarket.rs` | The two exchanges, and the only exchange-specific code. |
| `src/socket.rs` | One websocket: subscribes, stamps every frame with its receive time before parsing it, keeps the session alive, and reconnects. |
| `src/command.rs` | What the collector asks of a websocket, and the shard and replica that name each one. |
| `src/listing.rs` | The listing: complete rounds a minute apart, each page retried, the newest pages read again every 10 seconds, and all of it paced while the exchange answers 429. |
| `src/shards.rs` | Which shard carries which symbol, and which symbols two complete listings in a row left out. |
| `src/collector.rs` | Every symbol's feed and the minute snapshots. |
| `src/feed.rs`, `src/feed/` | One symbol across its replicas, which replica the output follows, and when a replica needs a refresh or a renewal. How far a replica trails, the stuck and silence renewals, and the top of book check have a file each. |
| `src/book.rs`, `src/fixed.rs`, `src/output.rs` | The book, exact decimal parsing, and the two output messages with the file they go to. |
| `src/service.rs`, `src/main.rs` | Wiring, and shutdown on SIGINT or SIGTERM. |
| `examples/replay.rs`, `examples/latency.rs` | Replaying and checking an output file, and the latency experiment. |

Every input to the collector brings its own time, so the collector never reads the clock or waits on a socket. That's what makes it testable without a network.

### The output

Each symbol's output starts with a snapshot. After that it only gets price level updates, with the absolute quantity the schema asks for (Kalshi sends relative deltas, so the book does the sum). A snapshot shows up again every minute, on the minute, and whenever the output switches source websocket.

Replaying the output always gives exactly the book of the websocket it follows. A property test checks that over 256 random sessions of up to 79 steps each. On a real file, `examples/replay.rs` replays everything with the template's reader, fails if an update comes before its symbol's first snapshot, and compares every later snapshot with the book rebuilt from the messages before it. A minute snapshot matches by construction, so one that differs comes from a source switch.

Every message carries the exchange's time and a receive time (`received_at` on snapshots, `backtest_timestamp` on updates). An update gets the time my socket read its frame. A snapshot gets the newest receive time the collector has processed from its websocket, so every change its book still lacks was read later, and a quiet websocket's snapshots carry an older time.

Per symbol, receive times in the file never go back: right after a switch, whatever the new source read before the old source's last message takes that message's time. Across symbols the order is close but not exact, since each websocket stamps its own frames.

Polymarket also sends a token's whole book after trades, and often: 64 times in 20 seconds across the 40 busiest tokens when I measured. Those go out as updates of the levels that changed.

The collector hands each message to a sink as it makes it. In the service the sink is the output file, so a minute tick writes its hundreds of thousands of snapshots to the file one at a time, and never holds them all in memory. If a write fails, the service stops with that error, still closes every websocket, and keeps everything written before it.

### Finding symbols

Both exchanges list their markets in pages, newest first: Kalshi 1,000 per page, Polymarket 100. Polymarket's request sorts them by id. Kalshi's listing comes newest first by creation time, which its docs don't promise. Kalshi's request asks for open markets with `mve_filter=exclude`, so multivariate (MVE) markets never get subscribed.

The service lists the markets in complete rounds a minute apart, and subscribes each page as soon as it arrives. Every 10 seconds it also reads the listing again from the first page, going on for as long as each page brings markets it didn't have. New markets show up there, so they get subscribed within seconds, even when more appear at once than a page holds. A market that goes live after newer ones were listed sits deeper in the listing, so it waits for the next round instead.

A page that fails is retried 3 times; if it still fails, that round ends without a complete listing. On a 429 the listing asks for the same page again, with its retries untouched, and spaces out all of its requests: each 429 doubles the gap, from 100 ms up to 10 seconds, and each request that goes through takes a sixteenth off. A Polymarket market whose token ids can't be read is skipped, and the rest of its page is kept.

A symbol is unsubscribed once no page has listed it since the previous complete listing's round started, which takes two complete listings in a row that leave it out. So a listing that ends early, with a page that has no next cursor, or a market created during a Polymarket round (about 8 minutes), can't get anything unsubscribed by mistake.

Kalshi had 177,509 open markets on 2026-11-05, and 205,180 markets had a book in the live run two days later. In Polymarket's live run 473,374 tokens had a book, two per market, which is at least 2,367 pages of the listing.

Each websocket carries 2,000 symbols. Below its Premier tier, [Kalshi allows](https://docs.kalshi.com/getting_started/rate_limits) each user 200 websockets and answers more handshakes with a 429, so the service never opens more than 200. Past 200,000 markets, each new one goes to the shard with the fewest, so shards grow past 2,000. A Premier key (500 websockets) would leave room for more shards.

Kalshi also caps the markets in one subscription (error 26) without publishing the number. In the live run below its 205,180 markets took all 200 websockets, the subscriptions grew to about 2,050 markets each, and none hit error 26. No session reconnected in that run, though, so no single `subscribe` asked for more than 2,000 at once, and I don't know how far past that a shard can grow. If a reconnect's `subscribe` ever passes the cap, the codec only logs the error, and the stuck renewals would ask for the same batch again, so that replica would stay without books. Splitting a session's subscribe into batches of 2,000 is what I'd build next.

### Extension 1: redundant websockets

Every shard of symbols is carried by 2 websockets, and each keeps its own copy of every book. The output follows one of them per symbol, the source. It switches, with a fresh snapshot, when the source loses its book or trails the other replica by 2 seconds. That covers three of the challenge's four cases: a dropped socket, a subscription that stops delivering, and delayed messages.

"Trails" means the other replica streamed a change this one doesn't have yet, and it trails by the age of the oldest such change, counted from when the other replica read it (or from when this one got its book, if that's later). So a replica that reads every change 50 ms after its partner trails by 50 ms, however busy the market. Past 4,096 such changes, the newest ones count as one, as old as the oldest of them, so a replica that far behind can only look further behind than it is. A streamed change is an update, or a whole book that changed levels. A book that changes nothing only says when it was taken, and counting it would flip sources back and forth on markets that rarely change.

A source that trails by less than 2 seconds keeps the output, so a shorter stall reaches the file as late receive times. [LATENCY.md](LATENCY.md) measures those stalls, and what writing whichever replica got a change first would save.

A replica loses its book of a symbol when:

- its session ends: a disconnect, a frame it can't read, a message it can't tie to a symbol, a gap in Kalshi's sequence, or Kalshi reporting that its buffer for the session overflowed, which means it dropped messages;
- a message about that symbol can't be read, say a price with more than six decimals, a negative quantity, or a time before 1970 that the output can't hold. The rest of the frame still decodes and the session goes on, so one bad value costs one book on one websocket;
- a change doesn't fit its book, like a Kalshi delta that would take a level below zero;
- its top of book stops matching Polymarket's;
- its subscription gets renewed, for being stuck or for a silent symbol (extension 2).

Polymarket has no sequence numbers, so the top of book check is how a missed message shows up there. Every price change carries the token's best bid and ask after it, and each replica compares that with its own book. It compares only once a change with a later timestamp arrives, because during a sweep Polymarket reports the final top on every frame of that timestamp. In a sample I measured, 8 of 4,174 frames reported a top their own change hadn't reached yet, all of them mid sweep. A whole book replaces the pending check. When the top really differs, the replica drops its book and refreshes it straight away.

Redundancy covers one websocket failing. It can't cover an exchange that drops every websocket carrying a given symbol, and Polymarket does that with some of its 5-minute crypto "up or down" markets. Of 9 websockets carrying 50 neighbouring tokens each, the one with `polyv2zec-updown-5m-1793910000` and `sol-updown-5m-1793909400` was dropped after 15 seconds, and the other 8 stayed up for the whole 75. Both replicas carry those markets, so both keep dropping, and their shard's 2,000 symbols have no book whenever both are down: up to about a minute at a time, since the wait between sessions doubles while they keep ending inside a minute.

Those markets are created together, so they sit next to each other in the listing, and grouping the two replicas differently would only spread them over more websockets. What I'd build next is a quarantine: markets whose shard keeps dropping on both replicas move to websockets of their own.

The fourth, a process crash, needs a second process, ideally on another machine. I left that out of the code.

### Extension 2: subscription monitoring

- A replica that's stuck on a symbol, trailing the other by 30 seconds or waiting 30 seconds for its first book, gets that subscription renewed. Each renewal doubles the next wait, up to an hour, until the replica streams a change again. A first book doesn't start the wait over, since a broken subscription can still answer every renewal with one.
- A symbol that has been changing and then goes silent on both replicas for more than 20 times its usual gap gets the replica that isn't the source renewed. The usual gap is a moving average of the time between its changes, and the silence it allows stays between 1 minute and 1 hour. If the market is alive, the renewed replica streams again and the lag rule above switches to it. While the silence lasts, each renewal waits twice as long as the one before, up to an hour. A fresh book starts the silence over, so replicas that just got their books back after an outage don't get renewed for it.
- A websocket that receives nothing for 30 seconds is dead, and gets replaced. Kalshi pings every 10 seconds, and Polymarket answers the `PING` the socket sends every 10.

Lag, stuck replicas and silence are counted in receive times: the newest receive time the collector has processed stands in for now. So a backlog in the queue, like the one a minute tick leaves while it writes its snapshots, never makes a replica look late. The clock only says when a minute snapshot is due, and the dead socket check runs on each socket's own timer, so a stalled exchange still gets caught.

A replica gets its book back with one of two commands. A refresh gets a fresh book of a symbol whose book stopped matching the exchange's: a change that doesn't fit, or a top of book that differs. A renewal unsubscribes and subscribes again, for a subscription that stopped delivering. On Kalshi a refresh is `update_subscription` with `get_snapshot`, which sends a new snapshot without touching the subscription, and a renewal is `update_subscription` with `delete_markets` followed by another `subscribe`, which joins the session's existing subscription. On Polymarket both mean unsubscribing and subscribing again. A message that can't be read is left to the stuck check, since refreshing right away would loop on a value that never reads.

To tell a broken subscription from a quiet market, the main reference is the other replica. A market where nothing happens is idle on both replicas and triggers nothing, while a broken subscription is idle on one as the other keeps receiving. The silence rule covers both going idle at once, and it measures each symbol against its own pace.

When a session ends, the socket waits 1 second before the next one, doubling up to a minute while sessions keep failing, less a random share of up to half so that sockets dropped together don't all come back at once. A session that lived a minute after its handshake starts the wait over. Commands that arrive while a socket is down or in its handshake are applied before the next session subscribes: subscriptions change what it subscribes, and refreshes and renewals are dropped, since that session subscribes everything fresh anyway. The websocket handshake and every listing request give up after 30 seconds.

### Extension 3: generic per exchange

An exchange is two traits in `src/exchange.rs`. `Exchange` reads listing pages, builds the handshake, and says how many symbols a websocket carries and how many websockets the exchange allows. `Codec` turns one session's subscriptions into frames and its frames into `Event`s. Kalshi is 456 lines and Polymarket 319 (without tests), and apart from `main.rs`, which picks one, nothing in the service knows which exchange it runs.

Kalshi brings signed handshakes (Ed25519, or RSA-PSS with SHA-256), sequence checks, snapshots on request, and NO bids turned into YES asks; Polymarket brings a text keepalive, two subscription forms, and the top of book it reports with every change.

Kalshi's demo environment showed the details the codec relies on:

- `ok` replies take a number in the subscription's `seq`, so the codec counts them in the sequence.
- An unknown ticker inside a batch `subscribe` gets an `ok`, and the valid markets still subscribe.
- Snapshots carry `sending_ts_ms` and deltas `ts_ms`, and an empty book arrives with no `yes_dollars_fp` or `no_dollars_fp` at all.
- `use_yes_price` prices NO bids as the YES asks they are. On the same markets, the REST order book (which prices the NO leg) had NO bids at 0.89, 0.90 and 0.91, and the websocket sent them as 0.11, 0.10 and 0.09. Without the flag it sent 0.89, 0.90 and 0.91.

### Extension 4: latency

[LATENCY.md](LATENCY.md) has the experiment and its numbers: 8 websockets to the same 200 Polymarket tokens for 15 minutes. With my PC's clock corrected (it was 3.37 seconds behind), an update reached me a median of 20 to 31 ms after Polymarket's timestamp. Every websocket got every update, but each one stalls now and then at its own moments, so taking whichever got an update first cut the p99 from as much as 3.9 seconds on a single websocket to 135 ms in round 3. Replacing the slower websockets with new ones gained at most a millisecond or two at the median, and a chosen source port can't leave Docker Desktop, so I couldn't test that one.

## Live runs

I ran both on 2026-11-07 on GitHub's hosted runners, Linux machines with 2 CPUs and 7.7 GiB of memory: Polymarket from 19:06 UTC, then Kalshi from 19:27. Each ran the release build and stopped with SIGINT. A script sampled the process every 30 seconds for memory, CPU and bytes received, and replayed the file once it stopped.

### Kalshi, 15.5 minutes

- 205,180 markets had a book, on 200 websockets (100 shards of 2 replicas), the most Kalshi allows. No subscription hit error 26.
- Memory reached 716 MiB, and the process used 1.35 of the runner's 2 CPUs on average. It took in 26.5 GB, about 28.5 MB/s.
- It wrote 3.09 GB: 28,419,519 price level updates and 3,231,382 snapshots.
- Kalshi answered 182 of the listing's requests with a 429, about one every 5 seconds. Each 429 set the gap between requests somewhere from 100 to 371 ms, 312 ms at the median, and every page went through.
- No session ended before SIGINT. SIGINT closed every websocket and the file, and the process exited with code 0, 0.4 seconds later.
- `examples/replay.rs` read the file back with the template's reader in 16.6 seconds. Every update came after a snapshot of its symbol, and all 3,026,202 repeated snapshots matched the book replayed from the messages before them.

### Polymarket, 16 minutes

- 473,374 tokens had a book, over 474 websockets (237 shards of 2 replicas). The websockets stopped growing after about 8 minutes, once the first listing was through. The run had one complete listing, so it unsubscribed nothing: that takes two in a row that leave a token out.
- Memory reached 1,546 MiB, and the process used 1.13 of the runner's 2 CPUs on average, 1.63 at most. It took in 21.9 GB, about 22.8 MB/s.
- It wrote 5.44 GB: 30,810,497 price level updates and 6,099,698 snapshots.
- Polymarket ended 8 sessions on 6 of the websockets: 7 closed with "slow consumer: send buffer full", and 1 reset without a closing handshake. Every socket came back with fresh books.
- SIGINT closed every websocket and the file, and the process exited with code 0, 3.0 seconds later.
- `examples/replay.rs` read the file back in 26.8 seconds, with the same checks passing. Of the repeated snapshots, 5,624,441 matched and 1,883 didn't: those mark a new source whose book differed from the one the file held, the other replica or a fresh session after both dropped.

### The dropped sessions

Polymarket's 8 dropped sessions were on 6 of its 474 websockets. I think those carry its busiest markets, like the 5-minute ones above, whose bursts fill Polymarket's send buffer faster than one websocket drains it. The service doesn't log which markets a shard holds, so I can't confirm it. While both replicas of a shard are down its symbols have no book, and the file skips their minute snapshots until a replica is back with a fresh one.

## Tests

`cargo test` runs 204 tests in about 3 seconds, and none of them leaves localhost:

- Unit tests next to the code, in every module but `exchange.rs`, whose shared code the codec and listing tests go through, and `command.rs`, which only declares types. Time is an argument, so lag, stuck replicas, silent symbols, the renewals' growing waits, a backlog in the queue and minute snapshots are all tested without waiting. So are the two complete listings in a row it takes to unsubscribe a symbol, and where a new symbol goes once the shards reach the exchange's limit on websockets. The feed's tests include busy streams: two minutes of a change every 10 ms, with the source 45 ms behind its partner, switch nothing and renew nothing.
- Two property tests for the output's promises above: replaying it gives the source's book, and a symbol's receive times never go back, with each replica's websocket reading at a pace of its own.
- Codec tests on real frames: Polymarket's recorded from its websocket, Kalshi's taken from its docs, plus edited copies with values that can't be read, a timestamp before 1970 among them.
- Socket tests over an in-memory websocket, with tokio's paused clock for keepalives and the dead socket check, which also ends a session whose frame can't go out for 30 seconds because the exchange stopped reading, during the first subscribe or later. And the reconnect loop against a websocket on 127.0.0.1: the server drops the first session, and the second one subscribes every symbol, including what changed while the socket was down or in its handshake. A socket stopped during its handshake sends its Close frame without subscribing anything. Each failed handshake waits twice as long as the one before, and a session that lived a minute starts the pauses over.
- Listing tests: a failing page is retried, a listing whose second page keeps failing sends its first page and no complete listing, a complete listing carries the time its round started, and the re-read goes on to the next page only while a page brings new symbols, which the real collector answers too. Each exchange's listing request is checked, and so are its HTTP calls, against a server on 127.0.0.1 that answers with a page that has a next cursor, the last page, and an HTTP error. Against the same server, a page that gets four 429s in a row, more than its 3 retries, still goes through, and each 429 doubles the gap between requests. Kalshi's request leaves out the MVE markets the challenge rules out.
- A write test: after a message the template's encoder rejects, closing still flushes what was written before it, and then reports the failure.
- Seven end-to-end tests of the whole service against a fake exchange on 127.0.0.1. It follows its frames with a ping and waits for the service's pong, so the interrupt only comes once every frame has been read, and each test reads the output back with the template's reader and checks the books:
  1. 112 recorded Polymarket frames over 4 websockets rebuild the same 8 books, then an interrupt on the channel `main.rs` feeds from SIGINT and SIGTERM closes all 4 with a Close frame.
  2. The same frames, then one replica's websocket drops: its partner's snapshots take over, and the socket reconnects and gets the frames again.
  3. A replica misses the change that moves a token's best bid while its partner hears nothing: the top check catches it, the service unsubscribes and subscribes the token again, and the fake exchange's book makes the file right.
  4. Kalshi's own codec on a session built from the messages in its docs, where a delta that skips a number of the sequence ends the session and the partner takes over its markets with a snapshot of each.
  5. One frame turns into 10,000 updates, most still queued when the interrupt comes, and all of them reach the file.
  6. A write fails: the service stops with that error, keeps what it wrote before it, and closes both websockets.
  7. A write fails after the interrupt, behind 100,000 queued updates, and all of them still reach the file.
- Tests of the latency experiment's arithmetic: matching updates across connections, percentiles, the SNTP packets and the clock correction.

Coverage of my code in `src/` is 98.6% of lines, from `cargo llvm-cov --ignore-filename-regex 'src/(util|side)'`, which leaves out the template's `src/util/` and `src/side.rs`. That figure counts the tests too, since they live in the same files. The code alone, without its test modules, is at 98.5%, and what's left is mostly `main`, which only wires the signals to `serve`.
