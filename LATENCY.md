# Latency

Extension 4 asks how to get exchange data sooner. I measured it on Polymarket with [`examples/latency.rs`](examples/latency.rs): how long an update takes to reach me, how much keeping the fastest of several websockets saves, and whether replacing slow websockets with new ones finds faster ones. Polymarket's websocket needs no key, so anyone can run the experiment again, and its price changes carry a hash that matches the same update across websockets.

## Setup

- My PC in Spain, with the experiment running inside Docker Desktop on Windows 11. Every websocket went through CloudFront's Madrid point of presence (`MAD53-P2`), which sits in front of `ws-subscriptions-clob.polymarket.com`.
- 8 websockets, each subscribed to the same 200 tokens: those of the 100 busiest markets by 24-hour volume.
- A price change is the same update on two websockets when its token, exchange timestamp, price, size, side and hash all match.
- 4 rounds of 3 minutes, each counting the updates Polymarket stamped after it started. After each round the 4 websockets with the highest median lag behind the fastest are closed, and 4 new ones get 15 seconds to subscribe before the next round.

For each websocket the tables give:

- **handshake**: how long connecting took, DNS, TCP, TLS and the websocket upgrade included;
- **behind the fastest**: for every update that reached all 8, how long after the first websocket this one got it;
- **age on arrival**: when it arrived, in true time, minus Polymarket's timestamp on the message;
- **first**: the share of updates it got first, a tie counting for each;
- **missed**: updates other websockets got that this one never did, not even 30 seconds after the round.

Every percentile is the nearest-rank one, so the median of 4 values, printed after each round, is the second lowest.

## The clock

My PC's clock was 3.37 seconds behind true time when the run started. The three SNTP servers I asked agreed on that to within 2 ms, and Windows' own check (`w32tm /stripchart` against `time.cloudflare.com`) had found 3.35 s twenty minutes earlier. The Windows Time service isn't running on this PC, so its clock drifts, and Docker's VM keeps to it: it fell another 17 ms behind during the 15 minutes of the run.

That's more than a hundred times the median latencies below, so every age is on the corrected clock. The experiment queries SNTP before and after each round and spreads the drift evenly between the two. The correction is good to about half the shortest SNTP round trip, 6 ms for the 12 ms to `time.cloudflare.com`. Polymarket stamps its messages in whole milliseconds, which adds up to 1 ms more to each age. The lag behind the fastest needs no correction, since it compares receive times on one clock.

## Results

The run of 2026-11-06, from 13:10 to 13:25 UTC. The full output is under [What the experiment printed](#what-the-experiment-printed). In short:

| Round | Price changes | One websocket, p50 age | One websocket, p99 age | Whichever got it first, p50 and p99 age |
|---:|---:|---:|---:|---:|
| 1 | 114,968 | 21.4 to 23.4 ms | 88.4 to 128.3 ms | 20.4 and 58.0 ms |
| 2 | 158,686 | 22.2 to 24.1 ms | 95.1 to 317.6 ms | 21.2 and 70.4 ms |
| 3 | 175,110 | 26.9 to 28.7 ms | 242.9 to 3,944.5 ms | 24.1 and 135.1 ms |
| 4 | 172,440 | 27.4 to 30.8 ms | 591.4 to 3,162.6 ms | 26.1 and 591.4 ms |

## What helped and what didn't

### Several websockets, taking whichever is first

Every websocket got every update: in the four rounds, all 621,204 price changes reached all 8, and none of them missed one. They just got them at different times.

At the median the 8 were within 3 ms of the fastest, so taking whichever got an update first saved 1 to 5 ms against a single websocket. They differ in the tail. Each websocket stalls now and then, from tens of milliseconds to a few seconds, and they stall at different moments. In round 3 single websockets had p99 ages from 243 ms to 3.9 s, and whichever got each update first had 135 ms.

What all 8 suffer at once, keeping the fastest can't fix. In round 4, 1% of the updates were 591 ms old or more on every websocket, so whatever held them up, it held them up for all 8.

### Reconnecting to find a faster websocket: little to gain

At the median there's little to find: in every round, every websocket's p50 lag behind the fastest stayed under 3 ms. In the tail, a websocket that's fast in one round can be slow in the next. Websocket 12, new in round 3, got 33% and then 44% of the updates first, and in round 4 its p99 lag was 44 ms while the others had 822 ms to 2.6 s. Websockets 0 and 1 were among the fastest in rounds 1 and 2, and had two of the three worst p99 lags in round 4. The half I kept beat the new half in two rounds of three (0.7 against 2.0 ms, 1.6 against 2.7 ms) and lost in the other (2.0 against 1.4 ms).

So looking for one fast websocket buys at most a millisecond or two at the median, and a better tail only until that websocket turns slow. Holding several and taking whichever is first gets the better tail without having to pick.

Nothing at connect time told the fast ones apart either. The 4 websockets new in round 3 were the only ones on the edge address `18.154.43.98` and the quickest to connect of the run (201 to 211 ms), and they included both the fastest and the slowest of that round.

### Another source port: not possible from here

Docker Desktop carries a container's connections through its own process on Windows: with a container holding a connection open, `Get-NetTCPConnection` on Windows showed that connection owned by `com.docker.backend`, from a port Windows picked. So a port chosen inside the container never reaches the network, and my router's NAT sits after that too. Every new websocket already comes from a new port, so the reconnect test above is as close as I could get from here.

### Connecting first after maintenance: not tested

Polymarket restarts its matching engine for maintenance and [announces it ahead](https://docs.polymarket.com/trading/matching-engine), about two days when it can, on Telegram and Discord. None fell inside my runs. Kalshi's maintenance has [a fixed window](https://docs.kalshi.com/getting_started/maintenance_and_pauses), every Thursday from 3:00 to 5:00 AM ET, so that's where I'd test it next.

## What it means for the collector

- Every receive time the collector writes comes from its host's clock, so a collector in production needs NTP on its host.
- The collector already holds 2 websockets per symbol, but each update in its output carries the receive time of the replica it follows. Writing the earlier of the two replicas' receive times would give it the fastest of 2. That needs the same update matched across replicas: by the key above on Polymarket, and on Kalshi with the `clock` that [its docs](https://docs.kalshi.com/websockets/orderbook-updates) say is the same on every connection.
- The collector switches source only once the source has trailed for 2 seconds, and each switch writes a snapshot. Most of the stalls above are shorter than that, so in its output they only show up as late receive times. The longest, up to 3.9 s, would switch the source.

## What the experiment printed

The local clock against SNTP:

- time.google.com:123: 3370.5 ms behind, round trip 30.5 ms
- time.cloudflare.com:123: 3369.9 ms behind, round trip 12.0 ms
- pool.ntp.org:123: 3368.9 ms behind, round trip 17.2 ms

200 tokens of the 100 busiest markets, 8 connections, 4 rounds of 180 s

Round 1, from 13:10:16 UTC: 114968 price changes, 114968 of them on all 8 connections. The local clock was 3369.3 ms behind at the start and 3373.2 ms behind at the end.

| conn | from round | pop | edge | handshake | first | behind the fastest, ms (p50 / p90 / p99) | age on arrival, ms (p50 / p90 / p99) | missed |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | MAD53-P2 | 18.154.43.106 | 223 ms | 26% | 0.6 / 6.7 / 54.0 | 21.7 / 36.6 / 99.5 | 0 |
| 1 | 1 | MAD53-P2 | 18.154.43.106 | 213 ms | 29% | 0.4 / 5.0 / 48.2 | 21.4 / 35.3 / 88.4 | 0 |
| 2 | 1 | MAD53-P2 | 18.154.43.106 | 222 ms | 3% | 2.4 / 9.1 / 63.3 | 23.1 / 38.6 / 100.8 | 0 |
| 3 | 1 | MAD53-P2 | 18.154.43.106 | 223 ms | 9% | 1.5 / 7.9 / 65.2 | 22.3 / 37.2 / 104.2 | 0 |
| 4 | 1 | MAD53-P2 | 18.154.43.106 | 237 ms | 2% | 2.9 / 9.6 / 72.7 | 23.4 / 38.6 / 128.3 | 0 |
| 5 | 1 | MAD53-P2 | 18.154.43.106 | 213 ms | 9% | 1.9 / 9.0 / 54.2 | 22.6 / 39.1 / 90.8 | 0 |
| 6 | 1 | MAD53-P2 | 18.154.43.106 | 212 ms | 16% | 1.0 / 8.1 / 71.2 | 21.9 / 38.9 / 124.4 | 0 |
| 7 | 1 | MAD53-P2 | 18.154.43.106 | 216 ms | 4% | 2.1 / 8.1 / 54.7 | 22.8 / 37.1 / 96.9 | 0 |
| whichever got it first |  |  |  |  |  |  | 20.4 / 28.6 / 58.0 |  |

Round 2, from 13:14:05 UTC: 158686 price changes, 158686 of them on all 8 connections. The local clock was 3373.9 ms behind at the start and 3377.8 ms behind at the end.

| conn | from round | pop | edge | handshake | first | behind the fastest, ms (p50 / p90 / p99) | age on arrival, ms (p50 / p90 / p99) | missed |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | MAD53-P2 | 18.154.43.106 | 223 ms | 24% | 0.7 / 7.1 / 148.0 | 22.4 / 37.3 / 198.4 | 0 |
| 1 | 1 | MAD53-P2 | 18.154.43.106 | 213 ms | 20% | 0.7 / 5.0 / 61.5 | 22.3 / 36.2 / 109.8 | 0 |
| 3 | 1 | MAD53-P2 | 18.154.43.106 | 223 ms | 7% | 1.7 / 8.9 / 74.7 | 23.2 / 39.3 / 135.1 | 0 |
| 6 | 1 | MAD53-P2 | 18.154.43.106 | 212 ms | 11% | 1.2 / 7.6 / 157.7 | 22.7 / 39.1 / 206.4 | 0 |
| 8 | 2 | MAD53-P2 | 18.154.43.106 | 216 ms | 4% | 2.0 / 9.1 / 270.3 | 23.3 / 40.4 / 317.6 | 0 |
| 9 | 2 | MAD53-P2 | 18.154.43.106 | 218 ms | 6% | 2.0 / 10.6 / 63.3 | 23.4 / 42.4 / 103.8 | 0 |
| 10 | 2 | MAD53-P2 | 18.154.43.106 | 213 ms | 22% | 0.6 / 4.5 / 87.5 | 22.2 / 35.0 / 147.4 | 0 |
| 11 | 2 | MAD53-P2 | 18.154.43.106 | 238 ms | 3% | 2.6 / 9.1 / 53.0 | 24.1 / 41.2 / 95.1 | 0 |
| whichever got it first |  |  |  |  |  |  | 21.2 / 30.4 / 70.4 |  |

The median p50 behind the fastest: 0.7 ms on the connections kept from round 1, 2.0 ms on the new ones.

Round 3, from 13:17:52 UTC: 175110 price changes, 175110 of them on all 8 connections. The local clock was 3379.1 ms behind at the start and 3383.4 ms behind at the end.

| conn | from round | pop | edge | handshake | first | behind the fastest, ms (p50 / p90 / p99) | age on arrival, ms (p50 / p90 / p99) | missed |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | MAD53-P2 | 18.154.43.106 | 223 ms | 12% | 2.0 / 136.2 / 981.2 | 28.1 / 196.5 / 1077.6 | 0 |
| 1 | 1 | MAD53-P2 | 18.154.43.106 | 213 ms | 10% | 1.8 / 64.0 / 359.6 | 27.5 / 128.4 / 439.6 | 0 |
| 6 | 1 | MAD53-P2 | 18.154.43.106 | 212 ms | 7% | 2.1 / 105.1 / 418.7 | 28.0 / 166.5 / 505.4 | 0 |
| 10 | 2 | MAD53-P2 | 18.154.43.106 | 213 ms | 9% | 2.1 / 326.9 / 2269.0 | 28.4 / 420.6 / 2355.4 | 0 |
| 12 | 3 | MAD53-P2 | 18.154.43.98 | 201 ms | 33% | 0.3 / 17.4 / 201.7 | 27.0 / 89.2 / 242.9 | 0 |
| 13 | 3 | MAD53-P2 | 18.154.43.98 | 203 ms | 6% | 2.7 / 276.7 / 2448.1 | 28.7 / 357.4 / 2533.5 | 0 |
| 14 | 3 | MAD53-P2 | 18.154.43.98 | 210 ms | 14% | 1.4 / 10.1 / 222.0 | 26.9 / 84.6 / 267.2 | 0 |
| 15 | 3 | MAD53-P2 | 18.154.43.98 | 211 ms | 6% | 2.5 / 258.0 / 3882.6 | 28.0 / 328.6 / 3944.5 | 0 |
| whichever got it first |  |  |  |  |  |  | 24.1 / 66.3 / 135.1 |  |

The median p50 behind the fastest: 2.0 ms on the connections kept from round 2, 1.4 ms on the new ones.

Round 4, from 13:21:39 UTC: 172440 price changes, 172440 of them on all 8 connections. The local clock was 3381.8 ms behind at the start and 3386.4 ms behind at the end.

| conn | from round | pop | edge | handshake | first | behind the fastest, ms (p50 / p90 / p99) | age on arrival, ms (p50 / p90 / p99) | missed |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 1 | MAD53-P2 | 18.154.43.106 | 223 ms | 12% | 1.9 / 489.4 / 2640.6 | 29.9 / 569.7 / 3162.6 | 0 |
| 1 | 1 | MAD53-P2 | 18.154.43.106 | 213 ms | 10% | 1.7 / 686.9 / 2238.1 | 29.5 / 805.4 / 2787.3 | 0 |
| 12 | 3 | MAD53-P2 | 18.154.43.98 | 201 ms | 44% | 0.0 / 2.5 / 43.9 | 27.4 / 119.8 / 591.4 | 0 |
| 14 | 3 | MAD53-P2 | 18.154.43.98 | 210 ms | 11% | 1.6 / 15.7 / 822.1 | 29.2 / 127.8 / 1391.6 | 0 |
| 16 | 4 | MAD53-P2 | 18.154.43.106 | 268 ms | 5% | 2.7 / 467.0 / 1967.9 | 30.5 / 634.5 / 2501.4 | 0 |
| 17 | 4 | MAD53-P2 | 18.154.43.106 | 269 ms | 4% | 2.7 / 467.0 / 1228.4 | 30.3 / 693.2 / 1532.0 | 0 |
| 18 | 4 | MAD53-P2 | 18.154.43.106 | 267 ms | 6% | 2.7 / 351.9 / 2125.9 | 30.8 / 471.1 / 2679.7 | 0 |
| 19 | 4 | MAD53-P2 | 18.154.43.106 | 257 ms | 4% | 2.9 / 693.0 / 2447.3 | 30.7 / 810.6 / 2976.1 | 0 |
| whichever got it first |  |  |  |  |  |  | 26.1 / 116.1 / 591.4 |  |

The median p50 behind the fastest: 1.6 ms on the connections kept from round 3, 2.7 ms on the new ones.

The local clock against SNTP, after the run:

- time.google.com:123: 3387.7 ms behind, round trip 31.9 ms
- time.cloudflare.com:123: 3386.6 ms behind, round trip 12.0 ms
- pool.ntp.org:123: 3388.1 ms behind, round trip 13.1 ms
