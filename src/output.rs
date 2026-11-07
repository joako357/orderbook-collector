//! The two messages this service writes, in the template's SBE format, and the file they go to.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, Utc};

use crate::Side;
use crate::book::{Book, Change, Level};
use crate::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use crate::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

/// The writer's chunk size. It has to be 64 MiB, the only size the template's reader opens.
const CHUNK: usize = 64 << 20;

/// When a message happened: the exchange's own time, and when this service read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Stamp {
    pub exchange: DateTime<Utc>,
    pub received: DateTime<Utc>,
}

/// Where the collector puts what it writes, one message at a time.
pub(crate) trait Sink {
    /// Takes the next message of the output.
    fn put(&mut self, message: Output);
}

/// One message of the output.
#[derive(Debug, PartialEq)]
pub enum Output {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

impl Output {
    /// The whole book of `symbol`.
    pub(crate) fn snapshot(symbol: &str, book: &Book, stamp: Stamp) -> Self {
        Self::Snapshot(SnapshotDomain {
            received_at: stamp.received,
            exchange_timestamp: stamp.exchange,
            bids: book
                .bids()
                .map(|Level { price, quantity }| SnapshotBidsEntryDomain {
                    price_level: price,
                    quantity,
                })
                .collect(),
            asks: book
                .asks()
                .map(|Level { price, quantity }| SnapshotAsksEntryDomain {
                    price_level: price,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        })
    }

    /// One level of `symbol`, with its new absolute quantity; zero means the level is gone.
    pub(crate) fn update(symbol: &str, side: Side, level: Level, stamp: Stamp) -> Self {
        Self::Update(PriceLevelUpdateDomain {
            backtest_timestamp: stamp.received,
            exchange_timestamp: stamp.exchange,
            price_level: level.price,
            quantity: level.quantity,
            side,
            symbol: symbol.to_owned(),
        })
    }

    /// Appends this message to the output file.
    fn write(&self, writer: &mut AlignedSbeWriter) -> anyhow::Result<()> {
        match self {
            Self::Snapshot(snapshot) => writer.write_msg(snapshot),
            Self::Update(update) => writer.write_msg(update),
        }
    }

    /// Reads back, one at a time and in order, every message of a file this service wrote.
    pub fn read_each(
        path: impl AsRef<Path>,
        mut each: impl FnMut(Self) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
        while let Some(message) = reader.read_msg()? {
            each(match message {
                AnyMessage::Snapshot(decoder) => {
                    Self::Snapshot(SnapshotDomain::try_from_decoder(decoder)?)
                }
                AnyMessage::PriceLevelUpdate(decoder) => {
                    Self::Update(PriceLevelUpdateDomain::try_from_decoder(decoder)?)
                }
                _ => anyhow::bail!("unknown message in the output file"),
            })?;
        }
        Ok(())
    }

    /// Applies this message to the books a reader of the output holds, the way a backtest
    /// replays it. An update for a symbol that has had no snapshot is an error: the output always
    /// starts a symbol with one.
    pub fn replay(&self, books: &mut BTreeMap<String, Book>) -> anyhow::Result<()> {
        match self {
            Self::Snapshot(snapshot) => {
                let bids = snapshot.bids.iter().map(|entry| Level {
                    price: entry.price_level,
                    quantity: entry.quantity,
                });
                let asks = snapshot.asks.iter().map(|entry| Level {
                    price: entry.price_level,
                    quantity: entry.quantity,
                });
                books.insert(snapshot.symbol.clone(), Book::new(bids, asks));
            }
            Self::Update(update) => {
                let book = books.get_mut(&update.symbol).with_context(|| {
                    format!("update for {} before its first snapshot", update.symbol)
                })?;
                book.apply(
                    update.side,
                    update.price_level,
                    Change::Set(update.quantity),
                )?;
            }
        }
        Ok(())
    }
}

/// The output file, as the collector's sink. Once a write fails it writes nothing more, and keeps
/// the error for `check` or `close` to report.
pub(crate) struct OutputFile {
    writer: AlignedSbeWriter,
    error: Option<anyhow::Error>,
}

impl OutputFile {
    pub(crate) fn create(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        Ok(Self {
            writer: AlignedSbeWriter::create(path, CHUNK)?,
            error: None,
        })
    }

    /// The first write that failed since the last check, if any.
    pub(crate) fn check(&mut self) -> anyhow::Result<()> {
        self.error.take().map_or(Ok(()), Err)
    }

    /// Flushes what is left and closes the file, then reports a write that failed since the last
    /// check.
    pub(crate) fn close(mut self) -> anyhow::Result<()> {
        let flushed = self.writer.flush();
        self.check().and(flushed)
    }
}

impl Sink for OutputFile {
    fn put(&mut self, message: Output) {
        if self.error.is_none()
            && let Err(error) = message.write(&mut self.writer)
        {
            self.error = Some(error);
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::book::tests::at;

    /// What the collector and the feeds write, for their tests to read.
    impl Sink for Vec<Output> {
        fn put(&mut self, message: Output) {
            self.push(message);
        }
    }

    /// An exchange time and a later receive time, so that swapping them shows.
    fn stamp() -> Stamp {
        Stamp {
            exchange: DateTime::from_timestamp_millis(1_700_000_000_123).unwrap(),
            received: DateTime::from_timestamp_micros(1_700_000_000_456_789).unwrap(),
        }
    }

    #[test]
    fn messages_round_trip_through_the_template_writer_and_reader() {
        let book = Book::new(
            at(&[(400_000, 2_000_000), (390_000, 1)]),
            at(&[(600_000, 5)]),
        );
        let removed = Level {
            price: 600_000,
            quantity: 0,
        };
        let messages = vec![
            Output::snapshot("EXAMPLE", &book, stamp()),
            Output::update("EXAMPLE", Side::Sell, removed, stamp()),
        ];

        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = AlignedSbeWriter::create(file.path(), CHUNK).unwrap();
        for message in &messages {
            message.write(&mut writer).unwrap();
        }
        writer.flush().unwrap();

        let mut read = Vec::new();
        Output::read_each(file.path(), |message| {
            read.push(message);
            Ok(())
        })
        .unwrap();
        assert_eq!(read, messages);
    }

    #[test]
    fn a_snapshot_is_stamped_with_the_receive_time_and_the_exchange_time() {
        let Output::Snapshot(snapshot) = Output::snapshot("S", &Book::default(), stamp()) else {
            panic!("expected a snapshot");
        };
        assert_eq!(snapshot.received_at, stamp().received);
        assert_eq!(snapshot.exchange_timestamp, stamp().exchange);
    }

    #[test]
    fn an_update_is_stamped_with_the_receive_time_as_backtest_time_and_the_exchange_time() {
        let level = Level {
            price: 40,
            quantity: 1,
        };
        let Output::Update(update) = Output::update("S", Side::Buy, level, stamp()) else {
            panic!("expected an update");
        };
        assert_eq!(update.backtest_timestamp, stamp().received);
        assert_eq!(update.exchange_timestamp, stamp().exchange);
    }

    #[test]
    fn a_snapshot_lists_both_sides_best_first() {
        let book = Book::new(at(&[(1, 10), (3, 30)]), at(&[(7, 70), (5, 50)]));
        let Output::Snapshot(snapshot) = Output::snapshot("S", &book, stamp()) else {
            panic!("expected a snapshot");
        };
        let bids: Vec<i64> = snapshot
            .bids
            .iter()
            .map(|level| level.price_level)
            .collect();
        let asks: Vec<i64> = snapshot
            .asks
            .iter()
            .map(|level| level.price_level)
            .collect();
        assert_eq!((bids, asks), (vec![3, 1], vec![5, 7]));
    }

    #[test]
    fn replaying_a_snapshot_then_updates_rebuilds_the_book() {
        let mut books = BTreeMap::new();
        let snapshot = Book::new(at(&[(40, 1)]), at(&[(60, 2)]));
        Output::snapshot("S", &snapshot, stamp())
            .replay(&mut books)
            .unwrap();
        let removed = Level {
            price: 60,
            quantity: 0,
        };
        Output::update("S", Side::Sell, removed, stamp())
            .replay(&mut books)
            .unwrap();
        assert_eq!(books["S"], Book::new(at(&[(40, 1)]), []));
    }

    #[test]
    fn replaying_an_update_before_its_symbol_has_a_snapshot_is_an_error() {
        let level = Level {
            price: 40,
            quantity: 1,
        };
        let update = Output::update("S", Side::Buy, level, stamp());
        assert!(update.replay(&mut BTreeMap::new()).is_err());
    }

    #[test]
    fn replaying_an_update_with_a_quantity_below_zero_is_an_error() {
        let mut books = BTreeMap::new();
        let snapshot = Output::snapshot("S", &Book::default(), stamp());
        snapshot.replay(&mut books).unwrap();
        let level = Level {
            price: 40,
            quantity: -1,
        };
        let update = Output::update("S", Side::Buy, level, stamp());
        assert!(update.replay(&mut books).is_err());
    }

    #[test]
    fn closing_after_a_write_failed_flushes_what_was_written_before_it_and_reports_the_failure() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut output = OutputFile::create(file.path()).unwrap();
        let epoch = Stamp {
            exchange: DateTime::UNIX_EPOCH,
            received: DateTime::UNIX_EPOCH,
        };
        // The template's encoder can't write a time before 1970.
        let before_1970 = Stamp {
            exchange: DateTime::UNIX_EPOCH - TimeDelta::milliseconds(1),
            ..epoch
        };
        output.put(Output::snapshot("written", &Book::default(), epoch));
        output.put(Output::snapshot("failed", &Book::default(), before_1970));
        output.put(Output::snapshot("dropped", &Book::default(), epoch));

        let error = format!("{:#}", output.close().unwrap_err());
        assert!(error.contains("exchange_timestamp"), "{error}");
        let mut read = Vec::new();
        Output::read_each(file.path(), |message| {
            read.push(message);
            Ok(())
        })
        .unwrap();
        assert_eq!(read, [Output::snapshot("written", &Book::default(), epoch)]);
    }
}
