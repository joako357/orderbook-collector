//! The top of book check, which is how a missed message shows on Polymarket. It has no sequence
//! numbers, but every price change carries the token's best bid and ask after it, and a book
//! whose top differs from those missed a change.

use chrono::{DateTime, Utc};

use crate::book::{Book, Top};

/// The top of book the exchange reported with a replica's latest change, until the replica's
/// book is checked against it.
///
/// During a sweep Polymarket reports the final top on every frame of the sweep's time, so a
/// reported top is only checked once a change with a later time arrives: by then every change of
/// its time is in the book.
#[derive(Default)]
pub(super) struct TopCheck {
    reported: Option<(DateTime<Utc>, Top)>,
}

impl TopCheck {
    /// The exchange reported `top` with the change at `exchange_time`.
    pub(super) fn report(&mut self, top: Top, exchange_time: DateTime<Utc>) {
        self.reported = Some((exchange_time, top));
    }

    /// A whole book arrived, which makes the pending check unnecessary.
    pub(super) fn clear(&mut self) {
        self.reported = None;
    }

    /// Checks `book`, before it takes a change at `exchange_time`, against a top reported for an
    /// earlier time, once. An error means the book missed a change.
    pub(super) fn check(
        &mut self,
        book: &Book,
        exchange_time: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if let Some((reported_at, top)) = self.reported
            && reported_at < exchange_time
        {
            self.reported = None;
            let held = book.top();
            anyhow::ensure!(
                held == top,
                "the exchange reported {top:?}, the book has {held:?}"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;
    use crate::book::tests::at;

    /// `n` milliseconds after the epoch.
    fn ms(n: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::milliseconds(n)
    }

    fn best_bid(price: i64) -> Top {
        Top {
            bid: Some(price),
            ask: None,
        }
    }

    fn bids(levels: &[(i64, i64)]) -> Book {
        Book::new(at(levels), [])
    }

    #[test]
    fn a_top_the_book_differs_from_once_a_later_change_arrives_is_an_error_reported_once() {
        let mut check = TopCheck::default();
        check.report(best_bid(60), ms(30));
        let error = check.check(&bids(&[(50, 1)]), ms(31)).unwrap_err();
        assert!(format!("{error:#}").contains("reported"), "{error:#}");
        assert!(check.check(&bids(&[(50, 1)]), ms(32)).is_ok());
    }

    #[test]
    fn a_top_the_book_matches_passes() {
        let mut check = TopCheck::default();
        check.report(best_bid(60), ms(30));
        assert!(check.check(&bids(&[(60, 1), (50, 2)]), ms(31)).is_ok());
    }

    #[test]
    fn a_top_is_only_checked_once_every_change_of_its_time_is_in() {
        let mut check = TopCheck::default();
        // A sweep: both changes carry time 30, and both report the top after the second.
        check.report(best_bid(60), ms(30));
        assert!(check.check(&bids(&[(50, 2)]), ms(30)).is_ok());
        check.report(best_bid(60), ms(30));
        assert!(check.check(&bids(&[(60, 3), (50, 2)]), ms(31)).is_ok());
    }

    #[test]
    fn a_whole_book_clears_the_pending_check() {
        let mut check = TopCheck::default();
        check.report(best_bid(60), ms(30));
        check.clear();
        assert!(check.check(&bids(&[(50, 1)]), ms(31)).is_ok());
    }
}
