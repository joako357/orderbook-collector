//! The silence renewal (extension 2). A symbol that has been changing and then goes silent on
//! every replica for much longer than it usually does gets the replica that isn't the source
//! renewed. If the market is alive, that replica streams again and the output switches to it.

use chrono::{DateTime, TimeDelta, Utc};

use crate::output::Stamp;

/// How many of its usual gaps between changes a symbol may stay silent on every replica before
/// the replica that isn't the source is renewed.
const SILENCE_GAPS: i32 = 20;

/// The shortest silence that renews a replica, however often the symbol usually changes.
pub(super) const MIN_SYMBOL_SILENCE: TimeDelta = TimeDelta::minutes(1);

/// The longest silence a symbol that has been active goes through without a renewal. It also
/// caps the wait that doubles after each renewal of a symbol that stays silent.
const MAX_SYMBOL_SILENCE: TimeDelta = TimeDelta::hours(1);

/// Each new gap moves the usual gap by `1 / GAP_WEIGHT` of the difference.
const GAP_WEIGHT: i32 = 8;

/// How often one symbol changes, and how long it has been silent.
#[derive(Default)]
pub(super) struct Silence {
    /// The newest change any replica streamed.
    last: Option<Stamp>,
    /// The usual time between two changes, as a moving average.
    usual_gap: Option<TimeDelta>,
    /// When the current silence started: at the latest change, or at a fresh book or a renewal
    /// since then.
    since: Option<DateTime<Utc>>,
    /// How long the current silence may last before the next renewal, once a renewal has made
    /// that longer than usual.
    longer_wait: Option<TimeDelta>,
}

impl Silence {
    /// A replica streamed a change. A change no newer than the newest one is the same change on
    /// another replica, or another part of the same exchange event, and doesn't count. A new
    /// change ends the silence, and with it the renewals' longer waits.
    pub(super) fn changed(&mut self, stamp: Stamp) {
        if let Some(last) = self.last {
            if stamp.exchange <= last.exchange {
                return;
            }
            let gap = (stamp.received - last.received).max(TimeDelta::zero());
            self.usual_gap = Some(
                self.usual_gap
                    .map_or(gap, |usual| usual + (gap - usual) / GAP_WEIGHT),
            );
        }
        self.last = Some(stamp);
        self.since = Some(stamp.received);
        self.longer_wait = None;
    }

    /// A replica got a fresh book at `received`, so the silence starts over from there: a
    /// subscription this fresh hasn't gone quiet.
    pub(super) fn restart(&mut self, received: DateTime<Utc>) {
        self.since = self.since.max(Some(received));
    }

    /// Whether the symbol has been silent at `now` for longer than its wait.
    pub(super) fn is_due(&self, now: DateTime<Utc>) -> bool {
        self.since
            .zip(self.wait())
            .is_some_and(|(since, wait)| now - since > wait)
    }

    /// The silence renewed a replica at `now`: the next renewal waits twice as long, up to
    /// `MAX_SYMBOL_SILENCE`, until the symbol changes again.
    pub(super) fn renewed(&mut self, now: DateTime<Utc>) {
        self.since = Some(now);
        self.longer_wait = self.wait().map(|wait| (wait * 2).min(MAX_SYMBOL_SILENCE));
    }

    /// How long the silence may last before it renews a replica: `SILENCE_GAPS` of the usual
    /// gaps, kept between `MIN_SYMBOL_SILENCE` and `MAX_SYMBOL_SILENCE`, unless renewals made it
    /// longer. `None` for a symbol that hasn't changed twice.
    fn wait(&self) -> Option<TimeDelta> {
        let usual = (self.usual_gap? * SILENCE_GAPS).clamp(MIN_SYMBOL_SILENCE, MAX_SYMBOL_SILENCE);
        Some(self.longer_wait.unwrap_or(usual))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: TimeDelta = TimeDelta::seconds(1);

    fn seconds(seconds: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::seconds(seconds)
    }

    fn minutes(minutes: i64) -> DateTime<Utc> {
        DateTime::UNIX_EPOCH + TimeDelta::minutes(minutes)
    }

    /// A change that one replica read the moment it happened.
    fn at(time: DateTime<Utc>) -> Stamp {
        Stamp {
            exchange: time,
            received: time,
        }
    }

    /// A symbol that changed at each of `times`.
    fn changed_at(times: impl IntoIterator<Item = DateTime<Utc>>) -> Silence {
        let mut silence = Silence::default();
        for time in times {
            silence.changed(at(time));
        }
        silence
    }

    #[test]
    fn the_silence_that_renews_is_twenty_usual_gaps_capped_at_an_hour() {
        let mut silence = changed_at([0, 2, 4].map(minutes));
        assert_eq!(silence.usual_gap, Some(TimeDelta::minutes(2)));
        assert!(!silence.is_due(minutes(4 + 40)), "20 gaps are 40 minutes");
        assert!(silence.is_due(minutes(4 + 41)));

        // Gaps of 100 minutes and more make 20 gaps far longer than the cap.
        for minute in [200, 300, 400] {
            silence.changed(at(minutes(minute)));
        }
        let last = minutes(400);
        assert!(!silence.is_due(last + MAX_SYMBOL_SILENCE));
        assert!(silence.is_due(last + MAX_SYMBOL_SILENCE + SECOND));
    }

    #[test]
    fn the_same_change_on_two_replicas_counts_once() {
        let mut silence = Silence::default();
        for second in 1..=3 {
            let exchange = seconds(second);
            for read_after in [5, 600] {
                let received = exchange + TimeDelta::milliseconds(read_after);
                silence.changed(Stamp { exchange, received });
            }
        }
        assert_eq!(silence.usual_gap, Some(SECOND));
    }

    #[test]
    fn a_symbol_that_never_changed_twice_is_never_due() {
        let silence = changed_at([seconds(1)]);
        assert!(!silence.is_due(seconds(1) + TimeDelta::days(1)));
    }

    #[test]
    fn a_fresh_book_starts_the_silence_over() {
        let mut silence = changed_at([0, 1].map(seconds));
        silence.restart(seconds(30));
        silence.restart(seconds(20));
        assert!(!silence.is_due(seconds(30) + MIN_SYMBOL_SILENCE));
        assert!(silence.is_due(seconds(31) + MIN_SYMBOL_SILENCE));
    }

    #[test]
    fn each_renewal_in_one_silence_waits_twice_as_long_as_the_last_up_to_an_hour() {
        let mut silence = changed_at([0, 1].map(seconds));
        let mut since = seconds(1);
        for minutes in [1, 2, 4, 8, 16, 32, 60, 60] {
            let wait = TimeDelta::minutes(minutes);
            assert!(!silence.is_due(since + wait), "{wait}");
            let renewed = since + wait + SECOND;
            assert!(silence.is_due(renewed), "{wait}");
            silence.renewed(renewed);
            since = renewed;
        }
    }

    #[test]
    fn a_change_ends_the_silence_and_its_longer_waits() {
        let mut silence = changed_at([0, 1].map(seconds));
        let renewed = seconds(1) + MIN_SYMBOL_SILENCE + SECOND;
        assert!(silence.is_due(renewed));
        silence.renewed(renewed);
        let changed = renewed + SECOND;
        silence.changed(at(changed));
        assert_eq!(silence.longer_wait, None);
        // 20 of the new usual gaps, between the floor and the cap.
        let wait = silence.usual_gap.unwrap() * SILENCE_GAPS;
        assert!(MIN_SYMBOL_SILENCE < wait && wait < MAX_SYMBOL_SILENCE);
        assert!(!silence.is_due(changed + wait));
        assert!(silence.is_due(changed + wait + SECOND));
    }
}
