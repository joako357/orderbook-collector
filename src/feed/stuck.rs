//! The stuck renewal (extension 2). A replica is stuck while it trails, or while it waits for its
//! first book, and once it has been stuck for long its subscription is renewed.

use chrono::TimeDelta;

/// How long a replica may be stuck before its first renewal.
const STUCK_AFTER: TimeDelta = TimeDelta::seconds(30);

/// The longest a stuck replica waits for its next renewal.
const MAX_STUCK: TimeDelta = TimeDelta::hours(1);

/// How long one replica may stay stuck before its next renewal. Each renewal doubles it, up to
/// `MAX_STUCK`, and it starts over at `STUCK_AFTER` once the replica streams a change. A first
/// book doesn't start it over, since a broken subscription can still answer every renewal with
/// one.
#[derive(Debug, PartialEq)]
pub(super) struct Stuck {
    wait: TimeDelta,
}

impl Default for Stuck {
    fn default() -> Self {
        Self { wait: STUCK_AFTER }
    }
}

impl Stuck {
    /// Whether a replica that has been stuck for `stuck_for` is due for a renewal.
    pub(super) fn is_due(&self, stuck_for: TimeDelta) -> bool {
        stuck_for >= self.wait
    }

    /// The replica's subscription is renewed, so its next renewal waits twice as long.
    pub(super) fn renewed(&mut self) {
        self.wait = (self.wait * 2).min(MAX_STUCK);
    }

    /// The replica streamed a change, so the next time it gets stuck counts from the start.
    pub(super) fn streamed(&mut self) {
        self.wait = STUCK_AFTER;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: TimeDelta = TimeDelta::milliseconds(1);

    #[test]
    fn a_replica_stuck_for_thirty_seconds_is_due_for_a_renewal() {
        let stuck = Stuck::default();
        assert!(!stuck.is_due(STUCK_AFTER - MS));
        assert!(stuck.is_due(STUCK_AFTER));
    }

    #[test]
    fn each_renewal_doubles_the_wait_up_to_an_hour() {
        let mut stuck = Stuck::default();
        for seconds in [30, 60, 120, 240, 480, 960, 1_920, 3_600, 3_600] {
            let wait = TimeDelta::seconds(seconds);
            assert!(!stuck.is_due(wait - MS) && stuck.is_due(wait), "{wait}");
            stuck.renewed();
        }
    }

    #[test]
    fn a_streamed_change_starts_the_wait_over() {
        let mut stuck = Stuck::default();
        stuck.renewed();
        stuck.renewed();
        stuck.streamed();
        assert!(stuck.is_due(STUCK_AFTER));
    }
}
