//! What the collector asks of each websocket. The collector and the sockets share these, so
//! neither depends on the other.

use crate::exchange::Symbol;

/// One websocket: the shard of symbols it carries, and which of the shard's replicas it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Conn {
    pub shard: usize,
    pub replica: usize,
}

/// What the collector asks of a socket. It hands each command over with the `Conn` it's for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Subscribe(Vec<Symbol>),
    Unsubscribe(Vec<Symbol>),
    /// Get a fresh book of these symbols, whose books stopped matching the exchange's.
    Refresh(Vec<Symbol>),
    /// Unsubscribe these symbols and subscribe them again, since their subscription stopped
    /// delivering.
    Renew(Vec<Symbol>),
}
