use std::ops::Deref;

use crate::{Connection, handles::KeepConnectionOpen};

/// A connection which owns additional state, e.g. memory referenced by connection attributes like
/// an access token. Created with [`Connection::with_state`].
///
/// The connection handle is always freed before the state is dropped. `ConnectionWithState` never
/// releases the state while the connection is alive. Code relying on this, e.g. to justify passing
/// a pointer into `state` to the driver, can depend on these guarantees.
pub struct ConnectionWithState<'env, S> {
    // Declared before `state`, so the connection handle is freed before the state is dropped.
    connection: Connection<'env>,
    _state: S,
}

impl<'env> Connection<'env> {
    /// Moves `state` into the connection. It is kept alive for as long as the connection, e.g. to
    /// own memory referenced by connection attributes.
    pub fn with_state<S>(self, state: S) -> ConnectionWithState<'env, S> {
        ConnectionWithState {
            connection: self,
            _state: state,
        }
    }
}

/// # Safety:
///
/// `ConnectionWithState` owns an open connection and keeps it alive during its lifetime.
unsafe impl<S> KeepConnectionOpen for ConnectionWithState<'_, S> {}

impl<'env, S> AsRef<Connection<'env>> for ConnectionWithState<'env, S> {
    fn as_ref(&self) -> &Connection<'env> {
        &self.connection
    }
}

impl<'env, S> Deref for ConnectionWithState<'env, S> {
    type Target = Connection<'env>;

    fn deref(&self) -> &Connection<'env> {
        &self.connection
    }
}
