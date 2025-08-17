use std::sync::{Arc, Mutex};

use crate::{
    Connection, CursorImpl, Error, ParameterCollectionRef, Prepared,
    connection::{ConnectionTransitions, FailedStateTransition},
    handles::{StatementConnection, StatementParent},
};

/// A convinient type alias in case you want to use a connection from multiple threads which share
/// ownership of it.
pub type SharedConnection<'env> = Arc<Mutex<Connection<'env>>>;

/// # Safety:
///
/// Connection is guaranteed to be alive and in connected state for the lifetime of
/// [`SharedConnection`].
unsafe impl StatementParent for SharedConnection<'_> {}

impl<'env> ConnectionTransitions for SharedConnection<'env> {
    type Cursor = CursorImpl<StatementConnection<Self>>;
    type Prepared = Prepared<StatementConnection<Self>>;

    fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<CursorImpl<StatementConnection<Self>>>, FailedStateTransition<Self>> {
        let guard = self
            .lock()
            .expect("Shared connection lock must not be poisned");
        // Result borrows the connection. We convert the cursor into a raw pointer, to not confuse
        // the borrow checker.
        let result = guard.execute(query, params, query_timeout_sec);
        let result = result.map(|opt| opt.map(|cursor| cursor.into_stmt().into_sys()));
        drop(guard);
        if let Err(error) = result {
            return Err(FailedStateTransition {
                error,
                previous: self,
            });
        }
        let Some(stmt_ptr) = result.unwrap() else {
            return Ok(None);
        };
        // Safe: The connection is the parent of the statement referenced by `stmt_ptr`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self) };
        // Safe: `stmt` is valid and in cursor state.
        let cursor = unsafe { CursorImpl::new(stmt) };
        Ok(Some(cursor))
    }

    fn into_prepared(self, query: &str) -> Result<Self::Prepared, Error> {
        todo!()
    }
}
