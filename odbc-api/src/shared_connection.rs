use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    Connection, CursorImpl, Error, OwnedCursor, ParameterCollectionRef, Preallocated, Prepared,
    connection::{ConnectionTransitions, FailedStateTransition},
    handles::{KeepConnectionOpen, StatementConnection},
};

/// Shared ownership of a connection, which can be used from multiple threads. E.g.
/// `Shared<Connection<'env>>`.
///
/// As opposed to an `Arc<Mutex<C>>` it never grants mutable access to `C`. Otherwise safe code could
/// replace the connection, while statements are still allocated on it.
/// 
/// This allows shared connections to be a shared paren of several prepared statements, because it
/// is safe to implement [`KeepConnectionOpen`] for it if the underlying type is
/// [`KeepConnectionOpen`].
#[derive(Debug)]
pub struct Shared<C>(Arc<Mutex<C>>);

impl<C> Shared<C> {
    pub fn new(connection: C) -> Self {
        Self(Arc::new(Mutex::new(connection)))
    }

    /// Acquires exclusive access to the connection, blocking the current thread until it is able
    /// to do so.
    pub fn lock(&self) -> SharedGuard<'_, C> {
        let connection = self
            .0
            .lock()
            .expect("Shared connection lock must not be poisoned");
        // Safe: `connection` is the guard of the mutex inside `self`.
        unsafe { SharedGuard::new(self, connection) }
    }
}

// Implemented manually, since derive would require `C: Clone`.
impl<C> Clone for Shared<C> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// # Safety:
///
/// `Shared` keeps `C` alive and never grants mutable access to it, so it can not be replaced.
/// 
/// Implementing this trait is the "raison d'être" for [`Shared`]. Using `Arc<Mutex<Connection>>`
/// instead would open a soundness hole, as someone owning a shared reference can not assume no
/// other owner will simply swap out the implementing Connection with a different one. This would
/// prevent an `Arc<Mutex<>>` to be a suitable owned parent for a statement handle.
unsafe impl<C> KeepConnectionOpen for Shared<C> where C: KeepConnectionOpen {}

/// Exclusive access to a [`Shared`] connection for as long as the guard lives. Instances are
/// created using [`Shared::lock`].
///
/// As opposed to a plain `MutexGuard` it can implement [`ConnectionTransitions`], because it also
/// has a reference to the [`Shared`] connection.
#[derive(Debug)]
pub struct SharedGuard<'a, C> {
    shared: &'a Shared<C>,
    connection: MutexGuard<'a, C>,
}

impl<'a, C> SharedGuard<'a, C> {
    /// # Safety
    ///
    /// `connection` must be the guard of the mutex inside `shared`.
    unsafe fn new(shared: &'a Shared<C>, connection: MutexGuard<'a, C>) -> Self {
        Self { shared, connection }
    }

    /// The locked connection, e.g. to query information about it.
    pub fn connection(&self) -> &C {
        &self.connection
    }
}

/// Implemented for a reference, so the guard is not consumed. Several statements can be created
/// while holding the lock only once. Each statement owns a clone of the shared connection.
impl<'env> ConnectionTransitions for &SharedGuard<'_, Connection<'env>> {
    type StatementParent = Shared<Connection<'env>>;

    fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<OwnedCursor<Self::StatementParent>>, FailedStateTransition<Self>> {
        let cursor = match self.connection.execute(query, params, query_timeout_sec) {
            Ok(Some(cursor)) => cursor,
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(FailedStateTransition {
                    error,
                    previous: self,
                });
            }
        };
        let stmt_ptr = cursor.into_stmt().into_sys();
        // Safe: `shared` owns the connection the statement has been allocated on. See
        // `SharedGuard::new`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self.shared.clone()) };
        // Safe: `stmt` is valid and in cursor state.
        let cursor = unsafe { CursorImpl::new(stmt) };
        Ok(Some(cursor))
    }

    fn into_prepared(
        self,
        query: &str,
    ) -> Result<Prepared<StatementConnection<Self::StatementParent>>, Error> {
        let stmt = self.connection.prepare(query)?;
        let stmt_ptr = stmt.into_handle().into_sys();
        // Safe: `shared` owns the connection the statement has been allocated on. See
        // `SharedGuard::new`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self.shared.clone()) };
        // `stmt` is valid and in prepared state.
        let prepared = Prepared::new(stmt);
        Ok(prepared)
    }

    fn into_preallocated(
        self,
    ) -> Result<Preallocated<StatementConnection<Self::StatementParent>>, Error> {
        let stmt = self.connection.preallocate()?;
        let stmt_ptr = stmt.into_handle().into_sys();
        // Safe: `shared` owns the connection the statement has been allocated on. See
        // `SharedGuard::new`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self.shared.clone()) };
        // Safe: `stmt` is valid and in freshly allocated state.
        let preallocated = unsafe { Preallocated::new(stmt) };
        Ok(preallocated)
    }
}
