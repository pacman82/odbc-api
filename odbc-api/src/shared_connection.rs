use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    Connection, CursorImpl, Error, OwnedCursor, ParameterCollectionRef, Preallocated, Prepared,
    connection::{ConnectionTransitions, FailedStateTransition},
    handles::{KeepConnectionOpen, StatementConnection},
};

/// A convinient type alias in case you want to use a connection from multiple threads which share
/// ownership of it.
pub type SharedConnection<'env> = Arc<Mutex<Connection<'env>>>;

/// # Safety:
///
/// Connection is guaranteed to be alive and in connected state for the lifetime of
/// [`SharedConnection`].
unsafe impl KeepConnectionOpen for SharedConnection<'_> {}

/// Deprecated: Prefer [`SharedConnectionGuard`]. It allows for performing several operations, while
/// acquiring the lock only once. Trait implementations can not be annotated with `#[deprecated]`.
impl<'env> ConnectionTransitions for SharedConnection<'env> {
    type StatementParent = Self;

    fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<OwnedCursor<Self>>, FailedStateTransition<Self>> {
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

    fn into_prepared(self, query: &str) -> Result<Prepared<StatementConnection<Self>>, Error> {
        let guard = self
            .lock()
            .expect("Shared connection lock must not be poisoned");
        let stmt = guard.prepare(query)?;
        let stmt_ptr = stmt.into_handle().into_sys();
        drop(guard);
        // Safe: The connection is the parent of the statement referenced by `stmt_ptr`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self) };
        // `stmt` is valid and in prepared state.
        let prepared = Prepared::new(stmt);
        Ok(prepared)
    }

    fn into_preallocated(self) -> Result<Preallocated<StatementConnection<Self>>, Error> {
        let guard = self
            .lock()
            .expect("Shared connection lock must not be poisoned");
        let stmt = guard.preallocate()?;
        let stmt_ptr = stmt.into_handle().into_sys();
        drop(guard);
        // Safe: The connection is the parent of the statement referenced by `stmt_ptr`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self) };
        // `stmt` is valid and in freshly allocated state.
        let preallocated = unsafe { Preallocated::new(stmt) };
        Ok(preallocated)
    }
}

/// Acquire exclusive access to a shared connection
///
/// Use this instead of a regular `MutexGuard` if you intend to create statements which share
/// ownership of the connection.
pub trait LockConnection<'env> {
    /// Acquires the lock of the shared connection, blocking the current thread until it is able to
    /// do so.
    fn lock_connection(&self) -> SharedConnectionGuard<'_, 'env>;
}

impl<'env> LockConnection<'env> for SharedConnection<'env> {
    fn lock_connection(&self) -> SharedConnectionGuard<'_, 'env> {
        let connection = self
            .lock()
            .expect("Shared connection lock must not be poisoned");
        // Safe: `connection` is the guard of the mutex inside `self`.
        unsafe { SharedConnectionGuard::new(self, connection) }
    }
}

/// Exclusive access to a [`SharedConnection`] for as long as the guard lives. Instances are
/// created using [`LockConnection::lock_connection`].
///
/// As opposed to a plain MutexGuard [`SharedConnectionGuard`] can implement
/// [`crate::ConnectionTransitions`] because it also has a reference to the `Arc`.
#[derive(Debug)]
pub struct SharedConnectionGuard<'a, 'env> {
    shared: &'a SharedConnection<'env>,
    connection: MutexGuard<'a, Connection<'env>>,
}

impl<'a, 'env> SharedConnectionGuard<'a, 'env> {
    /// # Safety
    ///
    /// `connection` must be the guard of the mutex inside `shared`.
    unsafe fn new(
        shared: &'a SharedConnection<'env>,
        connection: MutexGuard<'a, Connection<'env>>,
    ) -> Self {
        Self { shared, connection }
    }

    /// The locked connection, e.g. to query information about it.
    pub fn connection(&self) -> &Connection<'env> {
        &self.connection
    }
}

/// Implemented for a reference, so the guard is not consumed. Several statements can be created
/// while holding the lock only once. Each statement owns a clone of the shared connection.
impl<'env> ConnectionTransitions for &SharedConnectionGuard<'_, 'env> {
    type StatementParent = SharedConnection<'env>;

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
        // `SharedConnectionGuard::new`.
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
        // `SharedConnectionGuard::new`.
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
        // `SharedConnectionGuard::new`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self.shared.clone()) };
        // Safe: `stmt` is valid and in freshly allocated state.
        let preallocated = unsafe { Preallocated::new(stmt) };
        Ok(preallocated)
    }
}
