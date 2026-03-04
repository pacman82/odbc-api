use crate::{
    Error,
    handles::{AsStatementRef, Statement, StatementRef},
    sleep::{Sleep, wait_for},
};

use super::{
    RowSetBuffer, bind_row_set_buffer_to_statement, error_handling_for_fetch,
    unbind_buffer_from_cursor,
};

use std::thread::panicking;

/// The asynchronous sibiling of [`super::CursorImpl`]. Use this to fetch results in asynchronous
/// code.
///
/// Like [`super::CursorImpl`] this is an ODBC statement handle in cursor state. However unlike its
/// synchronous sibling this statement handle is in asynchronous polling mode.
pub struct CursorPolling<Stmt: AsStatementRef> {
    /// A statement handle in cursor state with asynchronous mode enabled.
    statement: Stmt,
}

impl<S> CursorPolling<S>
where
    S: AsStatementRef,
{
    /// Users of this library are encouraged not to call this constructor directly. This method is
    /// pubilc so users with an understanding of the raw ODBC C-API have a way to create an
    /// asynchronous cursor, after they left the safety rails of the Rust type System, in order to
    /// implement a use case not covered yet, by the safe abstractions within this crate.
    ///
    /// # Safety
    ///
    /// `statement` must be in Cursor state, for the invariants of this type to hold. Preferable
    /// `statement` should also have asynchrous mode enabled, otherwise constructing a synchronous
    /// [`super::CursorImpl`] is more suitable.
    pub unsafe fn new(statement: S) -> Self {
        Self { statement }
    }

    /// Binds this cursor to a buffer holding a row set.
    pub fn bind_buffer<B>(
        mut self,
        mut row_set_buffer: B,
    ) -> Result<BlockCursorPolling<Self, B>, Error>
    where
        B: RowSetBuffer,
    {
        let stmt = self.statement.as_stmt_ref();
        unsafe {
            bind_row_set_buffer_to_statement(stmt, &mut row_set_buffer)?;
        }
        Ok(BlockCursorPolling::new(row_set_buffer, self))
    }
}

impl<S> AsStatementRef for CursorPolling<S>
where
    S: AsStatementRef,
{
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}

impl<S> Drop for CursorPolling<S>
where
    S: AsStatementRef,
{
    fn drop(&mut self) {
        let mut stmt = self.statement.as_stmt_ref();
        if let Err(e) = stmt.close_cursor().into_result(&stmt) {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexpected error closing cursor: {e:?}")
            }
        }
    }
}

/// Asynchronously iterates in blocks (called row sets) over a result set, filling a buffers with
/// a lot of rows at once, instead of iterating the result set row by row. This is usually much
/// faster. Asynchronous sibiling of [`super::BlockCursor`].
pub struct BlockCursorPolling<C, B>
where
    C: AsStatementRef,
{
    buffer: B,
    cursor: C,
}

impl<C, B> BlockCursorPolling<C, B>
where
    C: AsStatementRef,
{
    fn new(buffer: B, cursor: C) -> Self {
        Self { buffer, cursor }
    }

    /// Fills the bound buffer with the next row set.
    ///
    /// # Return
    ///
    /// `None` if the result set is empty and all row sets have been extracted. `Some` with a
    /// reference to the internal buffer otherwise.
    pub async fn fetch(&mut self, sleep: impl Sleep) -> Result<Option<&B>, Error>
    where
        B: RowSetBuffer,
    {
        self.fetch_with_truncation_check(false, sleep).await
    }

    /// Fills the bound buffer with the next row set. Should `error_for_truncation` be `true`and any
    /// diagnostic indicate truncation of a value an error is returned.
    ///
    /// # Return
    ///
    /// `None` if the result set is empty and all row sets have been extracted. `Some` with a
    /// reference to the internal buffer otherwise.
    ///
    /// Call this method to find out whether there are any truncated values in the batch, without
    /// inspecting all its rows and columns.
    pub async fn fetch_with_truncation_check(
        &mut self,
        error_for_truncation: bool,
        mut sleep: impl Sleep,
    ) -> Result<Option<&B>, Error>
    where
        B: RowSetBuffer,
    {
        let mut stmt = self.cursor.as_stmt_ref();
        let result = unsafe { wait_for(|| stmt.fetch(), &mut sleep).await };
        let has_row = error_handling_for_fetch(result, stmt, &self.buffer, error_for_truncation)?;
        Ok(has_row.then_some(&self.buffer))
    }
}

impl<C, B> Drop for BlockCursorPolling<C, B>
where
    C: AsStatementRef,
{
    fn drop(&mut self) {
        if let Err(e) = unbind_buffer_from_cursor(&mut self.cursor) {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexpected error unbinding columns: {e:?}")
            }
        }
    }
}
