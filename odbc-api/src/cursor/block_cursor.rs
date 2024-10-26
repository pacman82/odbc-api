use std::{mem::MaybeUninit, ptr, thread::panicking};

use crate::{
    handles::{AsStatementRef, Statement as _},
    Error,
};

use super::{error_handling_for_fetch, unbind_buffer_from_cursor, Cursor, RowSetBuffer};

/// In order to save on network overhead, it is recommended to use block cursors instead of fetching
/// values individually. This can greatly reduce the time applications need to fetch data. You can
/// create a block cursor by binding preallocated memory to a cursor using [`Cursor::bind_buffer`].
/// A block cursor saves on a lot of IO overhead by fetching an entire set of rows (called *rowset*)
/// at once into the buffer bound to it. Reusing the same buffer for each rowset also saves on
/// allocations. A challange with using block cursors might be database schemas with columns there
/// individual fields can be very large. In these cases developers can choose to:
///
/// 1. Reserve less memory for each individual field than the schema indicates and deciding on a
///    sensible upper bound themselves. This risks truncation of values though, if they are larger
///    than the upper bound. Using [`BlockCursor::fetch_with_truncation_check`] instead of
///    [`Cursor::next_row`] your application can detect these truncations. This is usually the best
///    choice, since individual fields in a table rarely actually take up several GiB of memory.
/// 2. Calculate the number of rows dynamically based on the maximum expected row size.
///    [`crate::buffers::BufferDesc::bytes_per_row`], can be helpful with this task.
/// 3. Not use block cursors and fetch rows slowly with high IO overhead. Calling
///    [`CursorRow::get_data`] and [`CursorRow::get_text`] to fetch large individual values.
///
/// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/block-cursors>
pub struct BlockCursor<C: AsStatementRef, B> {
    buffer: B,
    cursor: C,
}

impl<C, B> BlockCursor<C, B>
where
    C: Cursor,
{
    pub(crate) fn new(buffer: B, cursor: C) -> Self {
        Self { buffer, cursor }
    }

    /// Fills the bound buffer with the next row set.
    ///
    /// # Return
    ///
    /// `None` if the result set is empty and all row sets have been extracted. `Some` with a
    /// reference to the internal buffer otherwise.
    ///
    /// ```
    /// use odbc_api::{buffers::TextRowSet, Cursor};
    ///
    /// fn print_all_values(mut cursor: impl Cursor) {
    ///     let batch_size = 100;
    ///     let max_string_len = 4000;
    ///     let buffer = TextRowSet::for_cursor(batch_size, &mut cursor, Some(4000)).unwrap();
    ///     let mut cursor = cursor.bind_buffer(buffer).unwrap();
    ///     // Iterate over batches
    ///     while let Some(batch) = cursor.fetch().unwrap() {
    ///         // ... print values in batch ...
    ///     }
    /// }
    /// ```
    pub fn fetch(&mut self) -> Result<Option<&B>, Error>
    where
        B: RowSetBuffer,
    {
        self.fetch_with_truncation_check(false)
    }

    /// Fills the bound buffer with the next row set. Should `error_for_truncation` be `true`and any
    /// diagnostic indicate truncation of a value an error is returned.
    ///
    /// # Return
    ///
    /// `None` if the result set is empty and all row sets have been extracted. `Some` with a
    /// reference to the internal buffer otherwise.
    ///
    /// Call this method to find out wether there are any truncated values in the batch, without
    /// inspecting all its rows and columns.
    ///
    /// ```
    /// use odbc_api::{buffers::TextRowSet, Cursor};
    ///
    /// fn print_all_values(mut cursor: impl Cursor) {
    ///     let batch_size = 100;
    ///     let max_string_len = 4000;
    ///     let buffer = TextRowSet::for_cursor(batch_size, &mut cursor, Some(4000)).unwrap();
    ///     let mut cursor = cursor.bind_buffer(buffer).unwrap();
    ///     // Iterate over batches
    ///     while let Some(batch) = cursor.fetch_with_truncation_check(true).unwrap() {
    ///         // ... print values in batch ...
    ///     }
    /// }
    /// ```
    pub fn fetch_with_truncation_check(
        &mut self,
        error_for_truncation: bool,
    ) -> Result<Option<&B>, Error>
    where
        B: RowSetBuffer,
    {
        let mut stmt = self.cursor.as_stmt_ref();
        unsafe {
            let result = stmt.fetch();
            let has_row =
                error_handling_for_fetch(result, stmt, &self.buffer, error_for_truncation)?;
            Ok(has_row.then_some(&self.buffer))
        }
    }

    /// Unbinds the buffer from the underlying statement handle. Potential usecases for this
    /// function include.
    ///
    /// 1. Binding a different buffer to the "same" cursor after letting it point to the next result
    ///    set obtained with [Cursor::more_results`].
    /// 2. Reusing the same buffer with a different statement.
    pub fn unbind(self) -> Result<(C, B), Error> {
        // In this method we want to deconstruct self and move cursor out of it. We need to
        // negotiate with the compiler a little bit though, since BlockCursor does implement `Drop`.

        // We want to move `cursor` out of self, which would make self partially uninitialized.
        let dont_drop_me = MaybeUninit::new(self);
        let self_ptr = dont_drop_me.as_ptr();

        // Safety: We know `dont_drop_me` is valid at this point so reading the ptr is okay
        let mut cursor = unsafe { ptr::read(&(*self_ptr).cursor) };
        let buffer = unsafe { ptr::read(&(*self_ptr).buffer) };

        // Now that we have cursor out of block cursor, we need to unbind the buffer.
        unbind_buffer_from_cursor(&mut cursor)?;

        Ok((cursor, buffer))
    }
}

impl<C, B> BlockCursor<C, B>
where
    B: RowSetBuffer,
    C: AsStatementRef,
{
    /// Maximum amount of rows fetched from the database in the next call to fetch.
    pub fn row_array_size(&self) -> usize {
        self.buffer.row_array_size()
    }
}

impl<C, B> Drop for BlockCursor<C, B>
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
