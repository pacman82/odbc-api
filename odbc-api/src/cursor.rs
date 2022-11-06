use odbc_sys::HStmt;

use crate::{
    buffers::Indicator,
    error::ExtendResult,
    handles::{AsStatementRef, SqlResult, State, Statement, StatementRef},
    parameter::{VarBinarySliceMut, VarCharSliceMut},
    sleep::{wait_for, Sleep},
    Error, OutputParameter, ResultSetMetadata,
};

use std::{cmp::max, thread::panicking};

/// Cursors are used to process and iterate the result sets returned by executing queries.
///
/// # Example: Fetching result in batches
///
/// ```rust
/// use odbc_api::{Cursor, buffers::{BufferDescription, BufferKind, ColumnarAnyBuffer}, Error};
///
/// /// Fetches all values from the first column of the cursor as i32 in batches of 100 and stores
/// /// them in a vector.
/// fn fetch_all_ints(cursor: impl Cursor) -> Result<Vec<i32>, Error> {
///     let mut all_ints = Vec::new();
///     // Batch size determines how many values we fetch at once.
///     let batch_size = 100;
///     // We expect the first column to hold INTEGERs (or a type convertible to INTEGER). Use
///     // the metadata on the result set, if you want to investige the types of the columns at
///     // runtime.
///     let description = BufferDescription {
///         kind: BufferKind::I32,
///         nullable: false,
///     };
///     // This is the buffer we bind to the driver, and repeatedly use to fetch each batch
///     let buffer = ColumnarAnyBuffer::from_description(batch_size, [description]);
///     // Bind buffer to cursor
///     let mut row_set_buffer = cursor.bind_buffer(buffer)?;
///     // Fetch data batch by batch
///     while let Some(batch) = row_set_buffer.fetch()? {
///         all_ints.extend_from_slice(batch.column(0).as_slice().unwrap())
///     }
///     Ok(all_ints)
/// }
/// ```
pub trait Cursor: ResultSetMetadata {
    /// Advances the cursor to the next row in the result set. This is **Slow**. Bind buffers
    /// instead, for good performance.
    ///
    /// While this method is very convenient due to the fact that the application does not have to
    /// declare and bind specific buffers it is also in many situations extremely slow. Concrete
    /// performance depends on the ODBC driver in question, but it is likely it performs a roundtrip
    /// to the datasource for each individual row. It is also likely an extra conversion is
    /// performed then requesting individual fields, since the C buffer type is not known to the
    /// driver in advance. Consider binding a buffer to the cursor first using
    /// [`Self::bind_buffer`].
    fn next_row(&mut self) -> Result<Option<CursorRow<'_>>, Error> {
        let row_available = unsafe {
            self.as_stmt_ref()
                .fetch()
                .into_result_bool(&self.as_stmt_ref())?
        };
        let ret = if row_available {
            Some(unsafe { CursorRow::new(self.as_stmt_ref()) })
        } else {
            None
        };
        Ok(ret)
    }

    /// Binds this cursor to a buffer holding a row set.
    fn bind_buffer<B>(self, row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        Self: Sized,
        B: RowSetBuffer;
}

/// An individual row of an result set. See [`crate::Cursor::next_row`].
pub struct CursorRow<'s> {
    statement: StatementRef<'s>,
}

impl<'s> CursorRow<'s> {
    /// # Safety
    ///
    /// `statement` must be in a cursor state.
    unsafe fn new(statement: StatementRef<'s>) -> Self {
        CursorRow { statement }
    }
}

impl<'s> CursorRow<'s> {
    /// Fills a suitable target buffer with a field from the current row of the result set. This
    /// method drains the data from the field. It can be called repeatedly to if not all the data
    /// fit in the output buffer at once. It should not called repeatedly to fetch the same value
    /// twice. Column index starts at `1`.
    pub fn get_data(
        &mut self,
        col_or_param_num: u16,
        target: &mut impl OutputParameter,
    ) -> Result<(), Error> {
        self.statement
            .get_data(col_or_param_num, target)
            .into_result(&self.statement)
            .provide_context_for_diagnostic(|record, function| {
                if record.state == State::INDICATOR_VARIABLE_REQUIRED_BUT_NOT_SUPPLIED {
                    Error::UnableToRepresentNull(record)
                } else {
                    Error::Diagnostics { record, function }
                }
            })
    }

    /// Retrieves arbitrary large character data from the row and stores it in the buffer. Column
    /// index starts at `1`.
    ///
    /// # Return
    ///
    /// `true` indicates that the value has not been `NULL` and the value has been placed in `buf`.
    /// `false` indicates that the value is `NULL`. The buffer is cleared in that case.
    pub fn get_text(&mut self, col_or_param_num: u16, buf: &mut Vec<u8>) -> Result<bool, Error> {
        // Utilize all of the allocated buffer. Make sure buffer can at least hold the terminating
        // zero.
        buf.resize(max(1, buf.capacity()), 0);
        // We repeatedly fetch data and add it to the buffer. The buffer length is therefore the
        // accumulated value size. This variable keeps track of the number of bytes we added with
        // the current call to get_data.
        let mut fetch_size = buf.len();
        let mut target = VarCharSliceMut::from_buffer(buf.as_mut_slice(), Indicator::Null);
        // Fetch binary data into buffer.
        self.get_data(col_or_param_num, &mut target)?;
        let not_null = loop {
            match target.indicator() {
                // Value is `NULL`. We are done here.
                Indicator::Null => {
                    buf.clear();
                    break false;
                }
                // We do not know how large the value is. Let's fetch the data with repeated calls
                // to get_data.
                Indicator::NoTotal => {
                    let old_len = buf.len();
                    // Use an exponential strategy for increasing buffer size. +1 For handling
                    // initial buffer size of 1.
                    buf.resize(old_len * 2, 0);
                    fetch_size = old_len + 1;
                    target =
                        VarCharSliceMut::from_buffer(&mut buf[(old_len - 1)..], Indicator::Null);
                    self.get_data(col_or_param_num, &mut target)?;
                }
                // We did get the complete value, including the terminating zero. Let's resize the
                // buffer to match the retrieved value exactly (excluding terminating zero).
                Indicator::Length(len) if len < fetch_size => {
                    // Since the indicator refers to value length without terminating zero, this
                    // also implicitly drops the terminating zero at the end of the buffer.
                    let shrink_by = fetch_size - len;
                    buf.resize(buf.len() - shrink_by, 0);
                    break true;
                }
                // We did not get all of the value in one go, but the data source has been friendly
                // enough to tell us how much is missing.
                Indicator::Length(len) => {
                    let still_missing = len - fetch_size + 1;
                    fetch_size = still_missing + 1;
                    let old_len = buf.len();
                    buf.resize(old_len + still_missing, 0);
                    target =
                        VarCharSliceMut::from_buffer(&mut buf[(old_len - 1)..], Indicator::Null);
                    self.get_data(col_or_param_num, &mut target)?;
                }
            }
        };
        Ok(not_null)
    }

    /// Retrieves arbitrary large binary data from the row and stores it in the buffer. Column index
    /// starts at `1`.
    ///
    /// # Return
    ///
    /// `true` indicates that the value has not been `NULL` and the value has been placed in `buf`.
    /// `false` indicates that the value is `NULL`. The buffer is cleared in that case.
    pub fn get_binary(&mut self, col_or_param_num: u16, buf: &mut Vec<u8>) -> Result<bool, Error> {
        // Utilize all of the allocated buffer. Make sure buffer can at least hold one element.
        buf.resize(max(1, buf.capacity()), 0);
        // We repeatedly fetch data and add it to the buffer. The buffer length is therefore the
        // accumulated value size. This variable keeps track of the number of bytes we added with
        // the current call to get_data.
        let mut fetch_size = buf.len();
        let mut target = VarBinarySliceMut::from_buffer(buf.as_mut_slice(), Indicator::Null);
        // Fetch binary data into buffer.
        self.get_data(col_or_param_num, &mut target)?;
        let not_null = loop {
            match target.indicator() {
                // Value is `NULL`. We are done here.
                Indicator::Null => {
                    buf.clear();
                    break false;
                }
                // We do not know how large the value is. Let's fetch the data with repeated calls
                // to get_data.
                Indicator::NoTotal => {
                    let old_len = buf.len();
                    // Use an exponential strategy for increasing buffer size.
                    buf.resize(old_len * 2, 0);
                    target = VarBinarySliceMut::from_buffer(&mut buf[old_len..], Indicator::Null);
                    self.get_data(col_or_param_num, &mut target)?;
                }
                // We did get the complete value, including the terminating zero. Let's resize the
                // buffer to match the retrieved value exactly (excluding terminating zero).
                Indicator::Length(len) if len <= fetch_size => {
                    let shrink_by = fetch_size - len;
                    buf.resize(buf.len() - shrink_by, 0);
                    break true;
                }
                // We did not get all of the value in one go, but the data source has been friendly
                // enough to tell us how much is missing.
                Indicator::Length(len) => {
                    let still_missing = len - fetch_size;
                    fetch_size = still_missing;
                    let old_len = buf.len();
                    buf.resize(old_len + still_missing, 0);
                    target = VarBinarySliceMut::from_buffer(&mut buf[old_len..], Indicator::Null);
                    self.get_data(col_or_param_num, &mut target)?;
                }
            }
        };
        Ok(not_null)
    }
}

/// Cursors are used to process and iterate the result sets returned by executing queries. Created
/// by either a prepared query or direct execution. Usually utilized through the [`crate::Cursor`]
/// trait.
pub struct CursorImpl<Stmt: AsStatementRef> {
    /// A statement handle in cursor mode.
    statement: Stmt,
}

impl<S> Drop for CursorImpl<S>
where
    S: AsStatementRef,
{
    fn drop(&mut self) {
        let mut stmt = self.statement.as_stmt_ref();
        if let Err(e) = stmt.close_cursor().into_result(&stmt) {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexpected error closing cursor: {:?}", e)
            }
        }
    }
}

impl<S> AsStatementRef for CursorImpl<S>
where
    S: AsStatementRef,
{
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}

impl<S> ResultSetMetadata for CursorImpl<S> where S: AsStatementRef {}

impl<S> Cursor for CursorImpl<S>
where
    S: AsStatementRef,
{
    fn bind_buffer<B>(mut self, mut row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        B: RowSetBuffer,
    {
        let stmt = self.statement.as_stmt_ref();
        unsafe {
            bind_row_set_buffer_to_statement(stmt, &mut row_set_buffer)?;
        }
        Ok(RowSetCursor::new(row_set_buffer, self))
    }
}

impl<S> CursorImpl<S>
where
    S: AsStatementRef,
{
    /// Users of this library are encouraged not to call this constructor directly but rather invoke
    /// [`crate::Connection::execute`] or [`crate::Prepared::execute`] to get a cursor and utilize
    /// it using the [`crate::Cursor`] trait. This method is pubilc so users with an understanding
    /// of the raw ODBC C-API have a way to create a cursor, after they left the safety rails of the
    /// Rust type System, in order to implement a use case not covered yet, by the safe abstractions
    /// within this crate.
    ///
    /// # Safety
    ///
    /// `statement` must be in Cursor state, for the invariants of this type to hold.
    pub unsafe fn new(statement: S) -> Self {
        Self { statement }
    }

    pub(crate) fn as_sys(&mut self) -> HStmt {
        self.as_stmt_ref().as_sys()
    }
}

/// A Row set buffer binds row, or column wise buffers to a cursor in order to fill them with row
/// sets with each call to fetch.
///
/// # Safety
///
/// Implementers of this trait must ensure that every pointer bound in `bind_to_cursor` stays valid
/// even if an instance is moved in memory. Bound members should therefore be likely references
/// themselves. To bind stack allocated buffers it is recommended to implement this trait on the
/// reference type instead.
pub unsafe trait RowSetBuffer {
    /// Declares the bind type of the Row set buffer. `0` Means a columnar binding is used. Any non
    /// zero number is interpreted as the size of a single row in a row wise binding style.
    fn bind_type(&self) -> usize;

    /// The batch size for bulk cursors, if retrieving many rows at once.
    fn row_array_size(&self) -> usize;

    /// Mutable reference to the number of fetched rows.
    ///
    /// # Safety
    ///
    /// Implementations of this method must take care that the returned referenced stays valid, even
    /// if `self` should be moved.
    fn mut_num_fetch_rows(&mut self) -> &mut usize;

    /// Binds the buffer either column or row wise to the cursor.
    ///
    /// # Safety
    ///
    /// It's the implementations responsibility to ensure that all bound buffers are valid until
    /// unbound or the statement handle is deleted.
    unsafe fn bind_colmuns_to_cursor(&mut self, cursor: StatementRef<'_>) -> Result<(), Error>;
}

unsafe impl<T: RowSetBuffer> RowSetBuffer for &mut T {
    fn bind_type(&self) -> usize {
        (**self).bind_type()
    }

    fn row_array_size(&self) -> usize {
        (**self).row_array_size()
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        (*self).mut_num_fetch_rows()
    }

    unsafe fn bind_colmuns_to_cursor(&mut self, cursor: StatementRef<'_>) -> Result<(), Error> {
        (*self).bind_colmuns_to_cursor(cursor)
    }
}

/// Iterates in blocks (called row sets) over a result set, filling a buffers with a lot of rows at
/// once, instead of iterating the result set row by row. This is usually much faster.
pub struct RowSetCursor<C: AsStatementRef, B> {
    buffer: B,
    cursor: C,
}

impl<C, B> RowSetCursor<C, B>
where
    C: Cursor,
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
    pub fn fetch(&mut self) -> Result<Option<&B>, Error> {
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
    ) -> Result<Option<&B>, Error> {
        let mut stmt = self.cursor.as_stmt_ref();
        unsafe {
            let result = stmt.fetch();
            let has_row = error_handling_for_fetch(result, stmt, error_for_truncation)?;
            Ok(has_row.then_some(&self.buffer))
        }
    }
}

impl<C, B> Drop for RowSetCursor<C, B>
where
    C: AsStatementRef,
{
    fn drop(&mut self) {
        unsafe {
            let mut stmt = self.cursor.as_stmt_ref();
            if let Err(e) = stmt
                .unbind_cols()
                .into_result(&stmt)
                .and_then(|()| stmt.set_num_rows_fetched(None).into_result(&stmt))
            {
                // Avoid panicking, if we already have a panic. We don't want to mask the original
                // error.
                if !panicking() {
                    panic!("Unexpected error unbinding columns: {:?}", e)
                }
            }
        }
    }
}

/// The asynchronous sibiling of [`CursorImpl`]. Use this to fetch results in asynchronous code.
///
/// Like [`CursorImpl`] this is an ODBC statement handle in cursor state. However unlike its
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
    /// [`CursorImpl`] is more suitable.
    pub unsafe fn new(statement: S) -> Self {
        Self { statement }
    }

    /// Binds this cursor to a buffer holding a row set.
    pub fn bind_buffer<B>(
        mut self,
        mut row_set_buffer: B,
    ) -> Result<RowSetCursorPolling<Self, B>, Error>
    where
        B: RowSetBuffer,
    {
        let stmt = self.statement.as_stmt_ref();
        unsafe {
            bind_row_set_buffer_to_statement(stmt, &mut row_set_buffer)?;
        }
        Ok(RowSetCursorPolling::new(row_set_buffer, self))
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
                panic!("Unexpected error closing cursor: {:?}", e)
            }
        }
    }
}

/// Asynchronously iterates in blocks (called row sets) over a result set, filling a buffers with
/// a lot of rows at once, instead of iterating the result set row by row. This is usually much
/// faster. Asynchronous sibiling of [`self::RowSetCursor`].
pub struct RowSetCursorPolling<C, B>
where
    C: AsStatementRef,
{
    buffer: B,
    cursor: C,
}

impl<C, B> RowSetCursorPolling<C, B>
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
    pub async fn fetch(&mut self, sleep: impl Sleep) -> Result<Option<&B>, Error> {
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
    /// Call this method to find out wether there are any truncated values in the batch, without
    /// inspecting all its rows and columns.
    pub async fn fetch_with_truncation_check(
        &mut self,
        error_for_truncation: bool,
        mut sleep: impl Sleep,
    ) -> Result<Option<&B>, Error> {
        let mut stmt = self.cursor.as_stmt_ref();
        unsafe {
            let result = wait_for(|| stmt.fetch(), &mut sleep).await;
            let has_row = error_handling_for_fetch(result, stmt, error_for_truncation)?;
            Ok(has_row.then_some(&self.buffer))
        }
    }
}

/// Binds a row set buffer to a statment. Implementation is shared between synchronous and
/// asynchronous cursors.
unsafe fn bind_row_set_buffer_to_statement(
    mut stmt: StatementRef<'_>,
    row_set_buffer: &mut impl RowSetBuffer,
) -> Result<(), Error> {
    stmt.set_row_bind_type(row_set_buffer.bind_type())
        .into_result(&stmt)?;
    let size = row_set_buffer.row_array_size();
    stmt.set_row_array_size(size)
        .into_result(&stmt)
        // SAP anywhere has been seen to return with an "invalid attribute" error instead of
        // a success with "option value changed" info. Let us map invalid attributes during
        // setting row set array size to something more precise.
        .provide_context_for_diagnostic(|record, function| {
            if record.state == State::INVALID_ATTRIBUTE_VALUE {
                Error::InvalidRowArraySize { record, size }
            } else {
                Error::Diagnostics { record, function }
            }
        })?;
    stmt.set_num_rows_fetched(Some(row_set_buffer.mut_num_fetch_rows()))
        .into_result(&stmt)?;
    row_set_buffer.bind_colmuns_to_cursor(stmt)?;
    Ok(())
}

/// Error handling for bulk fetching is shared between synchronous and asynchronous usecase.
fn error_handling_for_fetch(
    result: SqlResult<()>,
    mut stmt: StatementRef,
    error_for_truncation: bool,
) -> Result<bool, Error> {
    let has_row = result
        .on_success(|| true)
        .into_result_with(&stmt.as_stmt_ref(), error_for_truncation, Some(false), None)
        // Oracles ODBC driver does not support 64Bit integers. Furthermore, it does not
        // tell the it to the user than binding parameters, but rather now then we fetch
        // results. The error code retruned is `HY004` rather then `HY003` which should
        // be used to indicate invalid buffer types.
        .provide_context_for_diagnostic(|record, function| {
            if record.state == State::INVALID_SQL_DATA_TYPE {
                Error::OracleOdbcDriverDoesNotSupport64Bit(record)
            } else {
                Error::Diagnostics { record, function }
            }
        })?;
    Ok(has_row)
}

impl<C, B> Drop for RowSetCursorPolling<C, B>
where
    C: AsStatementRef,
{
    fn drop(&mut self) {
        unsafe {
            let mut stmt = self.cursor.as_stmt_ref();
            if let Err(e) = stmt
                .unbind_cols()
                .into_result(&stmt)
                .and_then(|()| stmt.set_num_rows_fetched(None).into_result(&stmt))
            {
                // Avoid panicking, if we already have a panic. We don't want to mask the original
                // error.
                if !panicking() {
                    panic!("Unexpected error unbinding columns: {:?}", e)
                }
            }
        }
    }
}
