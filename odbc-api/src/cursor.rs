use odbc_sys::{HStmt, SqlDataType};

use crate::{
    borrow_mut_statement::BorrowMutStatement,
    buffers::Indicator,
    handles::{State, Statement},
    parameter::{VarBinarySliceMut, VarCharSliceMut},
    ColumnDescription, DataType, Error, Output,
};

use std::{
    char::{decode_utf16, REPLACEMENT_CHARACTER},
    cmp::max,
    convert::TryInto,
    thread::panicking,
};

/// Provides Metadata of the resulting the result set. Implemented by `Cursor` types and prepared
/// queries. Fetching metadata from a prepared query might be expensive (driver dependent), so your
/// application should fetch the Metadata it requires from the `Cursor` if possible.
///
/// See also:
/// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/result-set-metadata>
pub trait ResultSetMetadata {
    /// Statement type of the cursor. This is always an instantiation of
    /// [`crate::handles::Statement`] with a generic parameter indicating the lifetime of the
    /// associated connection.
    ///
    /// So this trait could have had a lifetime parameter instead and provided access to the
    /// underlying type. However by using the projection of only the cursor methods of the
    /// underlying statement, consumers of this trait no only have to worry about the lifetime of
    /// the statement itself (e.g. the prepared query) and not about the lifetime of the connection
    /// it belongs to.
    type Statement: Statement;

    /// Get a shared reference to the underlying statement handle. This method is used to implement
    /// other more high level methods like [`Self::describe_col`] on top of it. It is usually not
    /// intended to be called by users of this library directly, but may serve as an escape hatch
    /// for low level usecases.
    fn stmt_ref(&self) -> &Self::Statement;

    /// Fetch a column description using the column index.
    ///
    /// # Parameters
    ///
    /// * `column_number`: Column index. `0` is the bookmark column. The other column indices start
    /// with `1`.
    /// * `column_description`: Holds the description of the column after the call. This method does
    /// not provide strong exception safety as the value of this argument is undefined in case of an
    /// error.
    fn describe_col(
        &self,
        column_number: u16,
        column_description: &mut ColumnDescription,
    ) -> Result<(), Error> {
        let stmt = self.stmt_ref();
        stmt.describe_col(column_number, column_description)
            .into_result(stmt)
    }

    /// Number of columns in result set.
    fn num_result_cols(&self) -> Result<i16, Error> {
        let stmt = self.stmt_ref();
        stmt.num_result_cols().into_result(stmt)
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn is_unsigned_column(&self, column_number: u16) -> Result<bool, Error> {
        let stmt = self.stmt_ref();
        stmt.is_unsigned_column(column_number).into_result(stmt)
    }

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_octet_length(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_octet_length(column_number).into_result(stmt)
    }

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_display_size(column_number).into_result(stmt)
    }

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    fn col_precision(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_precision(column_number).into_result(stmt)
    }

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    fn col_scale(&self, column_number: u16) -> Result<isize, Error> {
        let stmt = self.stmt_ref();
        stmt.col_scale(column_number).into_result(stmt)
    }

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    fn col_name(&self, column_number: u16, buf: &mut Vec<u16>) -> Result<(), Error> {
        let stmt = self.stmt_ref();
        stmt.col_name(column_number, buf).into_result(stmt)
    }

    /// Use this if you want to iterate over all column names and allocate a `String` for each one.
    ///
    /// This is a wrapper around `col_name` introduced for convenience.
    fn column_names(&self) -> Result<ColumnNamesIt<'_, Self>, Error> {
        ColumnNamesIt::new(self)
    }

    /// Data type of the specified column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_data_type(&self, column_number: u16) -> Result<DataType, Error> {
        let stmt = self.stmt_ref();
        let kind = stmt.col_concise_type(column_number).into_result(stmt)?;
        let dt = match kind {
            SqlDataType::UNKNOWN_TYPE => DataType::Unknown,
            SqlDataType::EXT_VAR_BINARY => DataType::Varbinary {
                length: self.col_octet_length(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_LONG_VAR_BINARY => DataType::LongVarbinary {
                length: self.col_octet_length(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_BINARY => DataType::Binary {
                length: self.col_octet_length(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_W_VARCHAR => DataType::WVarchar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_W_CHAR => DataType::WChar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_LONG_VARCHAR => DataType::LongVarchar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::CHAR => DataType::Char {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::VARCHAR => DataType::Varchar {
                length: self.col_display_size(column_number)?.try_into().unwrap(),
            },
            SqlDataType::NUMERIC => DataType::Numeric {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
                scale: self.col_scale(column_number)?.try_into().unwrap(),
            },
            SqlDataType::DECIMAL => DataType::Decimal {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
                scale: self.col_scale(column_number)?.try_into().unwrap(),
            },
            SqlDataType::INTEGER => DataType::Integer,
            SqlDataType::SMALLINT => DataType::SmallInt,
            SqlDataType::FLOAT => DataType::Float {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
            },
            SqlDataType::REAL => DataType::Real,
            SqlDataType::DOUBLE => DataType::Double,
            SqlDataType::DATE => DataType::Date,
            SqlDataType::TIME => DataType::Time {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
            },
            SqlDataType::TIMESTAMP => DataType::Timestamp {
                precision: self.col_precision(column_number)?.try_into().unwrap(),
            },
            SqlDataType::EXT_BIG_INT => DataType::BigInt,
            SqlDataType::EXT_TINY_INT => DataType::TinyInt,
            SqlDataType::EXT_BIT => DataType::Bit,
            other => {
                let mut column_description = ColumnDescription::default();
                self.describe_col(column_number, &mut column_description)?;
                DataType::Other {
                    data_type: other,
                    column_size: column_description.data_type.column_size(),
                    decimal_digits: column_description.data_type.decimal_digits(),
                }
            }
        };
        Ok(dt)
    }
}

/// Cursors are used to process and iterate the result sets returned by executing queries.
pub trait Cursor: ResultSetMetadata {
    /// Provides access to the underlying statement handle.
    ///
    /// # Safety
    ///
    /// Assigning to this statement handle or binding buffers to it may invalidate the invariants
    /// of safe wrapper types (i.e. [`crate::RowSetCursor`]). Some actions like closing the cursor
    /// may just result in ODBC transition errors, others like binding columns may even cause actual
    /// invalid memory access if not used with care.
    unsafe fn stmt(&mut self) -> &mut Self::Statement;

    /// Advances the cursor to the next row in the result set.
    ///
    /// While this method is very convenient due to the fact that the application does not have to
    /// declare and bind specific buffers it is also in many situations extremely slow. Concrete
    /// performance depends on the ODBC driver in question, but it is likely it performs a roundtrip
    /// to the datasource for each individual row. It is also likely an extra conversion is
    /// performed then requesting individual fields, since the C buffer type is not known to the
    /// driver in advance. Consider binding a buffer to the cursor first using
    /// [`Self::bind_buffer`].
    fn next_row(&mut self) -> Result<Option<CursorRow<'_, Self::Statement>>, Error> {
        let row_available = unsafe {
            self.stmt()
                .fetch()
                .map(|res| res.into_result(self.stmt()))
                .transpose()?
                .is_some()
        };
        let ret = if row_available {
            Some(CursorRow::new(unsafe { self.stmt() }))
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
pub struct CursorRow<'c, S: ?Sized> {
    statement: &'c mut S,
}

impl<'c, S: ?Sized> CursorRow<'c, S> {
    fn new(statement: &'c mut S) -> Self {
        CursorRow { statement }
    }
}

impl<'c, S> CursorRow<'c, S>
where
    S: Statement,
{
    /// Fills a suitable target buffer with a field from the current row of the result set. This
    /// method drains the data from the field. It can be called repeatedly to if not all the data
    /// fit in the output buffer at once. It should not called repeatedly to fetch the same value
    /// twice. Column index starts at `1`.
    pub fn get_data(
        &mut self,
        col_or_param_num: u16,
        target: &mut impl Output,
    ) -> Result<(), Error> {
        self.statement
            .get_data(col_or_param_num, target)
            .into_result(self.statement)
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

/// An iterator calling `col_name` for each column_name and converting the result into UTF-8. See
/// `Cursor::column_names`.
pub struct ColumnNamesIt<'c, C: ?Sized> {
    cursor: &'c C,
    buffer: Vec<u16>,
    column: u16,
    num_cols: u16,
}

impl<'c, C: ResultSetMetadata + ?Sized> ColumnNamesIt<'c, C> {
    fn new(cursor: &'c C) -> Result<Self, Error> {
        Ok(Self {
            cursor,
            // Some ODBC drivers do not report the required size to hold the column name. Starting
            // with a reasonable sized buffers, allows us to fetch reasonable sized column alias
            // even from those.
            buffer: Vec::with_capacity(128),
            num_cols: cursor.num_result_cols()?.try_into().unwrap(),
            column: 1,
        })
    }
}

impl<C> Iterator for ColumnNamesIt<'_, C>
where
    C: ResultSetMetadata,
{
    type Item = Result<String, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.column <= self.num_cols {
            let result = self
                .cursor
                .col_name(self.column, &mut self.buffer)
                .map(|()| {
                    decode_utf16(self.buffer.iter().copied())
                        .map(|decoding_result| decoding_result.unwrap_or(REPLACEMENT_CHARACTER))
                        .collect()
                });
            self.column += 1;
            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let num_cols = self.num_cols as usize;
        (num_cols, Some(num_cols))
    }
}

impl<C> ExactSizeIterator for ColumnNamesIt<'_, C> where C: Cursor {}

/// Cursors are used to process and iterate the result sets returned by executing queries. Created
/// by either a prepared query or direct execution. Usually utilized through the [`crate::Cursor`]
/// trait.
pub struct CursorImpl<Stmt: BorrowMutStatement> {
    statement: Stmt,
}

impl<'o, S> Drop for CursorImpl<S>
where
    S: BorrowMutStatement,
{
    fn drop(&mut self) {
        let stmt = self.statement.borrow_mut();
        if let Err(e) = stmt.close_cursor().into_result(stmt) {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexpected error closing cursor: {:?}", e)
            }
        }
    }
}

impl<S> ResultSetMetadata for CursorImpl<S>
where
    S: BorrowMutStatement,
{
    type Statement = S::Statement;

    fn stmt_ref(&self) -> &Self::Statement {
        self.statement.borrow()
    }
}

impl<S> Cursor for CursorImpl<S>
where
    S: BorrowMutStatement,
{
    unsafe fn stmt(&mut self) -> &mut Self::Statement {
        self.statement.borrow_mut()
    }

    fn bind_buffer<B>(mut self, mut row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        B: RowSetBuffer,
    {
        let stmt = self.statement.borrow_mut();
        unsafe {
            stmt.set_row_bind_type(row_set_buffer.bind_type())
                .into_result(stmt)?;
            let size = row_set_buffer.row_array_size();
            stmt.set_row_array_size(size)
                .into_result(stmt)
                // SAP anywhere has been seen to return with an "invalid attribute" error instead of
                // a success with "option value changed" info. Let us map invalid attributes during
                // setting row set array size to something more precise.
                .map_err(|error| match error {
                    Error::Diagnostics { record, .. }
                        if record.state == State::INVALID_ATTRIBUTE_VALUE =>
                    {
                        Error::InvalidRowArraySize { record, size }
                    }
                    error => error,
                })?;
            stmt.set_num_rows_fetched(Some(row_set_buffer.mut_num_fetch_rows()))
                .into_result(stmt)?;
            row_set_buffer.bind_to_cursor(&mut self)?;
        }
        Ok(RowSetCursor::new(row_set_buffer, self))
    }
}

impl<S> CursorImpl<S>
where
    S: BorrowMutStatement,
{
    pub(crate) fn new(statement: S) -> Self {
        Self { statement }
    }

    pub(crate) fn as_sys(&self) -> HStmt {
        self.statement.borrow().as_sys()
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
    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error>;
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

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        (*self).bind_to_cursor(cursor)
    }
}

/// A row set cursor iterates in blocks over row sets, filling them in buffers, instead of iterating
/// the result set row by row. This is usually much faster.
pub struct RowSetCursor<C: Cursor, B> {
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
    pub fn fetch(&mut self) -> Result<Option<&B>, Error> {
        unsafe {
            if let Some(res) = self.cursor.stmt().fetch() {
                res.into_result(self.cursor.stmt())?;
                Ok(Some(&self.buffer))
            } else {
                Ok(None)
            }
        }
    }
}

impl<C, B> Drop for RowSetCursor<C, B>
where
    C: Cursor,
{
    fn drop(&mut self) {
        unsafe {
            let stmt = self.cursor.stmt();
            if let Err(e) = stmt
                .unbind_cols()
                .into_result(stmt)
                .and_then(|()| stmt.set_num_rows_fetched(None).into_result(stmt))
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
