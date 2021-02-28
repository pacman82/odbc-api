use crate::{
    handles::{Statement, StatementImpl},
    parameter::VarCharMut,
    ColumnDescription, DataType, Error, Output,
};

use odbc_sys::{NO_TOTAL, NULL_DATA};

use std::{
    borrow::BorrowMut,
    char::{decode_utf16, REPLACEMENT_CHARACTER},
    cmp::max,
    convert::TryInto,
    marker::PhantomData,
    thread::panicking,
};

/// Cursors are used to process and iterate the result sets returned by executing queries.
pub trait Cursor {
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

    /// Provides access to the underlying statement handle.
    ///
    /// # Safety
    ///
    /// Assinging to this statement handle or binding buffers to it may invalidate the invariants
    /// of safe wrapper types (i.e. [`crate::RowSetCursor`]). Some actions like closing the cursor
    /// may just result in ODBC transition errors, others like binding columns may even cause actual
    /// invalid memory access if not used with care.
    unsafe fn stmt(&mut self) -> &mut Self::Statement;

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
    ) -> Result<(), Error>;

    /// Number of columns in result set.
    fn num_result_cols(&self) -> Result<i16, Error>;

    /// Advances the cursor to the next row in the result set.
    ///
    /// While this method is very convinient due to the fact that the application does not have to
    /// declare and bind specific buffers it is also in many situations extremly slow. Concrete
    /// performance depends on the ODBC driver in question, but it is likely it performs a roundtrip
    /// to the datasource for each individual row. It is also likely an extra conversion is
    /// performed then requesting individual fields, since the C buffer type is not known to the
    /// driver in advance. Consider binding a buffer to the cursor first using
    /// [`Self::bind_buffer`].
    fn next_row(&mut self) -> Result<Option<CursorRow<'_, Self::Statement>>, Error> {
        let row_available = unsafe { self.stmt().fetch()? };
        let ret = if row_available {
            Some(CursorRow::new(unsafe { self.stmt() }))
        } else {
            None
        };
        Ok(ret)
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn is_unsigned_column(&self, column_number: u16) -> Result<bool, Error>;

    /// Binds this cursor to a buffer holding a row set.
    fn bind_buffer<B>(self, row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        Self: Sized,
        B: RowSetBuffer;

    /// Data type of the specified column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_data_type(&self, column_number: u16) -> Result<DataType, Error>;

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_octet_length(&self, column_number: u16) -> Result<isize, Error>;

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&self, column_number: u16) -> Result<isize, Error>;

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    fn col_precision(&self, column_number: u16) -> Result<isize, Error>;

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    fn col_scale(&self, column_number: u16) -> Result<isize, Error>;

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    fn col_name(&self, column_number: u16, buf: &mut Vec<u16>) -> Result<(), Error>;

    /// Use this if you want to iterate over all column names and allocate a `String` for each one.
    ///
    /// This is a wrapper around `col_name` introduced for convenience.
    fn column_names(&self) -> Result<ColumnNamesIt<'_, Self>, Error> {
        ColumnNamesIt::new(self)
    }
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
    /// Fills a suitable taregt buffer with a field from the current row of the result set. This
    /// method drains the data from the field. It can be called repeatedly to if not all the data
    /// fit in the output buffer at once. It should not called repeatedly to fetch the same value
    /// twice. Column index starts at `1`.
    pub fn get_data(
        &mut self,
        col_or_param_num: u16,
        target: &mut impl Output,
    ) -> Result<(), Error> {
        self.statement.get_data(col_or_param_num, target)
    }

    /// Retrieves arbitrary large character data from the row and stores it in the buffer. Column
    /// index starts at `1`.
    pub fn get_text(&mut self, col_or_param_num: u16, buf: &mut Vec<u8>) -> Result<(), Error> {
        // Utilize all of the allocated buffer. Make sure buffer can at least hold the terminating
        // zero.
        buf.resize(max(1, buf.capacity()), 0);
        // We repeatedly fetch data and add it to the buffer. The buffer length is therfore the
        // accumulated value size. This variable keeps track of the number of bytes we added with
        // the current call to get_data.
        let mut fetch_size: isize = buf.len().try_into().unwrap();
        let mut target = VarCharMut::from_buffer(buf.as_mut_slice(), None);
        // Fetch binary data into buffer.
        self.get_data(col_or_param_num, &mut target)?;
        loop {
            match target.indicator() {
                // Value is `NULL`. We are done here.
                NULL_DATA => break,
                // We do not know how large the value is. Let's fetch the data with repeated calls
                // to get_data.
                NO_TOTAL => {
                    let old_len = buf.len();
                    // Use an exponantial strategy for increasing buffer size. +1 For handling
                    // initial buffer size of 1.
                    buf.resize(old_len * 2, 0);
                    target = VarCharMut::from_buffer(&mut buf[(old_len - 1)..], None);
                    self.get_data(col_or_param_num, &mut target)?;
                }
                // We did get the complete value, inculding the terminating zero. Let's resize the
                // buffer to match the retrieved value exactly (exculding terminating zero).
                indicator if indicator < fetch_size => {
                    assert!(indicator >= 0);
                    // Since the indicator refers to value length without terminating zero, this
                    // also implicitly drops the terminating zero at the end of the buffer.
                    let shrink_by: usize = (fetch_size - indicator).try_into().unwrap();
                    buf.resize(buf.len() - shrink_by, 0);
                    break;
                }
                // We did not get all of the value in one go, but the data source has been friendly
                // enough to tell us how much is missing.
                indicator => {
                    let still_missing = (indicator - fetch_size + 1) as usize;
                    fetch_size = (still_missing + 1).try_into().unwrap();
                    let old_len = buf.len();
                    buf.resize(old_len + still_missing, 0);
                    target = VarCharMut::from_buffer(&mut buf[(old_len - 1)..], None);
                    self.get_data(col_or_param_num, &mut target)?;
                }
            }
        }
        Ok(())
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

impl<'c, C: Cursor + ?Sized> ColumnNamesIt<'c, C> {
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
    C: Cursor,
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
pub struct CursorImpl<'open_connection, Stmt: BorrowMut<StatementImpl<'open_connection>>> {
    statement: Stmt,
    // If we would not implement the drop handler, we could do without the Phantom member and an
    // overall simpler declaration (without any lifetimes), since we could instead simply
    // specialize each implementation. Since drop handlers can not specialized, though we need
    // to deal with this.
    connection: PhantomData<StatementImpl<'open_connection>>,
}

impl<'o, S> Drop for CursorImpl<'o, S>
where
    S: BorrowMut<StatementImpl<'o>>,
{
    fn drop(&mut self) {
        if let Err(e) = self.statement.borrow_mut().close_cursor() {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexpected error closing cursor: {:?}", e)
            }
        }
    }
}

impl<'o, S> Cursor for CursorImpl<'o, S>
where
    S: BorrowMut<StatementImpl<'o>>,
{
    type Statement = StatementImpl<'o>;

    unsafe fn stmt(&mut self) -> &mut Self::Statement {
        self.statement.borrow_mut()
    }

    fn describe_col(
        &self,
        column_number: u16,
        column_description: &mut ColumnDescription,
    ) -> Result<(), Error> {
        self.statement
            .borrow()
            .describe_col(column_number, column_description)?;
        Ok(())
    }

    fn num_result_cols(&self) -> Result<i16, Error> {
        self.statement.borrow().num_result_cols()
    }

    fn is_unsigned_column(&self, column_number: u16) -> Result<bool, Error> {
        self.statement.borrow().is_unsigned_column(column_number)
    }

    fn bind_buffer<B>(mut self, mut row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        B: RowSetBuffer,
    {
        let stmt = self.statement.borrow_mut();
        unsafe {
            stmt.set_row_bind_type(row_set_buffer.bind_type())?;
            stmt.set_row_array_size(row_set_buffer.row_array_size())?;
            stmt.set_num_rows_fetched(Some(row_set_buffer.mut_num_fetch_rows()))?;
            row_set_buffer.bind_to_cursor(&mut self)?;
        }
        Ok(RowSetCursor::new(row_set_buffer, self))
    }

    fn col_data_type(&self, column_number: u16) -> Result<DataType, Error> {
        self.statement.borrow().col_data_type(column_number)
    }

    fn col_octet_length(&self, column_number: u16) -> Result<isize, Error> {
        self.statement.borrow().col_octet_length(column_number)
    }

    fn col_display_size(&self, column_number: u16) -> Result<isize, Error> {
        self.statement.borrow().col_display_size(column_number)
    }

    fn col_precision(&self, column_number: u16) -> Result<isize, Error> {
        self.statement.borrow().col_precision(column_number)
    }

    fn col_scale(&self, column_number: u16) -> Result<isize, Error> {
        self.statement.borrow().col_scale(column_number)
    }

    fn col_name(&self, column_number: u16, buf: &mut Vec<u16>) -> Result<(), Error> {
        self.statement.borrow().col_name(column_number, buf)
    }

    fn column_names(&self) -> Result<ColumnNamesIt<'_, Self>, Error> {
        ColumnNamesIt::new(self)
    }
}

impl<'o, S> CursorImpl<'o, S>
where
    S: BorrowMut<StatementImpl<'o>>,
{
    pub(crate) fn new(statement: S) -> Self {
        Self {
            statement,
            connection: PhantomData,
        }
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
    fn bind_type(&self) -> u32;

    /// The batch size for bulk cursors, if retrieving many rows at once.
    fn row_array_size(&self) -> u32;

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
    fn bind_type(&self) -> u32 {
        (**self).bind_type()
    }

    fn row_array_size(&self) -> u32 {
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
        let has_data = unsafe { self.cursor.stmt().fetch()? };
        if has_data {
            Ok(Some(&self.buffer))
        } else {
            Ok(None)
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
                .and_then(|()| stmt.set_num_rows_fetched(None))
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
