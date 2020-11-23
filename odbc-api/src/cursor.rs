use crate::{
    handles::{CDataMut, Description, Statement},
    ColumnDescription, Error,
};

use odbc_sys::SqlDataType;

use std::{
    borrow::BorrowMut,
    char::{decode_utf16, REPLACEMENT_CHARACTER},
    convert::TryInto,
    marker::PhantomData,
    thread::panicking,
};

/// Cursors are used to process and iterate the result sets returned by executing queries.
pub trait Cursor: Sized {
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

    /// Returns the next set of rows in the result set.
    ///
    /// If any columns are bound, it returns the data in those columns. If the application has
    /// specified a pointer to a row status array or a buffer in which to return the number of rows
    /// fetched, `fetch` also returns this information. Calls to `fetch` can be mixed with calls to
    /// `fetch_scroll`.
    ///
    /// # Safety
    ///
    /// Fetch dereferences bound buffers and is therefore unsafe.
    unsafe fn fetch(&mut self) -> Result<bool, Error>;

    /// Release all column buffers bound by `bind_col`. Except bookmark column.
    fn unbind_cols(&mut self) -> Result<(), Error>;

    /// Binds application data buffers to columns in the result set
    ///
    /// * `column_number`: `0` is the bookmark column. It is not included in some result sets. All
    /// other columns are numbered starting with `1`. It is an error to bind a higher-numbered
    /// column than there are columns in the result set. This error cannot be detected until the
    /// result set has been created, so it is returned by `fetch`, not `bind_col`.
    /// * `target_type`: The identifier of the C data type of the `value` buffer. When it is
    /// retrieving data from the data source with `fetch`, the driver converts the data to this
    /// type. When it sends data to the source, the driver converts the data from this type.
    /// * `target_value`: Pointer to the data buffer to bind to the column.
    /// * `target_length`: Length of target value in bytes. (Or for a single element in case of bulk
    /// aka. block fetching data).
    /// * `indicator`: Buffer is going to hold length or indicator values.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to make sure the bound columns live until they are no
    /// longer bound.
    unsafe fn bind_col(
        &mut self,
        column_number: u16,
        target: &mut impl CDataMut,
    ) -> Result<(), Error>;

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn is_unsigned_column(&self, column_number: u16) -> Result<bool, Error>;

    /// Binds this cursor to a buffer holding a row set.
    fn bind_buffer<B>(self, row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        B: RowSetBuffer;

    /// SqlDataType
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_type(&self, column_number: u16) -> Result<SqlDataType, Error>;

    /// The concise data type. For the datetime and interval data types, this field returns the
    /// concise data type; for example, `TIME` or `INTERVAL_YEAR`.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_concise_type(&self, column_number: u16) -> Result<SqlDataType, Error>;

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

    fn application_row_descriptor(&self) -> Result<Description<'_>, Error>;
}

/// An iterator calling `col_name` for each column_name and converting the result into UTF-8. See
/// `Cursor::column_names`.
pub struct ColumnNamesIt<'c, C> {
    cursor: &'c C,
    buffer: Vec<u16>,
    column: u16,
    num_cols: u16,
}

impl<'c, C: Cursor> ColumnNamesIt<'c, C> {
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
/// by either a prepared query or direct execution. Usually utilized throught the `Cursor` trait.
pub struct CursorImpl<'open_connection, Stmt: BorrowMut<Statement<'open_connection>>> {
    statement: Stmt,
    // If we would not implement the drop handler, we could do without the Phantom member and an
    // overall simpler declaration (without any lifetimes), since we could instead simply
    // specialize each implementation. Since drop handlers can not specialized, though we need
    // to deal with this.
    connection: PhantomData<Statement<'open_connection>>,
}

impl<'o, S> Drop for CursorImpl<'o, S>
where
    S: BorrowMut<Statement<'o>>,
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
    S: BorrowMut<Statement<'o>>,
{
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

    unsafe fn fetch(&mut self) -> Result<bool, Error> {
        self.statement.borrow_mut().fetch()
    }

    fn unbind_cols(&mut self) -> Result<(), Error> {
        self.statement.borrow_mut().unbind_cols()
    }

    unsafe fn bind_col(
        &mut self,
        column_number: u16,
        target: &mut impl CDataMut,
    ) -> Result<(), Error> {
        self.statement.borrow_mut().bind_col(column_number, target)
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
            stmt.set_num_rows_fetched(row_set_buffer.mut_num_fetch_rows())?;
            row_set_buffer.bind_to_cursor(&mut self)?;
        }
        Ok(RowSetCursor::new(row_set_buffer, self))
    }

    fn col_type(&self, column_number: u16) -> Result<SqlDataType, Error> {
        self.statement.borrow().col_type(column_number)
    }

    fn col_concise_type(&self, column_number: u16) -> Result<SqlDataType, Error> {
        self.statement.borrow().col_type(column_number)
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

    fn application_row_descriptor(&self) -> Result<Description, Error> {
        self.statement.borrow().application_row_descriptor()
    }

    fn column_names(&self) -> Result<ColumnNamesIt<'_, Self>, Error> {
        ColumnNamesIt::new(self)
    }
}

impl<'o, S> CursorImpl<'o, S>
where
    S: BorrowMut<Statement<'o>>,
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
    /// It's the implementations responsibility to ensure that all bound buffers are valid as
    /// specified and live long enough.
    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error>;
}

unsafe impl<T: RowSetBuffer> RowSetBuffer for &mut T {
    fn row_array_size(&self) -> u32 {
        (**self).row_array_size()
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        (*self).mut_num_fetch_rows()
    }

    fn bind_type(&self) -> u32 {
        (**self).bind_type()
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error> {
        (*self).bind_to_cursor(cursor)
    }
}

/// A row set cursor iterates in blocks over row sets, filling them in buffers, instead of iterating
/// the result set row by row. This is usually much faster.
pub struct RowSetCursor<C, B> {
    buffer: B,
    cursor: C,
}

impl<C, B> RowSetCursor<C, B> {
    fn new(buffer: B, cursor: C) -> Self {
        Self { buffer, cursor }
    }
}

impl<C, B> RowSetCursor<C, B>
where
    C: Cursor,
{
    /// Fills the bound buffer with the next row set.
    ///
    /// # Return
    ///
    /// `None` if the result set is empty and all row sets have been extracted. `Some` with a
    /// reference to the internal buffer otherwise.
    pub fn fetch(&mut self) -> Result<Option<&B>, Error> {
        let has_data = unsafe { self.cursor.fetch()? };
        if has_data {
            Ok(Some(&self.buffer))
        } else {
            Ok(None)
        }
    }

    /// Unbind the buffer, leaving the cursor free to bind another buffer to it.
    pub fn unbind(mut self) -> Result<C, Error> {
        self.cursor.unbind_cols()?;
        Ok(self.cursor)
    }
}
