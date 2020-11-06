use crate::{
    buffers::BindColArgs, handles::Description, handles::Statement, ColumnDescription, Error,
};
use odbc_sys::{Len, SqlDataType, ULen};
use std::{borrow::BorrowMut, marker::PhantomData, thread::panicking};

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
    fn fetch(&mut self) -> Result<bool, Error>;

    /// Sets the batch size for bulk cursors, if retrieving many rows at once.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that buffers bound using `bind_col` can hold the
    /// specified amount of rows.
    unsafe fn set_row_array_size(&mut self, size: u32) -> Result<(), Error>;

    /// Bind an integer to hold the number of rows retrieved with fetch in the current row set.
    ///
    /// # Safety
    ///
    /// `num_rows` must not be moved and remain valid, as long as it remains bound to the cursor.
    unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut ULen) -> Result<(), Error>;

    /// Sets the binding type to columnar binding for batch cursors.
    ///
    /// Any Positive number indicates a row wise binding with that row length. `0` indicates a
    /// columnar binding.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the bound buffers match the memory layout
    /// specified by this function.
    unsafe fn set_row_bind_type(&mut self, row_size: u32) -> Result<(), Error>;

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
        bind_params: BindColArgs,
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
    fn col_octet_length(&self, column_number: u16) -> Result<Len, Error>;

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&self, column_number: u16) -> Result<Len, Error>;

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    fn col_precision(&self, column_number: u16) -> Result<Len, Error>;

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    fn col_scale(&self, column_number: u16) -> Result<Len, Error>;

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    fn col_name(&self, column_number: u16, buf: &mut Vec<u16>) -> Result<(), Error>;

    fn application_row_descriptor(&self) -> Result<Description, Error>;
}

/// Cursors are used to process and iterate the result sets returned by executing queries.
pub struct CursorImpl<'open_connection, Stmt: BorrowMut<Statement<'open_connection>>> {
    statement: Stmt,
    // If we would not implement the drop handler, we could do without the Phantom member and an
    // overall simpler declaration (without any lifetimes), since we could instead simply specialize
    // each implementation. Since drop handlers can not specialized, though we need to deal with
    // this.
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

    fn fetch(&mut self) -> Result<bool, Error> {
        self.statement.borrow_mut().fetch()
    }

    unsafe fn set_row_array_size(&mut self, size: u32) -> Result<(), Error> {
        self.statement.borrow_mut().set_row_array_size(size)
    }

    unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut ULen) -> Result<(), Error> {
        self.statement.borrow_mut().set_num_rows_fetched(num_rows)
    }

    unsafe fn set_row_bind_type(&mut self, row_size: u32) -> Result<(), Error> {
        self.statement.borrow_mut().set_row_bind_type(row_size)
    }

    fn unbind_cols(&mut self) -> Result<(), Error> {
        self.statement.borrow_mut().unbind_cols()
    }

    unsafe fn bind_col(
        &mut self,
        column_number: u16,
        bind_params: BindColArgs,
    ) -> Result<(), Error> {
        let BindColArgs {
            target_type,
            target_value,
            target_length,
            indicator,
        } = bind_params;
        self.statement.borrow_mut().bind_col(
            column_number,
            target_type,
            target_value,
            target_length,
            indicator,
        )
    }

    fn is_unsigned_column(&self, column_number: u16) -> Result<bool, Error> {
        self.statement.borrow().is_unsigned_column(column_number)
    }

    fn bind_buffer<B>(mut self, mut row_set_buffer: B) -> Result<RowSetCursor<Self, B>, Error>
    where
        B: RowSetBuffer,
    {
        unsafe {
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

    fn col_octet_length(&self, column_number: u16) -> Result<Len, Error> {
        self.statement.borrow().col_octet_length(column_number)
    }

    fn col_display_size(&self, column_number: u16) -> Result<Len, Error> {
        self.statement.borrow().col_display_size(column_number)
    }

    fn col_precision(&self, column_number: u16) -> Result<Len, Error> {
        self.statement.borrow().col_precision(column_number)
    }

    fn col_scale(&self, column_number: u16) -> Result<Len, Error> {
        self.statement.borrow().col_scale(column_number)
    }

    fn col_name(&self, column_number: u16, buf: &mut Vec<u16>) -> Result<(), Error> {
        self.statement.borrow().col_name(column_number, buf)
    }

    fn application_row_descriptor(&self) -> Result<Description, Error> {
        self.statement.borrow().application_row_descriptor()
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
    /// Binds the buffer either column or row wise to the cursor.
    ///
    /// # Safety
    ///
    /// It's the implementations responsibility to ensure that all bound buffers are valid as
    /// specified and live long enough.
    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), Error>;
}

unsafe impl<T: RowSetBuffer> RowSetBuffer for &mut T {
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
        if self.cursor.fetch()? {
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
