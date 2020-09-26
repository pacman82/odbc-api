use crate::{
    buffers::BindColParameters, handles::Description, handles::Statement, ColumnDescription, Error,
};
use odbc_sys::{Len, SmallInt, SqlDataType, UInteger, ULen, USmallInt, WChar};
use std::thread::panicking;

/// Cursors are used to process and iterate the result sets returned by executing queries.
pub struct Cursor<'open_connection> {
    statement: Statement<'open_connection>,
}

impl<'o> Drop for Cursor<'o> {
    fn drop(&mut self) {
        if let Err(e) = self.statement.close_cursor() {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexepected error closing cursor: {:?}", e)
            }
        }
    }
}

impl<'o> Cursor<'o> {
    pub(crate) fn new(statement: Statement<'o>) -> Self {
        Self { statement }
    }

    /// Fetch a column description using the column index.
    ///
    /// # Parameters
    ///
    /// * `column_number`: Column index. `0` is the bookmark column. The other column indices start
    /// with `1`.
    /// * `column_description`: Holds the description of the column after the call. This method does
    /// not provide strong exception safety as the value of this argument is undefined in case of an
    /// error.
    pub fn describe_col(
        &self,
        column_number: USmallInt,
        column_description: &mut ColumnDescription,
    ) -> Result<(), Error> {
        self.statement
            .describe_col(column_number, column_description)?;
        Ok(())
    }

    /// Number of columns in result set.
    pub fn num_result_cols(&self) -> Result<SmallInt, Error> {
        self.statement.num_result_cols()
    }

    /// Returns the next rowset in the result set.
    ///
    /// If any columns are bound, it returns the data in those columns. If the application has
    /// specified a pointer to a row status array or a buffer in which to return the number of rows
    /// fetched, `fetch` also returns this information. Calls to `fetch` can be mixed with calls to
    /// `fetch_scroll`.
    pub fn fetch(&mut self) -> Result<bool, Error> {
        self.statement.fetch()
    }

    /// Sets the batch size for bulk cursors, if retrieving many rows at once.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that buffers bound using `bind_col` can hold the
    /// specified amount of rows.
    pub unsafe fn set_row_array_size(&mut self, size: UInteger) -> Result<(), Error> {
        self.statement.set_row_array_size(size)
    }

    /// Bind an integer to hold the number of rows retrieved with fetch in the current row set.
    ///
    /// # Safety
    ///
    /// `num_rows` must not be moved and remain valid, as long as it remains bound to the cursor.
    pub unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut ULen) -> Result<(), Error> {
        self.statement.set_num_rows_fetched(num_rows)
    }

    /// Sets the binding type to columnar binding for batch cursors.
    ///
    /// Any Positive number indicates a row wise binding with that row length. `0` indicates a
    /// columnar binding.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that the bound buffers match the memory layout
    /// specified by this function.
    pub unsafe fn set_row_bind_type(&mut self, row_size: u32) -> Result<(), Error> {
        self.statement.set_row_bind_type(row_size)
    }

    /// Release all columen buffers bound by `bind_col`. Except bookmark column.
    pub fn unbind_cols(&mut self) -> Result<(), Error> {
        self.statement.unbind_cols()
    }

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
    pub unsafe fn bind_col(
        &mut self,
        column_number: USmallInt,
        bind_params: BindColParameters,
    ) -> Result<(), Error> {
        let BindColParameters {
            target_type,
            target_value,
            target_length,
            indicator,
        } = bind_params;
        self.statement.bind_col(
            column_number,
            target_type,
            target_value,
            target_length,
            indicator,
        )
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn is_unsigned_column(&self, column_number: USmallInt) -> Result<bool, Error> {
        self.statement.is_unsigned_column(column_number)
    }

    /// Binds this cursor to a buffer holding a row set.
    pub fn bind_row_set_buffer<'b, B>(
        mut self,
        row_set_buffer: &'b mut B,
    ) -> Result<RowSetCursor<'b, 'o, B>, Error>
    where
        B: RowSetBuffer,
    {
        unsafe {
            row_set_buffer.bind_to_cursor(&mut self)?;
        }
        Ok(RowSetCursor::new(row_set_buffer, self))
    }

    /// SqlDataType
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_type(&self, column_number: USmallInt) -> Result<SqlDataType, Error> {
        self.statement.col_type(column_number)
    }

    /// The concise data type. For the datetime and interval data types, this field returns the
    /// concise data type; for example, `TIME` or `INTERVAL_YEAR`.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_concise_type(&self, column_number: USmallInt) -> Result<SqlDataType, Error> {
        self.statement.col_type(column_number)
    }

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_octet_length(&self, column_number: USmallInt) -> Result<Len, Error> {
        self.statement.col_octet_length(column_number)
    }

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_display_size(&self, column_number: USmallInt) -> Result<Len, Error> {
        self.statement.col_display_size(column_number)
    }

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    pub fn col_precision(&self, column_number: USmallInt) -> Result<Len, Error> {
        self.statement.col_precision(column_number)
    }

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    pub fn col_scale(&self, column_number: USmallInt) -> Result<Len, Error> {
        self.statement.col_scale(column_number)
    }

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    pub fn col_name(&self, column_number: USmallInt, buf: &mut Vec<WChar>) -> Result<(), Error> {
        self.statement.col_name(column_number, buf)
    }

    pub fn application_row_descriptor(&self) -> Result<Description, Error> {
        self.statement.application_row_descriptor()
    }
}

/// A Row set buffer binds row, or column wise buffers to a cursor in order to fill them with row
/// sets with each call to fetch.
pub unsafe trait RowSetBuffer {
    unsafe fn bind_to_cursor(&mut self, cursor: &mut Cursor) -> Result<(), Error>;
}

/// A row set cursor iterates blockwise over row sets, filling them in buffers, instead of iterating
/// the result set row by row.
pub struct RowSetCursor<'b, 'o, B> {
    buffer: &'b mut B,
    cursor: Cursor<'o>,
}

impl<'b, 'o, B> RowSetCursor<'b, 'o, B> {
    fn new(buffer: &'b mut B, cursor: Cursor<'o>) -> Self {
        Self { buffer, cursor }
    }

    /// Fills the bound buffer with the next row set.
    ///
    /// # Return
    ///
    /// `None` if the result set is empty and all row sets have been extracetd. `Some` with a
    /// reference to the internal buffer otherwise.
    pub fn fetch(&mut self) -> Result<Option<&B>, Error> {
        if self.cursor.fetch()? {
            Ok(Some(self.buffer))
        } else {
            Ok(None)
        }
    }

    /// Unbind the buffer, leaving the cursor free to bind another buffer to it.
    pub fn unbind(mut self) -> Result<Cursor<'o>, Error> {
        self.cursor.unbind_cols()?;
        Ok(self.cursor)
    }
}
