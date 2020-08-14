use crate::{handles::Statement, ColumnDescription, Error};
use odbc_sys::{SmallInt, USmallInt, UInteger, ULen, Len, CDataType, Pointer};
use std::thread::panicking;

pub struct Cursor<'open_connection> {
    statement: Statement<'open_connection>,
}

impl<'o> Drop for Cursor<'o> {
    fn drop(&mut self) {
        if let Err(e) = self.statement.close_cursor() {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexepected error disconnecting: {:?}", e)
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
    pub unsafe fn set_row_array_size(&mut self, size: UInteger) -> Result<(), Error>{
        self.statement.set_row_array_size(size)
    }

    /// Bind an integer to hold the number of rows retrieved with fetch in the current row set.
    pub unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut ULen) -> Result<(), Error> {
        self.statement.set_num_rows_fetched(num_rows)
    }

    /// Sets the binding type to columnar binding for batch cursors.
    ///
    /// Any Positive number indicates a row wise binding with that row length. `0` indicates a
    /// columnar binding.
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
        target_type: CDataType,
        target_value: Pointer,
        target_length: Len,
        indicator: *mut Len,
    ) -> Result<(), Error> {
        self.statement.bind_col(column_number, target_type, target_value, target_length, indicator)
    }
}
