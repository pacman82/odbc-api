use super::{
    as_handle::AsHandle,
    buffer::{buf_ptr, clamp_small_int, mut_buf_ptr},
    column_description::{ColumnDescription, Nullable},
    error::{Error, ToResult},
};
use odbc_sys::{
    CDataType, FreeStmtOption, HDbc, HStmt, Handle, HandleType, Len, Pointer, SQLBindCol,
    SQLCloseCursor, SQLDescribeColW, SQLExecDirectW, SQLFetch, SQLFreeHandle, SQLFreeStmt,
    SQLNumResultCols, SQLSetStmtAttrW, SmallInt, SqlDataType, SqlReturn, StatementAttribute,
    UInteger, ULen, USmallInt,
};
use std::{convert::TryInto, marker::PhantomData, thread::panicking};
use widestring::U16Str;

/// Wraps a valid (i.e. successfully allocated) ODBC statement handle.
pub struct Statement<'s> {
    parent: PhantomData<&'s HDbc>,
    handle: HStmt,
}

unsafe impl<'c> AsHandle for Statement<'c> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl<'s> Drop for Statement<'s> {
    fn drop(&mut self) {
        unsafe {
            match SQLFreeHandle(HandleType::Stmt, self.handle as Handle) {
                SqlReturn::SUCCESS => (),
                other => {
                    // Avoid panicking, if we already have a panic. We don't want to mask the
                    // original error.
                    if !panicking() {
                        panic!("Unexepected return value of SQLFreeHandle: {:?}", other)
                    }
                }
            }
        }
    }
}

impl<'s> Statement<'s> {
    /// # Safety
    ///
    /// `handle` must be a valid (successfully allocated) statement handle.
    pub unsafe fn new(handle: HStmt) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    /// Executes a preparable statement, using the current values of the parameter marker variables
    /// if any parameters exist in the statement. SQLExecDirect is the fastest way to submit an SQL
    /// statement for one-time execution.
    ///
    /// # Return
    ///
    /// Returns `true` if a cursor is created. If `false` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows. If a cursor is created. `close_cursor` should be called after it is no longer used.
    pub fn exec_direct(&mut self, statement_text: &U16Str) -> Result<bool, Error> {
        unsafe {
            match SQLExecDirectW(
                self.handle,
                buf_ptr(statement_text.as_slice()),
                statement_text.len().try_into().unwrap(),
            ) {
                SqlReturn::NO_DATA => Ok(false),
                other => other.to_result(self).map(|()| true),
            }
        }
    }

    /// Close an open cursor.
    pub fn close_cursor(&mut self) -> Result<(), Error> {
        unsafe { SQLCloseCursor(self.handle) }.to_result(self)
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
        let name = &mut column_description.name;
        // Use maximum available capacity.
        name.resize(name.capacity(), 0);
        let mut name_length: SmallInt = 0;
        column_description.data_type = SqlDataType::UnknownType;
        column_description.column_size = 0;
        column_description.decimal_digits = 0;
        let mut nullable = odbc_sys::Nullable::UNKNOWN;

        unsafe {
            SQLDescribeColW(
                self.handle,
                column_number,
                mut_buf_ptr(name),
                clamp_small_int(name.len()),
                &mut name_length,
                &mut column_description.data_type,
                &mut column_description.column_size,
                &mut column_description.decimal_digits,
                &mut nullable,
            )
            .to_result(self)?;
        }

        column_description.nullable = match nullable {
            odbc_sys::Nullable::UNKNOWN => Nullable::Unknown,
            odbc_sys::Nullable::NO_NULLS => Nullable::NoNulls,
            odbc_sys::Nullable::NULLABLE => Nullable::Nullable,
            other => panic!("ODBC returned invalid value for Nullable: {:?}", other),
        };

        if name_length > clamp_small_int(name.len()) {
            // Buffer is to small to hold name, retry with larger buffer
            name.resize(name_length as usize + 1, 0);
            self.describe_col(column_number, column_description)
        } else {
            name.resize(name_length as usize, 0);
            Ok(())
        }
    }

    /// Number of columns in result set.
    pub fn num_result_cols(&self) -> Result<SmallInt, Error> {
        let mut out: SmallInt = 0;
        unsafe { SQLNumResultCols(self.handle, &mut out) }.to_result(self)?;
        Ok(out)
    }

    /// Sets the batch size for bulk cursors, if retrieving many rows at once.
    pub unsafe fn set_row_array_size(&mut self, size: UInteger) -> Result<(), Error> {
        assert!(size > 0);
        SQLSetStmtAttrW(
            self.handle,
            StatementAttribute::RowArraySize,
            size as Pointer,
            0,
        )
        .to_result(self)
    }

    /// Bind an integer to hold the number of rows retrieved with fetch in the current row set.
    pub unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut ULen) -> Result<(), Error> {
        SQLSetStmtAttrW(
            self.handle,
            StatementAttribute::RowsFetchedPtr,
            num_rows as *mut ULen as Pointer,
            0,
        )
        .to_result(self)
    }

    /// Sets the binding type to columnar binding for batch cursors.
    ///
    /// Any Positive number indicates a row wise binding with that row length. `0` indicates a
    /// columnar binding.
    pub unsafe fn set_row_bind_type(&mut self, row_size: u32) -> Result<(), Error> {
        SQLSetStmtAttrW(
            self.handle,
            StatementAttribute::RowBindType,
            row_size as Pointer,
            0,
        )
        .to_result(self)
    }

    /// Returns the next rowset in the result set.
    ///
    /// It can be called only while a result set exists: I.e., after a call that creates a result
    /// set and before the cursor over that result set is closed. If any columns are bound, it
    /// returns the data in those columns. If the application has specified a pointer to a row
    /// status array or a buffer in which to return the number of rows fetched, `fetch` also returns
    /// this information. Calls to `fetch` can be mixed with calls to `fetch_scroll`.
    pub fn fetch(&mut self) -> Result<bool, Error> {
        unsafe {
            match SQLFetch(self.handle) {
                SqlReturn::NO_DATA => Ok(false),
                other => other.to_result(self).map(|()| true),
            }
        }
    }

    /// Release all columen buffers bound by `bind_col`. Except bookmark column.
    pub fn unbind_cols(&mut self) -> Result<(), Error> {
        unsafe { SQLFreeStmt(self.handle, FreeStmtOption::Unbind) }.to_result(self)
    }

    /// Binds application data buffers to columns in the result set.
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
        SQLBindCol(
            self.handle,
            column_number,
            target_type,
            target_value,
            target_length,
            indicator,
        )
        .to_result(self)
    }
}
