use super::{
    as_handle::AsHandle,
    buffer::{buf_ptr, clamp_small_int, mut_buf_ptr},
    column_description::{ColumnDescription, Nullable},
    data_type::DataType,
    drop_handle,
    error::{Error, IntoResult},
    Description,
};
use odbc_sys::{
    CDataType, Desc, FreeStmtOption, HDbc, HDesc, HStmt, Handle, HandleType, Len, ParamType,
    Pointer, SQLBindCol, SQLBindParameter, SQLCloseCursor, SQLColAttributeW, SQLDescribeColW,
    SQLExecDirectW, SQLExecute, SQLFetch, SQLFreeStmt, SQLGetStmtAttr, SQLNumResultCols,
    SQLPrepareW, SQLSetStmtAttrW, SmallInt, SqlDataType, SqlReturn, StatementAttribute, UInteger,
    ULen, USmallInt, WChar,
};
use std::{convert::TryInto, marker::PhantomData, ptr::null_mut};
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
            drop_handle(self.handle as Handle, HandleType::Stmt);
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

    /// Executes a prepareable statement, using the current values of the parameter marker variables
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
                other => other.into_result(self).map(|()| true),
            }
        }
    }

    /// Send an SQL statement to the data source for preparation. The application can include one or
    /// more parameter markers in the SQL statement. To include a parameter marker, the application
    /// embeds a question mark (?) into the SQL string at the appropriate position.
    pub fn prepare(&mut self, statement_text: &U16Str) -> Result<(), Error> {
        unsafe {
            SQLPrepareW(
                self.handle,
                buf_ptr(statement_text.as_slice()),
                statement_text.len().try_into().unwrap(),
            )
        }
        .into_result(self)
    }

    /// Executes a statement prepared by `prepare`. After the application processes or discards the
    /// results from a call to `execute`, the application can call SQLExecute again with new
    /// parameter values.
    pub fn execute(&mut self) -> Result<(), Error> {
        unsafe { SQLExecute(self.handle) }.into_result(self)
    }

    /// Close an open cursor.
    pub fn close_cursor(&mut self) -> Result<(), Error> {
        unsafe { SQLCloseCursor(self.handle) }.into_result(self)
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
        let mut data_type = SqlDataType::UNKNOWN_TYPE;
        let mut column_size = 0;
        let mut decimal_digits = 0;
        let mut nullable = odbc_sys::Nullable::UNKNOWN;

        unsafe {
            SQLDescribeColW(
                self.handle,
                column_number,
                mut_buf_ptr(name),
                clamp_small_int(name.len()),
                &mut name_length,
                &mut data_type,
                &mut column_size,
                &mut decimal_digits,
                &mut nullable,
            )
            .into_result(self)?;
        }

        column_description.nullable = match nullable {
            odbc_sys::Nullable::UNKNOWN => Nullable::Unknown,
            odbc_sys::Nullable::NO_NULLS => Nullable::NoNulls,
            odbc_sys::Nullable::NULLABLE => Nullable::Nullable,
            other => panic!("ODBC returned invalid value for Nullable: {:?}", other),
        };

        if name_length + 1 > clamp_small_int(name.len()) {
            // Buffer is to small to hold name, retry with larger buffer
            name.resize(name_length as usize + 1, 0);
            self.describe_col(column_number, column_description)
        } else {
            name.resize(name_length as usize, 0);
            column_description.data_type = DataType::new(data_type, column_size, decimal_digits);
            Ok(())
        }
    }

    /// Number of columns in result set.
    pub fn num_result_cols(&self) -> Result<SmallInt, Error> {
        let mut out: SmallInt = 0;
        unsafe { SQLNumResultCols(self.handle, &mut out) }.into_result(self)?;
        Ok(out)
    }

    /// Sets the batch size for bulk cursors, if retrieving many rows at once.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that buffers bound using `bind_col` can hold the
    /// specified amount of rows.
    pub unsafe fn set_row_array_size(&mut self, size: UInteger) -> Result<(), Error> {
        assert!(size > 0);
        SQLSetStmtAttrW(
            self.handle,
            StatementAttribute::RowArraySize,
            size as Pointer,
            0,
        )
        .into_result(self)
    }

    /// Bind an integer to hold the number of rows retrieved with fetch in the current row set.
    ///
    /// # Safety
    ///
    /// `num_rows` must not be moved and remain valid, as long as it remains bound to the cursor.
    pub unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut ULen) -> Result<(), Error> {
        SQLSetStmtAttrW(
            self.handle,
            StatementAttribute::RowsFetchedPtr,
            num_rows as *mut ULen as Pointer,
            0,
        )
        .into_result(self)
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
        SQLSetStmtAttrW(
            self.handle,
            StatementAttribute::RowBindType,
            row_size as Pointer,
            0,
        )
        .into_result(self)
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
                other => other.into_result(self).map(|()| true),
            }
        }
    }

    /// Release all column buffers bound by `bind_col`. Except bookmark column.
    pub fn unbind_cols(&mut self) -> Result<(), Error> {
        unsafe { SQLFreeStmt(self.handle, FreeStmtOption::Unbind) }.into_result(self)
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
        .into_result(self)
    }

    /// Binds a buffer to a parameter marker in an SQL statement.
    ///
    /// See <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function>.
    ///
    /// # Safety
    ///
    /// * It is up to the caller to ensure the lifetimes of the bound parameters.
    /// * Calling this function may influence other statements that share the APD.
    pub unsafe fn bind_parameter(
        &mut self,
        parameter_number: u16,
        input_output_type: ParamType,
        value_type: CDataType,
        parameter_type: SqlDataType,
        column_size: u64,
        decimal_digits: SmallInt,
        parameter_value_ptr: Pointer,
        buffer_length: Len,
        str_len_or_ind_ptr: *mut Len,
    ) -> Result<(), Error> {
        SQLBindParameter(
            self.handle,
            parameter_number,
            input_output_type,
            value_type,
            parameter_type,
            column_size,
            decimal_digits,
            parameter_value_ptr,
            buffer_length,
            str_len_or_ind_ptr,
        )
        .into_result(self)
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn is_unsigned_column(&self, column_number: USmallInt) -> Result<bool, Error> {
        let out = unsafe { self.numeric_col_attribute(Desc::Unsigned, column_number)? };
        match out {
            0 => Ok(false),
            1 => Ok(true),
            _ => panic!("Unsigned column attribute must be either 0 or 1."),
        }
    }

    /// Returns a number identifying the SQL type of the column in the result set.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_type(&self, column_number: USmallInt) -> Result<SqlDataType, Error> {
        let out = unsafe { self.numeric_col_attribute(Desc::Type, column_number)? };
        Ok(SqlDataType(out.try_into().unwrap()))
    }

    /// The concise data type. For the datetime and interval data types, this field returns the
    /// concise data type; for example, `TIME` or `INTERVAL_YEAR`.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_concise_type(&self, column_number: USmallInt) -> Result<SqlDataType, Error> {
        let out = unsafe { self.numeric_col_attribute(Desc::ConciseType, column_number)? };
        Ok(SqlDataType(out.try_into().unwrap()))
    }

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_octet_length(&self, column_number: USmallInt) -> Result<Len, Error> {
        unsafe { self.numeric_col_attribute(Desc::OctetLength, column_number) }
    }

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    pub fn col_display_size(&self, column_number: USmallInt) -> Result<Len, Error> {
        unsafe { self.numeric_col_attribute(Desc::DisplaySize, column_number) }
    }

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    pub fn col_precision(&self, column_number: USmallInt) -> Result<Len, Error> {
        unsafe { self.numeric_col_attribute(Desc::Precision, column_number) }
    }

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    pub fn col_scale(&self, column_number: USmallInt) -> Result<Len, Error> {
        unsafe { self.numeric_col_attribute(Desc::Scale, column_number) }
    }

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    pub fn col_name(&self, column_number: USmallInt, buf: &mut Vec<WChar>) -> Result<(), Error> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: SmallInt = 0;
        // Let's utilize all of `buf`s capacity.
        buf.resize(buf.capacity(), 0);
        unsafe {
            SQLColAttributeW(
                self.handle,
                column_number,
                Desc::Name,
                mut_buf_ptr(buf) as Pointer,
                (buf.len() * 2).try_into().unwrap(),
                &mut string_length_in_bytes as *mut SmallInt,
                null_mut(),
            )
            .into_result(self)?;
            if clamp_small_int(buf.len() * 2) < string_length_in_bytes + 2 {
                buf.resize((string_length_in_bytes / 2 + 1).try_into().unwrap(), 0);
                SQLColAttributeW(
                    self.handle,
                    column_number,
                    Desc::Name,
                    mut_buf_ptr(buf) as Pointer,
                    (buf.len() * 2).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut SmallInt,
                    null_mut(),
                )
                .into_result(self)?;
            }
            // Resize buffer to exact string length without terminal zero
            buf.resize(((string_length_in_bytes + 1) / 2).try_into().unwrap(), 0);
        }
        Ok(())
    }

    pub fn application_row_descriptor(&self) -> Result<Description, Error> {
        let mut hdesc: HDesc = null_mut();
        unsafe {
            SQLGetStmtAttr(
                self.handle,
                StatementAttribute::AppRowDesc,
                &mut hdesc as *mut HDesc as Pointer,
                0,
                null_mut(),
            )
            .into_result(self)?;
            Ok(Description::new(hdesc))
        }
    }

    /// # Safety
    ///
    /// It is the callers responsibility to ensure that `attribute` refers to a numeric attribute.
    unsafe fn numeric_col_attribute(
        &self,
        attribute: Desc,
        column_number: USmallInt,
    ) -> Result<Len, Error> {
        let mut out: Len = 0;
        SQLColAttributeW(
            self.handle,
            column_number,
            attribute,
            null_mut(),
            0,
            null_mut(),
            &mut out as *mut Len,
        )
        .into_result(self)?;
        Ok(out)
    }
}
