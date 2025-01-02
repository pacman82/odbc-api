use super::{
    as_handle::AsHandle,
    bind::{CDataMut, DelayedInput, HasDataType},
    buffer::{clamp_small_int, mut_buf_ptr},
    column_description::{ColumnDescription, Nullability},
    data_type::DataType,
    drop_handle,
    sql_char::{binary_length, is_truncated_bin, resize_to_fit_without_tz},
    sql_result::ExtSqlReturn,
    CData, Descriptor, SqlChar, SqlResult, SqlText,
};
use log::debug;
use odbc_sys::{
    Desc, FreeStmtOption, HDbc, HStmt, Handle, HandleType, Len, ParamType, Pointer, SQLBindCol,
    SQLBindParameter, SQLCloseCursor, SQLDescribeParam, SQLExecute, SQLFetch, SQLFreeStmt,
    SQLGetData, SQLMoreResults, SQLNumParams, SQLNumResultCols, SQLParamData, SQLPutData,
    SQLRowCount, SqlDataType, SqlReturn, StatementAttribute, IS_POINTER,
};
use std::{ffi::c_void, marker::PhantomData, mem::ManuallyDrop, num::NonZeroUsize, ptr::null_mut};

#[cfg(feature = "odbc_version_3_80")]
use odbc_sys::SQLCompleteAsync;

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use odbc_sys::{
    SQLColAttribute as sql_col_attribute, SQLColumns as sql_columns,
    SQLDescribeCol as sql_describe_col, SQLExecDirect as sql_exec_direc,
    SQLForeignKeys as sql_foreign_keys, SQLPrepare as sql_prepare,
    SQLSetStmtAttr as sql_set_stmt_attr, SQLTables as sql_tables,
};

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use odbc_sys::{
    SQLColAttributeW as sql_col_attribute, SQLColumnsW as sql_columns,
    SQLDescribeColW as sql_describe_col, SQLExecDirectW as sql_exec_direc,
    SQLForeignKeysW as sql_foreign_keys, SQLPrepareW as sql_prepare,
    SQLSetStmtAttrW as sql_set_stmt_attr, SQLTablesW as sql_tables,
};

/// An owned valid (i.e. successfully allocated) ODBC statement handle.
pub struct StatementImpl<'s> {
    parent: PhantomData<&'s HDbc>,
    handle: HStmt,
}

unsafe impl AsHandle for StatementImpl<'_> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

impl Drop for StatementImpl<'_> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Stmt);
        }
    }
}

impl StatementImpl<'_> {
    /// # Safety
    ///
    /// `handle` must be a valid (successfully allocated) statement handle.
    pub unsafe fn new(handle: HStmt) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    /// Transfer ownership of this statement to a raw system handle. It is the users responsibility
    /// to call [`crate::sys::SQLFreeHandle`].
    pub fn into_sys(self) -> HStmt {
        // We do not want to run the drop handler, but transfer ownership instead.
        ManuallyDrop::new(self).handle
    }

    /// Special wrapper to a borrowed statement. Acts like a mutable reference to an owned
    /// statement, but allows the lifetime of the tracked connection to stay covariant.
    pub fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        StatementRef {
            parent: self.parent,
            handle: self.handle,
        }
    }
}

/// A borrowed valid (i.e. successfully allocated) ODBC statement handle. This can be used instead
/// of a mutable reference to a [`StatementImpl`]. The main advantage here is that the lifetime
/// paramater remains covariant, whereas if we would just take a mutable reference to an owned
/// statement it would become invariant.
pub struct StatementRef<'s> {
    parent: PhantomData<&'s HDbc>,
    handle: HStmt,
}

impl StatementRef<'_> {
    pub(crate) unsafe fn new(handle: HStmt) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }
}

impl Statement for StatementRef<'_> {
    fn as_sys(&self) -> HStmt {
        self.handle
    }
}

unsafe impl AsHandle for StatementRef<'_> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Stmt
    }
}

/// Allows us to be generic over the ownership type (mutably borrowed or owned) of a statement
pub trait AsStatementRef {
    /// Get an exclusive reference to the underlying statement handle. This method is used to
    /// implement other more higher level methods on top of it. It is not intended to be called by
    /// users of this crate directly, yet it may serve as an escape hatch for low level use cases.
    fn as_stmt_ref(&mut self) -> StatementRef<'_>;
}

impl AsStatementRef for StatementImpl<'_> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.as_stmt_ref()
    }
}

impl AsStatementRef for &mut StatementImpl<'_> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        (*self).as_stmt_ref()
    }
}

impl AsStatementRef for StatementRef<'_> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        unsafe { StatementRef::new(self.handle) }
    }
}

/// An ODBC statement handle. In this crate it is implemented by [`self::StatementImpl`]. In ODBC
/// Statements are used to execute statements and retrieve results. Both parameter and result
/// buffers are bound to the statement and dereferenced during statement execution and fetching
/// results.
///
/// The trait allows us to reason about statements without taking the lifetime of their connection
/// into account. It also allows for the trait to be implemented by a handle taking ownership of
/// both, the statement and the connection.
pub trait Statement: AsHandle {
    /// Gain access to the underlying statement handle without transferring ownership to it.
    fn as_sys(&self) -> HStmt;

    /// Binds application data buffers to columns in the result set.
    ///
    /// * `column_number`: `0` is the bookmark column. It is not included in some result sets. All
    ///   other columns are numbered starting with `1`. It is an error to bind a higher-numbered
    ///   column than there are columns in the result set. This error cannot be detected until the
    ///   result set has been created, so it is returned by `fetch`, not `bind_col`.
    /// * `target_type`: The identifier of the C data type of the `value` buffer. When it is
    ///   retrieving data from the data source with `fetch`, the driver converts the data to this
    ///   type. When it sends data to the source, the driver converts the data from this type.
    /// * `target_value`: Pointer to the data buffer to bind to the column.
    /// * `target_length`: Length of target value in bytes. (Or for a single element in case of bulk
    ///   aka. block fetching data).
    /// * `indicator`: Buffer is going to hold length or indicator values.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to make sure the bound columns live until they are no
    /// longer bound.
    unsafe fn bind_col(&mut self, column_number: u16, target: &mut impl CDataMut) -> SqlResult<()> {
        SQLBindCol(
            self.as_sys(),
            column_number,
            target.cdata_type(),
            target.mut_value_ptr(),
            target.buffer_length(),
            target.mut_indicator_ptr(),
        )
        .into_sql_result("SQLBindCol")
    }

    /// Returns the next row set in the result set.
    ///
    /// It can be called only while a result set exists: I.e., after a call that creates a result
    /// set and before the cursor over that result set is closed. If any columns are bound, it
    /// returns the data in those columns. If the application has specified a pointer to a row
    /// status array or a buffer in which to return the number of rows fetched, `fetch` also returns
    /// this information. Calls to `fetch` can be mixed with calls to `fetch_scroll`.
    ///
    /// # Safety
    ///
    /// Fetch dereferences bound column pointers.
    unsafe fn fetch(&mut self) -> SqlResult<()> {
        SQLFetch(self.as_sys()).into_sql_result("SQLFetch")
    }

    /// Retrieves data for a single column in the result set or for a single parameter.
    fn get_data(&mut self, col_or_param_num: u16, target: &mut impl CDataMut) -> SqlResult<()> {
        unsafe {
            SQLGetData(
                self.as_sys(),
                col_or_param_num,
                target.cdata_type(),
                target.mut_value_ptr(),
                target.buffer_length(),
                target.mut_indicator_ptr(),
            )
        }
        .into_sql_result("SQLGetData")
    }

    /// Release all column buffers bound by `bind_col`. Except bookmark column.
    fn unbind_cols(&mut self) -> SqlResult<()> {
        unsafe { SQLFreeStmt(self.as_sys(), FreeStmtOption::Unbind) }.into_sql_result("SQLFreeStmt")
    }

    /// Bind an integer to hold the number of rows retrieved with fetch in the current row set.
    /// Calling [`Self::unset_num_rows_fetched`] is going to unbind the value from the statement.
    ///
    /// # Safety
    ///
    /// `num_rows` must not be moved and remain valid, as long as it remains bound to the cursor.
    unsafe fn set_num_rows_fetched(&mut self, num_rows: &mut usize) -> SqlResult<()> {
        let value = num_rows as *mut usize as Pointer;
        sql_set_stmt_attr(
            self.as_sys(),
            StatementAttribute::RowsFetchedPtr,
            value,
            IS_POINTER,
        )
        .into_sql_result("SQLSetStmtAttr")
    }

    /// Unsets the integer set by [`Self::set_num_rows_fetched`].
    ///
    /// This being a seperate method from [`Self::set_num_rows_fetched` allows us to write us
    /// cleanup code with less `unsafe` statements since this operation is always safe.
    fn unset_num_rows_fetched(&mut self) -> SqlResult<()> {
        unsafe {
            sql_set_stmt_attr(
                self.as_sys(),
                StatementAttribute::RowsFetchedPtr,
                null_mut(),
                IS_POINTER,
            )
            .into_sql_result("SQLSetStmtAttr")
        }
    }

    /// Fetch a column description using the column index.
    ///
    /// # Parameters
    ///
    /// * `column_number`: Column index. `0` is the bookmark column. The other column indices start
    ///   with `1`.
    /// * `column_description`: Holds the description of the column after the call. This method does
    ///   not provide strong exception safety as the value of this argument is undefined in case of
    ///   an error.
    fn describe_col(
        &self,
        column_number: u16,
        column_description: &mut ColumnDescription,
    ) -> SqlResult<()> {
        let name = &mut column_description.name;
        // Use maximum available capacity.
        name.resize(name.capacity(), 0);
        let mut name_length: i16 = 0;
        let mut data_type = SqlDataType::UNKNOWN_TYPE;
        let mut column_size = 0;
        let mut decimal_digits = 0;
        let mut nullable = odbc_sys::Nullability::UNKNOWN;

        let res = unsafe {
            sql_describe_col(
                self.as_sys(),
                column_number,
                mut_buf_ptr(name),
                clamp_small_int(name.len()),
                &mut name_length,
                &mut data_type,
                &mut column_size,
                &mut decimal_digits,
                &mut nullable,
            )
            .into_sql_result("SQLDescribeCol")
        };

        if res.is_err() {
            return res;
        }

        column_description.nullability = Nullability::new(nullable);

        if name_length + 1 > clamp_small_int(name.len()) {
            // Buffer is to small to hold name, retry with larger buffer
            name.resize(name_length as usize + 1, 0);
            self.describe_col(column_number, column_description)
        } else {
            name.resize(name_length as usize, 0);
            column_description.data_type = DataType::new(data_type, column_size, decimal_digits);
            res
        }
    }

    /// Executes a statement, using the current values of the parameter marker variables if any
    /// parameters exist in the statement. SQLExecDirect is the fastest way to submit an SQL
    /// statement for one-time execution.
    ///
    /// # Safety
    ///
    /// While `self` as always guaranteed to be a valid allocated handle, this function may
    /// dereference bound parameters. It is the callers responsibility to ensure these are still
    /// valid. One strategy is to reset potentially invalid parameters right before the call using
    /// `reset_parameters`.
    ///
    /// # Return
    ///
    /// * [`SqlResult::NeedData`] if execution requires additional data from delayed parameters.
    /// * [`SqlResult::NoData`] if a searched update or delete statement did not affect any rows at
    ///   the data source.
    unsafe fn exec_direct(&mut self, statement: &SqlText) -> SqlResult<()> {
        sql_exec_direc(
            self.as_sys(),
            statement.ptr(),
            statement.len_char().try_into().unwrap(),
        )
        .into_sql_result("SQLExecDirect")
    }

    /// Close an open cursor.
    fn close_cursor(&mut self) -> SqlResult<()> {
        unsafe { SQLCloseCursor(self.as_sys()) }.into_sql_result("SQLCloseCursor")
    }

    /// Send an SQL statement to the data source for preparation. The application can include one or
    /// more parameter markers in the SQL statement. To include a parameter marker, the application
    /// embeds a question mark (?) into the SQL string at the appropriate position.
    fn prepare(&mut self, statement: &SqlText) -> SqlResult<()> {
        unsafe {
            sql_prepare(
                self.as_sys(),
                statement.ptr(),
                statement.len_char().try_into().unwrap(),
            )
        }
        .into_sql_result("SQLPrepare")
    }

    /// Executes a statement prepared by `prepare`. After the application processes or discards the
    /// results from a call to `execute`, the application can call SQLExecute again with new
    /// parameter values.
    ///
    /// # Safety
    ///
    /// While `self` as always guaranteed to be a valid allocated handle, this function may
    /// dereference bound parameters. It is the callers responsibility to ensure these are still
    /// valid. One strategy is to reset potentially invalid parameters right before the call using
    /// `reset_parameters`.
    ///
    /// # Return
    ///
    /// * [`SqlResult::NeedData`] if execution requires additional data from delayed parameters.
    /// * [`SqlResult::NoData`] if a searched update or delete statement did not affect any rows at
    ///   the data source.
    unsafe fn execute(&mut self) -> SqlResult<()> {
        SQLExecute(self.as_sys()).into_sql_result("SQLExecute")
    }

    /// Number of columns in result set.
    ///
    /// Can also be used to check, whether or not a result set has been created at all.
    fn num_result_cols(&self) -> SqlResult<i16> {
        let mut out: i16 = 0;
        unsafe { SQLNumResultCols(self.as_sys(), &mut out) }
            .into_sql_result("SQLNumResultCols")
            .on_success(|| out)
    }

    /// Number of placeholders of a prepared query.
    fn num_params(&self) -> SqlResult<u16> {
        let mut out: i16 = 0;
        unsafe { SQLNumParams(self.as_sys(), &mut out) }
            .into_sql_result("SQLNumParams")
            .on_success(|| out.try_into().unwrap())
    }

    /// Sets the batch size for bulk cursors, if retrieving many rows at once.
    ///
    /// # Safety
    ///
    /// It is the callers responsibility to ensure that buffers bound using `bind_col` can hold the
    /// specified amount of rows.
    unsafe fn set_row_array_size(&mut self, size: usize) -> SqlResult<()> {
        assert!(size > 0);
        sql_set_stmt_attr(
            self.as_sys(),
            StatementAttribute::RowArraySize,
            size as Pointer,
            0,
        )
        .into_sql_result("SQLSetStmtAttr")
    }

    /// Specifies the number of values for each parameter. If it is greater than 1, the data and
    /// indicator buffers of the statement point to arrays. The cardinality of each array is equal
    /// to the value of this field.
    ///
    /// # Safety
    ///
    /// The bound buffers must at least hold the number of elements specified in this call then the
    /// statement is executed.
    unsafe fn set_paramset_size(&mut self, size: usize) -> SqlResult<()> {
        assert!(size > 0);
        sql_set_stmt_attr(
            self.as_sys(),
            StatementAttribute::ParamsetSize,
            size as Pointer,
            0,
        )
        .into_sql_result("SQLSetStmtAttr")
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
    unsafe fn set_row_bind_type(&mut self, row_size: usize) -> SqlResult<()> {
        sql_set_stmt_attr(
            self.as_sys(),
            StatementAttribute::RowBindType,
            row_size as Pointer,
            0,
        )
        .into_sql_result("SQLSetStmtAttr")
    }

    fn set_metadata_id(&mut self, metadata_id: bool) -> SqlResult<()> {
        unsafe {
            sql_set_stmt_attr(
                self.as_sys(),
                StatementAttribute::MetadataId,
                metadata_id as usize as Pointer,
                0,
            )
            .into_sql_result("SQLSetStmtAttr")
        }
    }

    /// Enables or disables asynchronous execution for this statement handle. If asynchronous
    /// execution is not enabled on connection level it is disabled by default and everything is
    /// executed synchronously.
    ///
    /// This is equivalent to stetting `SQL_ATTR_ASYNC_ENABLE` in the bare C API.
    ///
    /// See
    /// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/executing-statements-odbc>
    fn set_async_enable(&mut self, on: bool) -> SqlResult<()> {
        unsafe {
            sql_set_stmt_attr(
                self.as_sys(),
                StatementAttribute::AsyncEnable,
                on as usize as Pointer,
                0,
            )
            .into_sql_result("SQLSetStmtAttr")
        }
    }

    /// Binds a buffer holding an input parameter to a parameter marker in an SQL statement. This
    /// specialized version takes a constant reference to parameter, but is therefore limited to
    /// binding input parameters. See [`Statement::bind_parameter`] for the version which can bind
    /// input and output parameters.
    ///
    /// See <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function>.
    ///
    /// # Safety
    ///
    /// * It is up to the caller to ensure the lifetimes of the bound parameters.
    /// * Calling this function may influence other statements that share the APD.
    /// * `parameter` must be complete, i.e not be truncated.
    unsafe fn bind_input_parameter(
        &mut self,
        parameter_number: u16,
        parameter: &(impl HasDataType + CData + ?Sized),
    ) -> SqlResult<()> {
        let parameter_type = parameter.data_type();
        SQLBindParameter(
            self.as_sys(),
            parameter_number,
            ParamType::Input,
            parameter.cdata_type(),
            parameter_type.data_type(),
            parameter_type
                .column_size()
                .map(NonZeroUsize::get)
                .unwrap_or_default(),
            parameter_type.decimal_digits(),
            // We cast const to mut here, but we specify the input_output_type as input.
            parameter.value_ptr() as *mut c_void,
            parameter.buffer_length(),
            // We cast const to mut here, but we specify the input_output_type as input.
            parameter.indicator_ptr() as *mut isize,
        )
        .into_sql_result("SQLBindParameter")
    }

    /// Binds a buffer holding a single parameter to a parameter marker in an SQL statement. To bind
    /// input parameters using constant references see [`Statement::bind_input_parameter`].
    ///
    /// See <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function>.
    ///
    /// # Safety
    ///
    /// * It is up to the caller to ensure the lifetimes of the bound parameters.
    /// * Calling this function may influence other statements that share the APD.
    /// * `parameter` must be complete, i.e not be truncated. If `input_output_type` indicates
    ///   [`ParamType::Input`] or [`ParamType::InputOutput`].
    unsafe fn bind_parameter(
        &mut self,
        parameter_number: u16,
        input_output_type: ParamType,
        parameter: &mut (impl CDataMut + HasDataType),
    ) -> SqlResult<()> {
        let parameter_type = parameter.data_type();
        SQLBindParameter(
            self.as_sys(),
            parameter_number,
            input_output_type,
            parameter.cdata_type(),
            parameter_type.data_type(),
            parameter_type
                .column_size()
                .map(NonZeroUsize::get)
                .unwrap_or_default(),
            parameter_type.decimal_digits(),
            parameter.value_ptr() as *mut c_void,
            parameter.buffer_length(),
            parameter.mut_indicator_ptr(),
        )
        .into_sql_result("SQLBindParameter")
    }

    /// Binds an input stream to a parameter marker in an SQL statement. Use this to stream large
    /// values at statement execution time. To bind preallocated constant buffers see
    /// [`Statement::bind_input_parameter`].
    ///
    /// See <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function>.
    ///
    /// # Safety
    ///
    /// * It is up to the caller to ensure the lifetimes of the bound parameters.
    /// * Calling this function may influence other statements that share the APD.
    unsafe fn bind_delayed_input_parameter(
        &mut self,
        parameter_number: u16,
        parameter: &mut (impl DelayedInput + HasDataType),
    ) -> SqlResult<()> {
        let paramater_type = parameter.data_type();
        SQLBindParameter(
            self.as_sys(),
            parameter_number,
            ParamType::Input,
            parameter.cdata_type(),
            paramater_type.data_type(),
            paramater_type
                .column_size()
                .map(NonZeroUsize::get)
                .unwrap_or_default(),
            paramater_type.decimal_digits(),
            parameter.stream_ptr(),
            0,
            // We cast const to mut here, but we specify the input_output_type as input.
            parameter.indicator_ptr() as *mut isize,
        )
        .into_sql_result("SQLBindParameter")
    }

    /// `true` if a given column in a result set is unsigned or not a numeric type, `false`
    /// otherwise.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn is_unsigned_column(&self, column_number: u16) -> SqlResult<bool> {
        unsafe { self.numeric_col_attribute(Desc::Unsigned, column_number) }.map(|out| match out {
            0 => false,
            1 => true,
            _ => panic!("Unsigned column attribute must be either 0 or 1."),
        })
    }

    /// Returns a number identifying the SQL type of the column in the result set.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_type(&self, column_number: u16) -> SqlResult<SqlDataType> {
        unsafe { self.numeric_col_attribute(Desc::Type, column_number) }.map(|ret| {
            SqlDataType(ret.try_into().expect(
                "Failed to retrieve data type from ODBC driver. The SQLLEN could not be converted to
                a 16 Bit integer. If you are on a 64Bit Platform, this may be because your \
                database driver being compiled against a SQLLEN with 32Bit size instead of 64Bit. \
                E.g. IBM offers libdb2o.* and libdb2.*. With libdb2o.* being the one with the \
                correct size.",
            ))
        })
    }

    /// The concise data type. For the datetime and interval data types, this field returns the
    /// concise data type; for example, `TIME` or `INTERVAL_YEAR`.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_concise_type(&self, column_number: u16) -> SqlResult<SqlDataType> {
        unsafe { self.numeric_col_attribute(Desc::ConciseType, column_number) }.map(|ret| {
            SqlDataType(ret.try_into().expect(
                "Failed to retrieve data type from ODBC driver. The SQLLEN could not be \
                    converted to a 16 Bit integer. If you are on a 64Bit Platform, this may be \
                    because your database driver being compiled against a SQLLEN with 32Bit size \
                    instead of 64Bit. E.g. IBM offers libdb2o.* and libdb2.*. With libdb2o.* being \
                    the one with the correct size.",
            ))
        })
    }

    /// Returns the size in bytes of the columns. For variable sized types the maximum size is
    /// returned, excluding a terminating zero.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_octet_length(&self, column_number: u16) -> SqlResult<isize> {
        unsafe { self.numeric_col_attribute(Desc::OctetLength, column_number) }
    }

    /// Maximum number of characters required to display data from the column.
    ///
    /// `column_number`: Index of the column, starting at 1.
    fn col_display_size(&self, column_number: u16) -> SqlResult<isize> {
        unsafe { self.numeric_col_attribute(Desc::DisplaySize, column_number) }
    }

    /// Precision of the column.
    ///
    /// Denotes the applicable precision. For data types SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP, and all
    /// the interval data types that represent a time interval, its value is the applicable
    /// precision of the fractional seconds component.
    fn col_precision(&self, column_number: u16) -> SqlResult<isize> {
        unsafe { self.numeric_col_attribute(Desc::Precision, column_number) }
    }

    /// The applicable scale for a numeric data type. For DECIMAL and NUMERIC data types, this is
    /// the defined scale. It is undefined for all other data types.
    fn col_scale(&self, column_number: u16) -> SqlResult<Len> {
        unsafe { self.numeric_col_attribute(Desc::Scale, column_number) }
    }

    /// The column alias, if it applies. If the column alias does not apply, the column name is
    /// returned. If there is no column name or a column alias, an empty string is returned.
    fn col_name(&self, column_number: u16, buffer: &mut Vec<SqlChar>) -> SqlResult<()> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: i16 = 0;
        // Let's utilize all of `buf`s capacity.
        buffer.resize(buffer.capacity(), 0);
        unsafe {
            let mut res = sql_col_attribute(
                self.as_sys(),
                column_number,
                Desc::Name,
                mut_buf_ptr(buffer) as Pointer,
                binary_length(buffer).try_into().unwrap(),
                &mut string_length_in_bytes as *mut i16,
                null_mut(),
            )
            .into_sql_result("SQLColAttribute");

            if res.is_err() {
                return res;
            }

            if is_truncated_bin(buffer, string_length_in_bytes.try_into().unwrap()) {
                // If we could rely on every ODBC driver sticking to the specifcation it would
                // probably best to resize by `string_length_in_bytes / 2 + 1`. Yet e.g. SQLite
                // seems to report the length in characters, so to work with a wide range of DB
                // systems, and since buffers for names are not expected to become super large we
                // omit the division by two here.
                buffer.resize((string_length_in_bytes + 1).try_into().unwrap(), 0);

                res = sql_col_attribute(
                    self.as_sys(),
                    column_number,
                    Desc::Name,
                    mut_buf_ptr(buffer) as Pointer,
                    binary_length(buffer).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i16,
                    null_mut(),
                )
                .into_sql_result("SQLColAttribute");
            }
            // Resize buffer to exact string length without terminal zero
            resize_to_fit_without_tz(buffer, string_length_in_bytes.try_into().unwrap());

            res
        }
    }

    /// # Safety
    ///
    /// It is the callers responsibility to ensure that `attribute` refers to a numeric attribute.
    unsafe fn numeric_col_attribute(&self, attribute: Desc, column_number: u16) -> SqlResult<Len> {
        let mut out: Len = 0;
        sql_col_attribute(
            self.as_sys(),
            column_number,
            attribute,
            null_mut(),
            0,
            null_mut(),
            &mut out as *mut Len,
        )
        .into_sql_result("SQLColAttribute")
        .on_success(|| {
            debug!(
                "SQLColAttribute called with attribute '{attribute:?}' for column \
                '{column_number}' reported {out}."
            );
            out
        })
    }

    /// Sets the SQL_DESC_COUNT field of the APD to 0, releasing all parameter buffers set for the
    /// given StatementHandle.
    fn reset_parameters(&mut self) -> SqlResult<()> {
        unsafe {
            SQLFreeStmt(self.as_sys(), FreeStmtOption::ResetParams).into_sql_result("SQLFreeStmt")
        }
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Parameters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    fn describe_param(&self, parameter_number: u16) -> SqlResult<ParameterDescription> {
        let mut data_type = SqlDataType::UNKNOWN_TYPE;
        let mut parameter_size = 0;
        let mut decimal_digits = 0;
        let mut nullable = odbc_sys::Nullability::UNKNOWN;
        unsafe {
            SQLDescribeParam(
                self.as_sys(),
                parameter_number,
                &mut data_type,
                &mut parameter_size,
                &mut decimal_digits,
                &mut nullable,
            )
        }
        .into_sql_result("SQLDescribeParam")
        .on_success(|| ParameterDescription {
            data_type: DataType::new(data_type, parameter_size, decimal_digits),
            nullability: Nullability::new(nullable),
        })
    }

    /// Use to check if which additional parameters need data. Should be called after binding
    /// parameters with an indicator set to [`crate::sys::DATA_AT_EXEC`] or a value created with
    /// [`crate::sys::len_data_at_exec`].
    ///
    /// Return value contains a parameter identifier passed to bind parameter as a value pointer.
    fn param_data(&mut self) -> SqlResult<Option<Pointer>> {
        unsafe {
            let mut param_id: Pointer = null_mut();
            // Use cases for `PARAM_DATA_AVAILABLE` and `NO_DATA` not implemented yet.
            match SQLParamData(self.as_sys(), &mut param_id as *mut Pointer) {
                SqlReturn::NEED_DATA => SqlResult::Success(Some(param_id)),
                other => other.into_sql_result("SQLParamData").on_success(|| None),
            }
        }
    }

    /// Executes a columns query using this statement handle.
    fn columns(
        &mut self,
        catalog_name: &SqlText,
        schema_name: &SqlText,
        table_name: &SqlText,
        column_name: &SqlText,
    ) -> SqlResult<()> {
        unsafe {
            sql_columns(
                self.as_sys(),
                catalog_name.ptr(),
                catalog_name.len_char().try_into().unwrap(),
                schema_name.ptr(),
                schema_name.len_char().try_into().unwrap(),
                table_name.ptr(),
                table_name.len_char().try_into().unwrap(),
                column_name.ptr(),
                column_name.len_char().try_into().unwrap(),
            )
            .into_sql_result("SQLColumns")
        }
    }

    /// Returns the list of table, catalog, or schema names, and table types, stored in a specific
    /// data source. The driver returns the information as a result set.
    ///
    /// The catalog, schema and table parameters are search patterns by default unless
    /// [`Self::set_metadata_id`] is called with `true`. In that case they must also not be `None`
    /// since otherwise a NulPointer error is emitted.
    fn tables(
        &mut self,
        catalog_name: &SqlText,
        schema_name: &SqlText,
        table_name: &SqlText,
        table_type: &SqlText,
    ) -> SqlResult<()> {
        unsafe {
            sql_tables(
                self.as_sys(),
                catalog_name.ptr(),
                catalog_name.len_char().try_into().unwrap(),
                schema_name.ptr(),
                schema_name.len_char().try_into().unwrap(),
                table_name.ptr(),
                table_name.len_char().try_into().unwrap(),
                table_type.ptr(),
                table_type.len_char().try_into().unwrap(),
            )
            .into_sql_result("SQLTables")
        }
    }

    /// This can be used to retrieve either a list of foreign keys in the specified table or a list
    /// of foreign keys in other table that refer to the primary key of the specified table.
    ///
    /// Like [`Self::tables`] this changes the statement to a cursor over the result set.
    fn foreign_keys(
        &mut self,
        pk_catalog_name: &SqlText,
        pk_schema_name: &SqlText,
        pk_table_name: &SqlText,
        fk_catalog_name: &SqlText,
        fk_schema_name: &SqlText,
        fk_table_name: &SqlText,
    ) -> SqlResult<()> {
        unsafe {
            sql_foreign_keys(
                self.as_sys(),
                pk_catalog_name.ptr(),
                pk_catalog_name.len_char().try_into().unwrap(),
                pk_schema_name.ptr(),
                pk_schema_name.len_char().try_into().unwrap(),
                pk_table_name.ptr(),
                pk_table_name.len_char().try_into().unwrap(),
                fk_catalog_name.ptr(),
                fk_catalog_name.len_char().try_into().unwrap(),
                fk_schema_name.ptr(),
                fk_schema_name.len_char().try_into().unwrap(),
                fk_table_name.ptr(),
                fk_table_name.len_char().try_into().unwrap(),
            )
            .into_sql_result("SQLForeignKeys")
        }
    }

    /// To put a batch of binary data into the data source at statement execution time. May return
    /// [`SqlResult::NeedData`]
    ///
    /// Panics if batch is empty.
    fn put_binary_batch(&mut self, batch: &[u8]) -> SqlResult<()> {
        // Probably not strictly necessary. MSSQL returns an error than inserting empty batches.
        // Still strikes me as a programming error. Maybe we could also do nothing instead.
        if batch.is_empty() {
            panic!("Attempt to put empty batch into data source.")
        }

        unsafe {
            SQLPutData(
                self.as_sys(),
                batch.as_ptr() as Pointer,
                batch.len().try_into().unwrap(),
            )
            .into_sql_result("SQLPutData")
        }
    }

    /// Number of rows affected by an `UPDATE`, `INSERT`, or `DELETE` statement.
    ///
    /// See:
    ///
    /// <https://docs.microsoft.com/en-us/sql/relational-databases/native-client-odbc-api/sqlrowcount>
    /// <https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlrowcount-function>
    fn row_count(&self) -> SqlResult<isize> {
        let mut ret = 0isize;
        unsafe {
            SQLRowCount(self.as_sys(), &mut ret as *mut isize)
                .into_sql_result("SQLRowCount")
                .on_success(|| ret)
        }
    }

    /// In polling mode can be used instead of repeating the function call. In notification mode
    /// this completes the asynchronous operation. This method panics, in case asynchronous mode is
    /// not enabled. [`SqlResult::NoData`] if no asynchronous operation is in progress, or (specific
    /// to notification mode) the driver manager has not notified the application.
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcompleteasync-function>
    #[cfg(feature = "odbc_version_3_80")]
    fn complete_async(&mut self, function_name: &'static str) -> SqlResult<SqlResult<()>> {
        let mut ret = SqlReturn::ERROR;
        unsafe {
            // Possible return codes are (according to MS ODBC docs):
            // * INVALID_HANDLE: The handle indicated by HandleType and Handle is not a valid
            //   handle. => Must not happen due self always being a valid statement handle.
            // * ERROR: ret is NULL or asynchronous processing is not enabled on the handle. => ret
            //   is never NULL. User may choose not to enable asynchronous processing though.
            // * NO_DATA: In notification mode, an asynchronous operation is not in progress or the
            //   Driver Manager has not notified the application. In polling mode, an asynchronous
            //   operation is not in progress.
            SQLCompleteAsync(self.handle_type(), self.as_handle(), &mut ret.0 as *mut _)
                .into_sql_result("SQLCompleteAsync")
        }
        .on_success(|| ret.into_sql_result(function_name))
    }

    /// Determines whether more results are available on a statement containing SELECT, UPDATE,
    /// INSERT, or DELETE statements and, if so, initializes processing for those results.
    /// [`SqlResult::NoData`] is returned to indicate that there are no more result sets.
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlmoreresults-function>
    ///
    /// # Safety
    ///
    /// Since a different result set might have a different schema, care needs to be taken that
    /// bound buffers are used correctly.
    unsafe fn more_results(&mut self) -> SqlResult<()> {
        unsafe { SQLMoreResults(self.as_sys()).into_sql_result("SQLMoreResults") }
    }

    /// Application Row Descriptor (ARD) associated with the statement handle. It describes the row
    /// afte the conversions for the application have been applied. It can be used to query
    /// information as well as to set specific desired conversions. E.g. precision and scale for
    /// numeric structs. Usually applications have no need to interact directly with the ARD
    /// though.
    fn application_row_descriptor(&mut self) -> SqlResult<Descriptor<'_>> {
        unsafe {
            let mut hdesc: odbc_sys::HDesc = null_mut();
            let hdesc_out = &mut hdesc as *mut odbc_sys::HDesc as Pointer;
            odbc_sys::SQLGetStmtAttr(
                self.as_sys(),
                odbc_sys::StatementAttribute::AppRowDesc,
                hdesc_out,
                0,
                null_mut(),
            )
            .into_sql_result("SQLGetStmtAttr")
            .on_success(|| Descriptor::new(hdesc))
        }
    }
}

impl Statement for StatementImpl<'_> {
    /// Gain access to the underlying statement handle without transferring ownership to it.
    fn as_sys(&self) -> HStmt {
        self.handle
    }
}

/// Description of a parameter associated with a parameter marker in a prepared statement. Returned
/// by [`crate::Prepared::describe_param`].
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ParameterDescription {
    /// Indicates whether the parameter may be NULL not.
    pub nullability: Nullability,
    /// The SQL Type associated with that parameter.
    pub data_type: DataType,
}
