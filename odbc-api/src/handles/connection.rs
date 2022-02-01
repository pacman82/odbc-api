use super::{
    as_handle::AsHandle,
    buffer::{clamp_int, clamp_small_int, mut_buf_ptr},
    drop_handle,
    sql_char::{SqlChar, SqlText},
    sql_result::ExtSqlReturn,
    statement::StatementImpl,
    OutputStringBuffer, SqlResult,
};
use odbc_sys::{
    CompletionType, ConnectionAttribute, DriverConnectOption, HDbc, HEnv, HStmt, HWnd, Handle,
    HandleType, InfoType, Pointer, SQLAllocHandle, SQLDisconnect, SQLEndTran, IS_UINTEGER,
};
use std::{ffi::c_void, marker::PhantomData, mem::size_of, ptr::null_mut};

#[cfg(feature = "narrow")]
use odbc_sys::{
    SQLConnect as sql_connect, SQLDriverConnect as sql_driver_connect,
    SQLGetConnectAttr as sql_get_connect_attr, SQLGetInfo as sql_get_info,
    SQLSetConnectAttr as sql_set_connect_attr,
};

#[cfg(not(feature = "narrow"))]
use odbc_sys::{
    SQLConnectW as sql_connect, SQLDriverConnectW as sql_driver_connect,
    SQLGetConnectAttrW as sql_get_connect_attr, SQLGetInfoW as sql_get_info,
    SQLSetConnectAttrW as sql_set_connect_attr,
};

/// The connection handle references storage of all information about the connection to the data
/// source, including status, transaction state, and error information.
pub struct Connection<'c> {
    parent: PhantomData<&'c HEnv>,
    handle: HDbc,
}

unsafe impl<'c> AsHandle for Connection<'c> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Dbc
    }
}

impl<'c> Drop for Connection<'c> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Dbc);
        }
    }
}

impl<'c> Connection<'c> {
    /// # Safety
    ///
    /// Call this method only with a valid (successfully allocated) ODBC connection handle.
    pub unsafe fn new(handle: HDbc) -> Self {
        Self {
            handle,
            parent: PhantomData,
        }
    }

    /// Directly acces the underlying ODBC handle.
    pub fn as_sys(&self) -> HDbc {
        self.handle
    }

    /// Establishes connections to a driver and a data source.
    ///
    /// * See [Connecting with SQLConnect][1]
    /// * See [SQLConnectFunction][2]
    ///
    /// # Arguments
    ///
    /// * `data_source_name` - Data source name. The data might be located on the same computer as
    /// the program, or on another computer somewhere on a network.
    /// * `user` - User identifier.
    /// * `pwd` - Authentication string (typically the password).
    ///
    /// [1]: https://docs.microsoft.com//sql/odbc/reference/develop-app/connecting-with-sqlconnect
    /// [2]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqlconnect-function
    pub fn connect(
        &mut self,
        data_source_name: &SqlText,
        user: &SqlText,
        pwd: &SqlText,
    ) -> SqlResult<()> {
        unsafe {
            sql_connect(
                self.handle,
                data_source_name.ptr(),
                data_source_name.len_char(),
                user.ptr(),
                user.len_char(),
                pwd.ptr(),
                pwd.len_char(),
            )
            .into_sql_result("SQLConnect")
        }
    }

    /// An alternative to `connect`. It supports data sources that require more connection
    /// information than the three arguments in `connect` and data sources that are not defined in
    /// the system information.
    pub fn connect_with_connection_string(&mut self, connection_string: &SqlText) -> SqlResult<()> {
        unsafe {
            let parent_window = null_mut();
            let completed_connection_string = None;

            self.driver_connect(
                connection_string,
                parent_window,
                completed_connection_string,
                DriverConnectOption::NoPrompt,
            )
            // Since we did pass NoPrompt we know the user can not abort the prompt.
            .unwrap()
        }
    }

    /// An alternative to `connect` for connecting with a connection string. Allows for completing
    /// a connection string with a GUI prompt on windows.
    ///
    /// # Return
    ///
    /// `None` in case the prompt completing the connection string has been aborted.
    ///
    /// # Safety
    ///
    /// `parent_window` must either be a valid window handle or `NULL`.
    pub unsafe fn driver_connect(
        &mut self,
        connection_string: &SqlText,
        parent_window: HWnd,
        mut completed_connection_string: Option<&mut OutputStringBuffer>,
        driver_completion: DriverConnectOption,
    ) -> Option<SqlResult<()>> {
        let (out_connection_string, out_buf_len, actual_len_ptr) = completed_connection_string
            .as_mut()
            .map(|osb| (osb.mut_buf_ptr(), osb.buf_len(), osb.mut_actual_len_ptr()))
            .unwrap_or((null_mut(), 0, null_mut()));

        sql_driver_connect(
            self.handle,
            parent_window,
            connection_string.ptr(),
            connection_string.len_char(),
            out_connection_string,
            out_buf_len,
            actual_len_ptr,
            driver_completion,
        )
        .into_opt_sql_result("SQLDriverConnect")
    }

    /// Disconnect from an ODBC data source.
    pub fn disconnect(&mut self) -> SqlResult<()> {
        unsafe { SQLDisconnect(self.handle).into_sql_result("SQLDisconnect") }
    }

    /// Allocate a new statement handle. The `Statement` must not outlive the `Connection`.
    pub fn allocate_statement(&self) -> SqlResult<StatementImpl<'_>> {
        let mut out = null_mut();
        unsafe {
            SQLAllocHandle(HandleType::Stmt, self.as_handle(), &mut out)
                .into_sql_result("SQLAllocHandle")
                .on_success(|| StatementImpl::new(out as HStmt))
        }
    }

    /// Specify the transaction mode. By default, ODBC transactions are in auto-commit mode (unless
    /// SQLSetConnectAttr and SQLSetConnectOption are not supported, which is unlikely). Switching
    /// from manual-commit mode to auto-commit mode automatically commits any open transaction on
    /// the connection.
    pub fn set_autocommit(&self, enabled: bool) -> SqlResult<()> {
        let val = if enabled { 1u32 } else { 0u32 };
        unsafe {
            sql_set_connect_attr(
                self.handle,
                ConnectionAttribute::AutoCommit,
                val as Pointer,
                0, // will be ignored according to ODBC spec
            )
            .into_sql_result("SQLSetConnectAttr")
        }
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&self) -> SqlResult<()> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Commit)
                .into_sql_result("SQLEndTran")
        }
    }

    /// Roll back a transaction in manual-commit mode.
    pub fn rollback(&self) -> SqlResult<()> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Rollback)
                .into_sql_result("SQLEndTran")
        }
    }

    /// Fetch the name of the database management system used by the connection and store it into
    /// the provided `buf`.
    pub fn fetch_database_management_system_name(&self, buf: &mut Vec<SqlChar>) -> SqlResult<()> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: i16 = 0;
        // Let's utilize all of `buf`s capacity.
        buf.resize(buf.capacity(), 0);

        // Size of a character (used to calculate translate characters to bytes)
        let sc = size_of::<SqlChar>();

        unsafe {
            let mut res = sql_get_info(
                self.handle,
                InfoType::DbmsName,
                mut_buf_ptr(buf) as Pointer,
                (buf.len() * sc).try_into().unwrap(),
                &mut string_length_in_bytes as *mut i16,
            )
            .into_sql_result("SQLGetInfo");

            if res.is_err() {
                return res;
            }

            // Call has been a success but let's check if the buffer had been large enough.
            if clamp_small_int(buf.len() * sc) < string_length_in_bytes + sc as i16 {
                // It seems we must try again with a large enough buffer.
                buf.resize((string_length_in_bytes / sc as i16 + 1) as usize, 0);
                res = sql_get_info(
                    self.handle,
                    InfoType::DbmsName,
                    mut_buf_ptr(buf) as Pointer,
                    (buf.len() * sc).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i16,
                )
                .into_sql_result("SQLGetInfo");

                if res.is_err() {
                    return res;
                }
            }

            // Resize buffer to exact string length without terminal zero
            buf.resize(string_length_in_bytes as usize / sc, 0);
            res
        }
    }

    fn info_u16(&self, info_type: InfoType) -> SqlResult<u16> {
        unsafe {
            let mut value = 0u16;
            sql_get_info(
                self.handle,
                info_type,
                &mut value as *mut u16 as Pointer,
                // Buffer length should not be required in this case, according to the ODBC
                // documentation at https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15#arguments
                // However, in practice some drivers (such as Microsoft Access) require it to be
                // specified explicitly here, otherwise they return an error without diagnostics.
                size_of::<*mut u16>() as i16,
                null_mut(),
            )
            .into_sql_result("SQLGetInfo")
            .on_success(|| value)
        }
    }

    /// Maximum length of catalog names.
    pub fn max_catalog_name_len(&self) -> SqlResult<u16> {
        self.info_u16(InfoType::MaxCatalogNameLen)
    }

    /// Maximum length of schema names.
    pub fn max_schema_name_len(&self) -> SqlResult<u16> {
        self.info_u16(InfoType::MaxSchemaNameLen)
    }

    /// Maximum length of table names.
    pub fn max_table_name_len(&self) -> SqlResult<u16> {
        self.info_u16(InfoType::MaxTableNameLen)
    }

    /// Maximum length of column names.
    pub fn max_column_name_len(&self) -> SqlResult<u16> {
        self.info_u16(InfoType::MaxColumnNameLen)
    }

    /// Fetch the name of the current catalog being used by the connection and store it into the
    /// provided `buf`.
    pub fn fetch_current_catalog(&self, buf: &mut Vec<SqlChar>) -> SqlResult<()> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: i32 = 0;
        // Let's utilize all of `buf`s capacity.
        buf.resize(buf.capacity(), 0);
        // Size of charactor in bytes
        let sc = size_of::<SqlChar>();
        // and as an i32 so we don't have to cast every time we use it
        let sci = sc as i32;
        // Buffer length in bytes
        let buffer_length = (buf.len() * sc).try_into().unwrap();

        unsafe {
            let mut res = sql_get_connect_attr(
                self.handle,
                ConnectionAttribute::CurrentCatalog,
                mut_buf_ptr(buf) as Pointer,
                buffer_length,
                &mut string_length_in_bytes as *mut i32,
            )
            .into_sql_result("SQLGetConnectAttr");

            if res.is_err() {
                return res;
            }

            let is_truncated = clamp_int(buf.len() * sc) < string_length_in_bytes + sci;
            if is_truncated {
                buf.resize((string_length_in_bytes / sci + 1).try_into().unwrap(), 0);
                let buffer_length = (buf.len() * sc).try_into().unwrap();
                res = sql_get_connect_attr(
                    self.handle,
                    ConnectionAttribute::CurrentCatalog,
                    mut_buf_ptr(buf) as Pointer,
                    buffer_length,
                    &mut string_length_in_bytes as *mut i32,
                )
                .into_sql_result("SQLGetConnectAttr");
            }

            if res.is_err() {
                return res;
            }

            // Resize buffer to exact string length without terminal zero
            buf.resize((string_length_in_bytes / sci).try_into().unwrap(), 0);
            res
        }
    }

    /// Indicates the state of the connection. If `true` the connection has been lost. If `false`,
    /// the connection is still active.
    pub fn is_dead(&self) -> SqlResult<bool> {
        unsafe {
            self.attribute_u32(ConnectionAttribute::ConnectionDead)
                .map(|v| match v {
                    0 => false,
                    1 => true,
                    other => panic!("Unexpected result value from SQLGetConnectAttrW: {}", other),
                })
        }
    }

    /// # Safety
    ///
    /// Caller must ensure connection attribute is numeric.
    unsafe fn attribute_u32(&self, attribute: ConnectionAttribute) -> SqlResult<u32> {
        let mut out: u32 = 0;
        sql_get_connect_attr(
            self.handle,
            attribute,
            &mut out as *mut u32 as *mut c_void,
            IS_UINTEGER,
            null_mut(),
        )
        .into_sql_result("SQLGetConnectAttr")
        .on_success(|| out)
    }
}
