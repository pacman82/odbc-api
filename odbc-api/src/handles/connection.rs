use super::{
    as_handle::AsHandle,
    buffer::mut_buf_ptr,
    drop_handle,
    sql_char::{
        binary_length, is_truncated_bin, resize_to_fit_with_tz, resize_to_fit_without_tz, SqlChar,
        SqlText,
    },
    sql_result::ExtSqlReturn,
    statement::StatementImpl,
    OutputStringBuffer, SqlResult,
};
use log::debug;
use odbc_sys::{
    CompletionType, ConnectionAttribute, DriverConnectOption, HDbc, HEnv, HStmt, HWnd, Handle,
    HandleType, InfoType, Pointer, SQLAllocHandle, SQLDisconnect, SQLEndTran, IS_UINTEGER,
};
use std::{ffi::c_void, marker::PhantomData, mem::size_of, ptr::null_mut};

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use odbc_sys::{
    SQLConnect as sql_connect, SQLDriverConnect as sql_driver_connect,
    SQLGetConnectAttr as sql_get_connect_attr, SQLGetInfo as sql_get_info,
    SQLSetConnectAttr as sql_set_connect_attr,
};

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use odbc_sys::{
    SQLConnectW as sql_connect, SQLDriverConnectW as sql_driver_connect,
    SQLGetConnectAttrW as sql_get_connect_attr, SQLGetInfoW as sql_get_info,
    SQLSetConnectAttrW as sql_set_connect_attr,
};

/// The connection handle references storage of all information about the connection to the data
/// source, including status, transaction state, and error information.
///
/// Connection is not `Sync`, this implies that many methods which one would suspect should take
/// `&mut self` are actually `&self`. This is important if several statement exists which borrow
/// the same connection.
pub struct Connection<'c> {
    parent: PhantomData<&'c HEnv>,
    handle: HDbc,
}

unsafe impl AsHandle for Connection<'_> {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Dbc
    }
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Dbc);
        }
    }
}

/// According to the ODBC documentation this is safe. See:
/// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading>
///
/// In addition to that, this has not caused trouble in a while. So we mark sending connections to
/// other threads as safe. Reading through the documentation, one might get the impression that
/// Connections are also `Sync`. This could be theoretically true on the level of the handle, but at
/// the latest once the interior mutability due to error handling comes in to play, higher level
/// abstraction have to content themselves with `Send`. This is currently how far my trust with most
/// ODBC drivers.
unsafe impl Send for Connection<'_> {}

impl Connection<'_> {
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
    ///   the program, or on another computer somewhere on a network.
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
                data_source_name.len_char().try_into().unwrap(),
                user.ptr(),
                user.len_char().try_into().unwrap(),
                pwd.ptr(),
                pwd.len_char().try_into().unwrap(),
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
            let mut completed_connection_string = OutputStringBuffer::empty();

            self.driver_connect(
                connection_string,
                parent_window,
                &mut completed_connection_string,
                DriverConnectOption::NoPrompt,
            )
            // Since we did pass NoPrompt we know the user can not abort the prompt.
            .map(|_connection_string_is_complete| ())
        }
    }

    /// An alternative to `connect` for connecting with a connection string. Allows for completing
    /// a connection string with a GUI prompt on windows.
    ///
    /// # Return
    ///
    /// [`SqlResult::NoData`] in case the prompt completing the connection string has been aborted.
    ///
    /// # Safety
    ///
    /// `parent_window` must either be a valid window handle or `NULL`.
    pub unsafe fn driver_connect(
        &mut self,
        connection_string: &SqlText,
        parent_window: HWnd,
        completed_connection_string: &mut OutputStringBuffer,
        driver_completion: DriverConnectOption,
    ) -> SqlResult<()> {
        sql_driver_connect(
            self.handle,
            parent_window,
            connection_string.ptr(),
            connection_string.len_char().try_into().unwrap(),
            completed_connection_string.mut_buf_ptr(),
            completed_connection_string.buf_len(),
            completed_connection_string.mut_actual_len_ptr(),
            driver_completion,
        )
        .into_sql_result("SQLDriverConnect")
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
        let val = enabled as u32;
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

    /// Number of seconds to wait for a login request to complete before returning to the
    /// application. The default is driver-dependent. If `0` the timeout is dasabled and a
    /// connection attempt will wait indefinitely.
    ///
    /// If the specified timeout exceeds the maximum login timeout in the data source, the driver
    /// substitutes that value and uses the maximum login timeout instead.
    ///
    /// This corresponds to the `SQL_ATTR_LOGIN_TIMEOUT` attribute in the ODBC specification.
    ///
    /// See:
    /// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function>
    pub fn set_login_timeout_sec(&self, timeout: u32) -> SqlResult<()> {
        unsafe {
            sql_set_connect_attr(
                self.handle,
                ConnectionAttribute::LoginTimeout,
                timeout as Pointer,
                0,
            )
            .into_sql_result("SQLSetConnectAttr")
        }
    }

    /// Specifying the network packet size in bytes. Note: Many data sources either do not support
    /// this option or only can return but not set the network packet size. If the specified size
    /// exceeds the maximum packet size or is smaller than the minimum packet size, the driver
    /// substitutes that value and returns SQLSTATE 01S02 (Option value changed). If the application
    /// sets packet size after a connection has already been made, the driver will return SQLSTATE
    /// HY011 (Attribute cannot be set now).
    ///
    /// See:
    /// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function>
    pub fn set_packet_size(&self, packet_size: u32) -> SqlResult<()> {
        unsafe {
            sql_set_connect_attr(
                self.handle,
                ConnectionAttribute::PacketSize,
                packet_size as Pointer,
                0,
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

        unsafe {
            let mut res = sql_get_info(
                self.handle,
                InfoType::DbmsName,
                mut_buf_ptr(buf) as Pointer,
                binary_length(buf).try_into().unwrap(),
                &mut string_length_in_bytes as *mut i16,
            )
            .into_sql_result("SQLGetInfo");

            if res.is_err() {
                return res;
            }

            // Call has been a success but let's check if the buffer had been large enough.
            if is_truncated_bin(buf, string_length_in_bytes.try_into().unwrap()) {
                // It seems we must try again with a large enough buffer.
                resize_to_fit_with_tz(buf, string_length_in_bytes.try_into().unwrap());
                res = sql_get_info(
                    self.handle,
                    InfoType::DbmsName,
                    mut_buf_ptr(buf) as Pointer,
                    binary_length(buf).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i16,
                )
                .into_sql_result("SQLGetInfo");

                if res.is_err() {
                    return res;
                }
            }

            // Resize buffer to exact string length without terminal zero
            resize_to_fit_without_tz(buf, string_length_in_bytes.try_into().unwrap());
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
    pub fn fetch_current_catalog(&self, buffer: &mut Vec<SqlChar>) -> SqlResult<()> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: i32 = 0;
        // Let's utilize all of `buf`s capacity.
        buffer.resize(buffer.capacity(), 0);

        unsafe {
            let mut res = sql_get_connect_attr(
                self.handle,
                ConnectionAttribute::CurrentCatalog,
                mut_buf_ptr(buffer) as Pointer,
                binary_length(buffer).try_into().unwrap(),
                &mut string_length_in_bytes as *mut i32,
            )
            .into_sql_result("SQLGetConnectAttr");

            if res.is_err() {
                return res;
            }

            if is_truncated_bin(buffer, string_length_in_bytes.try_into().unwrap()) {
                resize_to_fit_with_tz(buffer, string_length_in_bytes.try_into().unwrap());
                res = sql_get_connect_attr(
                    self.handle,
                    ConnectionAttribute::CurrentCatalog,
                    mut_buf_ptr(buffer) as Pointer,
                    binary_length(buffer).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i32,
                )
                .into_sql_result("SQLGetConnectAttr");
            }

            if res.is_err() {
                return res;
            }

            // Resize buffer to exact string length without terminal zero
            resize_to_fit_without_tz(buffer, string_length_in_bytes.try_into().unwrap());
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
                    other => panic!("Unexpected result value from SQLGetConnectAttr: {other}"),
                })
        }
    }

    /// Networ packet size in bytes.
    pub fn packet_size(&self) -> SqlResult<u32> {
        unsafe { self.attribute_u32(ConnectionAttribute::PacketSize) }
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
        .on_success(|| {
            let handle = self.handle;
            debug!(
                "SQLGetConnectAttr called with attribute '{attribute:?}' for connection \
                '{handle:?}' reported '{out}'."
            );
            out
        })
    }
}
