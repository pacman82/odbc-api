use super::{
    as_handle::AsHandle, buffer::buf_ptr, drop_handle, error::Error, error::IntoResult,
    statement::StatementImpl,
};
use odbc_sys::{
    CompletionType, ConnectionAttribute, DriverConnectOption, HDbc, HEnv, HStmt, Handle,
    HandleType, Pointer, SQLAllocHandle, SQLConnectW, SQLDisconnect, SQLDriverConnectW, SQLEndTran,
    SQLSetConnectAttrW,
};
use std::{convert::TryInto, marker::PhantomData, ptr::null_mut};
use widestring::{U16Str, U16CStr};
use raw_window_handle::HasRawWindowHandle;

// Currently only windows driver manager supports prompt.
#[cfg(target_os="windows")]
use raw_window_handle::RawWindowHandle;

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
        data_source_name: &U16Str,
        user: &U16Str,
        pwd: &U16Str,
    ) -> Result<(), Error> {
        unsafe {
            SQLConnectW(
                self.handle,
                buf_ptr(data_source_name.as_slice()),
                data_source_name.len().try_into().unwrap(),
                buf_ptr(user.as_slice()),
                user.len().try_into().unwrap(),
                buf_ptr(pwd.as_slice()),
                pwd.len().try_into().unwrap(),
            )
            .into_result(self)?;
            Ok(())
        }
    }

    /// An alternative to `connect`. It supports data sources that require more connection
    /// information than the three arguments in `connect` and data sources that are not defined in
    /// the system information.
    pub fn connect_with_connection_string(
        &mut self,
        connection_string: &U16Str,
    ) -> Result<(), Error> {
        unsafe {
            let window_handle = null_mut();
            let out_connection_string = null_mut();
            let out_connection_string_len = null_mut();
            SQLDriverConnectW(
                self.handle,
                window_handle,
                buf_ptr(connection_string.as_slice()),
                connection_string.len().try_into().unwrap(),
                out_connection_string,
                0,
                out_connection_string_len,
                DriverConnectOption::NoPrompt,
            )
            .into_result(self)?;
            Ok(())
        }
    }

    /// An alternative to `connect` and `connect_with_connection_string` that allows the user to be
    /// prompted for any additional required information (`UID`, `PWD`, etc.).
    pub fn connect_with_complete_prompt(
        &mut self,
        connection_string: &U16Str,
        window: Option<&impl HasRawWindowHandle>,
        max_complete_connection_string_len: usize,
    ) -> Result<String, Error> {
        let window_handle = match window.map(HasRawWindowHandle::raw_window_handle) {
            #[cfg(target_os = "windows")]
            Some(RawWindowHandle::Windows(handle)) => handle.hwnd,
            _ => null_mut(),
        };

        let mut out_connection_string_buffer = vec![0; max_complete_connection_string_len + 1];

        unsafe {
            let out_connection_string_len = null_mut();
            SQLDriverConnectW(
                self.handle,
                window_handle,
                buf_ptr(connection_string.as_slice()),
                connection_string.len().try_into().unwrap(),
                out_connection_string_buffer.as_mut_ptr(),
                out_connection_string_buffer.len().try_into().unwrap(),
                out_connection_string_len,
                DriverConnectOption::Complete,
            )
            .into_result(self)?;

            let out_connection_string_raw = U16CStr::from_slice_with_nul(&out_connection_string_buffer).unwrap();
            let out_connection_string = out_connection_string_raw.to_string().unwrap();

            Ok(out_connection_string)
        }
    }

    /// Disconnect from an ODBC data source.
    pub fn disconnect(&mut self) -> Result<(), Error> {
        unsafe { SQLDisconnect(self.handle).into_result(self) }
    }

    /// Allocate a new statement handle. The `Statement` must not outlive the `Connection`.
    pub fn allocate_statement(&self) -> Result<StatementImpl<'_>, Error> {
        let mut out = null_mut();
        unsafe {
            SQLAllocHandle(HandleType::Stmt, self.as_handle(), &mut out).into_result(self)?;
            Ok(StatementImpl::new(out as HStmt))
        }
    }

    /// Specify the transaction mode. By default, ODBC transactions are in auto-commit mode (unless
    /// SQLSetConnectAttr and SQLSetConnectOption are not supported, which is unlikely). Switching
    /// from manual-commit mode to auto-commit mode automatically commits any open transaction on
    /// the connection.
    pub fn set_autocommit(&self, enabled: bool) -> Result<(), Error> {
        let val = if enabled { 1u32 } else { 0u32 };
        unsafe {
            SQLSetConnectAttrW(
                self.handle,
                ConnectionAttribute::AutoCommit,
                val as Pointer,
                0, // will be ignored according to ODBC spec
            )
            .into_result(self)
        }
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Commit).into_result(self)
        }
    }

    /// Roll back a transaction in manual-commit mode.
    pub fn rollback(&self) -> Result<(), Error> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Rollback)
                .into_result(self)
        }
    }
}
