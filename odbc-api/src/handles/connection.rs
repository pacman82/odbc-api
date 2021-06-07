use super::{
    as_handle::AsHandle,
    buffer::{buf_ptr, clamp_small_int, mut_buf_ptr, OutputStringBuffer},
    drop_handle,
    error::Error,
    error::IntoResult,
    statement::StatementImpl,
};
use odbc_sys::{
    CompletionType, ConnectionAttribute, DriverConnectOption, HDbc, HEnv, HStmt, HWnd, Handle,
    HandleType, InfoType, Pointer, SQLAllocHandle, SQLConnectW, SQLDisconnect, SQLDriverConnectW,
    SQLEndTran, SQLGetInfoW, SQLSetConnectAttrW, SqlReturn,
};
use std::{convert::TryInto, marker::PhantomData, ptr::null_mut};
use widestring::U16Str;

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
            let parent_window = null_mut();
            let completed_connection_string = None;

            self.driver_connect(
                connection_string,
                parent_window,
                completed_connection_string,
                DriverConnectOption::NoPrompt,
            )
        }
    }

    /// An alternative to `connect` for connecting with a connection string. Allows for completing
    /// a connection string with a GUI prompt on windows.
    ///
    /// # Safety
    ///
    /// `parent_window` must either be a valid window handle or `NULL`.
    pub unsafe fn driver_connect(
        &mut self,
        connection_string: &U16Str,
        parent_window: HWnd,
        mut completed_connection_string: Option<&mut OutputStringBuffer>,
        driver_completion: DriverConnectOption,
    ) -> Result<(), Error> {
        let (out_connection_string, out_buf_len, actual_len_ptr) = completed_connection_string
            .as_mut()
            .map(|osb| (osb.mut_buf_ptr(), osb.buf_len(), osb.mut_actual_len_ptr()))
            .unwrap_or((null_mut(), 0, null_mut()));

        let ret = SQLDriverConnectW(
            self.handle,
            parent_window,
            buf_ptr(connection_string.as_slice()),
            connection_string.len().try_into().unwrap(),
            out_connection_string,
            out_buf_len,
            actual_len_ptr,
            driver_completion,
        );

        if ret == SqlReturn::NO_DATA {
            Err(Error::AbortedConnectionStringCompletion)
        } else {
            ret.into_result(self)
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

    /// Fetch the name of the database management system used by the connection and store it into
    /// the provided `buf`.
    pub fn fetch_database_management_system_name(&self, buf: &mut Vec<u16>) -> Result<(), Error> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: i16 = 0;
        // Let's utilize all of `buf`s capacity.
        buf.resize(buf.capacity(), 0);

        unsafe {
            SQLGetInfoW(
                self.handle,
                InfoType::DbmsName,
                mut_buf_ptr(buf) as Pointer,
                (buf.len() * 2).try_into().unwrap(),
                &mut string_length_in_bytes as *mut i16,
            )
            .into_result(self)?;

            if clamp_small_int(buf.len() * 2) < string_length_in_bytes + 2 {
                buf.resize((string_length_in_bytes / 2 + 1).try_into().unwrap(), 0);
                SQLGetInfoW(
                    self.handle,
                    InfoType::DbmsName,
                    mut_buf_ptr(buf) as Pointer,
                    (buf.len() * 2).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i16,
                )
                .into_result(self)?;
            }

            // Resize buffer to exact string length without terminal zero
            buf.resize(((string_length_in_bytes + 1) / 2).try_into().unwrap(), 0);
        }

        Ok(())
    }
}
