use crate::{buffer::buf_ptr, error::ToResult, AsHandle, Error};
use odbc_sys::{
    DriverConnectOption, HDbc, Handle, HandleType, SQLConnectW, SQLDisconnect, SQLDriverConnectW,
    SQLFreeHandle, SqlReturn,
};
use std::{convert::TryInto, ptr::null_mut, thread::panicking};
use widestring::U16Str;

/// The connection handle references storage of all information about the connection to the data
/// source, including status, transaction state, and error information.
pub struct Connection {
    handle: HDbc,
}

unsafe impl AsHandle for Connection {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Dbc
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe {
            match SQLFreeHandle(HandleType::Dbc, self.handle as Handle) {
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

impl Connection {
    /// # Safety
    ///
    /// Call this method only with a valid (successfully allocated) ODBC connection handle.
    pub unsafe fn new(handle: HDbc) -> Self {
        Self { handle }
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
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqlconnect-function
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
            .to_result(self)?;
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
            .to_result(self)?;
            Ok(())
        }
    }

    // /// An alternative to `connect`. It supports data sources that require more connection
    // /// information than the three arguments in SQLConnect, dialog boxes to prompt the user for all
    // /// connection information, and data sources that are not defined in the system information.
    // ///
    // /// # Return
    // ///
    // /// Length of output connection string
    // pub fn driver_connect(
    //     &mut self,
    //     in_connection_string: &U16Str,
    //     out_connection_string: &mut Vec<WChar>,
    //     driver_completion: DriverConnectOption,
    // ) -> Result<SmallInt, Error> {
    //     unsafe {
    //         let window_handle = null_mut();
    //         let mut out_connection_string_len = 0;
    //         SQLDriverConnectW(
    //             self.handle,
    //             window_handle,
    //             buf_ptr(in_connection_string.as_slice()),
    //             in_connection_string.len().try_into().unwrap(),
    //             mut_buf_ptr(out_connection_string),
    //             out_connection_string.len().try_into().unwrap(),
    //             &mut out_connection_string_len,
    //             driver_completion,
    //         )
    //         .to_result(self)?;
    //         Ok(out_connection_string_len)
    //     }
    // }

    /// Disconnect from an ODBC data source.
    pub fn disconnect(&mut self) -> Result<(), Error> {
        unsafe { SQLDisconnect(self.handle).to_result(self) }
    }
}
