use crate::{buffer::buf_ptr, error::ToResult, statement::Statement, AsHandle, Error};
use odbc_sys::{
    CompletionType, ConnectionAttribute, DriverConnectOption, HDbc, HEnv, HStmt, Handle,
    HandleType, Pointer, SQLAllocHandle, SQLConnectW, SQLDisconnect, SQLDriverConnectW, SQLEndTran,
    SQLFreeHandle, SQLSetConnectAttrW, SqlReturn,
};
use std::{convert::TryInto, marker::PhantomData, ptr::null_mut, thread::panicking};
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

    /// Disconnect from an ODBC data source.
    pub fn disconnect(&mut self) -> Result<(), Error> {
        unsafe { SQLDisconnect(self.handle).to_result(self) }
    }

    /// Allocate a new statement handle. The `Statement` must not outlive the `Connection`.
    pub fn allocate_statement(&self) -> Result<Statement, Error> {
        let mut out = null_mut();
        unsafe {
            SQLAllocHandle(HandleType::Stmt, self.as_handle(), &mut out).to_result(self)?;
            Ok(Statement::new(out as HStmt))
        }
    }

    /// Specify the transaction mode. By default, ODBC transactions are in auto-commit mode (unless
    /// SQLSetConnectAttr and SQLSetConnectOption are not supported, which is unlikely). Switching
    /// from manual-commit mode to auto-commit mode automatically commits any open transaction on
    /// the connection.
    pub fn set_autocommit(&mut self, enabled: bool) -> Result<(), Error> {
        let val = if enabled { 1u32 } else { 0u32 };
        unsafe {
            SQLSetConnectAttrW(
                self.handle,
                ConnectionAttribute::AutoCommit,
                val as Pointer,
                0, // will be ignored according to ODBC spec
            )
            .to_result(self)
        }
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&mut self) -> Result<(), Error> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Commit).to_result(self)
        }
    }

    /// Roll back a transaction in manual-commit mode.
    pub fn rollback(&mut self) -> Result<(), Error> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Rollback).to_result(self)
        }
    }

    // /// `true` if the data source is read only
    // pub fn is_read_only(&mut self) -> Result<bool, Error> {
    //     unsafe {
    //         let mut buffer:[WChar; 2] = [0; 2];
    //         SQLGetInfoW(
    //             self.handle,
    //             InfoType::DataSourceReadOnly,
    //             mut_buf_ptr(&mut buffer) as Pointer,
    //             (buffer.len() * size_of::<WChar>()) as SmallInt,
    //             null_mut(),
    //         )
    //         .to_result(self)?;
    //         Ok(match buffer[0] as char {
    //             'N' => false,
    //             'Y' => true,
    //             _ => panic!(r#"Driver may only return "N" or "Y""#),
    //         })
    //     }
    // }
}
