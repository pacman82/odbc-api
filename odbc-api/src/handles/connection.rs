use crate::buffers::{BufferDescription, BufferKind};

use super::{
    as_handle::AsHandle,
    buffer::{buf_ptr, clamp_int, clamp_small_int, mut_buf_ptr, OutputStringBuffer},
    drop_handle,
    error::Error,
    error::IntoResult,
    statement::StatementImpl,
};
use odbc_sys::{
    CompletionType, ConnectionAttribute, DriverConnectOption, HDbc, HEnv, HStmt, HWnd, Handle,
    HandleType, InfoType, Pointer, SQLAllocHandle, SQLConnectW, SQLDisconnect, SQLDriverConnectW,
    SQLEndTran, SQLGetConnectAttrW, SQLGetInfoW, SQLSetConnectAttrW, SqlReturn,
};
use std::{convert::TryInto, ffi::c_void, marker::PhantomData, mem::size_of, ptr::null_mut};
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
            .into_result(self, "SQLConnectW")?;
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
            ret.into_result(self, "SQLDriverConnectW")
        }
    }

    /// Disconnect from an ODBC data source.
    pub fn disconnect(&mut self) -> Result<(), Error> {
        unsafe { SQLDisconnect(self.handle).into_result(self, "SQLDisconnect") }
    }

    /// Allocate a new statement handle. The `Statement` must not outlive the `Connection`.
    pub fn allocate_statement(&self) -> Result<StatementImpl<'_>, Error> {
        let mut out = null_mut();
        unsafe {
            SQLAllocHandle(HandleType::Stmt, self.as_handle(), &mut out)
                .into_result(self, "SQLAllocHandle")?;
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
            .into_result(self, "SQLSetConnectAttrW")
        }
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&self) -> Result<(), Error> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Commit)
                .into_result(self, "SQLEndTran")
        }
    }

    /// Roll back a transaction in manual-commit mode.
    pub fn rollback(&self) -> Result<(), Error> {
        unsafe {
            SQLEndTran(HandleType::Dbc, self.as_handle(), CompletionType::Rollback)
                .into_result(self, "SQLEndTran")
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
            .into_result(self, "SQLGetInfoW")?;

            if clamp_small_int(buf.len() * 2) < string_length_in_bytes + 2 {
                buf.resize((string_length_in_bytes / 2 + 1).try_into().unwrap(), 0);
                SQLGetInfoW(
                    self.handle,
                    InfoType::DbmsName,
                    mut_buf_ptr(buf) as Pointer,
                    (buf.len() * 2).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i16,
                )
                .into_result(self, "SQLGetInfoW")?;
            }

            // Resize buffer to exact string length without terminal zero
            buf.resize(((string_length_in_bytes + 1) / 2).try_into().unwrap(), 0);
        }

        Ok(())
    }

    fn get_info_u16(&self, info_type: InfoType) -> Result<u16, Error> {
        unsafe {
            let mut value = 0u16;
            SQLGetInfoW(
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
            .into_result(self, "SQLGetInfoW")?;
            Ok(value)
        }
    }

    /// Maximum length of catalog names in bytes.
    pub fn max_catalog_name_len(&self) -> Result<u16, Error> {
        self.get_info_u16(InfoType::MaxCatalogNameLen)
    }

    /// Maximum length of schema names in bytes.
    pub fn max_schema_name_len(&self) -> Result<u16, Error> {
        self.get_info_u16(InfoType::MaxSchemaNameLen)
    }

    /// Maximum length of table names in bytes.
    pub fn max_table_name_len(&self) -> Result<u16, Error> {
        self.get_info_u16(InfoType::MaxTableNameLen)
    }

    /// Maximum length of column names in bytes.
    pub fn max_column_name_len(&self) -> Result<u16, Error> {
        self.get_info_u16(InfoType::MaxColumnNameLen)
    }

    /// Fetch the name of the current catalog being usd by the connection and store it into the
    /// provided `buf`.
    pub fn fetch_current_catalog(&self, buf: &mut Vec<u16>) -> Result<(), Error> {
        // String length in bytes, not characters. Terminating zero is excluded.
        let mut string_length_in_bytes: i32 = 0;
        // Let's utilize all of `buf`s capacity.
        buf.resize(buf.capacity(), 0);

        unsafe {
            SQLGetConnectAttrW(
                self.handle,
                ConnectionAttribute::CurrentCatalog,
                mut_buf_ptr(buf) as Pointer,
                (buf.len() * 2).try_into().unwrap(),
                &mut string_length_in_bytes as *mut i32,
            )
            .into_result(self, "SQLGetConnectAttrW")?;

            if clamp_int(buf.len() * 2) < string_length_in_bytes + 2 {
                buf.resize((string_length_in_bytes / 2 + 1).try_into().unwrap(), 0);
                SQLGetConnectAttrW(
                    self.handle,
                    ConnectionAttribute::CurrentCatalog,
                    mut_buf_ptr(buf) as Pointer,
                    (buf.len() * 2).try_into().unwrap(),
                    &mut string_length_in_bytes as *mut i32,
                )
                .into_result(self, "SQLGetConnectAttrW")?;
            }

            // Resize buffer to exact string length without terminal zero
            buf.resize(((string_length_in_bytes + 1) / 2).try_into().unwrap(), 0);
        }

        Ok(())
    }

    /// Indicates the state of the connection. If `true` the connection has been lost. If `false`,
    /// the connection is still active.
    pub fn is_dead(&self) -> Result<bool, Error> {
        unsafe {
            match self.numeric_attribute(ConnectionAttribute::ConnectionDead)? {
                0 => Ok(false),
                1 => Ok(true),
                other => panic!("Unexpected return value from SQLGetConnectAttrW: {}", other),
            }
        }
    }

    /// # Safety
    ///
    /// Caller must ensure connection attribute is numeric.
    unsafe fn numeric_attribute(&self, attribute: ConnectionAttribute) -> Result<usize, Error> {
        let mut out: usize = 0;
        SQLGetConnectAttrW(
            self.handle,
            attribute,
            &mut out as *mut usize as *mut c_void,
            0,
            null_mut(),
        )
        .into_result(self, "SQLGetConnectAttrW")?;
        Ok(out)
    }

    /// The buffer descriptions for all standard buffers (not including extensions) returned in the
    /// columns query (e.g. [`Connection::columns`]).
    ///
    /// # Arguments
    ///
    /// * `type_name_max_len` - The maximum expected length of type names.
    /// * `remarks_max_len` - The maximum expected length of remarks.
    /// * `column_default_max_len` - The maximum expected length of column defaults.
    pub fn columns_buffer_description(
        &self,
        type_name_max_len: usize,
        remarks_max_len: usize,
        column_default_max_len: usize,
    ) -> Result<Vec<BufferDescription>, Error> {
        let null_i16 = BufferDescription {
            kind: BufferKind::I16,
            nullable: true,
        };

        let not_null_i16 = BufferDescription {
            kind: BufferKind::I16,
            nullable: false,
        };

        let null_i32 = BufferDescription {
            kind: BufferKind::I32,
            nullable: true,
        };

        // The definitions for these descriptions are taken from the documentation of `SQLColumns`
        // located at https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function
        let catalog_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_catalog_name_len()? as usize,
            },
            nullable: true,
        };

        let schema_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_schema_name_len()? as usize,
            },
            nullable: true,
        };

        let table_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_table_name_len()? as usize,
            },
            nullable: false,
        };

        let column_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_column_name_len()? as usize,
            },
            nullable: false,
        };

        let data_type_desc = not_null_i16;

        let type_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: type_name_max_len,
            },
            nullable: false,
        };

        let column_size_desc = null_i32;
        let buffer_len_desc = null_i32;
        let decimal_digits_desc = null_i16;
        let precision_radix_desc = null_i16;
        let nullable_desc = not_null_i16;

        let remarks_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: remarks_max_len,
            },
            nullable: true,
        };

        let column_default_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: column_default_max_len,
            },
            nullable: true,
        };

        let sql_data_type_desc = not_null_i16;
        let sql_datetime_sub_desc = null_i16;
        let char_octet_len_desc = null_i32;
        let ordinal_pos_desc = BufferDescription {
            kind: BufferKind::I32,
            nullable: false,
        };

        // We expect strings to be `YES`, `NO`, or a zero-length string, so `3` should be
        // sufficient.
        const IS_NULLABLE_LEN_MAX_LEN: usize = 3;
        let is_nullable_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: IS_NULLABLE_LEN_MAX_LEN,
            },
            nullable: true,
        };

        Ok(vec![
            catalog_name_desc,
            schema_name_desc,
            table_name_desc,
            column_name_desc,
            data_type_desc,
            type_name_desc,
            column_size_desc,
            buffer_len_desc,
            decimal_digits_desc,
            precision_radix_desc,
            nullable_desc,
            remarks_desc,
            column_default_desc,
            sql_data_type_desc,
            sql_datetime_sub_desc,
            char_octet_len_desc,
            ordinal_pos_desc,
            is_nullable_desc,
        ])
    }
}
