use crate::{
    buffers::{BufferDescription, BufferKind},
    execute::{execute_columns, execute_tables, execute_with_parameters},
    handles::{self, State, Statement, StatementImpl},
    parameter_collection::ParameterCollection,
    statement_connection::StatementConnection,
    CursorImpl, Error, Preallocated, Prepared,
};
use odbc_sys::HDbc;
use std::{borrow::Cow, mem::ManuallyDrop, str, thread::panicking};
use widestring::{U16Str, U16String};

impl<'conn> Drop for Connection<'conn> {
    fn drop(&mut self) {
        match self.connection.disconnect().into_result(&self.connection) {
            Ok(()) => (),
            Err(Error::Diagnostics {
                record,
                function: _,
            }) if record.state == State::INVALID_STATE_TRANSACTION => {
                // Invalid transaction state. Let's rollback the current transaction and try again.
                if let Err(e) = self.rollback() {
                    // Avoid panicking, if we already have a panic. We don't want to mask the original
                    // error.
                    if !panicking() {
                        panic!(
                            "Unexpected error rolling back transaction (In order to recover \
                                from invalid transaction state during disconnect): {:?}",
                            e
                        )
                    }
                }
                // Transaction is rolled back. Now let's try again to disconnect.
                if let Err(e) = self.connection.disconnect().into_result(&self.connection) {
                    // Avoid panicking, if we already have a panic. We don't want to mask the original
                    // error.
                    if !panicking() {
                        panic!("Unexpected error disconnecting): {:?}", e)
                    }
                }
            }
            Err(e) => {
                // Avoid panicking, if we already have a panic. We don't want to mask the original
                // error.
                if !panicking() {
                    panic!("Unexpected error disconnecting: {:?}", e)
                }
            }
        }
    }
}

/// The connection handle references storage of all information about the connection to the data
/// source, including status, transaction state, and error information.
pub struct Connection<'c> {
    connection: handles::Connection<'c>,
}

impl<'c> Connection<'c> {
    pub(crate) fn new(connection: handles::Connection<'c>) -> Self {
        Self { connection }
    }

    /// Transfers ownership of the handle to this open connection to the raw ODBC pointer.
    pub fn into_sys(self) -> HDbc {
        // We do not want to run the drop handler, but transfer ownership instead.
        ManuallyDrop::new(self).connection.as_sys()
    }

    /// Executes an sql statement using a wide string. See [`Self::execute`].
    pub fn execute_utf16(
        &self,
        query: &U16Str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<StatementImpl<'_>>>, Error> {
        let lazy_statement = move || self.allocate_statement();
        execute_with_parameters(lazy_statement, Some(query), params)
    }

    /// Executes an SQL statement. This is the fastest way to submit an SQL statement for one-time
    /// execution.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. See the [`crate::parameter`] module level documentation for more
    ///   information on how to pass parameters.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let env = Environment::new()?;
    ///
    /// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
    /// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
    ///     // Use cursor to process query results.  
    /// }
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn execute(
        &self,
        query: &str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<StatementImpl<'_>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
    }

    /// In some use cases there you only execute a single statement, or the time to open a
    /// connection does not matter users may wish to choose to not keep a connection alive seperatly
    /// from the cursor, in order to have an easier time withe the borrow checker.
    ///
    /// ```no_run
    /// use lazy_static::lazy_static;
    /// use odbc_api::{Environment, Error, Cursor};
    ///
    /// lazy_static! {
    ///     static ref ENV: Environment = unsafe { Environment::new().unwrap() };
    /// }
    ///
    /// const CONNECTION_STRING: &str =
    ///     "Driver={ODBC Driver 17 for SQL Server};\
    ///     Server=localhost;UID=SA;\
    ///     PWD=<YourStrong@Passw0rd>;";
    ///
    /// fn execute_query(query: &str) -> Result<Option<impl Cursor>, Error> {
    ///     let conn = ENV.connect_with_connection_string(CONNECTION_STRING)?;
    ///
    ///     // connect.execute(&query, ()) // Compiler error: Would return local ref to `conn`.
    ///
    ///     conn.into_cursor(&query, ())
    /// }
    /// ```
    pub fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<StatementConnection<'c>>>, Error> {
        let cursor = match self.execute(query, params) {
            Ok(Some(cursor)) => cursor,
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
        };
        let cursor = ManuallyDrop::new(cursor);
        let handle = cursor.as_sys();
        let statement = unsafe { StatementConnection::new(handle, self) };
        Ok(Some(CursorImpl::new(statement)))
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare_utf16(&self, query: &U16Str) -> Result<Prepared<'_>, Error> {
        let mut stmt = self.allocate_statement()?;
        stmt.prepare(query).into_result(&stmt)?;
        Ok(Prepared::new(stmt))
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare(&self, query: &str) -> Result<Prepared<'_>, Error> {
        let query = U16String::from_str(query);
        self.prepare_utf16(&query)
    }

    /// Allocates an SQL statement handle. This is recommended if you want to sequentially execute
    /// different queries over the same connection, as you avoid the overhead of allocating a
    /// statement handle for each query.
    ///
    /// Should you want to repeatedly execute the same query with different parameters try
    /// [`Self::prepare`] instead.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{Connection, Error};
    /// use std::io::{self, stdin, Read};
    ///
    /// fn interactive(conn: &Connection) -> io::Result<()>{
    ///     let mut statement = conn.preallocate().unwrap();
    ///     let mut query = String::new();
    ///     stdin().read_line(&mut query)?;
    ///     while !query.is_empty() {
    ///         match statement.execute(&query, ()) {
    ///             Err(e) => println!("{}", e),
    ///             Ok(None) => println!("No results set generated."),
    ///             Ok(Some(cursor)) => {
    ///                 // ...print cursor contents...
    ///             },
    ///         }
    ///         stdin().read_line(&mut query)?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn preallocate(&self) -> Result<Preallocated<'_>, Error> {
        let stmt = self.allocate_statement()?;
        Ok(Preallocated::new(stmt))
    }

    /// Specify the transaction mode. By default, ODBC transactions are in auto-commit mode.
    /// Switching from manual-commit mode to auto-commit mode automatically commits any open
    /// transaction on the connection. There is no open or begin transaction method. Each statement
    /// execution automatically starts a new transaction or adds to the existing one.
    ///
    /// In manual commit mode you can use [`Connection::commit`] or [`Connection::rollback`]. Keep
    /// in mind, that even `SELECT` statements can open new transactions. This library will rollback
    /// open transactions if a connection goes out of SCOPE. This however will log an error, since
    /// the transaction state is only discovered during a failed disconnect. It is preferable that
    /// the application makes sure all transactions are closed if in manual commit mode.
    pub fn set_autocommit(&self, enabled: bool) -> Result<(), Error> {
        self.connection
            .set_autocommit(enabled)
            .into_result(&self.connection)
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&self) -> Result<(), Error> {
        self.connection.commit().into_result(&self.connection)
    }

    /// To rollback a transaction in manual-commit mode.
    pub fn rollback(&self) -> Result<(), Error> {
        self.connection.rollback().into_result(&self.connection)
    }

    /// Indicates the state of the connection. If `true` the connection has been lost. If `false`,
    /// the connection is still active.
    pub fn is_dead(&self) -> Result<bool, Error> {
        self.connection.is_dead().into_result(&self.connection)
    }

    /// Allows sending this connection to different threads. This Connection will still be only be
    /// used by one thread at a time, but it may be a different thread each time.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::thread;
    /// use lazy_static::lazy_static;
    /// use odbc_api::Environment;
    /// lazy_static! {
    ///     static ref ENV: Environment = unsafe { Environment::new().unwrap() };
    /// }
    /// const MSSQL: &str =
    ///     "Driver={ODBC Driver 17 for SQL Server};\
    ///     Server=localhost;\
    ///     UID=SA;\
    ///     PWD=<YourStrong@Passw0rd>;\
    /// ";
    ///
    /// let conn = ENV.connect_with_connection_string("MSSQL").unwrap();
    /// let conn = unsafe { conn.promote_to_send() };
    /// let handle = thread::spawn(move || {
    ///     if let Some(cursor) = conn.execute("SELECT title FROM Movies ORDER BY year",())? {
    ///         // Use cursor to process results
    ///     }
    ///     Ok::<(), odbc_api::Error>(())
    /// });
    /// handle.join().unwrap()?;
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    ///
    /// # Safety
    ///
    /// According to the ODBC standard this should be safe. By calling this function you express your
    /// trust in the implementation of the ODBC driver your application is using.
    ///
    /// See: <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading?view=sql-server-ver15>
    ///
    /// This function may be removed in future versions of this crate and connections would be
    /// `Send` out of the Box. This will require sufficient testing in which a wide variety of
    /// database drivers prove to be thread safe. For now this API tries to error on the side of
    /// caution, and leaves the amount of trust you want to put in the driver implementation to the
    /// user. I have seen this go wrong in the past, but time certainly improved the situation. At
    /// one point this will be cargo cult and Connection can be `Send` by default (hopefully).
    ///
    /// Note to users of `unixodbc`: You may configure the threading level to make unixodbc
    /// synchronize access to the driver (and thereby making them thread safe if they are not thread
    /// safe by themself. This may however hurt your performance if the driver would actually be
    /// able to perform operations in parallel.
    ///
    /// See: <https://stackoverflow.com/questions/4207458/using-unixodbc-in-a-multithreaded-concurrent-setting>
    pub unsafe fn promote_to_send(self) -> force_send_sync::Send<Self> {
        force_send_sync::Send::new(self)
    }

    /// Fetch the name of the database management system used by the connection and store it into
    /// the provided `buf`.
    pub fn fetch_database_management_system_name(&self, buf: &mut Vec<u16>) -> Result<(), Error> {
        self.connection
            .fetch_database_management_system_name(buf)
            .into_result(&self.connection)
    }

    /// Get the name of the database management system used by the connection.
    pub fn database_management_system_name(&self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.fetch_database_management_system_name(&mut buf)?;
        let name = U16String::from_vec(buf);
        Ok(name.to_string().unwrap())
    }

    /// Maximum length of catalog names.
    pub fn max_catalog_name_len(&self) -> Result<u16, Error> {
        self.connection
            .max_catalog_name_len()
            .into_result(&self.connection)
    }

    /// Maximum length of schema names.
    pub fn max_schema_name_len(&self) -> Result<u16, Error> {
        self.connection
            .max_schema_name_len()
            .into_result(&self.connection)
    }

    /// Maximum length of table names.
    pub fn max_table_name_len(&self) -> Result<u16, Error> {
        self.connection
            .max_table_name_len()
            .into_result(&self.connection)
    }

    /// Maximum length of column names.
    pub fn max_column_name_len(&self) -> Result<u16, Error> {
        self.connection
            .max_column_name_len()
            .into_result(&self.connection)
    }

    /// Fetch the name of the current catalog being used by the connection and store it into the
    /// provided `buf`.
    pub fn fetch_current_catalog(&self, buf: &mut Vec<u16>) -> Result<(), Error> {
        self.connection
            .fetch_current_catalog(buf)
            .into_result(&self.connection)
    }

    /// Get the name of the current catalog being used by the connection.
    pub fn current_catalog(&self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.fetch_current_catalog(&mut buf)?;
        let name = U16String::from_vec(buf);
        Ok(name.to_string().unwrap())
    }

    /// A cursor describing columns of all tables matching the patterns. Patterns support as
    /// placeholder `%` for multiple characters or `_` for a single character. Use `\` to escape.The
    /// returned cursor has the columns:
    /// `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `TYPE_NAME`,
    /// `COLUMN_SIZE`, `BUFFER_LENGTH`, `DECIMAL_DIGITS`, `NUM_PREC_RADIX`, `NULLABLE`,
    /// `REMARKS`, `COLUMN_DEF`, `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`, `CHAR_OCTET_LENGTH`,
    /// `ORDINAL_POSITION`, `IS_NULLABLE`.
    ///
    /// In addition to that there may be a number of columns specific to the data source.
    pub fn columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<CursorImpl<StatementImpl<'_>>, Error> {
        execute_columns(
            self.allocate_statement()?,
            &U16String::from_str(catalog_name),
            &U16String::from_str(schema_name),
            &U16String::from_str(table_name),
            &U16String::from_str(column_name),
        )
    }

    /// List tables, schemas, views and catalogs of a datasource.
    ///
    /// # Parameters
    ///
    /// * `catalog_name`: Filter result by catalog name. Accept search patterns. Use `%` to match
    ///   any number of characters. Use `_` to match exactly on character. Use `\` to escape
    ///   characeters.
    /// * `schema_name`: Filter result by schema. Accepts patterns in the same way as
    ///   `catalog_name`.
    /// * `table_name`: Filter result by table. Accepts patterns in the same way as `catalog_name`.
    /// * `table_type`: Filters results by table type. E.g: 'TABLE', 'VIEW'. This argument accepts a
    ///   comma separeted list of table types. Omit it to not filter the result by table type at
    ///   all.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{Connection, Cursor, Error, buffers::TextRowSet};
    ///
    /// fn print_all_tables(conn: &Connection<'_>) -> Result<(), Error> {
    ///     // Set all filters to None, to really print all tables
    ///     let cursor = conn.tables(None, None, None, None)?;
    ///
    ///     // The column are gonna be TABLE_CAT,TABLE_SCHEM,TABLE_NAME,TABLE_TYPE,REMARKS, but may
    ///     // also contain additional driver specific columns.
    ///     for (index, name) in cursor.column_names()?.enumerate() {
    ///         if index != 0 {
    ///             print!(",")
    ///         }
    ///         print!("{}", name?);
    ///     }
    ///
    ///     let batch_size = 100;
    ///     let mut buffer = TextRowSet::for_cursor(batch_size, &cursor, Some(4096))?;
    ///     let mut row_set_cursor = cursor.bind_buffer(&mut buffer)?;
    ///
    ///     while let Some(row_set) = row_set_cursor.fetch()? {
    ///         for row_index in 0..row_set.num_rows() {
    ///             if row_index != 0 {
    ///                 print!("\n");
    ///             }
    ///             for col_index in 0..row_set.num_cols() {
    ///                 if col_index != 0 {
    ///                     print!(",");
    ///                 }
    ///                 let value = row_set
    ///                     .at_as_str(col_index, row_index)
    ///                     .unwrap()
    ///                     .unwrap_or("NULL");
    ///                 print!("{}", value);
    ///             }
    ///         }
    ///     }
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn tables(
        &self,
        catalog_name: Option<&str>,
        schema_name: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&str>,
    ) -> Result<CursorImpl<StatementImpl<'_>>, Error> {
        let statement = self.allocate_statement()?;

        let catalog_name = catalog_name.map(|s| U16String::from_str(s));
        let schema_name = schema_name.map(|s| U16String::from_str(s));
        let table_name = table_name.map(|s| U16String::from_str(s));
        let table_type = table_type.map(|s| U16String::from_str(s));
        execute_tables(
            statement,
            catalog_name.as_deref(),
            schema_name.as_deref(),
            table_name.as_deref(),
            table_type.as_deref(),
        )
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

    fn allocate_statement(&self) -> Result<StatementImpl<'_>, Error> {
        self.connection
            .allocate_statement()
            .into_result(&self.connection)
    }
}

/// You can use this method to escape a password so it is suitable to be appended to an ODBC
/// connection string as the value for the `PWD` attribute. This method is only of interest for
/// application in need to create their own connection strings.
///
/// See: <https://stackoverflow.com/questions/22398212/escape-semicolon-in-odbc-connection-string-in-app-config-file>
///
/// # Example
///
/// ```
/// use odbc_api::escape_attribute_value;
///
/// let password = "abc;123}";
/// let user = "SA";
/// let mut connection_string_without_credentials =
///     "Driver={ODBC Driver 17 for SQL Server};Server=localhost;";
///
/// let connection_string = format!(
///     "{}UID={};PWD={};",
///     connection_string_without_credentials,
///     user,
///     escape_attribute_value(password)
/// );
///
/// assert_eq!(
///     "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD={abc;123}}};",
///     connection_string
/// );
/// ```
///
/// ```
/// use odbc_api::escape_attribute_value;
/// assert_eq!("abc", escape_attribute_value("abc"));
/// assert_eq!("ab}c", escape_attribute_value("ab}c"));
/// assert_eq!("{ab;c}", escape_attribute_value("ab;c"));
/// assert_eq!("{a}}b;c}", escape_attribute_value("a}b;c"));
/// ```
pub fn escape_attribute_value(unescaped: &str) -> Cow<'_, str> {
    // Search the string for semicolon (';') if we do not find any, nothing is to do and we can work
    // without an extra allocation.
    if unescaped.contains(';') {
        // Surround the string with curly braces ('{','}') and escape every closing curly brace by
        // repeating it.
        let escaped = unescaped.replace("}", "}}");
        Cow::Owned(format!("{{{}}}", escaped))
    } else {
        Cow::Borrowed(unescaped)
    }
}
