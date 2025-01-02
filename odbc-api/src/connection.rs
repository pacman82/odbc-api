use crate::{
    buffers::BufferDesc,
    execute::{
        execute_columns, execute_foreign_keys, execute_tables, execute_with_parameters,
        execute_with_parameters_polling,
    },
    handles::{self, slice_to_utf8, SqlText, State, Statement, StatementImpl},
    statement_connection::StatementConnection,
    CursorImpl, CursorPolling, Error, ParameterCollectionRef, Preallocated, Prepared, Sleep,
};
use log::error;
use odbc_sys::HDbc;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
    mem::ManuallyDrop,
    str,
    thread::panicking,
};

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        match self.connection.disconnect().into_result(&self.connection) {
            Ok(()) => (),
            Err(Error::Diagnostics {
                record,
                function: _,
            }) if record.state == State::INVALID_STATE_TRANSACTION => {
                // Invalid transaction state. Let's rollback the current transaction and try again.
                if let Err(e) = self.rollback() {
                    // Connection might be in a suspended state. See documentation about suspended
                    // state here:
                    // <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlendtran-function>
                    //
                    // See also issue:
                    // <https://github.com/pacman82/odbc-api/issues/574#issuecomment-2286449125>

                    error!(
                        "Error during rolling back transaction (In order to recover from \
                        invalid transaction state during disconnect {}",
                        e
                    );
                }
                // Transaction might be rolled back or suspended. Now let's try again to disconnect.
                if let Err(e) = self.connection.disconnect().into_result(&self.connection) {
                    // Avoid panicking, if we already have a panic. We don't want to mask the
                    // original error.
                    if !panicking() {
                        panic!("Unexpected error disconnecting (after rollback attempt): {e:?}")
                    }
                }
            }
            Err(e) => {
                // Avoid panicking, if we already have a panic. We don't want to mask the original
                // error.
                if !panicking() {
                    panic!("Unexpected error disconnecting: {e:?}")
                }
            }
        }
    }
}

/// The connection handle references storage of all information about the connection to the data
/// source, including status, transaction state, and error information.
///
/// If you want to enable the connection pooling support build into the ODBC driver manager have a
/// look at [`crate::Environment::set_connection_pooling`].
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

    /// Transfer ownership of this open connection to a wrapper around the raw ODBC pointer. The
    /// wrapper allows you to call ODBC functions on the handle, but doesn't care if the connection
    /// is in the right state.
    ///
    /// You should not have a need to call this method if your use case is covered by this library,
    /// but, in case it is not, this may help you to break out of the type structure which might be
    /// to rigid for you, while simultaneously abondoning its safeguards.
    pub fn into_handle(self) -> handles::Connection<'c> {
        unsafe { handles::Connection::new(ManuallyDrop::new(self).connection.as_sys()) }
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
    /// use odbc_api::{Environment, ConnectionOptions};
    ///
    /// let env = Environment::new()?;
    ///
    /// let mut conn = env.connect(
    ///     "YourDatabase", "SA", "My@Test@Password1",
    ///     ConnectionOptions::default()
    /// )?;
    /// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
    ///     // Use cursor to process query results.  
    /// }
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn execute(
        &self,
        query: &str,
        params: impl ParameterCollectionRef,
    ) -> Result<Option<CursorImpl<StatementImpl<'_>>>, Error> {
        let query = SqlText::new(query);
        let lazy_statement = move || self.allocate_statement();
        execute_with_parameters(lazy_statement, Some(&query), params)
    }

    /// Asynchronous sibling of [`Self::execute`]. Uses polling mode to be asynchronous. `sleep`
    /// does govern the behaviour of polling, by waiting for the future in between polling. Sleep
    /// should not be implemented using a sleep which blocks the system thread, but rather utilize
    /// the methods provided by your async runtime. E.g.:
    ///
    /// ```
    /// use odbc_api::{Connection, IntoParameter, Error};
    /// use std::time::Duration;
    ///
    /// async fn insert_post<'a>(
    ///     connection: &'a Connection<'a>,
    ///     user: &str,
    ///     post: &str,
    /// ) -> Result<(), Error> {
    ///     // Poll every 50 ms.
    ///     let sleep = || tokio::time::sleep(Duration::from_millis(50));
    ///     let sql = "INSERT INTO POSTS (user, post) VALUES (?, ?)";
    ///     // Execute query using ODBC polling method
    ///     let params = (&user.into_parameter(), &post.into_parameter());
    ///     connection.execute_polling(&sql, params, sleep).await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// **Attention**: This feature requires driver support, otherwise the calls will just block
    /// until they are finished. At the time of writing this out of Microsoft SQL Server,
    /// PostgerSQL, SQLite and MariaDB this worked only with Microsoft SQL Server. For code generic
    /// over every driver you may still use this. The functions will return with the correct results
    /// just be aware that may block until they are finished.
    pub async fn execute_polling(
        &self,
        query: &str,
        params: impl ParameterCollectionRef,
        sleep: impl Sleep,
    ) -> Result<Option<CursorPolling<StatementImpl<'_>>>, Error> {
        let query = SqlText::new(query);
        let lazy_statement = move || {
            let mut stmt = self.allocate_statement()?;
            stmt.set_async_enable(true).into_result(&stmt)?;
            Ok(stmt)
        };
        execute_with_parameters_polling(lazy_statement, Some(&query), params, sleep).await
    }

    /// In some use cases there you only execute a single statement, or the time to open a
    /// connection does not matter users may wish to choose to not keep a connection alive seperatly
    /// from the cursor, in order to have an easier time with the borrow checker.
    ///
    /// ```no_run
    /// use odbc_api::{environment, Error, Cursor, ConnectionOptions};
    ///
    ///
    /// const CONNECTION_STRING: &str =
    ///     "Driver={ODBC Driver 18 for SQL Server};\
    ///     Server=localhost;UID=SA;\
    ///     PWD=My@Test@Password1;";
    ///
    /// fn execute_query(query: &str) -> Result<Option<impl Cursor>, Error> {
    ///     let env = environment()?;
    ///     let conn = env.connect_with_connection_string(
    ///         CONNECTION_STRING,
    ///         ConnectionOptions::default()
    ///     )?;
    ///
    ///     // connect.execute(&query, ()) // Compiler error: Would return local ref to `conn`.
    ///
    ///     let maybe_cursor = conn.into_cursor(&query, ())?;
    ///     Ok(maybe_cursor)
    /// }
    /// ```
    pub fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
    ) -> Result<Option<CursorImpl<StatementConnection<'c>>>, ConnectionAndError<'c>> {
        // With the current Rust version the borrow checker needs some convincing, so that it allows
        // us to return the Connection, even though the Result of execute borrows it.
        let mut error = None;
        let mut cursor = None;
        match self.execute(query, params) {
            Ok(Some(c)) => cursor = Some(c),
            Ok(None) => return Ok(None),
            Err(e) => error = Some(e),
        };
        if let Some(e) = error {
            drop(cursor);
            return Err(ConnectionAndError {
                error: e,
                connection: self,
            });
        }
        let cursor = cursor.unwrap();
        // The rust compiler needs some help here. It assumes otherwise that the lifetime of the
        // resulting cursor would depend on the lifetime of `params`.
        let mut cursor = ManuallyDrop::new(cursor);
        let handle = cursor.as_sys();
        // Safe: `handle` is a valid statement, and we are giving up ownership of `self`.
        let statement = unsafe { StatementConnection::new(handle, self) };
        // Safe: `statement is in the cursor state`.
        let cursor = unsafe { CursorImpl::new(statement) };
        Ok(Some(cursor))
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// Should your use case require you to execute the same query several times with different
    /// parameters, prepared queries are the way to go. These give the database a chance to cache
    /// the access plan associated with your SQL statement. It is not unlike compiling your program
    /// once and executing it several times.
    ///
    /// ```
    /// use odbc_api::{Connection, Error, IntoParameter};
    /// use std::io::{self, stdin, Read};
    ///
    /// fn interactive(conn: &Connection) -> io::Result<()>{
    ///     let mut prepared = conn.prepare("SELECT * FROM Movies WHERE title=?;").unwrap();
    ///     let mut title = String::new();
    ///     stdin().read_line(&mut title)?;
    ///     while !title.is_empty() {
    ///         match prepared.execute(&title.as_str().into_parameter()) {
    ///             Err(e) => println!("{}", e),
    ///             // Most drivers would return a result set even if no Movie with the title is found,
    ///             // the result set would just be empty. Well, most drivers.
    ///             Ok(None) => println!("No result set generated."),
    ///             Ok(Some(cursor)) => {
    ///                 // ...print cursor contents...
    ///             }
    ///         }
    ///         stdin().read_line(&mut title)?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare(&self, query: &str) -> Result<Prepared<StatementImpl<'_>>, Error> {
        let query = SqlText::new(query);
        let mut stmt = self.allocate_statement()?;
        stmt.prepare(&query).into_result(&stmt)?;
        Ok(Prepared::new(stmt))
    }

    /// Prepares an SQL statement which takes ownership of the connection. The advantage over
    /// [`Self::prepare`] is, that you do not need to keep track of the lifetime of the connection
    /// seperatly and can create types which do own the prepared query and only depend on the
    /// lifetime of the environment. The downside is that you can not use the connection for
    /// anything else anymore.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    ///
    /// ```no_run
    /// use odbc_api::{
    ///     environment, Error, ColumnarBulkInserter, StatementConnection,
    ///     buffers::{BufferDesc, AnyBuffer}, ConnectionOptions
    /// };
    ///
    /// const CONNECTION_STRING: &str =
    ///     "Driver={ODBC Driver 18 for SQL Server};\
    ///     Server=localhost;UID=SA;\
    ///     PWD=My@Test@Password1;";
    ///
    /// /// Supports columnar bulk inserts on a heterogenous schema (columns have different types),
    /// /// takes ownership of a connection created using an environment with static lifetime.
    /// type Inserter = ColumnarBulkInserter<StatementConnection<'static>, AnyBuffer>;
    ///
    /// /// Creates an inserter which can be reused to bulk insert birthyears with static lifetime.
    /// fn make_inserter(query: &str) -> Result<Inserter, Error> {
    ///     let env = environment()?;
    ///     let conn = env.connect_with_connection_string(
    ///         CONNECTION_STRING,
    ///         ConnectionOptions::default()
    ///     )?;
    ///     let prepared = conn.into_prepared("INSERT INTO Birthyear (name, year) VALUES (?, ?)")?;
    ///     let buffers = [
    ///         BufferDesc::Text { max_str_len: 255},
    ///         BufferDesc::I16 { nullable: false },
    ///     ];
    ///     let capacity = 400;
    ///     prepared.into_column_inserter(capacity, buffers)
    /// }
    /// ```
    pub fn into_prepared(self, query: &str) -> Result<Prepared<StatementConnection<'c>>, Error> {
        let query = SqlText::new(query);
        let mut stmt = self.allocate_statement()?;
        stmt.prepare(&query).into_result(&stmt)?;
        // Safe: `handle` is a valid statement, and we are giving up ownership of `self`.
        let stmt = unsafe { StatementConnection::new(stmt.into_sys(), self) };
        Ok(Prepared::new(stmt))
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
        unsafe { Ok(Preallocated::new(stmt)) }
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

    /// Network packet size in bytes. Requries driver support.
    pub fn packet_size(&self) -> Result<u32, Error> {
        self.connection.packet_size().into_result(&self.connection)
    }

    /// Get the name of the database management system used by the connection.
    pub fn database_management_system_name(&self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.connection
            .fetch_database_management_system_name(&mut buf)
            .into_result(&self.connection)?;
        let name = slice_to_utf8(&buf).unwrap();
        Ok(name)
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

    /// Get the name of the current catalog being used by the connection.
    pub fn current_catalog(&self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.connection
            .fetch_current_catalog(&mut buf)
            .into_result(&self.connection)?;
        let name = slice_to_utf8(&buf).expect("Return catalog must be correctly encoded");
        Ok(name)
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
            &SqlText::new(catalog_name),
            &SqlText::new(schema_name),
            &SqlText::new(table_name),
            &SqlText::new(column_name),
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
    /// use odbc_api::{Connection, Cursor, Error, ResultSetMetadata, buffers::TextRowSet};
    ///
    /// fn print_all_tables(conn: &Connection<'_>) -> Result<(), Error> {
    ///     // Set all filters to an empty string, to really print all tables
    ///     let mut cursor = conn.tables("", "", "", "")?;
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
    ///     let mut buffer = TextRowSet::for_cursor(batch_size, &mut cursor, Some(4096))?;
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
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_type: &str,
    ) -> Result<CursorImpl<StatementImpl<'_>>, Error> {
        let statement = self.allocate_statement()?;

        execute_tables(
            statement,
            &SqlText::new(catalog_name),
            &SqlText::new(schema_name),
            &SqlText::new(table_name),
            &SqlText::new(table_type),
        )
    }

    /// This can be used to retrieve either a list of foreign keys in the specified table or a list
    /// of foreign keys in other table that refer to the primary key of the specified table.
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlforeignkeys-function>
    pub fn foreign_keys(
        &self,
        pk_catalog_name: &str,
        pk_schema_name: &str,
        pk_table_name: &str,
        fk_catalog_name: &str,
        fk_schema_name: &str,
        fk_table_name: &str,
    ) -> Result<CursorImpl<StatementImpl<'_>>, Error> {
        let statement = self.allocate_statement()?;

        execute_foreign_keys(
            statement,
            &SqlText::new(pk_catalog_name),
            &SqlText::new(pk_schema_name),
            &SqlText::new(pk_table_name),
            &SqlText::new(fk_catalog_name),
            &SqlText::new(fk_schema_name),
            &SqlText::new(fk_table_name),
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
    pub fn columns_buffer_descs(
        &self,
        type_name_max_len: usize,
        remarks_max_len: usize,
        column_default_max_len: usize,
    ) -> Result<Vec<BufferDesc>, Error> {
        let null_i16 = BufferDesc::I16 { nullable: true };

        let not_null_i16 = BufferDesc::I16 { nullable: false };

        let null_i32 = BufferDesc::I32 { nullable: true };

        // The definitions for these descriptions are taken from the documentation of `SQLColumns`
        // located at https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function
        let catalog_name_desc = BufferDesc::Text {
            max_str_len: self.max_catalog_name_len()? as usize,
        };

        let schema_name_desc = BufferDesc::Text {
            max_str_len: self.max_schema_name_len()? as usize,
        };

        let table_name_desc = BufferDesc::Text {
            max_str_len: self.max_table_name_len()? as usize,
        };

        let column_name_desc = BufferDesc::Text {
            max_str_len: self.max_column_name_len()? as usize,
        };

        let data_type_desc = not_null_i16;

        let type_name_desc = BufferDesc::Text {
            max_str_len: type_name_max_len,
        };

        let column_size_desc = null_i32;
        let buffer_len_desc = null_i32;
        let decimal_digits_desc = null_i16;
        let precision_radix_desc = null_i16;
        let nullable_desc = not_null_i16;

        let remarks_desc = BufferDesc::Text {
            max_str_len: remarks_max_len,
        };

        let column_default_desc = BufferDesc::Text {
            max_str_len: column_default_max_len,
        };

        let sql_data_type_desc = not_null_i16;
        let sql_datetime_sub_desc = null_i16;
        let char_octet_len_desc = null_i32;
        let ordinal_pos_desc = BufferDesc::I32 { nullable: false };

        // We expect strings to be `YES`, `NO`, or a zero-length string, so `3` should be
        // sufficient.
        const IS_NULLABLE_LEN_MAX_LEN: usize = 3;
        let is_nullable_desc = BufferDesc::Text {
            max_str_len: IS_NULLABLE_LEN_MAX_LEN,
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

/// Implement `Debug` for [`Connection`], in order to play nice with derive Debugs for struct
/// holding a [`Connection`].
impl Debug for Connection<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connection")
    }
}

/// Options to be passed then opening a connection to a datasource.
#[derive(Default, Clone, Copy)]
pub struct ConnectionOptions {
    /// Number of seconds to wait for a login request to complete before returning to the
    /// application. The default is driver-dependent. If `0` the timeout is disabled and a
    /// connection attempt will wait indefinitely.
    ///
    /// If the specified timeout exceeds the maximum login timeout in the data source, the driver
    /// substitutes that value and uses the maximum login timeout instead.
    ///
    /// This corresponds to the `SQL_ATTR_LOGIN_TIMEOUT` attribute in the ODBC specification.
    ///
    /// See:
    /// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function>
    pub login_timeout_sec: Option<u32>,
    /// Packet size in bytes. Not all drivers support this option.
    pub packet_size: Option<u32>,
}

impl ConnectionOptions {
    /// Set the attributes corresponding to the connection options to an allocated connection
    /// handle. Usually you would rather provide the options then creating the connection with e.g.
    /// [`crate::Environment::connect_with_connection_string`] rather than calling this method
    /// yourself.
    pub fn apply(&self, handle: &handles::Connection) -> Result<(), Error> {
        if let Some(timeout) = self.login_timeout_sec {
            handle.set_login_timeout_sec(timeout).into_result(handle)?;
        }
        if let Some(packet_size) = self.packet_size {
            handle.set_packet_size(packet_size).into_result(handle)?;
        }
        Ok(())
    }
}

/// You can use this method to escape a password so it is suitable to be appended to an ODBC
/// connection string as the value for the `PWD` attribute. This method is only of interest for
/// application in need to create their own connection strings.
///
/// See:
///
/// * <https://stackoverflow.com/questions/22398212/escape-semicolon-in-odbc-connection-string-in-app-config-file>
/// * <https://docs.microsoft.com/en-us/dotnet/api/system.data.odbc.odbcconnection.connectionstring>
///
/// # Example
///
/// ```
/// use odbc_api::escape_attribute_value;
///
/// let password = "abc;123}";
/// let user = "SA";
/// let mut connection_string_without_credentials =
///     "Driver={ODBC Driver 18 for SQL Server};Server=localhost;";
///
/// let connection_string = format!(
///     "{}UID={};PWD={};",
///     connection_string_without_credentials,
///     user,
///     escape_attribute_value(password)
/// );
///
/// assert_eq!(
///     "Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=SA;PWD={abc;123}}};",
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
/// assert_eq!("{ab+c}", escape_attribute_value("ab+c"));
/// ```
pub fn escape_attribute_value(unescaped: &str) -> Cow<'_, str> {
    // Search the string for semicolon (';') if we do not find any, nothing is to do and we can work
    // without an extra allocation.
    //
    // * We escape ';' because it serves as a separator between key=value pairs
    // * We escape '+' because passwords with `+` must be escaped on PostgreSQL for some reason.
    if unescaped.contains(&[';', '+'][..]) {
        // Surround the string with curly braces ('{','}') and escape every closing curly brace by
        // repeating it.
        let escaped = unescaped.replace('}', "}}");
        Cow::Owned(format!("{{{escaped}}}"))
    } else {
        Cow::Borrowed(unescaped)
    }
}

/// An error type wrapping an [`Error`] and a [`Connection`]. It is used by
/// [`Connection::into_cursor`], so that in case of failure the user can reuse the connection to try
/// again. [`Connection::into_cursor`] could achieve the same by returning a tuple in case of an
/// error, but this type causes less friction in most scenarios because [`Error`] implements
/// [`From`] [`ConnectionAndError`] and it therfore works with the question mark operater (`?`).
#[derive(Debug)]
pub struct ConnectionAndError<'conn> {
    pub error: Error,
    pub connection: Connection<'conn>,
}

impl From<ConnectionAndError<'_>> for Error {
    fn from(value: ConnectionAndError) -> Self {
        value.error
    }
}

impl Display for ConnectionAndError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for ConnectionAndError<'_> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}
