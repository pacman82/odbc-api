use crate::{
    CursorImpl, CursorPolling, Error, ParameterCollectionRef, Preallocated, Prepared, Sleep,
    buffers::BufferDesc,
    execute::{
        execute_columns, execute_foreign_keys, execute_tables, execute_with_parameters_polling,
    },
    handles::{
        self, SqlText, State, Statement, StatementConnection, StatementImpl, StatementParent,
        slice_to_utf8,
    },
};
use log::error;
use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
    mem::{ManuallyDrop, MaybeUninit},
    ptr, str,
    sync::Arc,
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
                    // <https://learn.microsoft.com/sql/odbc/reference/syntax/sqlendtran-function>
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
///
/// In order to create multiple statements with the same connection and for other use cases,
/// operations like [`Self::execute`] or [`Self::prepare`] are taking a shared reference of `self`
/// rather than `&mut self`. However, since error handling is done through state changes of the
/// underlying connection managed by the ODBC driver, this implies that `Connection` must not be
/// `Sync`.
pub struct Connection<'c> {
    connection: handles::Connection<'c>,
}

impl<'c> Connection<'c> {
    pub(crate) fn new(connection: handles::Connection<'c>) -> Self {
        Self { connection }
    }

    /// Transfer ownership of this open connection to a wrapper around the raw ODBC pointer. The
    /// wrapper allows you to call ODBC functions on the handle, but doesn't care if the connection
    /// is in the right state.
    ///
    /// You should not have a need to call this method if your use case is covered by this library,
    /// but, in case it is not, this may help you to break out of the type structure which might be
    /// to rigid for you, while simultaneously abondoning its safeguards.
    pub fn into_handle(self) -> handles::Connection<'c> {
        // We do not want the compiler to invoke `Drop`, since drop would disconnect, yet we want to
        // transfer ownership to the connection handle.
        let dont_drop_me = MaybeUninit::new(self);
        let self_ptr = dont_drop_me.as_ptr();

        // Safety: We know `dont_drop_me` is (still) valid at this point so reading the ptr is okay
        unsafe { ptr::read(&(*self_ptr).connection) }
    }

    /// Executes an SQL statement. This is the fastest way to submit an SQL statement for one-time
    /// execution. In case you do **not** want to execute more statements on this connection, you
    /// may want to use [`Self::into_cursor`] instead, which would create a cursor taking ownership
    /// of the connection.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. See the [`crate::parameter`] module level documentation for more
    ///   information on how to pass parameters.
    /// * `query_timeout_sec`: Use this to limit the time the query is allowed to take, before
    ///   responding with data to the application. The driver may replace the number of seconds you
    ///   provide with a minimum or maximum value.
    ///
    ///   For the timeout to work the driver must support this feature. E.g. PostgreSQL, and
    ///   Microsoft SQL Server do, but SQLite or MariaDB do not.
    ///
    ///   You can specify ``0``, to deactivate the timeout, this is the default. So if you want no
    ///   timeout, just leave it at `None`. Only reason to specify ``0`` is if for some reason your
    ///   datasource does not have ``0`` as default.
    ///
    ///   This corresponds to `SQL_ATTR_QUERY_TIMEOUT` in the ODBC C API.
    ///
    ///   See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function>
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
    /// // This query does not use any parameters.
    /// let query_params = ();
    /// let timeout_sec = None;
    /// if let Some(cursor) = conn.execute(
    ///     "SELECT year, name FROM Birthdays;",
    ///     query_params,
    ///     timeout_sec)?
    /// {
    ///     // Use cursor to process query results.
    /// }
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn execute(
        &self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<CursorImpl<StatementImpl<'_>>>, Error> {
        // Only allocate the statement, if we know we are going to execute something.
        if params.parameter_set_size() == 0 {
            return Ok(None);
        }
        let mut statement = self.preallocate()?;
        if let Some(seconds) = query_timeout_sec {
            statement.set_query_timeout_sec(seconds)?;
        }
        statement.into_cursor(query, params)
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
        // Only allocate the statement, if we know we are going to execute something.
        if params.parameter_set_size() == 0 {
            return Ok(None);
        }
        let query = SqlText::new(query);
        let mut statement = self.allocate_statement()?;
        statement.set_async_enable(true).into_result(&statement)?;
        execute_with_parameters_polling(statement, Some(&query), params, sleep).await
    }

    /// Similar to [`Self::execute`], but takes ownership of the connection. This is useful if e.g.
    /// youwant to open a connection and execute a query in a function and return a self containing
    /// cursor.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. See the [`crate::parameter`] module level documentation for more
    ///   information on how to pass parameters.
    /// * `query_timeout_sec`: Use this to limit the time the query is allowed to take, before
    ///   responding with data to the application. The driver may replace the number of seconds you
    ///   provide with a minimum or maximum value.
    ///
    ///   For the timeout to work the driver must support this feature. E.g. PostgreSQL, and
    ///   Microsoft SQL Server do, but SQLite or MariaDB do not.
    ///
    ///   You can specify ``0``, to deactivate the timeout, this is the default. So if you want no
    ///   timeout, just leave it at `None`. Only reason to specify ``0`` is if for some reason your
    ///   datasource does not have ``0`` as default.
    ///
    ///   This corresponds to `SQL_ATTR_QUERY_TIMEOUT` in the ODBC C API.
    ///
    ///   See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function>
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
    ///     // connect.execute(&query, (), None) // Compiler error: Would return local ref to
    ///                                          // `conn`.
    ///
    ///     let maybe_cursor = conn.into_cursor(&query, (), None)?;
    ///     Ok(maybe_cursor)
    /// }
    /// ```
    pub fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<CursorImpl<StatementConnection<Connection<'c>>>>, ConnectionAndError<'c>>
    {
        // With the current Rust version the borrow checker needs some convincing, so that it allows
        // us to return the Connection, even though the Result of execute borrows it.
        let mut error = None;
        let mut cursor = None;
        match self.execute(query, params, query_timeout_sec) {
            Ok(Some(c)) => cursor = Some(c),
            Ok(None) => return Ok(None),
            Err(e) => error = Some(e),
        };
        if let Some(e) = error {
            drop(cursor);
            return Err(ConnectionAndError {
                error: e,
                previous: self,
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
    ///     environment, Error, ColumnarBulkInserter, handles::StatementConnection,
    ///     buffers::{BufferDesc, AnyBuffer}, ConnectionOptions, Connection
    /// };
    ///
    /// const CONNECTION_STRING: &str =
    ///     "Driver={ODBC Driver 18 for SQL Server};\
    ///     Server=localhost;UID=SA;\
    ///     PWD=My@Test@Password1;";
    ///
    /// /// Supports columnar bulk inserts on a heterogenous schema (columns have different types),
    /// /// takes ownership of a connection created using an environment with static lifetime.
    /// type Inserter = ColumnarBulkInserter<StatementConnection<Connection<'static>>, AnyBuffer>;
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
    pub fn into_prepared(
        self,
        query: &str,
    ) -> Result<Prepared<StatementConnection<Connection<'c>>>, Error> {
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
    pub fn preallocate(&self) -> Result<Preallocated<StatementImpl<'_>>, Error> {
        let stmt = self.allocate_statement()?;
        unsafe { Ok(Preallocated::new(stmt)) }
    }

    /// Creates a preallocated statement handle like [`Self::preallocate`]. Yet the statement handle
    /// also takes ownership of the connection.
    pub fn into_preallocated(
        self,
    ) -> Result<Preallocated<StatementConnection<Connection<'c>>>, Error> {
        let stmt = self.allocate_statement()?;
        // Safe: We know `stmt` is a valid statement handle and self is the connection which has
        // been used to allocate it.
        unsafe {
            let stmt = StatementConnection::new(stmt.into_sys(), self);
            Ok(Preallocated::new(stmt))
        }
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

/// We need to implement [`StatementParent`] for [`Connection`] in order to express ownership of a
/// connection for a statement handle. This is e.g. needed for [`Connection::into_cursor`].
///
/// # Safety:
///
/// Connection wraps an open Connection. It keeps the handle alive and valid during its lifetime.
unsafe impl StatementParent for Connection<'_> {}

/// We need to implement [`StatementParent`] for `Arc<Connection>` in order to be able to express
/// ownership of a shared connection from a statement handle. This is e.g. needed for
/// [`ConnectionTransition::into_cursor`].
///
/// # Safety:
///
/// `Arc<Connection>` wraps an open Connection. It keeps the handle alive and valid during its
/// lifetime.
unsafe impl StatementParent for Arc<Connection<'_>> {}

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

/// A pair of the error and the previous state, before the operation caused the error.
///
/// Some functions in this crate take a `self` and return another type in the result to express a
/// state transitions in the underlying ODBC handle. In order to make such operations retryable, or
/// offer other alternatives of recovery, they may return this error type instead of a plain
/// [`Error`].
#[derive(Debug)]
pub struct FailedStateTransition<S> {
    /// The ODBC error which caused the state transition to fail.
    pub error: Error,
    /// The state before the transition failed. This is useful to e.g. retry the operation, or
    /// recover in another way.
    pub previous: S,
}

impl<S> From<FailedStateTransition<S>> for Error {
    fn from(value: FailedStateTransition<S>) -> Self {
        value.error
    }
}

impl<S> Display for FailedStateTransition<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl<S> std::error::Error for FailedStateTransition<S>
where
    S: Debug,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.error.source()
    }
}

/// An error type wrapping an [`Error`] and a [`Connection`]. It is used by
/// [`Connection::into_cursor`], so that in case of failure the user can reuse the connection to try
/// again. [`Connection::into_cursor`] could achieve the same by returning a tuple in case of an
/// error, but this type causes less friction in most scenarios because [`Error`] implements
/// [`From`] [`ConnectionAndError`] and it therfore works with the question mark operater (`?`).
type ConnectionAndError<'conn> = FailedStateTransition<Connection<'conn>>;

/// Ability to transition ownership of the connection to various children which represent statement
/// handles in various states. E.g. [`crate::Prepared`] or [`crate::Cursor`]. Transfering ownership
/// of the connection could e.g. be useful if you want to clean the connection after you are done
/// with the child.
///
/// Having this in a trait rather than directly on [`Connection`] allows us to be generic over the
/// type of ownership we express. E.g. we can express shared ownership of a connection by
/// using an `Arc<Mutex<Connection>>` or `Arc<Connection>`. Or a still exclusive ownership using
/// a plain [`Connection`].
pub trait ConnectionTransitions: Sized {
    // Note to self. This might eveolve into a `Connection` trait. Which expresses ownership
    // of a connection (shared or not). It could allow to get a dereferened borrowed conection
    // which does not allow for state transtions as of now (like StatementRef). I may not want to
    // rock the boat that much right now.

    /// Cursor type after transfering ownership of the connection.
    type Cursor;
    /// Prepared statement type after transferring ownership of the connection.
    type Prepared;
    // /// Preallocated statement type after transferring ownership of the connection.
    // type Preallocated;
    //

    /// Similar to [`crate::Connection::into_cursor`], yet it operates on an `Arc<Mutex<Connection>>`.
    /// `Arc<Connection>` can be used if you want shared ownership of connections. However,
    /// `Arc<Connection>` is not `Send` due to `Connection` not being `Sync`. So sometimes you may want
    /// to wrap your `Connection` into an `Arc<Mutex<Connection>>` to allow shared ownership of the
    /// connection across threads. This function allows you to create a cursor from such a shared
    /// which also holds a strong reference to it.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. See the [`crate::parameter`] module level documentation for more
    ///   information on how to pass parameters.
    /// * `query_timeout_sec`: Use this to limit the time the query is allowed to take, before
    ///   responding with data to the application. The driver may replace the number of seconds you
    ///   provide with a minimum or maximum value.
    ///
    ///   For the timeout to work the driver must support this feature. E.g. PostgreSQL, and Microsoft
    ///   SQL Server do, but SQLite or MariaDB do not.
    ///
    ///   You can specify ``0``, to deactivate the timeout, this is the default. So if you want no
    ///   timeout, just leave it at `None`. Only reason to specify ``0`` is if for some reason your
    ///   datasource does not have ``0`` as default.
    ///
    ///   This corresponds to `SQL_ATTR_QUERY_TIMEOUT` in the ODBC C API.
    ///
    ///   See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetstmtattr-function>
    fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<Self::Cursor>, FailedStateTransition<Self>>;

    /// Prepares an SQL statement which takes ownership of the connection. The advantage over
    /// [`Connection::prepare`] is, that you do not need to keep track of the lifetime of the
    /// connection seperatly and can create types which do own the prepared query and only depend on
    /// the lifetime of the environment.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    ///
    /// ```no_run
    /// use odbc_api::{
    ///     environment, Error, ColumnarBulkInserter, ConnectionTransitions, Connection,
    ///     handles::StatementConnection, buffers::{BufferDesc, AnyBuffer}, ConnectionOptions,
    /// };
    ///
    /// const CONNECTION_STRING: &str =
    ///     "Driver={ODBC Driver 18 for SQL Server};\
    ///     Server=localhost;UID=SA;\
    ///     PWD=My@Test@Password1;";
    ///
    /// /// Supports columnar bulk inserts on a heterogenous schema (columns have different types),
    /// /// takes ownership of a connection created using an environment with static lifetime.
    /// type Inserter = ColumnarBulkInserter<StatementConnection<Connection<'static>>, AnyBuffer>;
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
    fn into_prepared(self, query: &str) -> Result<Self::Prepared, Error>;
}

impl<'env> ConnectionTransitions for Connection<'env> {
    type Cursor = CursorImpl<StatementConnection<Connection<'env>>>;
    type Prepared = Prepared<StatementConnection<Connection<'env>>>;
    // type Preallocated = Preallocated<StatementConnection<Connection<'_>>>;

    fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<Self::Cursor>, FailedStateTransition<Self>> {
        self.into_cursor(query, params, query_timeout_sec)
    }

    fn into_prepared(self, query: &str) -> Result<Self::Prepared, Error> {
        self.into_prepared(query)
    }
}

impl<'env> ConnectionTransitions for Arc<Connection<'env>> {
    type Cursor = CursorImpl<StatementConnection<Arc<Connection<'env>>>>;
    type Prepared = Prepared<StatementConnection<Arc<Connection<'env>>>>;
    // type Preallocated = Preallocated<StatementConnection<Arc<Connection<'_>>>>;

    fn into_cursor(
        self,
        query: &str,
        params: impl ParameterCollectionRef,
        query_timeout_sec: Option<usize>,
    ) -> Result<Option<Self::Cursor>, FailedStateTransition<Self>> {
        // Result borrows the connection. We convert the cursor into a raw pointer, to not confuse
        // the borrow checker.
        let result = self.execute(query, params, query_timeout_sec);
        let maybe_stmt_ptr = result
            .map(|opt| opt.map(|cursor| cursor.into_stmt().into_sys()))
            .map_err(|error| {
                // If the execute fails, we return a FailedStateTransition with the error and the
                // connection.
                FailedStateTransition {
                    error,
                    previous: Arc::clone(&self),
                }
            })?;
        let Some(stmt_ptr) = maybe_stmt_ptr else {
            return Ok(None);
        };
        // Safe: The connection is the parent of the statement referenced by `stmt_ptr`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self) };
        // Safe: `stmt` is valid and in cursor state.
        let cursor = unsafe { CursorImpl::new(stmt) };
        Ok(Some(cursor))
    }

    fn into_prepared(self, query: &str) -> Result<Self::Prepared, Error> {
        let stmt = self.prepare(query)?;
        let stmt_ptr = stmt.into_statement().into_sys();
        // Safe: The connection is the parent of the statement referenced by `stmt_ptr`.
        let stmt = unsafe { StatementConnection::new(stmt_ptr, self) };
        // `stmt` is valid and in prepared state.
        let prepared = Prepared::new(stmt);
        Ok(prepared)
    }
}
