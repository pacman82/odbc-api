use crate::{
    execute::execute_with_parameters,
    handles::{self, Statement, StatementImpl},
    parameter_collection::ParameterCollection,
    CursorImpl, Error, Preallocated, Prepared,
};
use std::{borrow::Cow, thread::panicking};
use widestring::{U16Str, U16String};

impl<'conn> Drop for Connection<'conn> {
    fn drop(&mut self) {
        match self.connection.disconnect() {
            Ok(()) => (),
            Err(Error::Diagnostics(record)) if record.is_invalid_state_transaction() => {
                // Invalid transaction state. Let's rollback the current transaction and try again.
                if let Err(e) = self.connection.rollback() {
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
                if let Err(e) = self.connection.disconnect() {
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

    /// Executes an sql statement using a wide string. See [`Self::execute`].
    pub fn execute_utf16(
        &self,
        query: &U16Str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'_, StatementImpl<'_>>>, Error> {
        let lazy_statement = move || self.connection.allocate_statement();
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
    /// // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
    /// // making this call safe.
    /// let env = unsafe {
    ///     Environment::new()?
    /// };
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
    ) -> Result<Option<CursorImpl<'_, StatementImpl<'_>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare_utf16(&self, query: &U16Str) -> Result<Prepared<'_>, Error> {
        let mut stmt = self.connection.allocate_statement()?;
        stmt.prepare(query)?;
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
        let stmt = self.connection.allocate_statement()?;
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
        self.connection.set_autocommit(enabled)
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&self) -> Result<(), Error> {
        self.connection.commit()
    }

    /// To rollback a transaction in manual-commit mode.
    pub fn rollback(&self) -> Result<(), Error> {
        self.connection.rollback()
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
