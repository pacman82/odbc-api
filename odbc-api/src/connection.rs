use crate::{
    handles::{self, Statement},
    parameter_collection::ParameterCollection,
    CursorImpl, Error, Prepared,
};
use std::thread::panicking;
use widestring::{U16Str, U16String};

impl<'conn> Drop for Connection<'conn> {
    fn drop(&mut self) {
        if let Err(e) = self.connection.disconnect() {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexpected error disconnecting: {:?}", e)
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

    /// Executes a statement. This is the fastest way to submit an SQL statement for one-time
    /// execution.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". A
    ///   question mark (`?`) may be used to indicate positional parameters in the statement text.
    /// * `params`: Used to bind the placeholders in the statement text to argument values You can
    ///   use `()` to represent no parameters.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    pub fn exec_direct_utf16(
        &self,
        query: &U16Str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<Statement>>, Error> {
        let mut stmt = self.connection.allocate_statement()?;

        let paramset_size = params.parameter_set_size();

        if paramset_size == 0 {
            Ok(None)
        } else {
            // Reset parameters so we do not dereference stale once by mistake if we call
            // `exec_direct`.
            stmt.reset_parameters()?;
            unsafe {
                stmt.set_paramset_size(paramset_size)?;
                // Bind new parameters passed by caller.
                params.bind_parameters_to(&mut stmt)?;
                stmt.exec_direct(query)?
            };
            // Check if a result set has been created.
            if stmt.num_result_cols()? == 0 {
                Ok(None)
            } else {
                Ok(Some(CursorImpl::new(stmt)))
            }
        }
    }

    /// Executes a prepareable statement. This is the fastest way to submit an SQL statement for
    /// one-time execution.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters.
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
    ) -> Result<Option<CursorImpl<Statement>>, Error> {
        let query = U16String::from_str(query);
        self.exec_direct_utf16(&query, params)
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare_utf16(&self, query: &U16Str) -> Result<Prepared, Error> {
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
    pub fn prepare(&self, query: &str) -> Result<Prepared, Error> {
        let query = U16String::from_str(query);
        self.prepare_utf16(&query)
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
    /// `Send` and `Sync` out of the Box. This will require sufficient testing in which a wide
    /// variety of database drivers prove to be thread safe. For now this API tries to error on the
    /// side of caution, and leaves the amount of trust you want to put in the driver implementation
    /// to the user. I have seen this go wrong in the past, but time certainly improved the
    /// situation. At one point this will be cargo cult and Connection can be `Send` and `Sync` by
    /// default (hopefully).
    ///
    /// Note to users of `unixodbc`: You may configure the threading level to make unixodbc
    /// synchronize access to the driver (and therby making them thread safe if they are not
    /// thread safe by themself. This may however hurt your performancy if the driver would actually
    /// be able to perform operations in parallel.
    ///
    /// See: <https://stackoverflow.com/questions/4207458/using-unixodbc-in-a-multithreaded-concurrent-setting>
    pub unsafe fn promote_to_send(self) -> force_send_sync::Send<Self> {
        force_send_sync::Send::new(self)
    }

    /// Allows sending and using this connection from multiple different threads.
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
    /// let conn = unsafe { conn.promote_to_send_and_sync() };
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
    /// `Send` and `Sync` out of the Box. This will require sufficient testing in which a wide
    /// variety of database drivers prove to be thread safe. For now this API tries to error on the
    /// side of caution, and leaves the amount of trust you want to put in the driver implementation
    /// to the user. I have seen this go wrong in the past, but time certainly improved the
    /// situation. At one point this will be cargo cult and Connection can be `Send` and `Sync` by
    /// default (hopefully).
    ///
    /// Note to users of `unixodbc`: You may configure the threading level to make unixodbc
    /// synchronize access to the driver (and therby making them thread safe if they are not
    /// thread safe by themself. This may however hurt your performancy if the driver would actually
    /// be able to perform operations in parallel.
    ///
    /// See: <https://stackoverflow.com/questions/4207458/using-unixodbc-in-a-multithreaded-concurrent-setting>
    pub unsafe fn promote_to_send_and_sync(self) -> force_send_sync::SendSync<Self> {
        force_send_sync::SendSync::new(self)
    }
}
