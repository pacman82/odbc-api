use crate::{
    handles::{self, Statement},
    CursorImpl, Error, Parameters, Prepared,
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

    /// Executes a prepareable statement. This is the fastest way to submit an SQL statement for
    /// one-time execution.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. A type implementing
    ///   `Parameters` is used to bind these parameters before executing the statement. You can use
    ///   `()` to represent no parameters.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    pub fn exec_direct_utf16(
        &mut self,
        query: &U16Str,
        params: impl Parameters,
    ) -> Result<Option<CursorImpl<Statement>>, Error> {
        let mut stmt = self.connection.allocate_statement()?;

        let has_result = unsafe {
            // Reset parameters so we do not dereference stale once by mistake if we call
            // `exec_direct`.
            stmt.reset_parameters()?;
            // Bind new parameters passed by caller.
            params.bind_input_parameters(&mut stmt)?;
            stmt.exec_direct(query)?
        };

        if has_result {
            Ok(Some(CursorImpl::new(stmt)))
        } else {
            // ODBC Driver returned NoData.
            Ok(None)
        }
    }

    /// Executes a prepareable statement. This is the fastest way to submit an SQL statement for
    /// one-time execution.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. A type implementing
    ///   `Parameters` is used to bind these parameters before executing the statement. You can use
    ///   `()` to represent no parameters.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    pub fn exec_direct(
        &mut self,
        query: &str,
        params: impl Parameters,
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
}
