pub use crate::{
    handles::{self, Statement},
    CursorImpl, Error, Parameters,
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

        unsafe {
            params.bind_input(&mut stmt)?;
        }

        if stmt.exec_direct(query)? {
            stmt.reset_parameters()?;
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
}
