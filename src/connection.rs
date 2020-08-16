pub use crate::{handles, Cursor, Error};
use widestring::{U16String, U16Str};
use std::thread::panicking;

impl<'conn> Drop for Connection<'conn> {
    fn drop(&mut self) {
        if let Err(e) = self.connection.disconnect() {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexepected error disconnecting: {:?}", e)
            }
        }
    }
}

pub struct Connection<'c> {
    connection: handles::Connection<'c>
}

impl<'c> Connection<'c> {
    pub (crate) fn new(connection: handles::Connection<'c>) -> Self {
        Self{ connection }
    }

    /// Executes a preparable statement, using the current values of the parameter marker variables
    /// if any parameters exist in the statement. This is the fastest way to submit an SQL statement
    /// for one-time execution.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    pub fn exec_direct_utf16(&mut self, query: &U16Str) -> Result<Option<Cursor>, Error>{
        let mut stmt = self.connection.allocate_statement()?;
        if stmt.exec_direct(query)?{
            Ok(Some(Cursor::new(stmt)))
        } else {
            // ODBC Driver returned NoData.
            Ok(None)
        }
    }

    /// Executes a preparable statement, using the current values of the parameter marker variables
    /// if any parameters exist in the statement. This is the fastest way to submit an SQL statement
    /// for one-time execution.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    pub fn exec_direct(&mut self, query: &str) -> Result<Option<Cursor>, Error>{
        let query = U16String::from_str(query);
        self.exec_direct_utf16(&query)
    }
}