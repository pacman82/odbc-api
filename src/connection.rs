pub use crate::{handles, Cursor, Error};
use widestring::U16Str;
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

    pub fn exec_direct(&self, query: &U16Str) -> Result<Option<Cursor>, Error>{
        let mut stmt = self.connection.allocate_statement()?;
        if stmt.exec_direct(query)?{
            Ok(Some(Cursor::new(stmt)))
        } else {
            // ODBC Driver returned NoData.
            Ok(None)
        }
    }
}