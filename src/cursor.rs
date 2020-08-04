use crate::handles::Statement;
use std::thread::panicking;

pub struct Cursor<'open_connection> {
    statement: Statement<'open_connection>
}

impl<'o> Drop for Cursor<'o> {
    fn drop(&mut self) {
        if let Err(e) = self.statement.close_cursor() {
            // Avoid panicking, if we already have a panic. We don't want to mask the original
            // error.
            if !panicking() {
                panic!("Unexepected error disconnecting: {:?}", e)
            }
        }
    }
}

impl<'o> Cursor<'o> {

    pub (crate) fn new(statement: Statement<'o>) -> Self {
        Self {statement}
    }
}