use crate::{Error, handles::Statement};

/// SQL Parameters used to execute a query.
///
/// ODBC allows to place question marks (`?`) in the statement text as placeholders. For each such
/// placeholder a parameter needs to be bound to the statement before executing it.
pub unsafe trait Parameters {
    unsafe fn bind_to_statement(&self, stmt: &mut Statement) -> Result<(), Error>;
}

/// The unit type is used to signal no parameters.
unsafe impl Parameters for () {
    unsafe fn bind_to_statement(&self, _stmt: &mut Statement) -> Result<(), Error>{
        Ok(())
    }
}
