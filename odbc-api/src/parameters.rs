use crate::{handles::Statement, Error};

/// SQL Parameters used to execute a query.
///
/// ODBC allows to place question marks (`?`) in the statement text as placeholders. For each such
/// placeholder a parameter needs to be bound to the statement before executing it.
pub unsafe trait Parameters {
    /// # Safety
    ///
    /// Implementers should take care that the values bound by this method to the statement live at
    /// least for the Duration of `self`. The most straight forward way of achieving this is of
    /// course, to bind members.
    unsafe fn bind_input(&self, stmt: &mut Statement) -> Result<(), Error>;
}

/// The unit type is used to signal no parameters.
unsafe impl Parameters for () {
    unsafe fn bind_input(&self, _stmt: &mut Statement) -> Result<(), Error> {
        Ok(())
    }
}
