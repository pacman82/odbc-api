use crate::{handles::Statement, Error, Parameter};

mod tuple;

/// SQL Parameters used to execute a query.
///
/// ODBC allows to place question marks (`?`) in the statement text as placeholders. For each such
/// placeholder a parameter needs to be bound to the statement before executing it.
pub unsafe trait ParameterCollection {
    /// Number of parameters in the collection. This can be different from the maximum batch size
    /// a buffer may be able to hold. Returning `0` will cause the the query not to be executed.
    fn parameter_set_size(&self) -> u32;

    /// # Safety
    ///
    /// Implementers should take care that the values bound by this method to the statement live at
    /// least for the Duration of `self`. The most straight forward way of achieving this is of
    /// course, to bind members.
    unsafe fn bind_parameters_to(&self, stmt: &mut Statement) -> Result<(), Error>;
}

unsafe impl<T> ParameterCollection for T
where
    T: Parameter,
{
    fn parameter_set_size(&self) -> u32 {
        1
    }

    unsafe fn bind_parameters_to(&self, stmt: &mut Statement) -> Result<(), Error> {
        stmt.bind_input_parameter(1, self)
    }
}

unsafe impl<T> ParameterCollection for &[T]
where
    T: Parameter,
{
    fn parameter_set_size(&self) -> u32 {
        1
    }

    unsafe fn bind_parameters_to(&self, stmt: &mut Statement) -> Result<(), Error> {
        for (index, parameter) in self.iter().enumerate() {
            stmt.bind_input_parameter(index as u16 + 1, parameter)?;
        }
        Ok(())
    }
}
