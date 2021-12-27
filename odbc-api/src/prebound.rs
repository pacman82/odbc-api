use crate::{
    execute::execute,
    handles::{Statement, StatementImpl},
    CursorImpl, Error, ParameterCollection,
};

/// A prepared statement with prebound parameters.
///
/// Helps you avoid (potentially costly) ODBC function calls than repeatedly executing the same
/// prepared query with different parameters. Using this instead of [`crate::Prepared::execute`]
/// directly results in the parameter buffers only to be bound once and modified between calls,
/// instead of new buffers bound.
pub struct Prebound<'open_connection, Parameters> {
    statement: StatementImpl<'open_connection>,
    parameters: Parameters,
}

impl<'o, P> Prebound<'o, P>
where
    P: ParameterCollection,
{
    /// # Safety
    ///
    /// * `statement` must be a prepared statement without any parameters bound.
    /// * `parameters` bound to statement. Must not be invalidated by moving. Nor should any
    ///   operations which can be performed through a mutable reference be able to invalide the
    ///   binding.
    pub unsafe fn new(mut statement: StatementImpl<'o>, mut parameters: P) -> Result<Self, Error> {
        statement.reset_parameters().into_result(&statement)?;
        parameters.bind_parameters_to(&mut statement)?;
        Ok(Self {
            statement,
            parameters,
        })
    }

    /// Execute the prepared statement
    pub fn execute(&mut self) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        unsafe { execute(&mut self.statement, None) }
    }

    /// Provides write access to the bound parameters
    pub fn params_mut(&mut self) -> &mut P {
        &mut self.parameters
    }
}
