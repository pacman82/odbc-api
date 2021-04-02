use crate::{
    execute::execute_with_parameters,
    handles::{ParameterDescription, Statement, StatementImpl},
    CursorImpl, Error, ParameterCollection,
};

/// A prepared query. Prepared queries are useful if the similar queries should executed more than
/// once.
pub struct Prepared<'open_connection> {
    statement: StatementImpl<'open_connection>,
}

impl<'o> Prepared<'o> {
    pub(crate) fn new(statement: StatementImpl<'o>) -> Self {
        Self { statement }
    }

    /// Execute the prepared statement.
    ///
    /// * `params`: Used to bind these parameters before executing the statement. You can use `()`
    ///   to represent no parameters. In regards to binding arrays of parameters: Should `params`
    ///   specify a parameter set size of `0`, nothing is executed, and `Ok(None)` is returned. See
    ///   the [`crate::parameter`] module level documentation for more information on how to pass
    ///   parameters.
    pub fn execute(
        &mut self,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut StatementImpl<'o>>>, Error> {
        execute_with_parameters(move || Ok(&mut self.statement), None, params)
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Parameters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    pub fn describe_param(&self, parameter_number: u16) -> Result<ParameterDescription, Error> {
        self.statement.describe_param(parameter_number)
    }
}
