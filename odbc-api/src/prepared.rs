use crate::{
    execute::execute_with_parameters,
    handles::{ParameterDescription, Statement, StatementImpl},
    CursorImpl, Error, ParameterRefCollection, ResultSetMetadata,
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

    /// Transfer ownership to the underlying statement handle.
    ///
    /// The resulting type is one level of indirection away from the raw pointer of the ODBC API. It
    /// no longer has any guarantees about bound buffers, but is still guaranteed to be a valid
    /// allocated statement handle. This serves together with
    /// [`crate::handles::StatementImpl::into_sys`] or [`crate::handles::Statement::as_sys`] this
    /// serves as an escape hatch to access the functionality provided by `crate::sys` not yet
    /// accessible through safe abstractions.
    pub fn into_statement(self) -> StatementImpl<'o> {
        self.statement
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
        params: impl ParameterRefCollection,
    ) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        execute_with_parameters(move || Ok(&mut self.statement), None, params)
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Parameters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    pub fn describe_param(&self, parameter_number: u16) -> Result<ParameterDescription, Error> {
        self.statement
            .describe_param(parameter_number)
            .into_result(&self.statement)
    }
}

impl<'o> ResultSetMetadata for Prepared<'o> {
    type Statement = StatementImpl<'o>;

    fn stmt_ref(&self) -> &Self::Statement {
        &self.statement
    }
}
