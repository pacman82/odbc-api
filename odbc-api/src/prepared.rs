use crate::{
    handles::{ParameterDescription, Statement},
    CursorImpl, Error, ParameterCollection,
};

/// A prepared query. Prepared queries are useful if the similar queries should executed more than
/// once.
pub struct Prepared<'open_connection> {
    statement: Statement<'open_connection>,
}

impl<'o> Prepared<'o> {
    pub(crate) fn new(statement: Statement<'o>) -> Self {
        Self { statement }
    }

    /// Execute the prepared statement.
    ///
    /// * `params`: Used to bind these parameters before executing the statement. You can use `()`
    ///   to represent no parameters. In regards to binding arrays of parameters: Should `params`
    ///   specify a paramset size of empty, nothing is executed, and `Ok(None)` is returned.
    pub fn execute(
        &mut self,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut Statement<'o>>>, Error> {
        let paramset_size = params.parameter_set_size();
        if paramset_size == 0 {
            Ok(None)
        } else {
            // Reset parameters so we do not dereference stale once by mistake if we call
            // `exec_direct`.
            self.statement.reset_parameters()?;
            unsafe {
                self.statement.set_paramset_size(paramset_size)?;
                // Bind new parameters passed by caller.
                params.bind_parameters_to(&mut self.statement)?;
                self.statement.execute()?
            };
            // Check if a result set has been created.
            if self.statement.num_result_cols()? == 0 {
                Ok(None)
            } else {
                Ok(Some(CursorImpl::new(&mut self.statement)))
            }
        }
    }

    /// Describes parameter marker associated with a prepared SQL statement.
    ///
    /// # Paramters
    ///
    /// * `parameter_number`: Parameter marker number ordered sequentially in increasing parameter
    ///   order, starting at 1.
    pub fn describe_param(&self, parameter_number: u16) -> Result<ParameterDescription, Error> {
        self.statement.describe_param(parameter_number)
    }
}
