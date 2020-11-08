use crate::{handles::Statement, CursorImpl, Error, ParameterCollection};

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
    ///   to represent no parameters.
    pub fn execute(
        &mut self,
        params: impl ParameterCollection,
    ) -> Result<CursorImpl<'o, &mut Statement<'o>>, Error> {
        unsafe {
            // Reset parameters so we do not dereference stale once by mistake if we call
            // `exec_direct`.
            self.statement.reset_parameters()?;
            // Bind new parameters passed by caller.
            params.bind_parameters_to(&mut self.statement)?;
            self.statement.execute()?;
        }
        Ok(CursorImpl::new(&mut self.statement))
    }
}
