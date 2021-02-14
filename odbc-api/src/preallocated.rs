use widestring::{U16Str, U16String};

use crate::{
    handles::Statement,
    CursorImpl, Error, ParameterCollection,
};

pub struct Preallocated<'open_connection> {
    statement: Statement<'open_connection>,
}

impl<'o> Preallocated<'o> {
    pub(crate) fn new(statement: Statement<'o>) -> Self {
        Self { statement }
    }

    pub fn execute_utf16(
        &mut self,
        query: &U16Str,
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
                self.statement.exec_direct(query)?
            };
            // Check if a result set has been created.
            if self.statement.num_result_cols()? == 0 {
                Ok(None)
            } else {
                Ok(Some(CursorImpl::new(&mut self.statement)))
            }
        }
    }

    pub fn execute(
        &mut self,
        query: &str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut Statement<'o>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
    }
}
