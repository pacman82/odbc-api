use std::borrow::BorrowMut;

use widestring::U16Str;

use crate::{
    handles::{Statement, StatementImpl},
    CursorImpl, Error, ParameterCollection,
};

/// Shared implementation for executing a query with parameters between [`Connection`],
/// [`Preallocated`] and [`Prepared`].
///
/// # Parameters
///
/// * `lazy_statement`: Factory for statement handle used to execute the query. We pass the
///   pass the statement lazily in order to avoid unnecessary allocating a statement handle in case
///   the parameter set is empty.
/// * `query`: SQL query to be executed. If `None` it is a assumed a prepared query is to be
///   executed.
/// * `params`: The parameters bound to the statement before query execution.
pub fn execute_with_parameters<'o, S>(
    lazy_statement: impl FnOnce() -> Result<S, Error>,
    query: Option<&U16Str>,
    params: impl ParameterCollection,
) -> Result<Option<CursorImpl<'o, S>>, Error>
where
    S: BorrowMut<StatementImpl<'o>>,
{
    let paramset_size = params.parameter_set_size();
    if paramset_size == 0 {
        Ok(None)
    } else {
        // Only allocate the statement, if we know we are going to execute something.
        let mut statement = lazy_statement()?;
        let stmt = statement.borrow_mut();
        // Reset parameters so we do not dereference stale once by mistake if we call
        // `exec_direct`.
        stmt.reset_parameters()?;
        unsafe {
            stmt.set_paramset_size(paramset_size)?;
            // Bind new parameters passed by caller.
            params.bind_parameters_to(stmt)?;
            if let Some(sql) = query {
                stmt.exec_direct(sql)?;
            } else {
                stmt.execute()?;
            }
        };
        // Check if a result set has been created.
        if stmt.num_result_cols()? == 0 {
            Ok(None)
        } else {
            Ok(Some(CursorImpl::new(statement)))
        }
    }
}
