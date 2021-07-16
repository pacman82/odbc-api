use std::{borrow::BorrowMut, intrinsics::transmute};

use widestring::U16Str;

use crate::{
    handles::{Statement, StatementImpl},
    parameter::Blob,
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
    let parameter_set_size = params.parameter_set_size();
    if parameter_set_size == 0 {
        return Ok(None);
    }

    // Only allocate the statement, if we know we are going to execute something.
    let mut statement = lazy_statement()?;
    let stmt = statement.borrow_mut();
    // Reset parameters so we do not dereference stale once by mistake if we call
    // `exec_direct`.
    stmt.reset_parameters()?;
    let need_data = unsafe {
        stmt.set_paramset_size(parameter_set_size)?;
        // Bind new parameters passed by caller.
        params.bind_parameters_to(stmt)?;
        if let Some(sql) = query {
            stmt.exec_direct(sql)?
        } else {
            stmt.execute()?
        }
    };

    if need_data {
        // Check if any delayed parameters have been bound which stream data to the database at
        // statement execution time. Loops over each bound stream.
        while let Some(blob_ptr) = stmt.param_data()? {
            // The safe interfaces currently exclusively bind pointers to `Blob` trait objects
            let blob_ref = unsafe {
                let blob_ptr: *mut &mut dyn Blob = transmute(blob_ptr);
                &mut *blob_ptr
            };
            // Loop over all batches within each blob
            while let Some(batch) = blob_ref.next_batch().unwrap() {
                stmt.put_binary_batch(batch)?;
            }
        }
    }

    // Check if a result set has been created.
    if stmt.num_result_cols()? == 0 {
        Ok(None)
    } else {
        Ok(Some(CursorImpl::new(statement)))
    }
}

/// Shared implementation for executing a columns query between [`Connection`] and
/// [`Preallocated`].
pub fn columns<'o, S>(
    mut statement: S,
    catalog_name: &U16Str,
    schema_name: &U16Str,
    table_name: &U16Str,
    column_name: &U16Str,
) -> Result<Option<CursorImpl<'o, S>>, Error>
where
    S: BorrowMut<StatementImpl<'o>>,
{
    let stmt = statement.borrow_mut();

    stmt.columns(catalog_name, schema_name, table_name, column_name)?;

    // Check if a result set has been created.
    if stmt.num_result_cols()? == 0 {
        Ok(None)
    } else {
        Ok(Some(CursorImpl::new(statement)))
    }
}
