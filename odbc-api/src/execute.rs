use std::intrinsics::transmute;

use crate::{
    handles::{AsStatementRef, SqlText, Statement},
    parameter::Blob,
    sleep::wait_for,
    CursorImpl, CursorPolling, Error, ParameterCollectionRef, Sleep,
};

/// Shared implementation for executing a query with parameters between [`crate::Connection`],
/// [`crate::Preallocated`] and [`crate::Prepared`].
///
/// # Parameters
///
/// * `lazy_statement`: Factory for statement handle used to execute the query. We pass the
///   pass the statement lazily in order to avoid unnecessary allocating a statement handle in case
///   the parameter set is empty.
/// * `query`: SQL query to be executed. If `None` it is a assumed a prepared query is to be
///   executed.
/// * `params`: The parameters bound to the statement before query execution.
pub fn execute_with_parameters<S>(
    lazy_statement: impl FnOnce() -> Result<S, Error>,
    query: Option<&SqlText<'_>>,
    params: impl ParameterCollectionRef,
) -> Result<Option<CursorImpl<S>>, Error>
where
    S: AsStatementRef,
{
    unsafe {
        if let Some(statement) = bind_parameters(lazy_statement, params)? {
            execute(statement, query)
        } else {
            Ok(None)
        }
    }
}

/// Asynchronous sibiling of [`execute_with_parameters`]
pub async fn execute_with_parameters_polling<S>(
    lazy_statement: impl FnOnce() -> Result<S, Error>,
    query: Option<&SqlText<'_>>,
    params: impl ParameterCollectionRef,
    sleep: impl Sleep,
) -> Result<Option<CursorPolling<S>>, Error>
where
    S: AsStatementRef,
{
    unsafe {
        if let Some(statement) = bind_parameters(lazy_statement, params)? {
            execute_polling(statement, query, sleep).await
        } else {
            Ok(None)
        }
    }
}

unsafe fn bind_parameters<S>(
    lazy_statement: impl FnOnce() -> Result<S, Error>,
    mut params: impl ParameterCollectionRef,
) -> Result<Option<S>, Error>
where
    S: AsStatementRef,
{
    let parameter_set_size = params.parameter_set_size();
    if parameter_set_size == 0 {
        return Ok(None);
    }

    // Only allocate the statement, if we know we are going to execute something.
    let mut statement = lazy_statement()?;
    let mut stmt = statement.as_stmt_ref();
    // Reset parameters so we do not dereference stale once by mistake if we call
    // `exec_direct`.
    stmt.reset_parameters().into_result(&stmt)?;
    stmt.set_paramset_size(parameter_set_size)
        .into_result(&stmt)?;
    // Bind new parameters passed by caller.
    params.bind_parameters_to(&mut stmt)?;
    Ok(Some(statement))
}

/// # Safety
///
/// * Execute may dereference pointers to bound parameters, so these must guaranteed to be valid
///   then calling this function.
/// * Furthermore all bound delayed parameters must be of type `*mut &mut dyn Blob`.
pub unsafe fn execute<S>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
) -> Result<Option<CursorImpl<S>>, Error>
where
    S: AsStatementRef,
{
    let mut stmt = statement.as_stmt_ref();
    let result = if let Some(sql) = query {
        // We execute an unprepared "one shot query"
        stmt.exec_direct(sql)
    } else {
        // We execute a prepared query
        stmt.execute()
    };

    // If delayed parameters (e.g. input streams) are bound we might need to put data in order to
    // execute.
    let need_data = result
        .on_success(|| false)
        .into_result_with(&stmt, Some(false), Some(true))?;

    if need_data {
        // Check if any delayed parameters have been bound which stream data to the database at
        // statement execution time. Loops over each bound stream.
        while let Some(blob_ptr) = stmt.param_data().into_result(&stmt)? {
            // The safe interfaces currently exclusively bind pointers to `Blob` trait objects
            let blob_ptr: *mut &mut dyn Blob = transmute(blob_ptr);
            let blob_ref = &mut *blob_ptr;
            // Loop over all batches within each blob
            while let Some(batch) = blob_ref.next_batch().map_err(Error::FailedReadingInput)? {
                stmt.put_binary_batch(batch).into_result(&stmt)?;
            }
        }
    }

    // Check if a result set has been created.
    if stmt.num_result_cols().into_result(&stmt)? == 0 {
        Ok(None)
    } else {
        // Safe: `statement` is in cursor state.
        let cursor = CursorImpl::new(statement);
        Ok(Some(cursor))
    }
}

/// # Safety
///
/// * Execute may dereference pointers to bound parameters, so these must guaranteed to be valid
///   then calling this function.
/// * Furthermore all bound delayed parameters must be of type `*mut &mut dyn Blob`.
pub async unsafe fn execute_polling<S>(
    mut statement: S,
    query: Option<&SqlText<'_>>,
    mut sleep: impl Sleep,
) -> Result<Option<CursorPolling<S>>, Error>
where
    S: AsStatementRef,
{
    let mut stmt = statement.as_stmt_ref();
    let result = if let Some(sql) = query {
        // We execute an unprepared "one shot query"
        wait_for(|| stmt.exec_direct(sql), &mut sleep).await
    } else {
        // We execute a prepared query
        wait_for(|| stmt.execute(), &mut sleep).await
    };

    // If delayed parameters (e.g. input streams) are bound we might need to put data in order to
    // execute.
    let need_data = result
        .on_success(|| false)
        .into_result_with(&stmt, Some(false), Some(true))?;

    if need_data {
        // Check if any delayed parameters have been bound which stream data to the database at
        // statement execution time. Loops over each bound stream.
        while let Some(blob_ptr) = stmt.param_data().into_result(&stmt)? {
            // The safe interfaces currently exclusively bind pointers to `Blob` trait objects
            let blob_ptr: *mut &mut dyn Blob = transmute(blob_ptr);
            let blob_ref = &mut *blob_ptr;
            // Loop over all batches within each blob
            while let Some(batch) = blob_ref.next_batch().map_err(Error::FailedReadingInput)? {
                let result = wait_for(|| stmt.put_binary_batch(batch), &mut sleep).await;
                result.into_result(&stmt)?;
            }
        }
    }

    // Check if a result set has been created.
    let num_result_cols = wait_for(|| stmt.num_result_cols(), &mut sleep)
        .await
        .into_result(&stmt)?;
    if num_result_cols == 0 {
        Ok(None)
    } else {
        // Safe: `statement` is in cursor state.
        let cursor = CursorPolling::new(statement);
        Ok(Some(cursor))
    }
}

/// Shared implementation for executing a columns query between [`crate::Connection`] and
/// [`crate::Preallocated`].
pub fn execute_columns<S>(
    mut statement: S,
    catalog_name: &SqlText,
    schema_name: &SqlText,
    table_name: &SqlText,
    column_name: &SqlText,
) -> Result<CursorImpl<S>, Error>
where
    S: AsStatementRef,
{
    let mut stmt = statement.as_stmt_ref();

    stmt.columns(catalog_name, schema_name, table_name, column_name)
        .into_result(&stmt)?;

    // We assume columns always creates a result set, since it works like a SELECT statement.
    debug_assert_ne!(stmt.num_result_cols().unwrap(), 0);

    // Safe: `statement` is in cursor state
    let cursor = unsafe { CursorImpl::new(statement) };
    Ok(cursor)
}

/// Shared implementation for executing a tables query between [`crate::Connection`] and
/// [`crate::Preallocated`].
pub fn execute_tables<S>(
    mut statement: S,
    catalog_name: &SqlText,
    schema_name: &SqlText,
    table_name: &SqlText,
    column_name: &SqlText,
) -> Result<CursorImpl<S>, Error>
where
    S: AsStatementRef,
{
    let mut stmt = statement.as_stmt_ref();

    stmt.tables(catalog_name, schema_name, table_name, column_name)
        .into_result(&stmt)?;

    // We assume tables always creates a result set, since it works like a SELECT statement.
    debug_assert_ne!(stmt.num_result_cols().unwrap(), 0);

    // Safe: `statement` is in Cursor state.
    let cursor = unsafe { CursorImpl::new(statement) };

    Ok(cursor)
}

/// Shared implementation for executing a foreign keys query between [`crate::Connection`] and
/// [`crate::Preallocated`].
pub fn execute_foreign_keys<S>(
    mut statement: S,
    pk_catalog_name: &SqlText,
    pk_schema_name: &SqlText,
    pk_table_name: &SqlText,
    fk_catalog_name: &SqlText,
    fk_schema_name: &SqlText,
    fk_table_name: &SqlText,
) -> Result<CursorImpl<S>, Error>
where
    S: AsStatementRef,
{
    let mut stmt = statement.as_stmt_ref();

    stmt.foreign_keys(
        pk_catalog_name,
        pk_schema_name,
        pk_table_name,
        fk_catalog_name,
        fk_schema_name,
        fk_table_name,
    )
    .into_result(&stmt)?;

    // We assume foreign keys always creates a result set, since it works like a SELECT statement.
    debug_assert_ne!(stmt.num_result_cols().unwrap(), 0);

    // Safe: `statement` is in Cursor state.
    let cursor = unsafe { CursorImpl::new(statement) };

    Ok(cursor)
}
