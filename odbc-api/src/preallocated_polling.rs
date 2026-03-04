use crate::{
    CursorPolling, Error, ParameterCollectionRef, Sleep,
    execute::execute_with_parameters_polling,
    handles::{AsStatementRef, SqlText, StatementRef},
};

/// Asynchronous sibling of [`crate::Preallocated`] using polling mode for execution. Can be
/// obtained using [`crate::Preallocated::into_polling`].
pub struct PreallocatedPolling<S> {
    /// A valid statement handle in polling mode
    statement: S,
}

impl<S> PreallocatedPolling<S> {
    pub(crate) fn new(statement: S) -> Self {
        Self { statement }
    }
}

impl<S> PreallocatedPolling<S>
where
    S: AsStatementRef,
{
    /// Executes a statement. This is the fastest way to sequentially execute different SQL
    /// Statements asynchronously.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. Check the [`crate::parameter`] module level documentation for
    ///   more information on how to pass parameters.
    /// * `sleep`: Governs the polling intervals
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows. Since we want to reuse the statement handle a returned cursor will not take ownership
    /// of it and instead burrow it.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{Connection, Error};
    /// use std::{io::{self, stdin, Read}, time::Duration};
    ///
    /// /// Execute many different queries sequentially.
    /// async fn execute_all(conn: &Connection<'_>, queries: &[&str]) -> Result<(), Error>{
    ///     let mut statement = conn.preallocate()?.into_polling()?;
    ///     let sleep = || tokio::time::sleep(Duration::from_millis(20));
    ///     for query in queries {
    ///         println!("Executing {query}");
    ///         match statement.execute(&query, (), sleep).await {
    ///             Err(e) => println!("{}", e),
    ///             Ok(None) => println!("No results set generated."),
    ///             Ok(Some(cursor)) => {
    ///                 // ...print cursor contents...
    ///             },
    ///         }
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub async fn execute(
        &mut self,
        query: &str,
        params: impl ParameterCollectionRef,
        sleep: impl Sleep,
    ) -> Result<Option<CursorPolling<StatementRef<'_>>>, Error> {
        let query = SqlText::new(query);
        let stmt = self.statement.as_stmt_ref();
        execute_with_parameters_polling(stmt, Some(&query), params, sleep).await
    }
}

impl<S> AsStatementRef for PreallocatedPolling<S>
where
    S: AsStatementRef,
{
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}
