use crate::{
    execute::{
        execute_columns, execute_foreign_keys, execute_tables, execute_with_parameters,
        execute_with_parameters_polling,
    },
    handles::{AsStatementRef, SqlText, Statement, StatementImpl, StatementRef},
    CursorImpl, CursorPolling, Error, ParameterCollectionRef, Sleep,
};

/// A preallocated SQL statement handle intended for sequential execution of different queries. See
/// [`crate::Connection::preallocate`].
///
/// # Example
///
/// ```
/// use odbc_api::{Connection, Error};
/// use std::io::{self, stdin, Read};
///
/// fn interactive(conn: &Connection<'_>) -> io::Result<()>{
///     let mut statement = conn.preallocate().unwrap();
///     let mut query = String::new();
///     stdin().read_line(&mut query)?;
///     while !query.is_empty() {
///         match statement.execute(&query, ()) {
///             Err(e) => println!("{}", e),
///             Ok(None) => println!("No results set generated."),
///             Ok(Some(cursor)) => {
///                 // ...print cursor contents...
///             },
///         }
///         stdin().read_line(&mut query)?;
///     }
///     Ok(())
/// }
/// ```
pub struct Preallocated<'open_connection> {
    /// A valid statement handle.
    statement: StatementImpl<'open_connection>,
}

impl<'o> Preallocated<'o> {
    /// Users which intend to write their application in safe Rust should prefer using
    /// [`crate::Connection::preallocate`] as opposed to this constructor.
    ///
    /// # Safety
    ///
    /// `statement` must be an allocated handled with no pointers bound for either results or
    /// arguments. The statement must not be prepared, but in the state of a "freshly" allocated
    /// handle.
    pub unsafe fn new(statement: StatementImpl<'o>) -> Self {
        Self { statement }
    }

    /// Executes a statement. This is the fastest way to sequentially execute different SQL
    /// Statements.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. Check the [`crate::parameter`] module level documentation for
    ///   more information on how to pass parameters.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows. Since we want to reuse the statement handle a returned cursor will not take ownership
    /// of it and instead borrow it.
    ///
    /// # Example
    ///
    /// ```
    /// use odbc_api::{Connection, Error};
    /// use std::io::{self, stdin, Read};
    ///
    /// fn interactive(conn: &Connection) -> io::Result<()>{
    ///     let mut statement = conn.preallocate().unwrap();
    ///     let mut query = String::new();
    ///     stdin().read_line(&mut query)?;
    ///     while !query.is_empty() {
    ///         match statement.execute(&query, ()) {
    ///             Err(e) => println!("{}", e),
    ///             Ok(None) => println!("No results set generated."),
    ///             Ok(Some(cursor)) => {
    ///                 // ...print cursor contents...
    ///             },
    ///         }
    ///         stdin().read_line(&mut query)?;
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn execute(
        &mut self,
        query: &str,
        params: impl ParameterCollectionRef,
    ) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        let query = SqlText::new(query);
        execute_with_parameters(move || Ok(&mut self.statement), Some(&query), params)
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

    /// List tables, schemas, views and catalogs of a datasource.
    ///
    /// # Parameters
    ///
    /// * `catalog_name`: Filter result by catalog name. Accept search patterns. Use `%` to match
    ///   any number of characters. Use `_` to match exactly on character. Use `\` to escape
    ///   characeters.
    /// * `schema_name`: Filter result by schema. Accepts patterns in the same way as
    ///   `catalog_name`.
    /// * `table_name`: Filter result by table. Accepts patterns in the same way as `catalog_name`.
    /// * `table_type`: Filters results by table type. E.g: 'TABLE', 'VIEW'. This argument accepts a
    ///   comma separeted list of table types. Omit it to not filter the result by table type at
    ///   all.
    pub fn tables(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_type: &str,
    ) -> Result<CursorImpl<&mut StatementImpl<'o>>, Error> {
        execute_tables(
            &mut self.statement,
            &SqlText::new(catalog_name),
            &SqlText::new(schema_name),
            &SqlText::new(table_name),
            &SqlText::new(table_type),
        )
    }

    /// A cursor describing columns of all tables matching the patterns. Patterns support as
    /// placeholder `%` for multiple characters or `_` for a single character. Use `\` to escape.The
    /// returned cursor has the columns:
    /// `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `COLUMN_NAME`, `DATA_TYPE`, `TYPE_NAME`,
    /// `COLUMN_SIZE`, `BUFFER_LENGTH`, `DECIMAL_DIGITS`, `NUM_PREC_RADIX`, `NULLABLE`,
    /// `REMARKS`, `COLUMN_DEF`, `SQL_DATA_TYPE`, `SQL_DATETIME_SUB`, `CHAR_OCTET_LENGTH`,
    /// `ORDINAL_POSITION`, `IS_NULLABLE`.
    ///
    /// In addition to that there may be a number of columns specific to the data source.
    pub fn columns(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<CursorImpl<&mut StatementImpl<'o>>, Error> {
        execute_columns(
            &mut self.statement,
            &SqlText::new(catalog_name),
            &SqlText::new(schema_name),
            &SqlText::new(table_name),
            &SqlText::new(column_name),
        )
    }

    /// This can be used to retrieve either a list of foreign keys in the specified table or a list
    /// of foreign keys in other table that refer to the primary key of the specified table.
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlforeignkeys-function>
    pub fn foreign_keys(
        &mut self,
        pk_catalog_name: &str,
        pk_schema_name: &str,
        pk_table_name: &str,
        fk_catalog_name: &str,
        fk_schema_name: &str,
        fk_table_name: &str,
    ) -> Result<CursorImpl<&mut StatementImpl<'o>>, Error> {
        execute_foreign_keys(
            &mut self.statement,
            &SqlText::new(pk_catalog_name),
            &SqlText::new(pk_schema_name),
            &SqlText::new(pk_table_name),
            &SqlText::new(fk_catalog_name),
            &SqlText::new(fk_schema_name),
            &SqlText::new(fk_table_name),
        )
    }

    /// Number of rows affected by the last `INSERT`, `UPDATE` or `DELETE` statment. May return
    /// `None` if row count is not available. Some drivers may also allow to use this to determine
    /// how many rows have been fetched using `SELECT`. Most drivers however only know how many rows
    /// have been fetched after they have been fetched.
    ///
    /// ```
    /// use odbc_api::{Connection, Error};
    ///
    /// /// Make everyone rich and return how many colleagues are happy now.
    /// fn raise_minimum_salary(
    ///     conn: &Connection<'_>,
    ///     new_min_salary: i32
    /// ) -> Result<usize, Error> {
    ///     // We won't use conn.execute directly, because we need a handle to ask about the number
    ///     // of changed rows. So let's allocate the statement explicitly.
    ///     let mut stmt = conn.preallocate()?;
    ///     stmt.execute(
    ///         "UPDATE Employees SET salary = ? WHERE salary < ?",
    ///         (&new_min_salary, &new_min_salary),
    ///     )?;
    ///     let number_of_updated_rows = stmt
    ///         .row_count()?
    ///         .expect("For UPDATE statements row count must always be available.");
    ///     Ok(number_of_updated_rows)
    /// }
    /// ```
    pub fn row_count(&mut self) -> Result<Option<usize>, Error> {
        self.statement
            .row_count()
            .into_result(&self.statement)
            .map(|count| {
                // ODBC returns -1 in case a row count is not available
                if count == -1 {
                    None
                } else {
                    Some(count.try_into().unwrap())
                }
            })
    }

    /// Call this method to enable asynchronous polling mode on the statement
    pub fn into_polling(mut self) -> Result<PreallocatedPolling<'o>, Error> {
        self.statement
            .set_async_enable(true)
            .into_result(&self.statement)?;
        Ok(PreallocatedPolling::new(self.statement))
    }
}

impl AsStatementRef for Preallocated<'_> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}

/// Asynchronous sibling of [`Preallocated`] using polling mode for execution. Can be obtained using
/// [`Preallocated::into_polling`].
pub struct PreallocatedPolling<'open_connection> {
    /// A valid statement handle in polling mode
    statement: StatementImpl<'open_connection>,
}

impl<'o> PreallocatedPolling<'o> {
    fn new(statement: StatementImpl<'o>) -> Self {
        Self { statement }
    }

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
    ) -> Result<Option<CursorPolling<&mut StatementImpl<'o>>>, Error> {
        let query = SqlText::new(query);
        execute_with_parameters_polling(
            move || Ok(&mut self.statement),
            Some(&query),
            params,
            sleep,
        )
        .await
    }
}

impl AsStatementRef for PreallocatedPolling<'_> {
    fn as_stmt_ref(&mut self) -> StatementRef<'_> {
        self.statement.as_stmt_ref()
    }
}
