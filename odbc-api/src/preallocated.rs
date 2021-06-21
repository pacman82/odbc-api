use widestring::{U16Str, U16String};

use crate::{
    execute::{columns, execute_with_parameters},
    handles::StatementImpl,
    CursorImpl, Error, ParameterCollection,
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
pub struct Preallocated<'open_connection> {
    statement: StatementImpl<'open_connection>,
}

impl<'o> Preallocated<'o> {
    pub(crate) fn new(statement: StatementImpl<'o>) -> Self {
        Self { statement }
    }

    /// Executes an sql statement using a wide string. See [`Self::execute`].
    pub fn execute_utf16(
        &mut self,
        query: &U16Str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut StatementImpl<'o>>>, Error> {
        execute_with_parameters(move || Ok(&mut self.statement), Some(query), params)
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
    /// of it and instead burrow it.
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
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'o, &mut StatementImpl<'o>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
    }

    /// Transfer ownership to the underlying statement handle.
    ///
    /// The resulting type is one level of indirection away from the raw pointer of the ODBC API. It
    /// no longer has any guarantees about bound buffers, but is still guaranteed to be a valid
    /// allocated statement handle. This serves together with
    /// [`crate::handles::StatementImpl::into_sys`] or [`crate::handles::StatementImpl::as_sys`]
    /// this serves as an escape hatch to access the functionality provided by `crate::sys` not yet
    /// accessible through safe abstractions.
    pub fn into_statement(self) -> StatementImpl<'o> {
        self.statement
    }

    /// Query all columns that match the provided catalog name, schema pattern, table pattern, and
    /// column pattern.
    pub fn columns(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<Option<CursorImpl<'o, &mut StatementImpl<'o>>>, Error> {
        columns(
            &mut self.statement,
            &U16String::from_str(catalog_name),
            &U16String::from_str(schema_name),
            &U16String::from_str(table_name),
            &U16String::from_str(column_name),
        )
    }
}
