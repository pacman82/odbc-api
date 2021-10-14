use widestring::{U16Str, U16String};

use crate::{CursorImpl, Error, ParameterCollection, execute::{execute_columns, execute_tables, execute_with_parameters}, handles::StatementImpl};

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
    ) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
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
    ) -> Result<Option<CursorImpl<&mut StatementImpl<'o>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
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
        catalog_name: Option<&str>,
        schema_name: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&str>,
    ) -> Result<CursorImpl<&mut  StatementImpl<'o>>, Error> {

        let catalog_name = catalog_name.map(|s| U16String::from_str(s));
        let schema_name = schema_name.map(|s| U16String::from_str(s));
        let table_name = table_name.map(|s| U16String::from_str(s));
        let table_type = table_type.map(|s| U16String::from_str(s));

        execute_tables(
            &mut self.statement,
            catalog_name.as_deref(),
            schema_name.as_deref(),
            table_name.as_deref(),
            table_type.as_deref(),
        )
    }

    /// Query all columns that match the provided catalog name, schema pattern, table pattern, and
    /// column pattern.
    pub fn columns(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<CursorImpl<&mut StatementImpl<'o>>, Error> {
        execute_columns(
            &mut self.statement,
            &U16String::from_str(catalog_name),
            &U16String::from_str(schema_name),
            &U16String::from_str(table_name),
            &U16String::from_str(column_name),
        )
    }
}
