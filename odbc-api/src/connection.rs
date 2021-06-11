use crate::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarRowSet, TextColumnIt},
    execute::execute_with_parameters,
    handles::{self, State, Statement, StatementImpl},
    parameter_collection::ParameterCollection,
    Cursor, CursorImpl, DataType, Error, ExtendedColumnDescription, Nullability, Preallocated,
    Prepared,
};
use odbc_sys::SqlDataType;
use std::{array::IntoIter, borrow::Cow, str, thread::panicking};
use widestring::{U16Str, U16String};

impl<'conn> Drop for Connection<'conn> {
    fn drop(&mut self) {
        match self.connection.disconnect() {
            Ok(()) => (),
            Err(Error::Diagnostics(record)) if record.state == State::INVALID_STATE_TRANSACTION => {
                // Invalid transaction state. Let's rollback the current transaction and try again.
                if let Err(e) = self.connection.rollback() {
                    // Avoid panicking, if we already have a panic. We don't want to mask the original
                    // error.
                    if !panicking() {
                        panic!(
                            "Unexpected error rolling back transaction (In order to recover \
                                from invalid transaction state during disconnect): {:?}",
                            e
                        )
                    }
                }
                // Transaction is rolled back. Now let's try again to disconnect.
                if let Err(e) = self.connection.disconnect() {
                    // Avoid panicking, if we already have a panic. We don't want to mask the original
                    // error.
                    if !panicking() {
                        panic!("Unexpected error disconnecting): {:?}", e)
                    }
                }
            }
            Err(e) => {
                // Avoid panicking, if we already have a panic. We don't want to mask the original
                // error.
                if !panicking() {
                    panic!("Unexpected error disconnecting: {:?}", e)
                }
            }
        }
    }
}

/// The connection handle references storage of all information about the connection to the data
/// source, including status, transaction state, and error information.
pub struct Connection<'c> {
    connection: handles::Connection<'c>,
}

impl<'c> Connection<'c> {
    pub(crate) fn new(connection: handles::Connection<'c>) -> Self {
        Self { connection }
    }

    /// Executes an sql statement using a wide string. See [`Self::execute`].
    pub fn execute_utf16(
        &self,
        query: &U16Str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'_, StatementImpl<'_>>>, Error> {
        let lazy_statement = move || self.connection.allocate_statement();
        execute_with_parameters(lazy_statement, Some(query), params)
    }

    /// Executes an SQL statement. This is the fastest way to submit an SQL statement for one-time
    /// execution.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;".
    /// * `params`: `?` may be used as a placeholder in the statement text. You can use `()` to
    ///   represent no parameters. See the [`crate::parameter`] module level documentation for more
    ///   information on how to pass parameters.
    ///
    /// # Return
    ///
    /// Returns `Some` if a cursor is created. If `None` is returned no cursor has been created (
    /// e.g. the query came back empty). Note that an empty query may also create a cursor with zero
    /// rows.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
    /// // making this call safe.
    /// let env = unsafe {
    ///     Environment::new()?
    /// };
    ///
    /// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
    /// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
    ///     // Use cursor to process query results.  
    /// }
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn execute(
        &self,
        query: &str,
        params: impl ParameterCollection,
    ) -> Result<Option<CursorImpl<'_, StatementImpl<'_>>>, Error> {
        let query = U16String::from_str(query);
        self.execute_utf16(&query, params)
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare_utf16(&self, query: &U16Str) -> Result<Prepared<'_>, Error> {
        let mut stmt = self.connection.allocate_statement()?;
        stmt.prepare(query)?;
        Ok(Prepared::new(stmt))
    }

    /// Prepares an SQL statement. This is recommended for repeated execution of similar queries.
    ///
    /// # Parameters
    ///
    /// * `query`: The text representation of the SQL statement. E.g. "SELECT * FROM my_table;". `?`
    ///   may be used as a placeholder in the statement text, to be replaced with parameters during
    ///   execution.
    pub fn prepare(&self, query: &str) -> Result<Prepared<'_>, Error> {
        let query = U16String::from_str(query);
        self.prepare_utf16(&query)
    }

    /// Allocates an SQL statement handle. This is recommended if you want to sequentially execute
    /// different queries over the same connection, as you avoid the overhead of allocating a
    /// statement handle for each query.
    ///
    /// Should you want to repeatedly execute the same query with different parameters try
    /// [`Self::prepare`] instead.
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
    pub fn preallocate(&self) -> Result<Preallocated<'_>, Error> {
        let stmt = self.connection.allocate_statement()?;
        Ok(Preallocated::new(stmt))
    }

    /// Specify the transaction mode. By default, ODBC transactions are in auto-commit mode.
    /// Switching from manual-commit mode to auto-commit mode automatically commits any open
    /// transaction on the connection. There is no open or begin transaction method. Each statement
    /// execution automatically starts a new transaction or adds to the existing one.
    ///
    /// In manual commit mode you can use [`Connection::commit`] or [`Connection::rollback`]. Keep
    /// in mind, that even `SELECT` statements can open new transactions. This library will rollback
    /// open transactions if a connection goes out of SCOPE. This however will log an error, since
    /// the transaction state is only discovered during a failed disconnect. It is preferable that
    /// the application makes sure all transactions are closed if in manual commit mode.
    pub fn set_autocommit(&self, enabled: bool) -> Result<(), Error> {
        self.connection.set_autocommit(enabled)
    }

    /// To commit a transaction in manual-commit mode.
    pub fn commit(&self) -> Result<(), Error> {
        self.connection.commit()
    }

    /// To rollback a transaction in manual-commit mode.
    pub fn rollback(&self) -> Result<(), Error> {
        self.connection.rollback()
    }

    /// Allows sending this connection to different threads. This Connection will still be only be
    /// used by one thread at a time, but it may be a different thread each time.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::thread;
    /// use lazy_static::lazy_static;
    /// use odbc_api::Environment;
    /// lazy_static! {
    ///     static ref ENV: Environment = unsafe { Environment::new().unwrap() };
    /// }
    /// const MSSQL: &str =
    ///     "Driver={ODBC Driver 17 for SQL Server};\
    ///     Server=localhost;\
    ///     UID=SA;\
    ///     PWD=<YourStrong@Passw0rd>;\
    /// ";
    ///
    /// let conn = ENV.connect_with_connection_string("MSSQL").unwrap();
    /// let conn = unsafe { conn.promote_to_send() };
    /// let handle = thread::spawn(move || {
    ///     if let Some(cursor) = conn.execute("SELECT title FROM Movies ORDER BY year",())? {
    ///         // Use cursor to process results
    ///     }
    ///     Ok::<(), odbc_api::Error>(())
    /// });
    /// handle.join().unwrap()?;
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    ///
    /// # Safety
    ///
    /// According to the ODBC standard this should be safe. By calling this function you express your
    /// trust in the implementation of the ODBC driver your application is using.
    ///
    /// See: <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading?view=sql-server-ver15>
    ///
    /// This function may be removed in future versions of this crate and connections would be
    /// `Send` out of the Box. This will require sufficient testing in which a wide variety of
    /// database drivers prove to be thread safe. For now this API tries to error on the side of
    /// caution, and leaves the amount of trust you want to put in the driver implementation to the
    /// user. I have seen this go wrong in the past, but time certainly improved the situation. At
    /// one point this will be cargo cult and Connection can be `Send` by default (hopefully).
    ///
    /// Note to users of `unixodbc`: You may configure the threading level to make unixodbc
    /// synchronize access to the driver (and thereby making them thread safe if they are not thread
    /// safe by themself. This may however hurt your performance if the driver would actually be
    /// able to perform operations in parallel.
    ///
    /// See: <https://stackoverflow.com/questions/4207458/using-unixodbc-in-a-multithreaded-concurrent-setting>
    pub unsafe fn promote_to_send(self) -> force_send_sync::Send<Self> {
        force_send_sync::Send::new(self)
    }

    /// Fetch the name of the database management system used by the connection and store it into
    /// the provided `buf`.
    pub fn fetch_database_management_system_name(&self, buf: &mut Vec<u16>) -> Result<(), Error> {
        self.connection.fetch_database_management_system_name(buf)
    }

    /// Get the name of the database management system used by the connection.
    pub fn database_management_system_name(&self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.fetch_database_management_system_name(&mut buf)?;
        let name = U16String::from_vec(buf);
        Ok(name.to_string().unwrap())
    }

    /// Maximum length of catalog names in bytes.
    pub fn max_catalog_name_len(&self) -> Result<usize, Error> {
        self.connection.max_catalog_name_len().map(|v| v as usize)
    }

    /// Maximum length of schema names in bytes.
    pub fn max_schema_name_len(&self) -> Result<usize, Error> {
        self.connection.max_schema_name_len().map(|v| v as usize)
    }

    /// Maximum length of table names in bytes.
    pub fn max_table_name_len(&self) -> Result<usize, Error> {
        self.connection.max_table_name_len().map(|v| v as usize)
    }

    /// Maximum length of column names in bytes.
    pub fn max_column_name_len(&self) -> Result<usize, Error> {
        self.connection.max_column_name_len().map(|v| v as usize)
    }

    /// Fetch the name of the current catalog being used by the connection and store it into the
    /// provided `buf`.
    pub fn fetch_current_catalog(&self, buf: &mut Vec<u16>) -> Result<(), Error> {
        self.connection.fetch_current_catalog(buf)
    }

    /// Get the name of the current catalog being used by the connection.
    pub fn current_catalog(&self) -> Result<String, Error> {
        let mut buf = Vec::new();
        self.fetch_current_catalog(&mut buf)?;
        let name = U16String::from_vec(buf);
        Ok(name.to_string().unwrap())
    }

    /// Get extended column detail for columns that match the provided catalog name, schema
    /// pattern, table pattern, and column pattern.
    ///
    /// # Parameters
    ///
    /// * `catalog_name`: The name of the catalog. An empty string can be used to indicate tables
    ///   without catalogs.
    /// * `schema_name`: A search pattern for schema names. An empty string can be used to indicate
    ///   tables without schemas.
    /// * `table_name`: A search pattern for table names.
    /// * `column_name`: A search pattern for column names.
    ///
    /// # Return
    ///
    /// Details about each column that matched the search criteria.
    pub fn columns(
        &self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<Vec<ExtendedColumnDescription>, Error> {
        let null_i16 = BufferDescription {
            kind: BufferKind::I16,
            nullable: true,
        };

        let not_null_i16 = BufferDescription {
            kind: BufferKind::I16,
            nullable: false,
        };

        let null_i32 = BufferDescription {
            kind: BufferKind::I32,
            nullable: true,
        };
        // The definitions for these descriptions are taken from the documentation of `SQLColumns`
        // located at https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlcolumns-function
        let catalog_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_catalog_name_len()?,
            },
            nullable: true,
        };

        let schema_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_schema_name_len()?,
            },
            nullable: true,
        };

        let table_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_table_name_len()?,
            },
            nullable: false,
        };

        let column_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: self.max_column_name_len()?,
            },
            nullable: false,
        };

        let data_type_desc = not_null_i16;

        const MAX_EXPECTED_TYPE_NAME_LEN: usize = 255;
        let type_name_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: MAX_EXPECTED_TYPE_NAME_LEN,
            },
            nullable: false,
        };

        let column_size_desc = null_i32;
        let buffer_len_desc = null_i32;
        let decimal_digits_desc = null_i16;
        let precision_radix_desc = null_i16;
        let nullable_desc = not_null_i16;

        const MAX_EXPECTED_REMARKS_LEN: usize = 255;
        let remarks_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: MAX_EXPECTED_REMARKS_LEN,
            },
            nullable: true,
        };

        const MAX_EXPECTED_COLUMN_DEFAULT_LEN: usize = 255;
        let column_default_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: MAX_EXPECTED_COLUMN_DEFAULT_LEN,
            },
            nullable: true,
        };

        let sql_data_type_desc = not_null_i16;
        let sql_datetime_sub_desc = null_i16;
        let char_octet_len_desc = null_i32;
        let ordinal_pos_desc = BufferDescription {
            kind: BufferKind::I32,
            nullable: false,
        };

        const MAX_EXPECTED_IS_NULLABLE_LEN: usize = 10;
        let is_nullable_desc = BufferDescription {
            kind: BufferKind::Text {
                max_str_len: MAX_EXPECTED_IS_NULLABLE_LEN,
            },
            nullable: true,
        };

        let buffer_descs = [
            catalog_name_desc,
            schema_name_desc,
            table_name_desc,
            column_name_desc,
            data_type_desc, // Currently unused
            type_name_desc,
            column_size_desc,
            buffer_len_desc,
            decimal_digits_desc,
            precision_radix_desc,
            nullable_desc,
            remarks_desc,
            column_default_desc,
            sql_data_type_desc,
            sql_datetime_sub_desc, // Currently unused
            char_octet_len_desc,
            ordinal_pos_desc,
            is_nullable_desc, // Currently unused
        ];

        const ROWS_PER_BATCH: u32 = 10;
        let row_set_buffer = ColumnarRowSet::new(ROWS_PER_BATCH, IntoIter::new(buffer_descs));

        let cursor = self.connection.load_columns_detail(
            self.connection.allocate_statement()?,
            &U16String::from_str(catalog_name),
            &U16String::from_str(schema_name),
            &U16String::from_str(table_name),
            &U16String::from_str(column_name),
        )?;

        let mut cursor = cursor.bind_buffer(row_set_buffer)?;

        let mut columns = Vec::new();

        while let Some(batch) = cursor.fetch()? {
            let unexpected_type = "Unexpected column type";
            let expect_text_col = |buffer_index| match batch.column(buffer_index) {
                AnyColumnView::Text(col) => col,
                _ => panic!("{}", unexpected_type),
            };
            let expect_nullable_i32_col = |buffer_index| match batch.column(buffer_index) {
                AnyColumnView::NullableI32(col) => col,
                _ => panic!("{}", unexpected_type),
            };
            let expect_nullable_i16_col = |buffer_index| match batch.column(buffer_index) {
                AnyColumnView::NullableI16(col) => col,
                _ => panic!("{}", unexpected_type),
            };
            let expect_i16_col = |buffer_index| match batch.column(buffer_index) {
                AnyColumnView::I16(col) => col,
                _ => panic!("{}", unexpected_type),
            };

            let mut catalog_names = expect_text_col(0);
            let mut schema_names = expect_text_col(1);
            let mut table_names = expect_text_col(2);
            let mut column_names = expect_text_col(3);
            let mut type_names = expect_text_col(5);
            let mut column_sizes = expect_nullable_i32_col(6);
            let mut buffer_lengths = expect_nullable_i32_col(7);
            let mut decimal_digits = expect_nullable_i16_col(8);
            let mut precision_radices = expect_nullable_i16_col(9);
            let nullables = expect_i16_col(10);
            let mut remarks = expect_text_col(11);
            let mut column_defaults = expect_text_col(12);
            let sql_data_types = expect_i16_col(13);
            let mut char_octet_lengths = expect_nullable_i32_col(15);
            let ordinal_positions = match batch.column(16) {
                AnyColumnView::I32(col) => col,
                _ => panic!("{}", unexpected_type),
            };

            // All iterators should be the same length (`num_rows`), so we can just iterate through
            // each row and `unwrap` each iterator
            for row_idx in 0..batch.num_rows() {
                let next_as_string = |iterator: &mut TextColumnIt<u8>| {
                    iterator
                        .next()
                        .unwrap()
                        .and_then(|r| str::from_utf8(r).ok())
                        .unwrap_or("")
                        .to_string()
                };

                columns.push(ExtendedColumnDescription {
                    catalog_name: next_as_string(&mut catalog_names),
                    schema_name: next_as_string(&mut schema_names),
                    table_name: next_as_string(&mut table_names),
                    column_name: next_as_string(&mut column_names),
                    data_type: DataType::new(
                        SqlDataType(sql_data_types[row_idx]),
                        column_sizes.next().unwrap().copied().unwrap_or(0) as usize,
                        decimal_digits.next().unwrap().copied().unwrap_or(0),
                    ),
                    type_name: next_as_string(&mut type_names),
                    buffer_length: buffer_lengths.next().unwrap().copied(),
                    precision_radix: precision_radices.next().unwrap().copied(),
                    nullability: Nullability::new(odbc_sys::Nullability(nullables[row_idx])),
                    remarks: next_as_string(&mut remarks),
                    column_default: next_as_string(&mut column_defaults),
                    char_octet_length: char_octet_lengths.next().unwrap().copied(),
                    ordinal_position: ordinal_positions[row_idx],
                });
            }
        }

        Ok(columns)
    }
}

/// You can use this method to escape a password so it is suitable to be appended to an ODBC
/// connection string as the value for the `PWD` attribute. This method is only of interest for
/// application in need to create their own connection strings.
///
/// See: <https://stackoverflow.com/questions/22398212/escape-semicolon-in-odbc-connection-string-in-app-config-file>
///
/// # Example
///
/// ```
/// use odbc_api::escape_attribute_value;
///
/// let password = "abc;123}";
/// let user = "SA";
/// let mut connection_string_without_credentials =
///     "Driver={ODBC Driver 17 for SQL Server};Server=localhost;";
///
/// let connection_string = format!(
///     "{}UID={};PWD={};",
///     connection_string_without_credentials,
///     user,
///     escape_attribute_value(password)
/// );
///
/// assert_eq!(
///     "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD={abc;123}}};",
///     connection_string
/// );
/// ```
///
/// ```
/// use odbc_api::escape_attribute_value;
/// assert_eq!("abc", escape_attribute_value("abc"));
/// assert_eq!("ab}c", escape_attribute_value("ab}c"));
/// assert_eq!("{ab;c}", escape_attribute_value("ab;c"));
/// assert_eq!("{a}}b;c}", escape_attribute_value("a}b;c"));
/// ```
pub fn escape_attribute_value(unescaped: &str) -> Cow<'_, str> {
    // Search the string for semicolon (';') if we do not find any, nothing is to do and we can work
    // without an extra allocation.
    if unescaped.contains(';') {
        // Surround the string with curly braces ('{','}') and escape every closing curly brace by
        // repeating it.
        let escaped = unescaped.replace("}", "}}");
        Cow::Owned(format!("{{{}}}", escaped))
    } else {
        Cow::Borrowed(unescaped)
    }
}
