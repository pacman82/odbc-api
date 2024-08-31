use std::iter::repeat;

use odbc_api::{
    buffers, environment,
    handles::{CDataMut, Statement, StatementRef},
    Connection, ConnectionOptions, Cursor, Error, RowSetBuffer, TruncationInfo,
};

pub struct Given<'a> {
    table_name: &'a str,
    column_types: &'a [&'a str],
    column_names: &'a [&'a str],
    /// String representation of values to be inserted into table in column wise order.
    values: &'a [&'a [Option<&'a str>]],
}

impl<'a> Given<'a> {
    pub fn new(table_name: &'a str) -> Self {
        Given {
            table_name,
            column_types: &[],
            column_names: &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"],
            values: &[],
        }
    }

    pub fn column_types(&mut self, column_types: &'a [&'a str]) -> &mut Self {
        self.column_types = column_types;
        self
    }

    pub fn column_names(&mut self, column_names: &'a [&'a str]) -> &mut Self {
        self.column_names = column_names;
        self
    }

    /// String representation of values to be filled into test table in column wise order.
    pub fn values_by_column(&mut self, values: &'a [&'a [Option<&'a str>]]) -> &mut Self {
        self.values = values;
        self
    }

    pub fn build(
        &self,
        profile: &Profile,
    ) -> Result<(Connection<'static>, Table<'a>), odbc_api::Error> {
        let (conn, table) =
            profile.create_table(self.table_name, self.column_types, self.column_names)?;
        if !self.values.is_empty() {
            let num_rows = self.values[0].len();
            let max_str_len = self.values.iter().map(|vals| {
                vals.iter()
                    .map(|text| text.unwrap_or("").len())
                    .max()
                    .expect("Columns may not be empty")
            });
            let mut inserter = conn
                .prepare(&table.sql_insert())?
                .into_text_inserter(num_rows, max_str_len)?;
            inserter.set_num_rows(num_rows);
            for r in 0..num_rows {
                for c in 0..self.values.len() {
                    inserter
                        .column_mut(c)
                        .set_cell(r, self.values[c][r].map(|text| text.as_bytes()))
                }
            }
            inserter.execute()?;
        }
        Ok((conn, table))
    }
}

/// Used to adapt test behaviour to different drivers and datasources
#[derive(Clone, Copy, Debug)]
pub struct Profile {
    /// Connection string used to connect with the data source
    pub connection_string: &'static str,
    /// Type of the identity autoincrementing column, used to index the test tables.
    pub index_type: &'static str,
    pub blob_type: &'static str,
}

impl Profile {
    /// Open a new connection using the connection string of the profile
    pub fn connection(&self) -> Result<Connection<'static>, Error> {
        environment()?
            .connect_with_connection_string(self.connection_string, ConnectionOptions::default())
    }

    // #[deprecated]
    /// Convenience function, setting up an empty table, and returning the connection used to create
    /// it.
    pub fn setup_empty_table(
        &self,
        table_name: &str,
        column_types: &[&str],
    ) -> Result<Connection<'static>, odbc_api::Error> {
        let (conn, _table) = self.create_table(
            table_name,
            column_types,
            &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"],
        )?;
        Ok(conn)
    }

    /// Convenience function, setting up an empty table, and returning the connection used to create
    /// it.
    pub fn create_table<'a>(
        &self,
        table_name: &'a str,
        column_types: &'a [&'a str],
        column_names: &'a [&'a str],
    ) -> Result<(Connection<'static>, Table<'a>), odbc_api::Error> {
        let conn = self.connection()?;
        let table = Table::new(table_name, column_types, column_names);
        conn.execute(&table.sql_drop_if_exists(), ())?;
        conn.execute(&table.sql_create_table(self.index_type), ())?;
        Ok((conn, table))
    }
}

/// Declarative description for a table to conveniently build queries for it
pub struct Table<'a> {
    pub name: &'a str,
    pub column_types: &'a [&'a str],
    pub column_names: &'a [&'a str],
}

impl<'a> Table<'a> {
    pub fn new(name: &'a str, column_types: &'a [&'a str], column_names: &'a [&'a str]) -> Self {
        Table {
            name,
            column_types,
            column_names: &column_names[..column_types.len()],
        }
    }

    /// SQL statement text dropping the table, if it exists
    pub fn sql_drop_if_exists(&self) -> String {
        format!("DROP TABLE IF EXISTS {};", self.name)
    }

    /// SQL statement text creating the table
    pub fn sql_create_table(&self, index_type: &str) -> String {
        let cols = self
            .column_types
            .iter()
            .zip(self.column_names)
            .map(|(ty, name)| format!("{name} {ty}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE {} (id {index_type},{cols});", self.name)
    }

    /// Select all columns but the Id column. Results ordered by id.
    pub fn sql_all_ordered_by_id(&self) -> String {
        let cols = self.column_names.join(",");
        format!("SELECT {cols} FROM {} ORDER BY Id;", self.name)
    }

    /// Parameterized insert statement
    pub fn sql_insert(&self) -> String {
        let cols = self.column_names.join(",");
        let placeholders = repeat("?")
            .take(self.column_names.len())
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "INSERT INTO {} ({cols}) VALUES ({placeholders});",
            self.name
        )
    }

    /// Queries all the tables fields and concatinates them to a single string. Rows separated by
    /// `\n` and fields separated by `,`.
    pub fn content_as_string(&self, conn: &Connection<'_>) -> String {
        let cursor = conn
            .execute(&self.sql_all_ordered_by_id(), ())
            .unwrap()
            .unwrap();
        cursor_to_string(cursor)
    }
}

pub fn cursor_to_string(mut cursor: impl Cursor) -> String {
    let batch_size = 20;
    let mut buffer = buffers::TextRowSet::for_cursor(batch_size, &mut cursor, Some(8192)).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let mut text = String::new();

    let mut first_batch = true;

    while let Some(row_set) = row_set_cursor.fetch().unwrap() {
        if first_batch {
            first_batch = false;
        } else {
            text.push('\n');
        }
        for row_index in 0..row_set.num_rows() {
            if row_index != 0 {
                text.push('\n');
            }
            for col_index in 0..row_set.num_cols() {
                if col_index != 0 {
                    text.push(',');
                }
                text.push_str(
                    row_set
                        .at_as_str(col_index, row_index)
                        .unwrap()
                        .unwrap_or("NULL"),
                );
            }
        }
    }

    text
}

/// A generic implementation of RowSetBuffer for a single column
pub struct SingleColumnRowSetBuffer<C> {
    num_rows_fetched: Box<usize>,
    batch_size: usize,
    /// invariant column.len() == batch_size
    column: C,
}

impl<T> SingleColumnRowSetBuffer<Vec<T>>
where
    T: Clone + Default,
{
    pub fn new(batch_size: usize) -> Self {
        SingleColumnRowSetBuffer {
            num_rows_fetched: Box::new(0),
            batch_size,
            column: vec![T::default(); batch_size],
        }
    }

    pub fn get(&self) -> &[T] {
        &self.column[0..*self.num_rows_fetched]
    }
}

unsafe impl<C> RowSetBuffer for SingleColumnRowSetBuffer<C>
where
    C: CDataMut,
{
    fn bind_type(&self) -> usize {
        0 // Columnar binding
    }

    fn row_array_size(&self) -> usize {
        self.batch_size
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows_fetched.as_mut()
    }

    unsafe fn bind_colmuns_to_cursor(
        &mut self,
        mut cursor: StatementRef<'_>,
    ) -> Result<(), odbc_api::Error> {
        cursor.bind_col(1, &mut self.column).into_result(&cursor)?;
        Ok(())
    }

    fn find_truncation(&self) -> Option<TruncationInfo> {
        unimplemented!()
    }
}
