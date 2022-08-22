use std::iter::repeat;

use lazy_static::lazy_static;
use odbc_api::{
    buffers::{self, TextColumn},
    handles::{CDataMut, Statement},
    Connection, Cursor, Environment, Error, RowSetBuffer, U16Str,
};

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    pub static ref ENV: Environment = {
        let _ = env_logger::builder().is_test(true).try_init();
        Environment::new().unwrap()
    };
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
        ENV.connect_with_connection_string(self.connection_string)
    }

    /// Convinience function, setting up an empty table, and returning the connection used to create
    /// it.
    pub fn setup_empty_table(
        &self,
        table_name: &str,
        column_types: &[&str],
    ) -> Result<Connection<'static>, odbc_api::Error> {
        let conn = self.connection()?;
        let table = Table::new(table_name, column_types);
        conn.execute(&table.sql_drop_if_exists(), ())?;
        conn.execute(&table.sql_create_table(self.index_type), ())?;
        Ok(conn)
    }

    /// Convinience function, setting up an empty table, and returning the connection used to create
    /// it.
    pub fn given<'a>(
        &self,
        table_name: &'a str,
        column_types: &'a [&'a str],
    ) -> Result<(Connection<'static>, Table<'a>), odbc_api::Error> {
        let conn = self.connection()?;
        let table = Table::new(table_name, column_types);
        conn.execute(&table.sql_drop_if_exists(), ())?;
        conn.execute(&table.sql_create_table(self.index_type), ())?;
        Ok((conn, table))
    }
}

/// Declarative description for a table to conviniently build queries for it
pub struct Table<'a> {
    pub name: &'a str,
    pub column_types: &'a [&'a str],
    pub column_names: &'a [&'a str],
}

impl<'a> Table<'a> {
    pub fn new(name: &'a str, column_types: &'a [&'a str]) -> Self {
        let column_names = &["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"];
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
            .map(|(ty, name)| format!("{} {}", name, ty))
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

impl SingleColumnRowSetBuffer<TextColumn<u16>> {
    pub fn with_wide_text_column(batch_size: usize, max_str_len: usize) -> Self {
        Self {
            num_rows_fetched: Box::new(0),
            batch_size,
            column: TextColumn::try_new(batch_size as usize, max_str_len).unwrap(),
        }
    }

    pub fn ustr_at(&self, index: usize) -> Option<&U16Str> {
        if index >= *self.num_rows_fetched {
            panic!("Out of bounds access. In SingleColumnRowSetBuffer")
        }

        // Safe due to out of bounds check above
        unsafe { self.column.ustr_at(index) }
    }
}

impl SingleColumnRowSetBuffer<TextColumn<u8>> {
    pub fn with_text_column(batch_size: usize, max_str_len: usize) -> Self {
        Self {
            num_rows_fetched: Box::new(0),
            batch_size,
            column: TextColumn::try_new(batch_size as usize, max_str_len).unwrap(),
        }
    }

    pub fn value_at(&self, index: usize) -> Option<&[u8]> {
        if index >= *self.num_rows_fetched {
            panic!("Out of bounds access. In SingleColumnRowSetBuffer")
        }

        // Safe due to out of bounds check above
        self.column.value_at(index)
    }
}

impl<T> SingleColumnRowSetBuffer<Vec<T>>
where
    T: Clone + Default,
{
    pub fn new(batch_size: usize) -> Self {
        SingleColumnRowSetBuffer {
            num_rows_fetched: Box::new(0),
            batch_size,
            column: vec![T::default(); batch_size as usize],
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

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), odbc_api::Error> {
        cursor
            .as_stmt_ref()
            .bind_col(1, &mut self.column)
            .into_result(&cursor.as_stmt_ref())?;
        Ok(())
    }
}
