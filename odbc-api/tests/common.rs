use lazy_static::lazy_static;
use odbc_api::{
    buffers,
    buffers::TextColumn,
    handles::{CDataMut, CursorMethods},
    Connection, Cursor, Environment, RowSetBuffer,
};

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    pub static ref ENV: Environment = unsafe { Environment::new().unwrap() };
}

/// Creates the table and assures it is empty. Columns are named a,b,c, etc.
pub fn setup_empty_table(
    conn: &Connection,
    table_name: &str,
    column_types: &[&str],
) -> Result<(), odbc_api::Error> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);

    let column_names = &["a", "b", "c", "d", "e"];
    let cols = column_types
        .iter()
        .zip(column_names)
        .map(|(ty, name)| format!("{} {}", name, ty))
        .collect::<Vec<_>>()
        .join(", ");

    let create_table = format!(
        "CREATE TABLE {} (id int IDENTITY(1,1),{});",
        table_name, cols
    );
    conn.execute(&drop_table, ())?;
    conn.execute(&create_table, ())?;
    Ok(())
}

pub fn cursor_to_string(cursor: impl Cursor) -> String {
    let batch_size = 20;
    let mut buffer = buffers::TextRowSet::for_cursor(batch_size, &cursor).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let mut text = String::new();

    while let Some(row_set) = row_set_cursor.fetch().unwrap() {
        for row_index in 0..row_set.num_rows() {
            if row_index != 0 {
                text.push_str("\n");
            }
            for col_index in 0..row_set.num_cols() {
                if col_index != 0 {
                    text.push_str(",");
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
    batch_size: u32,
    /// invariant column.len() == batch_size
    column: C,
}

impl SingleColumnRowSetBuffer<TextColumn> {
    pub fn with_text_column(batch_size: u32, max_str_len: usize) -> Self {
        Self {
            num_rows_fetched: Box::new(0),
            batch_size,
            column: TextColumn::new(batch_size as usize, max_str_len),
        }
    }

    pub fn value_at(&self, index: usize) -> Option<&[u8]> {
        if index >= *self.num_rows_fetched {
            panic!("Out of bounds access. In SingleColumnRowSetBuffer")
        }

        // Safe due to out of bounds check above
        unsafe { self.column.value_at(index) }
    }
}

impl<T> SingleColumnRowSetBuffer<Vec<T>>
where
    T: Clone + Default,
{
    pub fn new(batch_size: u32) -> Self {
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
    fn bind_type(&self) -> u32 {
        0 // Columnar binding
    }

    fn mut_num_fetch_rows(&mut self) -> &mut usize {
        self.num_rows_fetched.as_mut()
    }

    fn row_array_size(&self) -> u32 {
        self.batch_size
    }

    unsafe fn bind_to_cursor(&mut self, cursor: &mut impl Cursor) -> Result<(), odbc_api::Error> {
        cursor.stmt().bind_col(1, &mut self.column)?;
        Ok(())
    }
}
