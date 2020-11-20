use lazy_static::lazy_static;
use odbc_api::{buffers, Connection, Cursor, Environment};

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    pub static ref ENV: Environment = unsafe { Environment::new().unwrap() };
}

/// Creates the table and assuers it is empty
pub fn setup_empty_table(
    conn: &Connection,
    table_name: &str,
    column_name: &str,
    column_type: &str,
) -> Result<(), odbc_api::Error> {
    let drop_table = &format!("DROP TABLE IF EXISTS {}", table_name);
    let create_table = format!(
        "CREATE TABLE {} (id int IDENTITY(1,1), {} {});",
        table_name, column_name, column_type
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

#[test]
fn bogus_connection_string() {
    let conn = ENV.connect_with_connection_string("foobar");
    assert!(matches!(conn, Err(_)));
}
