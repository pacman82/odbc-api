use std::iter::repeat;

use stdext::function_name;

use odbc_api::{ConnectionOptions, Environment, IntoParameter};

pub const MSSQL_CONNECTION: &str = "Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;\
    TrustServerCertificate=yes;";

macro_rules! table_name {
    () => {
        // Make function name a valid table name
        function_name!()
            .strip_prefix("main::")
            .unwrap()
            .replace("::", "_")
            .replace(r#"_{{closure}}"#, "")
    };
}

/// First scenario driving the development of odbc-async. Can all the operations to insert one line
/// in a service be executed asynchronously?
#[tokio::test]
async fn otel_insert_mssql() {

    let table_name = table_name!();
    let table = Table::new(&table_name, &["INTEGER"], &["A"]);

    // Fine, this does not take long.
    let env = Environment::new().unwrap();

    // Blocking
    let conn = env.connect_with_connection_string(MSSQL_CONNECTION, ConnectionOptions::default()).unwrap();

    conn.execute(&table.sql_drop_if_exists(), (), None).unwrap();
    conn.execute(&table.sql_create_table("int IDENTITY(1,1)"), (), None).unwrap();

    conn.execute(&table.sql_insert(), &42.into_parameter(), None).unwrap();

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
}