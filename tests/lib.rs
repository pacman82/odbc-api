use odbc_api::{ColumnDescription, Environment, Nullable, U16String};
use lazy_static::lazy_static;
use env_logger;
use std::sync::Mutex;

const MSSQL: &str = "Driver={SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
// Therefore synchronize test execution.
lazy_static! {
    static ref SERIALIZE: Mutex<()> = Mutex::new(());
}

fn init() -> &'static Mutex<()> {
    // Set environment to something like:
    // RUST_LOG=autodbc=info cargo test
    let _ = env_logger::builder().is_test(true).try_init();
    &SERIALIZE
}

#[test]
fn bogus_connection_string() {
    let _ = init().lock();
    let env = unsafe { Environment::new().unwrap() };
    let conn = env.connect_with_connection_string("foobar");
    assert!(matches!(conn, Err(_)));
}

#[test]
fn connect_to_movies_db() {
    let _ = init().lock();
    let env = unsafe { Environment::new().unwrap() };
    let _conn = env.connect_with_connection_string(MSSQL).unwrap();
}

#[test]
fn describe_columns() {
    let _ = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title, year FROM Movies ORDER BY year;";
    let cursor = conn.exec_direct(sql).unwrap().unwrap();

    assert_eq!(cursor.num_result_cols().unwrap(), 2);
    let mut cd = ColumnDescription::default();
    cursor.describe_col(1, &mut cd).unwrap();


    cursor.describe_col(1, &mut cd).unwrap();
    let name = U16String::from_str("title");

    // Expectation title column
    let title_desc = ColumnDescription {
        name: name.into_vec(),
        data_type: odbc_sys::SqlDataType::VARCHAR,
        column_size: 255,
        decimal_digits: 0,
        nullable: Nullable::NoNulls,
    };

    assert_eq!(title_desc, cd);

    cursor.describe_col(2, &mut cd).unwrap();
    let name = U16String::from_str("year");

    // Expectation title column
    let year_desc = ColumnDescription {
        name: name.into_vec(),
        data_type: odbc_sys::SqlDataType::INTEGER,
        column_size: 10,
        decimal_digits: 0,
        nullable: Nullable::Nullable,
    };

    assert_eq!(year_desc, cd);
}
