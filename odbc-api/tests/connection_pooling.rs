//! Since connection pooling mode is a process level attribute these tests have to run in their own
//! process.

use lazy_static::lazy_static;
use odbc_api::Environment;
use odbc_sys::{AttrConnectionPooling, AttrCpMatch};

const MSSQL_CONNECTION: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    pub static ref ENV: Environment = unsafe {
        let _ = env_logger::builder().is_test(true).try_init();
        Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
        let mut env = Environment::new().unwrap();
        env.set_connection_pooling_matching(AttrCpMatch::Strict)
            .unwrap();
        env
    };
}

#[test]
fn connect() {
    // First connection should be created on demand
    {
        let conn = ENV
            .connect_with_connection_string(MSSQL_CONNECTION)
            .unwrap();
        assert!(!conn.is_dead().unwrap());
    }

    // Second connection should be from the pool
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    assert!(!conn.is_dead().unwrap());
}
