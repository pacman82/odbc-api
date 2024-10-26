//! Since connection pooling mode is a process level attribute these tests have to run in their own
//! process.

use odbc_api::{ConnectionOptions, Environment};
use odbc_sys::{AttrConnectionPooling, AttrCpMatch};
use std::sync::OnceLock;
use test_case::test_case;

const MSSQL_CONNECTION: &str =
    "Driver={ODBC Driver 18 for SQL Server};Server=localhost;UID=SA;PWD=My@Test@Password1;\
    TrustServerCertificate=yes;";

#[cfg(target_os = "windows")]
const SQLITE_3_CONNECTION: &str =
    "Driver={SQLite3 ODBC Driver};Database=sqlite-test.db;{Journal Mode}=WAL;";
#[cfg(not(target_os = "windows"))]
const SQLITE_3_CONNECTION: &str = "Driver={SQLite3};Database=sqlite-test.db;{Journal Mode}=WAL;";

#[cfg(target_os = "windows")]
const MARIADB_CONNECTION: &str = "Driver={MariaDB ODBC 3.1 Driver};\
    Server=localhost;DB=test_db;\
    UID=root;PWD=my-secret-pw;\
    Port=3306";

// Use 127.0.0.1 instead of localhost so the system uses the TCP/IP connector instead of the socket
// connector. Prevents error message: 'Can't connect to local MySQL server through socket'.
#[cfg(not(target_os = "windows"))]
const MARIADB_CONNECTION: &str = "Driver={MariaDB 3.1 Driver};\
    Server=127.0.0.1;DB=test_db;\
    UID=root;PWD=my-secret-pw;\
    Port=3306";

const POSTGRES_CONNECTION: &str = "Driver={PostgreSQL UNICODE};\
    Server=localhost;\
    Port=5432;\
    Database=test;\
    Uid=test;\
    Pwd=test;";

static ENV: OnceLock<Environment> = std::sync::OnceLock::new();

fn env_with_pool() -> &'static Environment {
    ENV.get_or_init(|| {
        let _ = env_logger::builder().is_test(true).try_init();
        // Manipulates global mutable state within the process. Since we know within this test
        // process this is the only place it is called, and `get_or_init` guarantees init is only
        // called once, we are good.
        unsafe {
            Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
        }
        let mut env = Environment::new().unwrap();
        env.set_connection_pooling_matching(AttrCpMatch::Strict)
            .unwrap();
        env
    })
}

#[test_case(MSSQL_CONNECTION; "Microsoft SQL Server")]
#[test_case(MARIADB_CONNECTION; "Maria DB")]
#[test_case(SQLITE_3_CONNECTION; "SQLite 3")]
#[test_case(POSTGRES_CONNECTION; "PostgreSQL")]
fn connect(connection_string: &str) {
    // First connection should be created on demand
    {
        let conn = env_with_pool()
            .connect_with_connection_string(
                connection_string,
                // Fail faster if we forgot to boot up docker containers
                ConnectionOptions {
                    login_timeout_sec: Some(2),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(!conn.is_dead().unwrap());
    }

    // Second connection should be from the pool
    let conn = env_with_pool()
        .connect_with_connection_string(connection_string, ConnectionOptions::default())
        .unwrap();
    assert!(!conn.is_dead().unwrap());
}
