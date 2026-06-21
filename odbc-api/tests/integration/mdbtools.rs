#![cfg(target_os = "linux")]

use odbc_api::{ConnectionOptions, Cursor, environment};

// nwind.mdb is the Northwind sample database downloaded from:
// https://github.com/mdbtools/mdbtestdata/blob/master/data/nwind.mdb
const MDBTOOLS_CONNECTION: &str = concat!(
    "Driver={MDBTools};DBQ=",
    env!("CARGO_MANIFEST_DIR"),
    "/tests/integration/nwind.mdb;",
);

/// Reproduces: https://github.com/pacman82/odbc-api/pull/903
/// mdbtools does not support SQL_ATTR_PARAMSET_SIZE; executing a prepared statement with no
/// parameters must not call SQLSetStmtAttr for that attribute.
#[test]
fn execute_query() {
    let env = environment().unwrap();
    let conn = env
        .connect_with_connection_string(MDBTOOLS_CONNECTION, ConnectionOptions::default())
        .unwrap();
    let mut cursor = conn
        .execute("SELECT ProductName FROM Products WHERE ProductID = 1", (), None)
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut field = Vec::new();
    row.get_text(1, &mut field).unwrap();
    assert_eq!(&b"Chai"[..], field.as_slice());
}
