//! Tests for the `derive` feature.
#![cfg(feature = "derive")]
use stdext::function_name;
use test_case::test_case;

use odbc_api::{Cursor, Fetch, buffers::RowVec, parameter::VarCharArray};

use crate::common::{Given, MARIADB, MSSQL, POSTGRES, Profile, SQLITE_3};

macro_rules! table_name {
    () => {
        // Make function name a valid table name
        function_name!()
            .replace("::", "_")
            .replace(r#"_{{closure}}"#, "")
    };
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_wise_bulk_query_using_custom_row(profile: &Profile) {
    // Given a cursor
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(50)"])
        .values_by_column(&[
            &[Some("42"), Some("5")],
            &[Some("Hello, World!"), Some("Hallo, Welt!")],
        ])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), (), None)
        .unwrap()
        .unwrap();

    // When
    #[derive(Clone, Copy, Default, Fetch)]
    struct MyRow {
        a: i32,
        b: VarCharArray<50>,
    }
    let row_set_buffer = RowVec::<MyRow>::new(10);
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(2, batch.num_rows());
    assert_eq!(42, batch[0].a);
    assert_eq!("Hello, World!", batch[0].b.as_str().unwrap().unwrap());
    assert_eq!(5, batch[1].a);
    assert_eq!("Hallo, Welt!", batch[1].b.as_str().unwrap().unwrap());
}
