use stdext::function_name;
use test_case::test_case;

use odbc_api::{
    buffers::{BufferDesc, ColumnarAnyBuffer, Item, TextRowSet},
    ColumnDescription, Cursor, ResultSetMetadata,
};

use crate::common::{cursor_to_string, Profile, MARIADB, MSSQL, POSTGRES, SQLITE_3};

// Check the max name length for the catalogs, schemas, tables, and columns.
#[test_case(MSSQL, 128, 128, 128, 128; "Microsoft SQL Server")]
#[test_case(MARIADB, 256, 0, 256, 255; "Maria DB")]
#[test_case(SQLITE_3, 255, 255, 255, 255; "SQLite 3")]
#[test_case(POSTGRES, 0, 63, 63, 63; "PostgreSQL")]
fn name_limits(
    profile: &Profile,
    expected_max_catalog_name_len: u16,
    expected_max_schema_name_len: u16,
    expected_max_table_name_len: u16,
    expected_max_column_name_len: u16,
) {
    let conn = profile.connection().unwrap();

    assert_eq!(
        conn.max_catalog_name_len().unwrap(),
        expected_max_catalog_name_len
    );
    assert_eq!(
        conn.max_schema_name_len().unwrap(),
        expected_max_schema_name_len
    );
    assert_eq!(
        conn.max_table_name_len().unwrap(),
        expected_max_table_name_len
    );
    assert_eq!(
        conn.max_column_name_len().unwrap(),
        expected_max_column_name_len
    );
}

// Check the current catalog being used by the connection.
#[test_case(MSSQL, "master"; "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db"; "Maria DB")]
#[test_case(SQLITE_3, ""; "SQLite 3")]
#[test_case(POSTGRES, "test"; "PostgreSQL")]
fn current_catalog(profile: &Profile, expected_catalog: &str) {
    let conn = profile.connection().unwrap();

    assert_eq!(conn.current_catalog().unwrap(), expected_catalog);
}

#[test_case(MSSQL, "dbo"; "Microsoft SQL Server")]
#[test_case(MARIADB, ""; "Maria DB")]
#[test_case(SQLITE_3, "dbo"; "SQLite 3")]
// #[test_case(POSTGRES, "test"; "PostgreSQL")] Errors out in linux
fn columns_query(profile: &Profile, schema: &str) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(10)"])
        .unwrap();

    let row_set_buffer =
        ColumnarAnyBuffer::try_from_descs(2, conn.columns_buffer_descs(255, 255, 255).unwrap())
            .unwrap();
    // Mariadb does not support schemas
    let columns = conn
        .columns(&conn.current_catalog().unwrap(), schema, &table_name, "a")
        .unwrap();

    let mut cursor = columns.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    const COLUMN_NAME_INDEX: usize = 3;
    let column_names = batch.column(COLUMN_NAME_INDEX).as_text_view().unwrap();

    const COLUMN_SIZE_INDEX: usize = 6;
    let column_sizes = i32::as_nullable_slice(batch.column(COLUMN_SIZE_INDEX)).unwrap();

    let column_has_name_a_and_size_10 = column_names
        .iter()
        .zip(column_sizes)
        .any(|(name, size)| str::from_utf8(name.unwrap()).unwrap() == "a" && *size.unwrap() == 10);

    assert!(column_has_name_a_and_size_10);
}

/// List tables for various data sources
/// Table name comparison is insensitive on Windows
#[test_case(MSSQL, "master,dbo,ListTables,TABLE,NULL"; "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db,NULL,ListTables,TABLE,"; "Maria DB")]
#[test_case(SQLITE_3, "NULL,NULL,ListTables,TABLE,NULL"; "SQLite 3")]
#[test_case(POSTGRES, ""; "PostgreSQL")]
fn list_tables(profile: &Profile, expected: &str) {
    // Table name is part of test expectation for this test
    let table_name = "ListTables";
    let conn = profile.setup_empty_table(table_name, &["INTEGER"]).unwrap();

    let cursor = conn.tables("", "", table_name, "").unwrap();
    let actual = cursor_to_string(cursor).to_lowercase();
    assert_eq!(expected.to_lowercase(), actual);
}

/// List tables for various data sources, using a preallocated statement
/// Table name comparison is insensitive on Windows
#[test_case(MSSQL, "master,dbo,ListTablesPreallocated,TABLE,NULL"; "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db,NULL,ListTablesPreallocated,TABLE,"; "Maria DB")]
#[test_case(SQLITE_3, "NULL,NULL,ListTablesPreallocated,TABLE,NULL"; "SQLite 3")]
#[test_case(POSTGRES, ""; "PostgreSQL")]
fn list_tables_preallocated(profile: &Profile, expected: &str) {
    // Table name is part of test expectation for this test
    let table_name = "ListTablesPreallocated";
    let conn = profile.setup_empty_table(table_name, &["INTEGER"]).unwrap();
    let mut preallocated = conn.preallocate().unwrap();

    let cursor = preallocated.tables("", "", table_name, "").unwrap();
    let actual = cursor_to_string(cursor).to_lowercase();

    assert_eq!(expected.to_lowercase(), actual);
}

/// List columns for various data sources
#[test_case(MSSQL, "master,dbo,ListColumns,a,4,int,10,4,0,10,1,NULL,NULL,4,NULL,NULL,2,YES,0,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,38"; "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db,NULL,ListColumns,a,4,INT,10,4,0,10,1,,NULL,4,NULL,2,2,YES"; "Maria DB")]
#[test_case(SQLITE_3, ",,ListColumns,a,4,INTEGER,9,10,10,0,1,NULL,NULL,4,NULL,16384,2,YES"; "SQLite 3")]
// #[test_case(POSTGRES, ""; "PostgreSQL")] Fails in linux
fn list_columns(profile: &Profile, expected: &str) {
    // Table name is part of test expectation for this test
    let table_name = "ListColumns";
    let conn = profile.setup_empty_table(table_name, &["INTEGER"]).unwrap();

    let cursor = conn.columns("", "", table_name, "a").unwrap();
    let actual = cursor_to_string(cursor).to_lowercase();

    assert_eq!(expected.to_lowercase(), actual);
}

/// List columns for various data sources, using a preallocated statement
#[test_case(MSSQL, "master,dbo,ListColumnsPreallocated,a,4,int,10,4,0,10,1,NULL,NULL,4,NULL,NULL,2,YES,0,0,0,0,NULL,NULL,NULL,NULL,NULL,NULL,38"; "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db,NULL,ListColumnsPreallocated,a,4,INT,10,4,0,10,1,,NULL,4,NULL,2,2,YES"; "Maria DB")]
#[test_case(SQLITE_3, ",,ListColumnsPreallocated,a,4,INTEGER,9,10,10,0,1,NULL,NULL,4,NULL,16384,2,YES"; "SQLite 3")]
// #[test_case(POSTGRES, ""; "PostgreSQL")] Fails in linux
fn list_columns_preallocated(profile: &Profile, expected: &str) {
    // Table name is part of test expectation for this test
    let table_name = "ListColumnsPreallocated";
    let conn = profile.setup_empty_table(table_name, &["INTEGER"]).unwrap();
    let mut preallocated = conn.preallocate().unwrap();

    let cursor = preallocated.columns("", "", table_name, "a").unwrap();
    let actual = cursor_to_string(cursor).to_lowercase();

    assert_eq!(expected.to_lowercase(), actual);
}

/// This test documents the amount of memory needed to hold the maximum row of the columns table
/// as described by the result sets metadata.
#[test_case(MSSQL, 10039; "Microsoft SQL Server")]
// Fails on CI in Windows, due to MariaDB not being recent enough
// #[test_case(MARIADB, 16975179; "Maria DB")]
#[test_case(SQLITE_3, 986; "SQLite 3")]
// #[test_case(POSTGRES, 1676; "PostgreSQL")] Fails in Linux
fn list_columns_oom(profile: &Profile, expected_row_size_in_bytes: usize) {
    let conn = profile.connection().unwrap();

    // This filter does not change the assertions, but makes the tests run so much faster for
    // Microsoft Sql Server (which seems to lock each table listed). This also likely prevents a
    // deadlock or transaction collision with other tests. Since the other tests destroy and create
    // tables a lot, listing them in parallel is dangerous. This filter gets rid of most of the
    // weirdness.
    let table_name = table_name!();
    let mut cursor = conn.columns("", "", &table_name, "").unwrap();
    let mut column_description = ColumnDescription::default();
    let mut size_of_row = 0;
    for index in 0..cursor.num_result_cols().unwrap() {
        cursor
            .describe_col(index as u16 + 1, &mut column_description)
            .unwrap();
        let buffer_description = BufferDesc::from_data_type(
            column_description.data_type,
            column_description.could_be_nullable(),
        )
        .unwrap();
        size_of_row += buffer_description.bytes_per_row();
    }
    assert_eq!(expected_row_size_in_bytes, size_of_row)
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn list_foreign_keys(profile: &Profile) {
    // Other table references table
    let pk_table_name = table_name!();
    let fk_table_name = format!("other_{pk_table_name}");
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {fk_table_name};"), (), None)
        .unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {pk_table_name};"), (), None)
        .unwrap();
    conn.execute(
        &format!("CREATE TABLE {pk_table_name} (id INTEGER, PRIMARY KEY(id));"),
        (),
        None,
    )
    .unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE {fk_table_name} (ext_id INTEGER, FOREIGN KEY (ext_id) REFERENCES \
            {pk_table_name}(id));"
        ),
        (),
        None,
    )
    .unwrap();

    let mut cursor = conn
        .foreign_keys("", "", "", "", "", &fk_table_name)
        .unwrap();
    let buffer = TextRowSet::for_cursor(10, &mut cursor, Some(256)).unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let retrieved_pk_table_name = batch.at_as_str(2, 0).unwrap().unwrap();
    let retrieved_fk_table_name = batch.at_as_str(6, 0).unwrap().unwrap();

    assert_eq!(retrieved_pk_table_name, pk_table_name);
    assert_eq!(retrieved_fk_table_name, fk_table_name);
    assert_eq!(batch.num_rows(), 1);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn list_foreign_keys_prealloc(profile: &Profile) {
    // Other table references table
    let pk_table_name = table_name!();
    let fk_table_name = format!("other_{pk_table_name}");
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {fk_table_name};"), (), None)
        .unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {pk_table_name};"), (), None)
        .unwrap();
    conn.execute(
        &format!("CREATE TABLE {pk_table_name} (id INTEGER, PRIMARY KEY(id));"),
        (),
        None,
    )
    .unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE {fk_table_name} (ext_id INTEGER, FOREIGN KEY (ext_id) REFERENCES \
            {pk_table_name}(id));"
        ),
        (),
        None,
    )
    .unwrap();

    let mut stmt = conn.preallocate().unwrap();
    let mut cursor = stmt
        .foreign_keys("", "", "", "", "", &fk_table_name)
        .unwrap();
    let buffer = TextRowSet::for_cursor(10, &mut cursor, Some(256)).unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let retrieved_pk_table_name = batch.at_as_str(2, 0).unwrap().unwrap();
    let retrieved_fk_table_name = batch.at_as_str(6, 0).unwrap().unwrap();

    assert_eq!(retrieved_pk_table_name, pk_table_name);
    assert_eq!(retrieved_fk_table_name, fk_table_name);
    assert_eq!(batch.num_rows(), 1);
}
