use stdext::function_name;
use test_case::test_case;

use odbc_api::{
    ColumnsRow, Cursor, PrimaryKeysRow, ResultSetMetadata, TablesRow,
    buffers::{ColumnarAnyBuffer, Item, TextRowSet},
};

use crate::common::{MARIADB, MSSQL, POSTGRES, Profile, SQLITE_3};

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
    let mut stmt = conn.preallocate().unwrap();
    let columns = stmt
        .columns_cursor(&conn.current_catalog().unwrap(), schema, &table_name, "a")
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

#[test_case(MSSQL, Some("master"), Some("dbo"), None; "Microsoft SQL Server")]
#[test_case(MARIADB, Some("test_db"), None, Some(""); "Maria DB")]
#[test_case(SQLITE_3, None, None, None; "SQLite 3")]
#[test_case(POSTGRES, Some("test"), Some("public"), Some(""); "PostgreSQL")]
fn list_tables_with_preallocated(
    profile: &Profile,
    catalog: Option<&str>,
    schema: Option<&str>,
    remarks: Option<&str>,
) {
    let table_name = table_name!();
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), (), None)
        .unwrap();
    conn.execute(&format!("CREATE TABLE {table_name} (a INTEGER)"), (), None)
        .unwrap();

    let mut stmt = conn.preallocate().unwrap();
    let iter = stmt.tables("", "", &table_name, "").unwrap();
    let rows: Vec<TablesRow> = iter.collect::<Result<_, _>>().unwrap();

    assert_eq!(1, rows.len());
    let row = &rows[0];
    assert_eq!(catalog, row.catalog.as_str().unwrap());
    assert_eq!(schema, row.schema.as_str().unwrap());
    assert_eq!(Some(table_name.as_str()), row.table.as_str().unwrap());
    assert_eq!(Some("TABLE"), row.table_type.as_str().unwrap(), "TABLE_TYPE");
    assert_eq!(remarks, row.remarks.as_str().unwrap(), "REMARKS");
}

#[test_case(MSSQL, Some("master"), Some("dbo"), None; "Microsoft SQL Server")]
#[test_case(MARIADB, Some("test_db"), None, Some(""); "Maria DB")]
#[test_case(SQLITE_3, None, None, None; "SQLite 3")]
#[test_case(POSTGRES, Some("test"), Some("public"), Some(""); "PostgreSQL")]
fn list_tables_with_connection(
    profile: &Profile,
    catalog: Option<&str>,
    schema: Option<&str>,
    remarks: Option<&str>,
) {
    let table_name = table_name!();
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), (), None)
        .unwrap();
    conn.execute(&format!("CREATE TABLE {table_name} (a INTEGER)"), (), None)
        .unwrap();

    let iter = conn.tables("", "", &table_name, "").unwrap();
    let rows: Vec<TablesRow> = iter.collect::<Result<_, _>>().unwrap();

    assert_eq!(1, rows.len());
    let row = &rows[0];
    assert_eq!(catalog, row.catalog.as_str().unwrap());
    assert_eq!(schema, row.schema.as_str().unwrap());
    assert_eq!(Some(table_name.as_str()), row.table.as_str().unwrap());
    assert_eq!(Some("TABLE"), row.table_type.as_str().unwrap(), "TABLE_TYPE");
    assert_eq!(remarks, row.remarks.as_str().unwrap(), "REMARKS");
}

#[test_case(MSSQL, "master", Some("dbo"), "int", 10, 4, 0, 10, None, None, None, Some("YES"); "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db", None, "INT", 10, 4, 0, 10, Some(""), Some("NULL"), Some(2), Some("YES"); "Maria DB")]
#[test_case(SQLITE_3, "", Some(""), "INTEGER", 9, 10, 10, 0, None, Some("NULL"), Some(16384), Some("YES"); "SQLite 3")]
#[test_case(POSTGRES, "test", Some("public"), "int4", 10, 4, 0, 10, Some(""), None, Some(-1), None; "PostgreSQL")]
fn list_columns_with_preallocated(
    profile: &Profile,
    catalog: &str,
    schema: Option<&str>,
    type_name: &str,
    column_size: i32,
    buffer_length: i32,
    decimal_digits: i16,
    num_prec_radix: i16,
    remarks: Option<&str>,
    column_default: Option<&str>,
    char_octet_length: Option<i32>,
    is_nullable: Option<&str>,
) {
    let table_name = table_name!();
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), (), None)
        .unwrap();
    conn.execute(&format!("CREATE TABLE {table_name} (a INTEGER)"), (), None)
        .unwrap();

    let mut stmt = conn.preallocate().unwrap();
    let iter = stmt.columns("", "", &table_name, "").unwrap();
    let rows: Vec<ColumnsRow> = iter.collect::<Result<_, _>>().unwrap();

    assert_eq!(1, rows.len());
    let row = &rows[0];
    assert_eq!(catalog, row.catalog.as_str().unwrap().unwrap());
    assert_eq!(schema, row.schema.as_str().unwrap());
    assert_eq!(Some(table_name.as_str()), row.table.as_str().unwrap());
    assert_eq!(Some("a"), row.column_name.as_str().unwrap());
    assert_eq!(4, row.data_type, "DATA_TYPE");
    assert_eq!(
        Some(type_name),
        row.type_name.as_str().unwrap(),
        "TYPE_NAME"
    );
    assert_eq!(Some(&column_size), row.column_size.as_opt(), "COLUMN_SIZE");
    assert_eq!(
        Some(&buffer_length),
        row.buffer_length.as_opt(),
        "BUFFER_LENGTH"
    );
    assert_eq!(
        Some(&decimal_digits),
        row.decimal_digits.as_opt(),
        "DECIMAL_DIGITS"
    );
    assert_eq!(
        Some(&num_prec_radix),
        row.num_prec_radix.as_opt(),
        "NUM_PREC_RADIX"
    );
    assert_eq!(1, row.nullable, "NULLABLE");
    assert_eq!(remarks, row.remarks.as_str().unwrap(), "REMARKS");
    assert_eq!(
        column_default,
        row.column_default.as_str().unwrap(),
        "COLUMN_DEF"
    );
    assert_eq!(4, row.sql_data_type, "SQL_DATA_TYPE");
    assert_eq!(None, row.sql_datetime_sub.as_opt(), "SQL_DATETIME_SUB");
    assert_eq!(
        char_octet_length.as_ref(),
        row.char_octet_length.as_opt(),
        "CHAR_OCTET_LENGTH"
    );
    assert_eq!(1, row.ordinal_position, "ORDINAL_POSITION");
    assert_eq!(
        is_nullable,
        row.is_nullable.as_str().unwrap(),
        "IS_NULLABLE"
    );
}

#[test_case(MSSQL, "master", Some("dbo"), "int", 10, 4, 0, 10, None, None, None, Some("YES"); "Microsoft SQL Server")]
#[test_case(MARIADB, "test_db", None, "INT", 10, 4, 0, 10, Some(""), Some("NULL"), Some(2), Some("YES"); "Maria DB")]
#[test_case(SQLITE_3, "", Some(""), "INTEGER", 9, 10, 10, 0, None, Some("NULL"), Some(16384), Some("YES"); "SQLite 3")]
#[test_case(POSTGRES, "test", Some("public"), "int4", 10, 4, 0, 10, Some(""), None, Some(-1), None; "PostgreSQL")]
fn list_columns_with_connection(
    profile: &Profile,
    catalog: &str,
    schema: Option<&str>,
    type_name: &str,
    column_size: i32,
    buffer_length: i32,
    decimal_digits: i16,
    num_prec_radix: i16,
    remarks: Option<&str>,
    column_default: Option<&str>,
    char_octet_length: Option<i32>,
    is_nullable: Option<&str>,
) {
    let table_name = table_name!();
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), (), None)
        .unwrap();
    conn.execute(&format!("CREATE TABLE {table_name} (a INTEGER)"), (), None)
        .unwrap();

    let iter = conn.columns("", "", &table_name, "").unwrap();
    let rows: Vec<ColumnsRow> = iter.collect::<Result<_, _>>().unwrap();

    assert_eq!(1, rows.len());
    let row = &rows[0];
    assert_eq!(catalog, row.catalog.as_str().unwrap().unwrap());
    assert_eq!(schema, row.schema.as_str().unwrap());
    assert_eq!(Some(table_name.as_str()), row.table.as_str().unwrap());
    assert_eq!(Some("a"), row.column_name.as_str().unwrap());
    assert_eq!(4, row.data_type, "DATA_TYPE");
    assert_eq!(
        Some(type_name),
        row.type_name.as_str().unwrap(),
        "TYPE_NAME"
    );
    assert_eq!(Some(&column_size), row.column_size.as_opt(), "COLUMN_SIZE");
    assert_eq!(
        Some(&buffer_length),
        row.buffer_length.as_opt(),
        "BUFFER_LENGTH"
    );
    assert_eq!(
        Some(&decimal_digits),
        row.decimal_digits.as_opt(),
        "DECIMAL_DIGITS"
    );
    assert_eq!(
        Some(&num_prec_radix),
        row.num_prec_radix.as_opt(),
        "NUM_PREC_RADIX"
    );
    assert_eq!(1, row.nullable, "NULLABLE");
    assert_eq!(remarks, row.remarks.as_str().unwrap(), "REMARKS");
    assert_eq!(
        column_default,
        row.column_default.as_str().unwrap(),
        "COLUMN_DEF"
    );
    assert_eq!(4, row.sql_data_type, "SQL_DATA_TYPE");
    assert_eq!(None, row.sql_datetime_sub.as_opt(), "SQL_DATETIME_SUB");
    assert_eq!(
        char_octet_length.as_ref(),
        row.char_octet_length.as_opt(),
        "CHAR_OCTET_LENGTH"
    );
    assert_eq!(1, row.ordinal_position, "ORDINAL_POSITION");
    assert_eq!(
        is_nullable,
        row.is_nullable.as_str().unwrap(),
        "IS_NULLABLE"
    );
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
        .foreign_keys_cursor("", "", "", "", "", &fk_table_name)
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

#[test_case(MSSQL, Some("master"), Some("dbo"); "Microsoft SQL Server")]
#[test_case(MARIADB, Some("test_db"), None; "Maria DB")]
#[test_case(SQLITE_3, Some(""), Some(""); "SQLite 3")]
#[test_case(POSTGRES, Some("test"), Some("public"); "PostgreSQL")]
fn list_private_keys_with_preallocated(
    profile: &Profile,
    catalog: Option<&str>,
    schema: Option<&str>,
) {
    let table_name = table_name!();
    // Given a table with a composite primary key (a,b) and a another column c
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), (), None)
        .unwrap();
    let statement =
        &format!("CREATE TABLE {table_name} (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY (a,b))");
    conn.execute(&statement, (), None).unwrap();

    // When we list the primary keys for that table
    let mut stmt = conn.preallocate().unwrap();
    let iter = stmt.primary_keys(None, None, &table_name).unwrap();
    let primary_keys_rows: Vec<_> = iter.collect::<Result<_, _>>().unwrap();

    // Then we expact the result set to describe the primary key of the table. The columns of the
    // result set are: TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,KEY_SEQ,PK_NAME.
    assert_eq!(2, primary_keys_rows.len());
    assert_eq!(catalog, primary_keys_rows[0].catalog.as_str().unwrap());
    assert_eq!(catalog, primary_keys_rows[1].catalog.as_str().unwrap());
    assert_eq!(schema, primary_keys_rows[0].schema.as_str().unwrap());
    assert_eq!(schema, primary_keys_rows[1].schema.as_str().unwrap());
    assert_eq!(
        Some(table_name.as_str()),
        primary_keys_rows[0].table.as_str().unwrap()
    );
    assert_eq!(
        Some(table_name.as_str()),
        primary_keys_rows[1].table.as_str().unwrap()
    );
    assert_eq!(Some("a"), primary_keys_rows[0].column.as_str().unwrap());
    assert_eq!(Some("b"), primary_keys_rows[1].column.as_str().unwrap());
    assert_eq!(1, primary_keys_rows[0].key_seq);
    assert_eq!(2, primary_keys_rows[1].key_seq);
    eprintln!(
        "{}",
        primary_keys_rows[0]
            .pk_name
            .as_str()
            .unwrap()
            .unwrap_or("NULL")
    );
    eprintln!(
        "{}",
        primary_keys_rows[1]
            .pk_name
            .as_str()
            .unwrap()
            .unwrap_or("NULL")
    );
}

#[test_case(MSSQL, Some("master"), Some("dbo"); "Microsoft SQL Server")]
#[test_case(MARIADB, Some("test_db"), None; "Maria DB")]
#[test_case(SQLITE_3, Some(""), Some(""); "SQLite 3")]
#[test_case(POSTGRES, Some("test"), Some("public"); "PostgreSQL")]
fn list_private_keys_with_connection(
    profile: &Profile,
    catalog: Option<&str>,
    schema: Option<&str>,
) {
    let table_name = table_name!();
    // Given a table with a composite primary key (a,b) and a another column c
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name}"), (), None)
        .unwrap();
    let statement =
        &format!("CREATE TABLE {table_name} (a INTEGER, b INTEGER, c INTEGER, PRIMARY KEY (a,b))");
    conn.execute(&statement, (), None).unwrap();

    // When we list the primary keys for that table
    let iter = conn.primary_keys(None, None, &table_name).unwrap();
    let primary_keys_rows: Vec<PrimaryKeysRow> = iter.collect::<Result<_, _>>().unwrap();

    // Then we expact the result set to describe the primary key of the table. The columns of the
    // result set are: TABLE_CAT,TABLE_SCHEM,TABLE_NAME,COLUMN_NAME,KEY_SEQ,PK_NAME.
    assert_eq!(2, primary_keys_rows.len());
    assert_eq!(catalog, primary_keys_rows[0].catalog.as_str().unwrap());
    assert_eq!(catalog, primary_keys_rows[1].catalog.as_str().unwrap());
    assert_eq!(schema, primary_keys_rows[0].schema.as_str().unwrap());
    assert_eq!(schema, primary_keys_rows[1].schema.as_str().unwrap());
    assert_eq!(
        Some(table_name.as_str()),
        primary_keys_rows[0].table.as_str().unwrap()
    );
    assert_eq!(
        Some(table_name.as_str()),
        primary_keys_rows[1].table.as_str().unwrap()
    );
    assert_eq!(Some("a"), primary_keys_rows[0].column.as_str().unwrap());
    assert_eq!(Some("b"), primary_keys_rows[1].column.as_str().unwrap());
    assert_eq!(1, primary_keys_rows[0].key_seq);
    assert_eq!(2, primary_keys_rows[1].key_seq);
    eprintln!(
        "{}",
        primary_keys_rows[0]
            .pk_name
            .as_str()
            .unwrap()
            .unwrap_or("NULL")
    );
    eprintln!(
        "{}",
        primary_keys_rows[1]
            .pk_name
            .as_str()
            .unwrap()
            .unwrap_or("NULL")
    );
}

/// Document the display sizes of the variable-length columns in the `SQLColumns` result set. The
/// ODBC spec does not provide `SQLGetInfo` types for TYPE_NAME, REMARKS, COLUMN_DEF or
/// IS_NULLABLE lengths, so we assert the values each driver reports here to inform the buffer
/// sizes for a strongly typed `ColumnsRow` struct.
#[test_case(MSSQL, 128, 254, 4000, 254; "Microsoft SQL Server")]
#[test_case(MARIADB, 16777216, 1024, 196596, 3; "Maria DB")]
#[test_case(SQLITE_3, 50, 50, 50, 50; "SQLite 3")]
#[test_case(POSTGRES, 128, 254, 254, 254; "PostgreSQL")]
fn columns_varchar_column_sizes(
    profile: &Profile,
    expected_type_name_display_size: usize,
    expected_remarks_display_size: usize,
    expected_column_def_display_size: usize,
    expected_is_nullable_display_size: usize,
) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["INTEGER"])
        .unwrap();

    let mut stmt = conn.preallocate().unwrap();
    let mut cursor = stmt.columns_cursor("", "", &table_name, "").unwrap();

    const TYPE_NAME: u16 = 6;
    const REMARKS: u16 = 12;
    const COLUMN_DEF: u16 = 13;
    const IS_NULLABLE: u16 = 18;

    let mut ds = |col: u16| -> usize {
        cursor
            .col_display_size(col)
            .unwrap()
            .map(|n: std::num::NonZeroUsize| n.get())
            .unwrap_or(0)
    };

    assert_eq!(expected_type_name_display_size, ds(TYPE_NAME), "TYPE_NAME");
    assert_eq!(expected_remarks_display_size, ds(REMARKS), "REMARKS");
    assert_eq!(
        expected_column_def_display_size,
        ds(COLUMN_DEF),
        "COLUMN_DEF"
    );
    assert_eq!(
        expected_is_nullable_display_size,
        ds(IS_NULLABLE),
        "IS_NULLABLE"
    );
}
