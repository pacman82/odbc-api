mod common;
mod connection_strings;

use stdext::function_name;
use sys::{CDataType, Numeric, Pointer, SqlDataType, Timestamp, NULL_DATA};
use tempfile::NamedTempFile;
use test_case::test_case;

use common::{cursor_to_string, Given, Profile, SingleColumnRowSetBuffer};
use connection_strings::{
    MARIADB_CONNECTION, MSSQL_CONNECTION, POSTGRES_CONNECTION, SQLITE_3_CONNECTION,
};

#[cfg(feature = "derive")]
use odbc_api::Fetch;
use odbc_api::{
    buffers::{
        BufferDesc, ColumnarAnyBuffer, ColumnarBuffer, Indicator, Item, RowVec, TextColumn,
        TextRowSet,
    },
    decimal_text_to_i128, environment,
    handles::{CData, CDataMut, OutputStringBuffer, ParameterDescription, Statement},
    parameter::{
        Blob, BlobRead, BlobSlice, InputParameter, VarBinaryArray, VarCharArray, VarCharSlice,
        VarCharSliceMut, VarWCharArray, WithDataType,
    },
    sys, Bit, ColumnDescription, ConcurrentBlockCursor, Connection, ConnectionOptions, Cursor,
    DataType, Error, InOut, IntoParameter, Narrow, Nullability, Nullable, Out, Preallocated,
    ResultSetMetadata, RowSetBuffer, TruncationInfo, U16Str, U16String,
};

use std::{
    ffi::CString,
    io::{self, Write},
    iter,
    num::NonZeroUsize,
    ptr::null_mut,
    str, thread,
    time::Duration,
};

const MSSQL: &Profile = &Profile {
    connection_string: MSSQL_CONNECTION,
    index_type: "int IDENTITY(1,1)",
    blob_type: "Image",
};

const SQLITE_3: &Profile = &Profile {
    connection_string: SQLITE_3_CONNECTION,
    index_type: "int IDENTITY(1,1)",
    blob_type: "BLOB",
};

const MARIADB: &Profile = &Profile {
    connection_string: MARIADB_CONNECTION,
    index_type: "INTEGER AUTO_INCREMENT PRIMARY KEY",
    blob_type: "BLOB",
};

const POSTGRES: &Profile = &Profile {
    connection_string: POSTGRES_CONNECTION,
    index_type: "SERIAL PRIMARY KEY",
    blob_type: "BYTEA",
};

macro_rules! table_name {
    () => {
        // Make function name a valid table name
        function_name!()
            .replace("::", "_")
            .replace(r#"_{{closure}}"#, "")
    };
}

#[test]
fn bogus_connection_string() {
    // When
    let result = environment()
        .unwrap()
        .connect_with_connection_string("foobar", ConnectionOptions::default());

    // Then

    // We expect an error, since "foobar" is obviously not a connection string we can use to connect
    // to any datasource (for starters it does not specify a driver).
    assert!(result.is_err());

    // We also want to be sure our error messages do not contain any Nul.
    let error = result.err().unwrap();
    if let Error::Diagnostics { record, function } = error {
        assert_eq!("SQLDriverConnect", function);
        // Make sure we remove any Nuls from the message, trailing or otherwise.
        assert!(!record.message.contains(&0));
    } else {
        panic!("Expected Error::Diagnostics")
    };
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn connect_to_db(profile: &Profile) {
    let conn = profile.connection().unwrap();
    assert!(!conn.is_dead().unwrap())
}

#[test_case(MSSQL, 4096; "Microsoft SQL Server")]
#[test_case(MARIADB, 8192; "Maria DB")]
#[test_case(SQLITE_3, 16384; "SQLite 3")]
#[test_case(POSTGRES, 4096; "PostgreSQL")]
fn default_packet_size(profile: &Profile, expected_packet_size: u32) {
    let conn = profile.connection().unwrap();
    let actual_packet_size = conn.packet_size().unwrap();

    assert_eq!(expected_packet_size, actual_packet_size)
}

#[test_case(MSSQL, 8000; "Microsoft SQL Server")]
#[test_case(MARIADB, 8192; "Maria DB")]
#[test_case(SQLITE_3, 16384; "SQLite 3")]
#[test_case(POSTGRES, 4096; "PostgreSQL")]
fn set_packet_size(profile: &Profile, expected_packet_size: u32) {
    let desired_packet_size = 8192;

    let conn = environment()
        .unwrap()
        .connect_with_connection_string(
            profile.connection_string,
            ConnectionOptions {
                packet_size: Some(desired_packet_size),
                ..Default::default()
            },
        )
        .unwrap();
    let actual_packet_size = conn.packet_size().unwrap();
    assert_eq!(expected_packet_size, actual_packet_size)
}

#[test_case(MSSQL; "Microsoft SQL Server")]
fn describe_columns(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&[
            "VARCHAR(255) NOT NULL",
            "INTEGER",
            "BINARY(12)",
            "VARBINARY(100)",
            "NCHAR(10)",
            "NUMERIC(3,2)",
            "DATETIME2",
            "TIME",
            "text",
            "Image",
            "DOUBLE PRECISION",
        ])
        .build(profile)
        .unwrap();

    let sql = table.sql_all_ordered_by_id();
    let mut cursor = conn.execute(&sql, ()).unwrap().unwrap();

    assert_eq!(cursor.num_result_cols().unwrap(), 11);
    let mut actual = ColumnDescription::default();

    let kind = DataType::Varchar {
        length: NonZeroUsize::new(255),
    };
    let expected = ColumnDescription::new("a", kind, Nullability::NoNulls);
    cursor.describe_col(1, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(1).unwrap());

    let kind = DataType::Integer;
    let expected = ColumnDescription::new("b", kind, Nullability::Nullable);
    cursor.describe_col(2, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(2).unwrap());

    let kind = DataType::Binary {
        length: NonZeroUsize::new(12),
    };
    let expected = ColumnDescription::new("c", kind, Nullability::Nullable);
    cursor.describe_col(3, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(3).unwrap());

    let kind = DataType::Varbinary {
        length: NonZeroUsize::new(100),
    };
    let expected = ColumnDescription::new("d", kind, Nullability::Nullable);
    cursor.describe_col(4, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(4).unwrap());

    let kind = DataType::WChar {
        length: NonZeroUsize::new(10),
    };
    let expected = ColumnDescription::new("e", kind, Nullability::Nullable);
    cursor.describe_col(5, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(5).unwrap());

    let kind = DataType::Numeric {
        precision: 3,
        scale: 2,
    };
    let expected = ColumnDescription::new("f", kind, Nullability::Nullable);
    cursor.describe_col(6, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(6).unwrap());

    let kind = DataType::Timestamp { precision: 7 };
    let expected = ColumnDescription::new("g", kind, Nullability::Nullable);
    cursor.describe_col(7, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(7).unwrap());

    let kind = DataType::Other {
        data_type: SqlDataType(-154),
        column_size: NonZeroUsize::new(16),
        decimal_digits: 7,
    };
    let expected = ColumnDescription::new("h", kind, Nullability::Nullable);
    cursor.describe_col(8, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(8).unwrap());

    let kind = DataType::LongVarchar {
        length: NonZeroUsize::new(2147483647),
    };
    let expected = ColumnDescription::new("i", kind, Nullability::Nullable);
    cursor.describe_col(9, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(9).unwrap());

    let kind = DataType::LongVarbinary {
        length: NonZeroUsize::new(2147483647),
    };
    let expected = ColumnDescription::new("j", kind, Nullability::Nullable);
    cursor.describe_col(10, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(10).unwrap());

    let kind = DataType::Float { precision: 53 };
    let expected = ColumnDescription::new("k", kind, Nullability::Nullable);
    cursor.describe_col(11, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(11).unwrap());
}

/// Fetch text from data source using the TextBuffer type
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_fetch_text(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(255)", "INT"])
        .build(profile)
        .unwrap();

    // Insert data
    let insert = format!("INSERT INTO {table_name} (a,b) VALUES (?,?), (?,?),(?,?)");
    conn.execute(
        &insert,
        (
            &"Interstellar".into_parameter(),
            &None::<i32>.into_parameter(),
            &"2001: A Space Odyssey".into_parameter(),
            &1968,
            &"Jurassic Park".into_parameter(),
            &1993,
        ),
    )
    .unwrap();

    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    // Cursor to string helper utilizes the text buffer
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

/// Into cursor should enable users to open a connection within a function and return a cursor.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn into_cursor(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(255)", "INT"])
        .values_by_column(&[
            &[
                Some("Interstellar"),
                Some("2001: A Space Odyssey"),
                Some("Jurassic Park"),
            ],
            &[None, Some("1968"), Some("1993")],
        ])
        .build(profile)
        .unwrap();

    let make_cursor = || {
        let query = table.sql_all_ordered_by_id();
        conn.into_cursor(&query, ()).unwrap().unwrap()
    };
    let cursor = make_cursor();

    // Cursor to string helper utilizes the text buffer
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

/// Strong exception safety for `into_cursor`. Our first query will fail, because it will query a
/// non-existing table, but our second one using the same connection will succeed. This is one
/// scenario in which it is useful not to "swallow" the connection in case of an error.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn into_cursor_reuse_connection_on_failure(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(255)", "INT"])
        .values_by_column(&[
            &[
                Some("Interstellar"),
                Some("2001: A Space Odyssey"),
                Some("Jurassic Park"),
            ],
            &[None, Some("1968"), Some("1993")],
        ])
        .build(profile)
        .unwrap();

    // When our first call to `.into_cursor` fails
    let result = conn.into_cursor("Non-existing-table", ());

    // Then we can extract a valid connection from the error type
    let error = result.map(|_| ()).unwrap_err();
    let conn = error.connection;
    // Extra verification to prove connection is indeed valid
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.into_cursor(&query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

/// Implementing `std::error::Error` allows using the question mark operator in functions using
/// anyhow's error type, among other things.
#[test]
fn connection_and_error_implements_std_error() {
    // Given any connection, we do not care which database
    let conn = SQLITE_3.connection().unwrap();

    // When we catch the error returned by `into_cursor`
    let result = conn.into_cursor("Non-existing-table", ()).map(|_| ());
    let connection_and_error = result.unwrap_err();
    let plain_error = connection_and_error.error;
    let result = connection_and_error
        .connection
        .into_cursor("Non-existing-table", ())
        .map(|_| ());
    let std_error: Box<dyn std::error::Error> = Box::new(result.unwrap_err().error);

    // Then we can extract a valid connection from the error type
    assert_eq!(std_error.to_string(), plain_error.to_string());
    assert!(std_error.source().is_none());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn column_name(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["VARCHAR(255)", "INT"])
        .build(profile)
        .unwrap();

    let sql = format!("SELECT a, b FROM {table_name};");
    let mut cursor = conn.execute(&sql, ()).unwrap().unwrap();

    let name = cursor.col_name(1).unwrap();
    assert_eq!("a", name);

    let name = cursor.col_name(2).unwrap();
    assert_eq!("b", name);

    // Test the same using column descriptions
    let mut desc = ColumnDescription::default();

    cursor.describe_col(1, &mut desc).unwrap();
    assert_eq!("a", desc.name_to_string().unwrap());

    cursor.describe_col(2, &mut desc).unwrap();
    assert_eq!("b", desc.name_to_string().unwrap());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_wide_column_to_char(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["CHAR(5)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();
    conn.execute(&insert_sql, &"Hello".into_parameter())
        .unwrap();
    let sql = table.sql_all_ordered_by_id();

    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = ColumnarBuffer::new(vec![(1, TextColumn::<u16>::new(1, 5))]);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(
        Some(U16String::from_str("Hello").as_ustr()),
        buf.column(0).get(0).map(U16Str::from_slice)
    );
}

/// Bind a BIT column to a Bit buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_bit(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["BIT"])
        .build(profile)
        .unwrap();
    let insert_sql = format!("INSERT INTO {table_name} (a) VALUES (?),(?);");
    conn.execute(&insert_sql, (&Bit::from_bool(false), &Bit::from_bool(true)))
        .unwrap();

    let sql = format!("SELECT a FROM {table_name};");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = ColumnarBuffer::new(vec![(1, vec![Bit(0); 3])]);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    let batch = row_set_cursor.fetch().unwrap().unwrap();

    assert!(!batch.column(0)[0].as_bool());
    assert!(batch.column(0)[1].as_bool());
}

/// Binds a buffer which is too short to a fixed sized character type. This provokes an indicator of
/// `NO_TOTAL` on MSSQL.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn truncate_fixed_sized(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["CHAR(5)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();
    conn.execute(&insert_sql, &"Hello".into_parameter())
        .unwrap();

    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut buf = ColumnarBuffer::new(vec![(1, TextColumn::new(1, 3))]);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    let batch = row_set_cursor.fetch().unwrap().unwrap();

    assert_eq!(Some(&b"Hel"[..]), batch.column(0).get(0));
}

/// Bind a VARCHAR column to a char buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_varchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["VARCHAR(100)"])
        .build(profile)
        .unwrap();
    let insert_sql = format!("INSERT INTO {table_name} (a) VALUES ('Hello, World!');");
    conn.execute(&insert_sql, ()).unwrap();

    let sql = format!("SELECT a FROM {table_name};");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = TextRowSet::from_max_str_lens(1, [100]).unwrap();
    // let mut buf = SingleColumnRowSetBuffer::with_text_column(1, 100);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(Some(&b"Hello, World!"[..]), buf.column(0).get(0));
}

/// Bind a VARCHAR column to a wchar buffer
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_varchar_to_wchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(100)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();
    conn.execute(&insert_sql, &"Hello, World!".into_parameter())
        .unwrap();
    let sql = table.sql_all_ordered_by_id();

    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = ColumnarBuffer::new(vec![(1, TextColumn::<u16>::new(1, 100))]);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    let batch = row_set_cursor.fetch().unwrap().unwrap();

    assert_eq!(
        U16String::from_str("Hello, World!").as_ustr(),
        U16Str::from_slice(batch.column(0).get(0).unwrap())
    );
}

/// utf16 to utf8 conversion with one character those utf-16 representation is smaller than its utf8
/// representation
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] //Doesn't work on Linux
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn nvarchar_to_text(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["NVARCHAR(1)"])
        .unwrap();
    // Trade mark sign (`™`) is longer in utf-8 (3 Bytes) than in utf-16 (2 Bytes).
    let insert_sql = format!("INSERT INTO {} (a) VALUES (?);", table_name);
    conn.execute(&insert_sql, &"™".into_parameter()).unwrap();

    let sql = format!("SELECT a FROM {};", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let text = cursor_to_string(cursor);

    assert_eq!("™", text);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_numeric_to_float(profile: &Profile) {
    // Setup table
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["NUMERIC(3,2)"])
        .build(profile)
        .unwrap();

    let insert_sql = format!("INSERT INTO {table_name} (a) VALUES (?);");
    conn.execute(&insert_sql, &1.23).unwrap();

    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let buf: SingleColumnRowSetBuffer<Vec<f64>> = SingleColumnRowSetBuffer::new(1);
    let mut row_set_cursor = cursor.bind_buffer(buf).unwrap();

    let actual = row_set_cursor.fetch().unwrap().unwrap().get();
    assert_eq!(1, actual.len());
    assert!((1.23f64 - actual[0]).abs() < f64::EPSILON);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] SQLLITE does not do this kind of precision
#[test_case(POSTGRES; "PostgreSQL")]
fn fetch_double_precision_as_f64(profile: &Profile) {
    // Setup table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DOUBLE PRECISION"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &123456789.12345678f64)
        .unwrap();

    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let buf: SingleColumnRowSetBuffer<Vec<f64>> = SingleColumnRowSetBuffer::new(1);
    let mut row_set_cursor = cursor.bind_buffer(buf).unwrap();

    let actual = row_set_cursor.fetch().unwrap().unwrap().get();
    assert_eq!(1, actual.len());
    assert!((123456789.12345678f64 - actual[0]).abs() < f64::EPSILON);
}

/// What relational types do different dbs report for "Double precision"
#[test_case(MSSQL, DataType::Float { precision: 53 }; "Microsoft SQL Server")]
#[test_case(MARIADB, DataType::Double; "Maria DB")]
#[test_case(SQLITE_3, DataType::Double; "SQLite 3")]
// Yeah, `0` this seems like a bug in PG
#[test_case(POSTGRES, DataType::Float { precision: 0 }; "PostgreSQL")]
fn data_type_reported_for_double_precision(profile: &Profile, expected_data_type: DataType) {
    // Given a cursor with metadata about double precision
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DOUBLE PRECISION"])
        .build(profile)
        .unwrap();
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    let actual_data_type = cursor.col_data_type(1).unwrap();

    assert_eq!(expected_data_type, actual_data_type);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_numeric_to_i64(profile: &Profile) {
    // Setup table
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["NUMERIC(10,0)"])
        .build(profile)
        .unwrap();
    let insert_sql = format!("INSERT INTO {table_name} (a) VALUES (?);");
    conn.execute(&insert_sql, &1234567890i64).unwrap();

    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let buf: SingleColumnRowSetBuffer<Vec<i64>> = SingleColumnRowSetBuffer::new(1);
    let mut row_set_cursor = cursor.bind_buffer(buf).unwrap();

    let actual = row_set_cursor.fetch().unwrap().unwrap().get();
    assert_eq!(1, actual.len());
    assert_eq!(1234567890, actual[0]);
}

/// Bind a columnar buffer to a VARBINARY(10) column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] // Convert syntax is different
// #[test_case(SQLITE_3; "SQLite 3")]
fn columnar_fetch_varbinary(profile: &Profile) {
    // Setup
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARBINARY(10)"])
        .build(profile)
        .unwrap();
    let insert_sql = format!(
        "INSERT INTO {table_name} (a) Values \
        (CONVERT(Varbinary(10), 'Hello')),\
        (CONVERT(Varbinary(10), 'World')),\
        (NULL)"
    );
    conn.execute(&insert_sql, ()).unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(
        DataType::Varbinary {
            length: NonZeroUsize::new(10)
        },
        data_type
    );
    let buffer_desc = BufferDesc::from_data_type(data_type, true).unwrap();
    assert_eq!(BufferDesc::Binary { length: 10 }, buffer_desc);
    let row_set_buffer = ColumnarAnyBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let mut col_it = batch.column(0).as_bin_view().unwrap().iter();

    assert_eq!(Some(&b"Hello"[..]), col_it.next().unwrap());
    assert_eq!(Some(&b"World"[..]), col_it.next().unwrap());
    assert_eq!(Some(None), col_it.next()); // Expecting NULL
    assert_eq!(None, col_it.next()); // Expecting iterator end.
}

#[test_case(MSSQL, "VARCHAR(max)"; "Microsoft SQL Server")]
#[test_case(MARIADB, "TEXT"; "Maria DB")]
#[test_case(SQLITE_3, "TEXT"; "SQLite 3")]
#[test_case(POSTGRES, "TEXT"; "PostgreSQL")]
fn upper_limit_for_varchar_max(profile: &Profile, large_text_type: &'static str) {
    // Given
    let table_name = table_name!();
    let types = [large_text_type];
    let (conn, table) = Given::new(&table_name)
        .column_types(&types)
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"Hello, World!".into_parameter())
        .unwrap();

    // When
    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name}"), ())
        .unwrap()
        .unwrap();
    let text_buffer = TextRowSet::for_cursor(10, &mut cursor, Some(50)).unwrap();
    let mut cursor = cursor.bind_buffer(text_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(
        "Hello, World!",
        str::from_utf8(batch.column(0).get(0).unwrap()).unwrap()
    );
}

/// Bind a columnar buffer to a BINARY(5) column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] // different convert syntax
// #[test_case(SQLITE_3; "SQLite 3")]
fn columnar_fetch_binary(profile: &Profile) {
    // Setup
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["BINARY(5)"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {table_name} (a) Values \
        (CONVERT(Binary(5), 'Hello')),\
        (CONVERT(Binary(5), 'World')),\
        (NULL)"
        ),
        (),
    )
    .unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(
        DataType::Binary {
            length: NonZeroUsize::new(5)
        },
        data_type
    );
    let buffer_desc = BufferDesc::from_data_type(data_type, true).unwrap();
    assert_eq!(BufferDesc::Binary { length: 5 }, buffer_desc);
    let row_set_buffer = ColumnarAnyBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let mut col_it = batch.column(0).as_bin_view().unwrap().iter();
    assert_eq!(Some(&b"Hello"[..]), col_it.next().unwrap());
    assert_eq!(Some(&b"World"[..]), col_it.next().unwrap());
    assert_eq!(Some(None), col_it.next()); // Expecting NULL
    assert_eq!(None, col_it.next()); // Expecting iterator end.
}

/// Bind a columnar buffer to a DATETIME2 column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_fetch_timestamp(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["DATETIME2(3)"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {table_name} (a) Values \
        ({{ ts '2021-03-20 15:24:12.12' }}),\
        ({{ ts '2020-03-20 15:24:12' }}),\
        ({{ ts '1970-01-01 00:00:00' }}),\
        (NULL)"
        ),
        (),
    )
    .unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), ())
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(DataType::Timestamp { precision: 3 }, data_type);
    let buffer_desc = BufferDesc::from_data_type(data_type, true).unwrap();
    assert_eq!(BufferDesc::Timestamp { nullable: true }, buffer_desc);
    let row_set_buffer = ColumnarAnyBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let mut col_it = batch.column(0).as_nullable_slice().unwrap();
    assert_eq!(
        Some(&Timestamp {
            year: 2021,
            month: 3,
            day: 20,
            hour: 15,
            minute: 24,
            second: 12,
            fraction: 120_000_000,
        }),
        col_it.next().unwrap()
    );
    assert_eq!(
        Some(&Timestamp {
            year: 2020,
            month: 3,
            day: 20,
            hour: 15,
            minute: 24,
            second: 12,
            fraction: 0,
        }),
        col_it.next().unwrap()
    );
    assert_eq!(
        Some(&Timestamp {
            year: 1970,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            fraction: 0,
        }),
        col_it.next().unwrap()
    );
    assert_eq!(Some(None), col_it.next()); // Expecting NULL
    assert_eq!(None, col_it.next()); // Expecting iterator end.
}

/// Insert values into a DATETIME2 column using a columnar buffer
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
// #[test_case(SQLITE_3; "SQLite 3")] default precision of 3 instead 7
fn columnar_insert_timestamp(profile: &Profile) {
    let table_name = table_name!();
    // Setup
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DATETIME2"])
        .build(profile)
        .unwrap();

    // Fill buffer with values
    let desc = BufferDesc::Timestamp { nullable: true };
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let mut prebound = prepared.into_column_inserter(10, [desc]).unwrap();

    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(Timestamp {
            year: 2020,
            month: 3,
            day: 20,
            hour: 16,
            minute: 13,
            second: 54,
            fraction: 0,
        }),
        Some(Timestamp {
            year: 2021,
            month: 3,
            day: 20,
            hour: 16,
            minute: 13,
            second: 54,
            fraction: 123456700,
        }),
        None,
    ];

    prebound.set_num_rows(input.len());
    let column = prebound.column_mut(0);
    let mut writer = Timestamp::as_nullable_slice_mut(column).unwrap();
    writer.write(input.iter().copied());

    // Bind buffer and insert values.
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let actual = table.content_as_string(&conn);
    let expected = "2020-03-20 16:13:54.0000000\n2021-03-20 16:13:54.1234567\nNULL";
    assert_eq!(expected, actual);
}

/// Insert values into a i32 column using a columnar buffer's raw values
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn columnar_insert_int_raw(profile: &Profile) {
    let table_name = table_name!();
    // Setup
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();

    // Fill buffer with values
    let desc = BufferDesc::I32 { nullable: true };
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let mut prebound = prepared.into_column_inserter(10, [desc]).unwrap();

    // Input values to insert.
    let input_values = [1, 0, 3];
    let mask = [true, false, true];

    prebound.set_num_rows(input_values.len());
    let mut writer = prebound.column_mut(0).as_nullable_slice::<i32>().unwrap();
    let (values, indicators) = writer.raw_values();
    values[..input_values.len()].copy_from_slice(&input_values);
    indicators
        .iter_mut()
        .zip(mask.iter())
        .for_each(|(indicator, &mask)| *indicator = if mask { 0 } else { NULL_DATA });

    // Bind buffer and insert values.
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let actual = table.content_as_string(&conn);
    let expected = "1\nNULL\n3";
    assert_eq!(expected, actual);
}

/// Insert values into a DATETIME2(3) column using a columnar buffer. Milliseconds precision is
/// different from the default precision 7 (100ns).
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_timestamp_ms(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["DATETIME2(3)"])
        .build(profile)
        .unwrap();
    let prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    // Fill buffer with values
    let desc = BufferDesc::Timestamp { nullable: true };
    let mut prebound = prepared.into_column_inserter(10, [desc]).unwrap();

    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(Timestamp {
            year: 2020,
            month: 3,
            day: 20,
            hour: 16,
            minute: 13,
            second: 54,
            fraction: 0,
        }),
        Some(Timestamp {
            year: 2021,
            month: 3,
            day: 20,
            hour: 16,
            minute: 13,
            second: 54,
            fraction: 123456700,
        }),
        None,
    ];

    prebound.set_num_rows(input.len());
    let mut writer = prebound.column_mut(0).as_nullable_slice().unwrap();
    writer.write(input.iter().copied());

    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "2020-03-20 16:13:54.000\n2021-03-20 16:13:54.123\nNULL";
    assert_eq!(expected, actual);
}

/// Insert values into a varbinary column using a columnar buffer
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] different binary text representation
// #[test_case(SQLITE_3; "SQLite 3")] different binary text representation
fn columnar_insert_varbinary(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARBINARY(13)"])
        .build(profile)
        .unwrap();
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    // Fill buffer with values
    let desc = BufferDesc::Binary { length: 5 };
    let mut prebound = prepared.into_column_inserter(4, [desc]).unwrap();
    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];
    prebound.set_num_rows(input.len());

    let mut writer = prebound.column_mut(0).as_bin_view().unwrap();
    // Reset length to make room for `Hello, World!`.
    writer.ensure_max_element_length(13, 0).unwrap();
    writer.set_cell(0, Some("Hello".as_bytes()));
    writer.set_cell(1, Some("World".as_bytes()));
    writer.set_cell(2, None);
    writer.set_cell(3, Some("Hello, World!".as_bytes()));

    // Bind buffer and insert values.
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "48656C6C6F\n576F726C64\nNULL\n48656C6C6F2C20576F726C6421";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn columnar_insert_varchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["VARCHAR(13)"])
        .build(profile)
        .unwrap();
    let prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    let desc = BufferDesc::Text {
        // Buffer size purposefully chosen too small, so we would get a panic if `set_max_len` would
        // not work.
        max_str_len: 5,
    };
    let mut prebound = prepared.into_column_inserter(4, [desc]).unwrap();
    // Fill buffer with values
    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];

    prebound.set_num_rows(input.len());
    let mut col_view = prebound.column_mut(0).as_text_view().unwrap();
    // Reset length to make room for `Hello, World!`.
    col_view.ensure_max_element_length(13, 0).unwrap();
    col_view.set_cell(0, Some("Hello".as_bytes()));
    col_view.set_cell(1, Some("World".as_bytes()));
    col_view.set_cell(2, None);
    col_view.set_cell(3, Some("Hello, World!".as_bytes()));

    // Bind buffer and insert values.
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn columnar_insert_text_as_sql_integer(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();

    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let parameter_buffers = vec![WithDataType {
        value: TextColumn::try_new(4, 5).unwrap(),
        data_type: DataType::Integer,
    }];
    // Safety: all values in the buffer are safe for insertion
    let mut prebound =
        unsafe { prepared.unchecked_bind_columnar_array_parameters(parameter_buffers) }.unwrap();
    prebound.set_num_rows(4);
    let mut writer = prebound.column_mut(0);
    writer.set_cell(0, Some("1".as_bytes()));
    writer.set_cell(1, Some("2".as_bytes()));
    writer.set_cell(2, None);
    writer.set_cell(3, Some("4".as_bytes()));
    // Bind buffer and insert values.
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let actual = table.content_as_string(&conn);
    let expected = "1\n2\nNULL\n4";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn insert_str_as_sql_integer(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    let parameter = WithDataType {
        value: "42".into_parameter(),
        data_type: DataType::Integer,
    };
    conn.execute(&insert_sql, &parameter).unwrap();

    // Bind buffer and insert values.
    let actual = table.content_as_string(&conn);
    let expected = "42";
    assert_eq!(expected, actual);
}

/// Frankly more about testing edge cases in the API than a real use case.
#[test_case(MSSQL; "Microsoft SQL Server")]
fn var_char_slice_mut_as_input_output_parameter(profile: &Profile) {
    let conn = profile.connection().unwrap();
    conn.execute(
        r#"
        IF EXISTS (SELECT name FROM sysobjects WHERE name = 'TestInOutText')  
        DROP PROCEDURE TestInOutText  
        "#,
        (),
    )
    .unwrap();

    conn.execute(
        r#"CREATE PROCEDURE TestInOutText   
        @OutParm VARCHAR(15) OUTPUT   
        AS
        SELECT @OutParm = 'Hello, World!'  
        RETURN 99  
        "#,
        (),
    )
    .unwrap();

    let mut buffer = [b'a'; 15];
    let indicator = Indicator::Length(buffer.len());
    let mut param = VarCharSliceMut::from_buffer(&mut buffer, indicator);
    // This is akward! Maybe we can do something so we do not need to wrap it in (InOut, ) in order
    // to bind it as an input output parameter.
    conn.execute("{call TestInOutText(?)}", (InOut(&mut param),))
        .unwrap();

    let actual = str::from_utf8(&buffer).unwrap();
    let expected = "Hello, World!\0a";
    assert_eq!(expected, actual);
}

/// Inserts a Vector of integers using a generic implementation
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn insert_vec_column_using_generic_code(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "INTEGER"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    fn insert_tuple2_vec<A: Item, B: Item>(
        conn: &Connection<'_>,
        insert_sql: &str,
        source: &[(A, B)],
    ) {
        let mut prepared = conn.prepare(insert_sql).unwrap();
        // Number of rows submitted in one round trip
        let capacity = source.len();
        // We do not need a nullable buffer since elements of source are not optional
        let descriptions = [A::buffer_desc(false), B::buffer_desc(false)];
        let mut inserter = prepared.column_inserter(capacity, descriptions).unwrap();
        // We send everything in one go.
        inserter.set_num_rows(source.len());
        // Now let's copy the row based tuple into the columnar structure
        for (index, (a, b)) in source.iter().enumerate() {
            inserter.column_mut(0).as_slice::<A>().unwrap()[index] = *a;
            inserter.column_mut(1).as_slice::<B>().unwrap()[index] = *b;
        }
        inserter.execute().unwrap();
    }
    insert_tuple2_vec(&conn, &insert_sql, &[(1, 2), (3, 4), (5, 6)]);

    let actual = table.content_as_string(&conn);
    assert_eq!("1,2\n3,4\n5,6", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn adaptive_columnar_insert_varchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["VARCHAR(13)"])
        .build(profile)
        .unwrap();

    // Fill buffer with values
    let desc = BufferDesc::Text {
        // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
        // encounter larger inputs.
        max_str_len: 1,
    };
    let prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    // Input values to insert.
    let input = [
        Some(&b"Hi"[..]),
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];
    let mut prebound = prepared.into_column_inserter(input.len(), [desc]).unwrap();
    prebound.set_num_rows(input.len());
    let mut col_view = prebound.column_mut(0).as_text_view().unwrap();
    for (index, &text) in input.iter().enumerate() {
        col_view
            .ensure_max_element_length(input[index].map(|s| s.len()).unwrap_or(0), index)
            .unwrap();
        col_view.set_cell(index, text)
    }

    // Bind buffer and insert values.
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hi\nHello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(SQLITE_3; "SQLite 3")]
fn adaptive_columnar_insert_varbin(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["VARBINARY(13)"])
        .build(profile)
        .unwrap();
    // Fill buffer with values
    let desc = BufferDesc::Binary {
        // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
        // encounter larger inputs.
        length: 1,
    };
    // Input values to insert.
    let input = [
        Some(&b"Hi"[..]),
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];

    // Bind buffer and insert values.
    let prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    let mut prebound = prepared.into_column_inserter(input.len(), [desc]).unwrap();
    prebound.set_num_rows(input.len());
    let mut writer = prebound.column_mut(0).as_bin_view().unwrap();
    for (row_index, &bytes) in input.iter().enumerate() {
        // Resize and rebind the buffer if it turns out to be to small.
        writer
            .ensure_max_element_length(bytes.map(|b| b.len()).unwrap_or(0), row_index)
            .unwrap();
        writer.set_cell(row_index, bytes)
    }

    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "4869\n48656C6C6F\n576F726C64\nNULL\n48656C6C6F2C20576F726C6421";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] Type NVARCHAR does not exist
fn columnar_insert_wide_varchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["NVARCHAR(13)"])
        .build(profile)
        .unwrap();

    let prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    let input = [
        Some(U16String::from_str("Hello")),
        Some(U16String::from_str("World")),
        None,
        Some(U16String::from_str("Hello, World!")),
    ];
    // Fill buffer with values
    let desc = BufferDesc::WText { max_str_len: 20 };
    let mut prebound = prepared.into_column_inserter(input.len(), [desc]).unwrap();
    prebound.set_num_rows(input.len());
    let mut writer = prebound.column_mut(0).as_w_text_view().unwrap();
    for (row_index, value) in input
        .iter()
        .map(|opt| opt.as_ref().map(|ustring| ustring.as_slice()))
        .enumerate()
    {
        writer.set_cell(row_index, value)
    }
    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_integer_parameter(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER", "INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a,b) VALUES (1,1), (2,2);");
    conn.execute(&insert, ()).unwrap();

    let sql = format!("SELECT a FROM {table_name} where b=?;");
    let cursor = conn.execute(&sql, &1).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("1", actual);

    let cursor = conn.execute(&sql, &2).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("2", actual);
}

/// Learning test. Insert a string ending with \0. Not a terminating zero, but the payload ending
/// itself having zero as the last element.
#[test_case(MSSQL, "Hell\0"; "Microsoft SQL Server")]
#[test_case(MARIADB, "Hell\0"; "Maria DB")]
#[test_case(SQLITE_3, "Hell"; "SQLite 3")]
#[test_case(POSTGRES, "Hell"; "PostgreSQL")]
fn insert_string_ending_with_nul(profile: &Profile, expected: &str) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(10)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_insert();
    let param = "Hell\0";
    conn.execute(&sql, &param.into_parameter()).unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!(actual, expected);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn prepared_statement(profile: &Profile) {
    // Setup
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["VARCHAR(13)", "INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a,b) VALUES ('First', 1), ('Second', 2);");
    conn.execute(&insert, ()).unwrap();

    // Prepare the statement once
    let sql = format!("SELECT a FROM {table_name} where b=?;");
    let mut prepared = conn.prepare(&sql).unwrap();

    // Execute it two times with different parameters
    {
        let cursor = prepared.execute(&1).unwrap().unwrap();
        let title = cursor_to_string(cursor);
        assert_eq!("First", title);
    }

    {
        let cursor = prepared.execute(&2).unwrap().unwrap();
        let title = cursor_to_string(cursor);
        assert_eq!("Second", title);
    }
}

/// Reuse a preallocated handle, two times in a row.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn preallocated(profile: &Profile) {
    // Prepare the statement once
    let (conn, _table) = Given::new("Preallocated")
        .column_types(&["VARCHAR(10)"])
        .build(profile)
        .unwrap();
    let mut prealloc = conn.preallocate().unwrap();

    // Execute it two statements in a row. One INSERT, one SELECT.
    {
        let res = prealloc
            .execute("INSERT INTO Preallocated (a) VALUES ('Hello')", ())
            .unwrap();
        assert!(res.is_none());
    }

    {
        let cursor = prealloc
            .execute("SELECT a FROM Preallocated ORDER BY id", ())
            .unwrap()
            .unwrap();
        let actual = cursor_to_string(cursor);
        let expected = "Hello";
        assert_eq!(expected, actual);
    }
}

/// Reuse a preallocated handle. Verify that columns bound to the statement during a previous
/// execution are not dereferenced during a second one.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn preallocation_soundness(profile: &Profile) {
    // Prepare the statement once
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(10)"])
        .build(profile)
        .unwrap();
    let mut prealloc = conn.preallocate().unwrap();

    {
        let res = prealloc
            .execute(&table.sql_insert(), &"Hello".into_parameter())
            .unwrap();
        assert!(res.is_none());
    }

    {
        let cursor = prealloc
            .execute(&table.sql_all_ordered_by_id(), ())
            .unwrap()
            .unwrap();
        let actual = cursor_to_string(cursor);
        let expected = "Hello";
        assert_eq!(expected, actual);
    }

    {
        let mut cursor = prealloc
            .execute(&table.sql_all_ordered_by_id(), ())
            .unwrap()
            .unwrap();

        // Fetch without binding buffers. If columns would still be bound we might see an invalid
        // memory access.
        let _row = cursor.next_row().unwrap().unwrap();
        assert!(cursor.next_row().unwrap().is_none());
    }
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn integer_parameter_as_string(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER", "INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a,b) VALUES (1,1), (2,2);");
    conn.execute(&insert, ()).unwrap();

    let sql = format!("SELECT a FROM {table_name} where b=?;");
    let cursor = conn.execute(&sql, &"2".into_parameter()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("2", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_optional_integer_parameter(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER", "INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a,b) VALUES (1,1), (2,2);");
    conn.execute(&insert, ()).unwrap();

    let sql = format!("SELECT a FROM {table_name} where b=?;");

    let cursor = conn
        .execute(&sql, &Some(2).into_parameter())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("2", actual);

    let cursor = conn
        .execute(&sql, &None::<i32>.into_parameter())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] SQLite will work only if increasing length to VARCHAR(2).
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn non_ascii_char(profile: &Profile) {
    let table_name = table_name!();

    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(1)"])
        .unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?), (?);", table_name),
        (&"A".into_parameter(), &"Ü".into_parameter()),
    )
    .unwrap();

    let sql = format!("SELECT a FROM {} ORDER BY id;", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let output = cursor_to_string(cursor);
    assert_eq!("A\nÜ", output);
}

// This test will not work in CI on windows, due to non UTF local
// #[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] NVARCHAR does not exist
fn wchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["NVARCHAR(1)"])
        .build(profile)
        .unwrap();

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?), (?);"),
        (&"A".into_parameter(), &"Ü".into_parameter()),
    )
    .unwrap();

    let sql = format!("SELECT a FROM {table_name} ORDER BY id;");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    let desc = BufferDesc::WText { max_str_len: 1 };
    let row_set_buffer = ColumnarAnyBuffer::try_from_descs(2, iter::once(desc)).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = row_set_cursor.fetch().unwrap().unwrap();
    let col = batch.column(0);
    let wtext_col = col.as_w_text_view().unwrap();
    assert_eq!(2, wtext_col.len());
    assert_eq!(
        &U16String::from_str("A"),
        &U16Str::from_slice(wtext_col.get(0).unwrap())
    );
    assert_eq!(
        &U16String::from_str("Ü"),
        &U16Str::from_slice(wtext_col.get(1).unwrap())
    );
    assert!(row_set_cursor.fetch().unwrap().is_none());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn wchar_as_char(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["NVARCHAR(1)"])
        .build(profile)
        .unwrap();

    // With the wide character ODBC function calls passing the arguments as literals worked but with
    // the narrow version "INSERT INTO WCharAsChar (a) VALUES ('A'), ('Ü');" fails. It erroneously
    // assumes the data wouldn't fit into the column, probably because the binary length is 2. As
    // such confusing character and binary length.
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?), (?);"),
        (&"A".into_parameter(), &"Ü".into_parameter()),
    )
    .unwrap();

    assert_eq!("A\nÜ", table.content_as_string(&conn));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_str_parameter_to_char(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["CHAR(5)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    conn.execute(&insert_sql, &"Hello".into_parameter())
        .unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!("Hello", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_narrow_parameter_to_varchar(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(10)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    // String Slice
    conn.execute(&insert_sql, &Narrow("Hello").into_parameter())
        .unwrap();
    // Option slice
    conn.execute(&insert_sql, &Narrow(Some("Hello")).into_parameter())
        .unwrap();
    conn.execute(&insert_sql, &Narrow(None::<&str>).into_parameter())
        .unwrap();
    conn.execute(&insert_sql, &Some(Narrow("Hello")).into_parameter())
        .unwrap();
    conn.execute(&insert_sql, &None::<Narrow<&str>>.into_parameter())
        .unwrap();
    // String
    conn.execute(&insert_sql, &Narrow("Hello".to_string()).into_parameter())
        .unwrap();
    // Option String
    conn.execute(
        &insert_sql,
        &Narrow(Some("Hello".to_string())).into_parameter(),
    )
    .unwrap();
    conn.execute(&insert_sql, &Narrow(None::<String>).into_parameter())
        .unwrap();
    conn.execute(
        &insert_sql,
        &Some(Narrow("Hello".to_string())).into_parameter(),
    )
    .unwrap();
    conn.execute(&insert_sql, &None::<Narrow<String>>.into_parameter())
        .unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!(
        "Hello\nHello\nNULL\nHello\nNULL\nHello\nHello\nNULL\nHello\nNULL",
        actual
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_u16_str_parameter_to_char(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["CHAR(5)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    let hello = U16String::from_str("Hello");
    let hello = hello.as_ustr();
    conn.execute(&insert_sql, &hello.into_parameter()).unwrap();
    conn.execute(&insert_sql, &Some(hello).into_parameter())
        .unwrap();
    conn.execute(&insert_sql, &None::<&U16Str>.into_parameter())
        .unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!("Hello\nHello\nNULL", &actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bind_u16_string_parameter_to_char(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["CHAR(5)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    // Usecase: Create an owned parameter from a UTF-16 string
    let hello = U16String::from_str("Hello");
    conn.execute(&insert_sql, &hello.clone().into_parameter())
        .unwrap();
    conn.execute(&insert_sql, &Some(hello).into_parameter())
        .unwrap();
    conn.execute(&insert_sql, &None::<U16String>.into_parameter())
        .unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!("Hello\nHello\nNULL", &actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn two_parameters_in_tuple(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a) VALUES (1), (2), (3), (4);");
    conn.execute(&insert, ()).unwrap();

    let sql = format!("SELECT a FROM {table_name} where ? < a AND a < ? ORDER BY id;");

    let cursor = conn.execute(&sql, (&1, &4)).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("2\n3", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn heterogenous_parameters_in_array(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(13)"])
        .build(profile)
        .unwrap();
    let insert_sql = format!(
        "INSERT INTO {table_name} (a, b) VALUES (1, 'Hello'), (2, 'Hello'), (3, 'Hello'), (3, 'Hallo')"
    );
    conn.execute(&insert_sql, ()).unwrap();

    // Execute test
    let query = format!("SELECT a,b FROM {table_name} where  a > ? AND b = ?;");
    let params: [Box<dyn InputParameter>; 2] = [Box::new(2), Box::new("Hello".into_parameter())];
    let cursor = conn.execute(&query, &params[..]).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("3,Hello", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn column_names_iterator(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(13)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_all_ordered_by_id();
    let mut cursor = conn.execute(&sql, ()).unwrap().unwrap();

    let names: Vec<_> = cursor
        .column_names()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(&["a", "b"], names.as_slice());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn column_names_from_prepared_query(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(13)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_all_ordered_by_id();
    let mut prepared = conn.prepare(&sql).unwrap();
    let names: Vec<_> = prepared
        .column_names()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(&["a", "b"], names.as_slice());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn metadata_from_prepared_insert_query(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(13)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_insert();
    let mut prepared = conn.prepare(&sql).unwrap();
    assert_eq!(0, prepared.num_result_cols().unwrap());
}

#[test_case(MSSQL, &[
    ParameterDescription {data_type: DataType::Integer, nullability: Nullability::Nullable},
    ParameterDescription {
        data_type: DataType::Varchar { length: NonZeroUsize::new(13) },
        nullability: Nullability::Nullable
    }
]; "Microsoft SQL Server")]
#[test_case(MARIADB, &[
    ParameterDescription {
        data_type: DataType::Varchar { length: NonZeroUsize::new(25165824) },
        nullability: Nullability::Unknown
    },
    ParameterDescription {
        data_type: DataType::Varchar { length: NonZeroUsize::new(25165824) },
        nullability: Nullability::Unknown
    }
]; "Maria DB")]
// PostgrelSQL and SQLite 3 expose different behaviours with various platforms and drivers
fn describe_parameters_of_prepared_statement(
    profile: &Profile,
    expected: &[ParameterDescription; 2],
) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(13)"])
        .build(profile)
        .unwrap();
    let sql = format!("SELECT a, b FROM {table_name} WHERE a=? AND b=?;");
    let mut prepared = conn.prepare(&sql).unwrap();

    let parameter_descriptions = prepared
        .parameter_descriptions()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(expected.as_slice(), parameter_descriptions);
    assert_eq!(2, prepared.num_params().unwrap());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_with_text_buffer(profile: &Profile) {
    // Given
    let conn = profile
        .setup_empty_table("BulkInsertWithTextBuffer", &["VARCHAR(50)"])
        .unwrap();

    // When
    // Fill a text buffer with three rows, and insert them into the database.
    let prepared = conn
        .prepare("INSERT INTO BulkInsertWithTextBuffer (a) Values (?)")
        .unwrap();
    let mut prebound = prepared
        .into_text_inserter(5, [50].iter().copied())
        .unwrap();
    prebound
        .append(["England"].iter().map(|s| Some(s.as_bytes())))
        .unwrap();
    prebound
        .append(["France"].iter().map(|s| Some(s.as_bytes())))
        .unwrap();
    prebound
        .append(["Germany"].iter().map(|s| Some(s.as_bytes())))
        .unwrap();
    prebound.execute().unwrap();

    // Then
    // Assert that the table contains the rows that have just been inserted.
    let expected = "England\nFrance\nGermany";
    let cursor = conn
        .execute("SELECT a FROM BulkInsertWithTextBuffer ORDER BY id;", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_with_columnar_buffer(profile: &Profile) {
    let conn = profile
        .setup_empty_table("BulkInsertWithColumnarBuffer", &["VARCHAR(50)", "INTEGER"])
        .unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let prepared = conn
        .prepare("INSERT INTO BulkInsertWithColumnarBuffer (a,b) Values (?,?)")
        .unwrap();
    let description = [
        BufferDesc::Text { max_str_len: 50 },
        BufferDesc::I32 { nullable: true },
    ];

    let mut prebound = prepared.into_column_inserter(5, description).unwrap();

    prebound.set_num_rows(3);
    // Fill first column with text
    let mut col_view = prebound.column_mut(0).as_text_view().unwrap();
    col_view.set_cell(0, Some("England".as_bytes()));
    col_view.set_cell(1, Some("France".as_bytes()));
    col_view.set_cell(2, Some("Germany".as_bytes()));

    // Fill second column with integers
    let input = [1, 2, 3];
    let mut col = prebound.column_mut(1).as_nullable_slice::<i32>().unwrap();
    col.write(input.iter().map(|&i| Some(i)));

    prebound.execute().unwrap();

    // Assert that the table contains the rows that have just been inserted.
    let expected = "England,1\nFrance,2\nGermany,3";

    let cursor = conn
        .execute(
            "SELECT a,b FROM BulkInsertWithColumnarBuffer ORDER BY id;",
            (),
        )
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_with_multiple_batches(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(50)", "INTEGER"])
        .unwrap();

    // When

    // First batch

    // Fill a buffer with three rows, and insert them into the database.
    let prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a,b) Values (?,?)"))
        .unwrap();
    let description = [
        BufferDesc::Text { max_str_len: 50 },
        BufferDesc::I32 { nullable: true },
    ];
    let mut prebound = prepared.into_column_inserter(5, description).unwrap();
    prebound.set_num_rows(3);
    // Fill first column with text
    let mut col_view = prebound.column_mut(0).as_text_view().unwrap();
    col_view.set_cell(0, Some("England".as_bytes()));
    col_view.set_cell(1, Some("France".as_bytes()));
    col_view.set_cell(2, Some("Germany".as_bytes()));

    // Fill second column with integers
    let input = [1, 2, 3];
    let mut col = prebound.column_mut(1).as_nullable_slice::<i32>().unwrap();
    col.write(input.iter().map(|&i| Some(i)));

    prebound.execute().unwrap();

    // Second Batch

    // Fill a buffer with one row, and insert them into the database.
    prebound.set_num_rows(1);
    // Fill first column with text
    let mut col_view = prebound.column_mut(0).as_text_view().unwrap();
    col_view.set_cell(0, Some("Spain".as_bytes()));

    // Fill second column with integers
    let input = [4];
    let mut col = prebound.column_mut(1).as_nullable_slice::<i32>().unwrap();
    col.write(input.iter().map(|&i| Some(i)));

    prebound.execute().unwrap();

    // Then

    // Assert that the table contains the rows that have just been inserted.
    let expected = "England,1\nFrance,2\nGermany,3\nSpain,4";

    let cursor = conn
        .execute(&format!("SELECT a,b FROM {table_name} ORDER BY id;"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn send_connection(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();

    // Insert in one thread, query in another, using the same connection.
    let insert_sql = format!("INSERT INTO {table_name} (a) VALUES (1),(2),(3)");
    conn.execute(&insert_sql, ()).unwrap();

    let actual = thread::scope(|s| {
        let handle = s.spawn(|| move || table.content_as_string(&conn));
        handle.join().unwrap()()
    });
    assert_eq!("1\n2\n3", actual)
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn parameter_option_strings(profile: &Profile) {
    let conn = profile
        .setup_empty_table("ParameterOptionStr", &["VARCHAR(50)"])
        .unwrap();
    let sql = "INSERT INTO ParameterOptionStr (a) VALUES (?);";
    let mut prepared = conn.prepare(sql).unwrap();
    prepared.execute(&None::<&str>.into_parameter()).unwrap();
    prepared.execute(&Some("Bernd").into_parameter()).unwrap();
    prepared.execute(&None::<String>.into_parameter()).unwrap();
    prepared
        .execute(&Some("Hello".to_string()).into_parameter())
        .unwrap();

    let cursor = conn
        .execute("SELECT a FROM ParameterOptionStr ORDER BY id", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "NULL\nBernd\nNULL\nHello";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Different string representation of binary data
// #[test_case(SQLITE_3; "SQLite 3")] Different string representation of binary data
// #[test_case(POSTGRES; "PostgreSQL")] Varbinary does not exist
fn parameter_option_bytes(profile: &Profile) {
    let table_name = table_name!();

    let conn = profile
        .setup_empty_table(&table_name, &["VARBINARY(50)"])
        .unwrap();
    let sql = format!("INSERT INTO {table_name} (a) VALUES (?);");
    let mut prepared = conn.prepare(&sql).unwrap();
    prepared.execute(&None::<&[u8]>.into_parameter()).unwrap();
    prepared
        .execute(&Some(&[1, 2, 3][..]).into_parameter())
        .unwrap();
    prepared.execute(&None::<Vec<u8>>.into_parameter()).unwrap();
    prepared
        .execute(&Some(vec![1, 2, 3]).into_parameter())
        .unwrap();

    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "NULL\n010203\nNULL\n010203";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn parameter_varchar_512(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_insert();
    let mut prepared = conn.prepare(&sql).unwrap();

    prepared.execute(&VarCharArray::<512>::NULL).unwrap();
    prepared
        .execute(&VarCharArray::<512>::new(b"Bernd"))
        .unwrap();

    // Then
    let actual = table.content_as_string(&conn);
    let expected = "NULL\nBernd";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Different string representation of binary data
// #[test_case(SQLITE_3; "SQLite 3")] Different string representation of binary data
// #[test_case(POSTGRES; "PostgreSQL")] Varbinary does not exist
fn parameter_varbinary_512(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARBINARY(50)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_insert();
    let mut prepared = conn.prepare(&sql).unwrap();

    prepared.execute(&VarBinaryArray::<512>::NULL).unwrap();
    prepared
        .execute(&VarBinaryArray::<512>::new(&[1, 2, 3]))
        .unwrap();

    let actual = table.content_as_string(&conn);
    let expected = "NULL\n010203";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn parameter_cstr(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    let sql = table.sql_insert();
    let mut prepared = conn.prepare(&sql).unwrap();

    let param = CString::new("Hello, World!").unwrap();

    prepared.execute(&param).unwrap();
    prepared.execute(param.as_c_str()).unwrap();

    let actual = table.content_as_string(&conn);
    let expected = "Hello, World!\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn read_into_columnar_buffer(profile: &Profile) {
    let conn = profile
        .setup_empty_table("ReadIntoColumnarBuffer", &["INTEGER", "VARCHAR(20)"])
        .unwrap();
    conn.execute(
        "INSERT INTO ReadIntoColumnarBuffer (a, b) VALUES (42, 'Hello, World!')",
        (),
    )
    .unwrap();

    // Get cursor querying table
    let cursor = conn
        .execute("SELECT a,b FROM ReadIntoColumnarBuffer ORDER BY id", ())
        .unwrap()
        .unwrap();

    let buffer_description = [
        BufferDesc::I32 { nullable: true },
        BufferDesc::Text { max_str_len: 20 },
    ];
    let buffer = ColumnarAnyBuffer::try_from_descs(20, buffer_description.iter().copied()).unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    // Assert existence of first batch
    let batch = cursor.fetch().unwrap().unwrap();

    let mut col = i32::as_nullable_slice(batch.column(0)).unwrap();
    assert_eq!(Some(&42), col.next().unwrap());
    assert_eq!(
        Some(&b"Hello, World!"[..]),
        batch.column(1).as_text_view().unwrap().get(0)
    );
    // Assert that there is no second batch.
    assert!(cursor.fetch().unwrap().is_none());
}

/// In use cases there the user supplies the query it may be necessary to ignore one column then
/// binding the buffers. This test constructs a result set with 3 columns and ignores the second
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn ignore_output_column(profile: &Profile) {
    let conn = profile
        .setup_empty_table("IgnoreOutputColumn", &["INTEGER", "INTEGER", "INTEGER"])
        .unwrap();
    let cursor = conn
        .execute("SELECT a, b, c FROM IgnoreOutputColumn", ())
        .unwrap()
        .unwrap();

    let bd = BufferDesc::I32 { nullable: true };
    let buffer = ColumnarAnyBuffer::from_descs_and_indices(20, [(1, bd), (3, bd)].iter().copied());
    let mut cursor = cursor.bind_buffer(buffer).unwrap();

    // Assert that there is no batch.
    assert!(cursor.fetch().unwrap().is_none());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
fn output_parameter(profile: &Profile) {
    let conn = profile.connection().unwrap();
    conn.execute(
        r#"
        IF EXISTS (SELECT name FROM sysobjects WHERE name = 'TestOutputParam')  
        DROP PROCEDURE TestOutputParam  
        "#,
        (),
    )
    .unwrap();

    conn.execute(
        r#"CREATE PROCEDURE TestOutputParam   
        @OutParm int OUTPUT   
        AS
        SELECT @OutParm = @OutParm + 5  
        RETURN 99  
        "#,
        (),
    )
    .unwrap();

    let mut ret = Nullable::<i32>::null();
    let mut param = Nullable::<i32>::new(7);

    conn.execute(
        "{? = call TestOutputParam(?)}",
        (Out(&mut ret), InOut(&mut param)),
    )
    .unwrap();

    // See magic numbers hardcoded in setup.sql
    assert_eq!(Some(99), ret.into_opt());
    assert_eq!(Some(7 + 5), param.into_opt());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn manual_commit_mode(profile: &Profile) {
    let conn = profile
        .setup_empty_table("ManualCommitMode", &["INTEGER"])
        .unwrap();

    // Manual commit mode needs to be explicitly enabled, since autocommit mode is default.
    conn.set_autocommit(false).unwrap();

    // Insert a value into the table.
    conn.execute("INSERT INTO ManualCommitMode (a) VALUES (5);", ())
        .unwrap();

    // But rollback the transaction immediately.
    conn.rollback().unwrap();

    // Check that the table is still empty.
    let cursor = conn
        .execute("SELECT a FROM ManualCommitMode", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!(actual, "");

    // Insert a value into the table.
    conn.execute("INSERT INTO ManualCommitMode (a) VALUES (42);", ())
        .unwrap();

    // This time we commit the transaction, though.
    conn.commit().unwrap();

    // Check that the table contains the value.
    let cursor = conn
        .execute("SELECT a FROM ManualCommitMode", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!(actual, "42");

    // Close transaction opened by SELECT Statement
    conn.commit().unwrap();
}

/// This test checks the behaviour if a connections goes out of scope with a transaction still
/// open.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn unfinished_transaction(profile: &Profile) {
    let conn = profile
        .setup_empty_table("UnfinishedTransaction", &["INTEGER"])
        .unwrap();

    // Manual commit mode needs to be explicitly enabled, since autocommit mode is default.
    conn.set_autocommit(false).unwrap();

    // Insert a value into the table.
    conn.execute("INSERT INTO UnfinishedTransaction (a) VALUES (5);", ())
        .unwrap();
}

/// Test behavior of strings with interior nul
#[test_case(MSSQL, "a\0b"; "Microsoft SQL Server")]
#[test_case(MARIADB, "a\0b"; "Maria DB")]
#[test_case(SQLITE_3, "a"; "SQLite 3")]
#[test_case(POSTGRES, "a"; "PostgreSQL")]
fn interior_nul(profile: &Profile, expected: &str) {
    let conn = profile
        .setup_empty_table("InteriorNul", &["VARCHAR(10)"])
        .unwrap();

    conn.execute(
        "INSERT INTO InteriorNul (a) VALUES (?);",
        &"a\0b".into_parameter(),
    )
    .unwrap();
    let cursor = conn
        .execute("SELECT A FROM InteriorNul;", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!(expected, actual);
}

/// Use get_data to retrieve an integer
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn get_data_int(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (42),(NULL)"),
        (),
    )
    .unwrap();
    let sql = table.sql_all_ordered_by_id();

    let mut cursor = conn.execute(&sql, ()).unwrap().unwrap();

    let mut actual = Nullable::<i32>::null();
    // First value is 42
    let mut row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut actual).unwrap();
    assert_eq!(Some(42), actual.into_opt());

    // Second row contains a NULL
    row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut actual).unwrap();
    assert_eq!(None, actual.into_opt());

    // Cursor has reached its end
    assert!(cursor.next_row().unwrap().is_none())
}

#[test_case(MSSQL, "DATETIME2"; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")]
fn get_data_timestamp(profile: &Profile, timestamp_type: &str) {
    let table_name = table_name!();
    let types = [timestamp_type];
    let (conn, table) = Given::new(&table_name)
        .column_types(&types)
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"2022-11-09 06:17:00".into_parameter())
        .unwrap();
    let sql = table.sql_all_ordered_by_id();

    let mut cursor = conn.execute(&sql, ()).unwrap().unwrap();

    let mut actual = Timestamp::default();
    let mut row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut actual).unwrap();

    assert_eq!(
        Timestamp {
            year: 2022,
            month: 11,
            day: 9,
            hour: 6,
            minute: 17,
            second: 0,
            fraction: 0
        },
        actual
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// SQLITE has a bug. It does not return an error but simply fills the integer with `0`. At least on
// windows this is the case.
// #[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] Return generic error HY000 instead
fn get_data_int_null(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &None::<i32>.into_parameter())
        .unwrap();
    let sql = table.sql_all_ordered_by_id();

    let mut cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut actual = 0i32;
    // Second row contains a NULL
    let mut row = cursor.next_row().unwrap().unwrap();
    // Failure due to the value being NULL, but i32 not being NULLABLE
    let result = row.get_data(1, &mut actual);

    assert!(result.is_err());
    let error = result.unwrap_err();
    assert!(matches!(error, Error::UnableToRepresentNull(_)));
    // Cursor has reached its end
    assert!(cursor.next_row().unwrap().is_none())
}

/// Use get_data to retrieve a string
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn get_data_string(profile: &Profile) {
    let table_name = table_name!();

    let conn = profile
        .setup_empty_table(&table_name, &["Varchar(50)"])
        .unwrap();

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES ('Hello, World!'), (NULL)"),
        (),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = VarCharArray::<32>::NULL;

    row.get_data(1, &mut actual).unwrap();
    assert_eq!(Some("Hello, World!"), actual.as_str().unwrap());

    // second row
    row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut actual).unwrap();
    assert!(actual.as_bytes().is_none());

    // Cursor has reached its end
    assert!(cursor.next_row().unwrap().is_none())
}

/// Use get_text to retrieve a string. Use a buffer which is one terminating zero short to get the
/// entire value.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn get_text(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["Varchar(50)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"Hello, World!".into_parameter())
        .unwrap();
    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    // We want to hit an edge case there there has been a sign error then calculating buffer size
    // with terminating zero.
    let mut actual = Vec::with_capacity("Hello, World!".len() - 1);
    let is_not_null = row.get_text(1, &mut actual).unwrap();

    assert!(is_not_null);
    assert_eq!(&b"Hello, World!"[..], &actual);
}

/// Use get_text to retrieve a string. Use a buffer which is one terminating zero short to get the
/// entire value.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn get_wide_text(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["Varchar(50)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"Hello, World!".into_parameter())
        .unwrap();
    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    // We want to hit an edge case there there has been a sign error then calculating buffer size
    // with terminating zero.
    let mut actual = Vec::with_capacity("Hello, World!".len() - 1);
    let is_not_null = row.get_wide_text(1, &mut actual).unwrap();

    let actual = U16String::from_vec(actual).to_string().unwrap();
    assert!(is_not_null);
    assert_eq!("Hello, World!", &actual);
}

/// Use get_data to retrieve a binary data
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] Varbinary does not exist
fn get_data_binary(profile: &Profile) {
    let table_name = table_name!();

    let conn = profile
        .setup_empty_table(&table_name, &["Varbinary(50)"])
        .unwrap();

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?), (NULL)"),
        &[1u8, 2, 3].into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = VarBinaryArray::<32>::NULL;

    row.get_data(1, &mut actual).unwrap();
    assert_eq!(Some(&[1u8, 2, 3][..]), actual.as_bytes());

    // second row
    row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut actual).unwrap();
    assert!(actual.as_bytes().is_none());

    // Cursor has reached its end
    assert!(cursor.next_row().unwrap().is_none())
}

/// Test insertion and retrieving of large string values using get_data. Try to provoke
/// `SQL_NO_TOTAL` as a return value in the indicator buffer.
#[test_case(MSSQL, "Varchar(max)"; "Microsoft SQL Server")]
#[test_case(MARIADB, "Text"; "Maria DB")]
#[test_case(SQLITE_3, "Text"; "SQLite 3")]
#[test_case(POSTGRES, "Text"; "PostgreSQL")]
fn large_strings(profile: &Profile, column_type: &str) {
    let table_name = table_name!();
    let column_types = [column_type];
    let (conn, table) = Given::new(&table_name)
        .column_types(&column_types)
        .build(profile)
        .unwrap();
    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();
    conn.execute(&table.sql_insert(), &input.as_str().into_parameter())
        .unwrap();

    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut buf = VarCharArray::<32>::NULL;
    let mut actual = String::new();
    loop {
        row.get_data(1, &mut buf).unwrap();
        actual += buf.as_str().unwrap().unwrap();
        if buf.is_complete() {
            break;
        }
    }

    assert_eq!(input, actual);
}

/// Test insertion and retrieving of large binary values using get_text. Try to provoke
/// `SQL_NO_TOTAL` as a return value in the indicator buffer.
#[test_case(POSTGRES, "BYTEA"; "PostgreSQL")]
fn large_binary_get_text(profile: &Profile, column_type: &str) {
    let table_name = table_name!();
    let column_types = [column_type];
    let (conn, table) = Given::new(&table_name)
        .column_types(&column_types)
        .build(profile)
        .unwrap();
    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();
    conn.execute(&table.sql_insert(), &input.as_str().into_parameter())
        .unwrap();

    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = Vec::new();
    row.get_text(1, &mut actual).unwrap();

    let expected = "61".repeat(2000);
    assert_eq!(expected, String::from_utf8(actual).unwrap());
}

/// Test insertion and retrieving of large string values using get_text. Try to provoke
/// `SQL_NO_TOTAL` as a return value in the indicator buffer.
#[test_case(MSSQL, "Varchar(max)"; "Microsoft SQL Server")]
#[test_case(MARIADB, "Text"; "Maria DB")]
#[test_case(SQLITE_3, "Text"; "SQLite 3")]
#[test_case(POSTGRES, "Text"; "PostgreSQL")]
fn large_strings_get_text(profile: &Profile, column_type: &str) {
    let table_name = table_name!();
    let column_types = [column_type];
    let (conn, table) = Given::new(&table_name)
        .column_types(&column_types)
        .build(profile)
        .unwrap();
    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();
    conn.execute(&table.sql_insert(), &input.as_str().into_parameter())
        .unwrap();

    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = Vec::new();
    row.get_text(1, &mut actual).unwrap();

    assert_eq!(input, String::from_utf8(actual).unwrap());
}

/// Retrieving of fixed size string values using get_text. Try to provoke `SQL_NO_TOTAL` as a return
/// value in the indicator buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn fixed_strings_get_text(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["Char(10)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"1234567890".into_parameter())
        .unwrap();

    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = vec![0; 1]; // Initial buffer too small.
    row.get_text(1, &mut actual).unwrap();

    assert_eq!("1234567890", String::from_utf8(actual).unwrap());
}

/// Retrieving of short string values using get_data. This also helps to assert that we correctly
/// shorten the vectors length if the capacity of the originally passed in vector had been larger
/// than the retrieved string.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn short_strings_get_text(profile: &Profile) {
    let conn = profile
        .setup_empty_table("ShortStringsGetText", &["Varchar(15)"])
        .unwrap();

    conn.execute(
        "INSERT INTO ShortStringsGetText (a) VALUES ('Hello, World!')",
        (),
    )
    .unwrap();

    let mut cursor = conn
        .execute("SELECT a FROM ShortStringsGetText ORDER BY id", ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();

    // Make initial buffer larger than the string we want to fetch.
    let mut actual = Vec::with_capacity(100);

    row.get_text(1, &mut actual).unwrap();

    assert_eq!("Hello, World!", std::str::from_utf8(&actual).unwrap());
}

/// Retrieving of short binary values using get_data. This also helps to assert that we correctly
/// shorten the vectors length if the capacity of the originally passed in vector had been larger
/// than the retrieved payload.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] Does not support Varbinary syntax
fn short_get_binary(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["Varbinary(15)"])
        .unwrap();

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?)"),
        &[1u8, 2, 3].into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();

    // Make initial buffer larger than the string we want to fetch.
    let mut actual = Vec::with_capacity(100);

    row.get_binary(1, &mut actual).unwrap();

    assert_eq!(&[1u8, 2, 3][..], &actual);
}

/// Test insertion and retrieving of values larger than the initially provided buffer using
/// get_binary.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Does not support Varbinary(max) syntax
// #[test_case(SQLITE_3; "SQLite 3")] Does not support Varbinary(max) syntax
// #[test_case(POSTGRES; "PostgreSQL")] Does not support Varbinary(max) syntax
fn large_get_binary(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["Varbinary(max)"])
        .unwrap();

    let input = vec![42; 2000];

    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (?)"),
        &input.as_slice().into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = Vec::new();

    row.get_binary(1, &mut actual).unwrap();

    assert_eq!(input, actual);
}

/// Demonstrates applying an upper limit to a text buffer and detecting truncation.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn capped_text_buffer(profile: &Profile) {
    let table_name = table_name!();

    // Prepare table content
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(13)"])
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES ('Hello, World!');"),
        (),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();

    let row_set_buffer = TextRowSet::for_cursor(1, &mut cursor, Some(5)).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = row_set_cursor.fetch().unwrap().unwrap();
    let field = batch.at_as_str(0, 0).unwrap().unwrap();
    // Only 'Hello' from 'Hello, World!' remains due to upper limit.
    assert_eq!("Hello", field);
    // Indicator reports actual length of the field on the database.
    assert_eq!(Indicator::Length(13), batch.indicator_at(0, 0));
    // Assert that maximum length is reported correctly.
    assert_eq!(5, batch.max_len(0));
}

/// Use a truncated varchar output as input.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn use_truncated_output_as_input(profile: &Profile) {
    let table_name = table_name!();

    // Prepare table content
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(13)"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES ('Hello, World!');"),
        (),
    )
    .unwrap();

    // Query 'Hello, World!' From the DB in a buffer with size 5. This should give us a Hello minus
    // the letter 'o' since we also need space for a terminating zero. => 'Hell'.
    let mut buf = VarCharArray::<5>::NULL;
    let query = format!("SELECT a FROM {table_name}");
    let mut cursor = conn.execute(&query, ()).unwrap().unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut buf).unwrap();
    assert_eq!("Hell", buf.as_str().unwrap().unwrap());
    assert!(!buf.is_complete());
    drop(cursor);

    let insert = table.sql_insert();
    buf.hide_truncation();
    conn.execute(&insert, &buf).unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!("Hello, World!\nHell", actual);
}

/// Assert that we prevent binding `VarCharSlice` buffers with truncated values as input parameters
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
#[should_panic(expected = "Truncated values must not be used be bound as input parameters.")]
fn insert_truncated_value(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();

    let memory = "Hello\0INVALID MEMORY\0";
    // Contains hello plus terminating zero.
    let valid = &memory.as_bytes()[..6];
    // Truncated value.
    let parameter = VarCharSlice::from_buffer(valid, Indicator::Length(memory.len()));
    let result = conn.execute(&table.sql_insert(), &parameter);

    match result {
        Err(e) => {
            // Failing is fine, especially with an error indicating truncation.
            eprintln!("{e}")
        }
        Ok(None) => {
            // If this was successful we should make sure we did not insert 'INVALID MEMORY' into
            // the database. The better database drivers do not do this, and this could be seen as
            // wrong, but we are only interested in unsafe behaviour.
            assert_eq!("Hello", table.content_as_string(&conn))
        }
        _ => panic!("Unexpected cursor"),
    }
}

/// Assert that we prevent binding `VarCharArray` buffers with truncated values as input parameters
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
#[should_panic(expected = "Truncated values must not be used be bound as input parameters.")]
fn insert_truncated_var_char_array(profile: &Profile) {
    let table_name = table_name!();

    // Prepare table content
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();

    let memory = "Hello, World!";
    // Truncated value. Buffer can only hold 'Hello'
    let parameter = VarCharArray::<5>::new(memory.as_bytes());
    let _ = conn.execute(&table.sql_insert(), &parameter);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn arbitrary_input_parameters(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(20)", "INT"])
        .build(profile)
        .unwrap();

    let insert_statement = format!("INSERT INTO {table_name} (a, b) VALUES (?, ?);");
    let param_a: Box<dyn InputParameter> = Box::new("Hello, World!".to_string().into_parameter());
    let param_b: Box<dyn InputParameter> = Box::new(42.into_parameter());
    let parameters = vec![param_a, param_b];

    conn.execute(&insert_statement, parameters.as_slice())
        .unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!("Hello, World!,42", actual)
}

/// Ensures access to driver and data source info is synchronized correctly when multiple threads
/// attempt to query it at the same time. First, we query the list of the known drivers and data
/// sources on the main thread. Then we spawn multiple threads that attempt to query these lists in
/// parallel. Finally we compare the lists to ensure they match the list we found on the main
/// thread.
#[test]
fn synchronized_access_to_driver_and_data_source_info() {
    let expected_drivers = environment().unwrap().drivers().unwrap();
    let expected_data_sources = environment().unwrap().data_sources().unwrap();

    const NUM_THREADS: usize = 5;
    let threads = iter::repeat(())
        .take(NUM_THREADS)
        .map(|_| {
            let expected_drivers = expected_drivers.clone();
            let expected_data_sources = expected_data_sources.clone();

            thread::spawn(move || {
                let drivers = environment().unwrap().drivers().unwrap();
                assert_eq!(expected_drivers, drivers);
                let data_sources_for_thread = environment().unwrap().data_sources().unwrap();
                assert_eq!(expected_data_sources, data_sources_for_thread);
            })
        })
        .collect::<Vec<_>>();

    for handle in threads {
        handle.join().unwrap();
    }
}

// #[test_case(MSSQL; "Microsoft SQL Server")] Linux driver allocates 42 GiB
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn insert_large_texts(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["Text"])
        .build(profile)
        .unwrap();

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");

    // Large data with 8000 characters.
    let data = String::from_utf8(vec![b'a'; 8000]).unwrap();

    conn.execute(&insert, &data.as_str().into_parameter())
        .unwrap();

    let actual = table.content_as_string(&conn);
    assert_eq!(data.len(), actual.len());
    assert!(data == actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn insert_i64_in_bulk(profile: &Profile) -> Result<(), odbc_api::Error> {
    // Given
    let table_name = table_name!();
    let (conn, table) = profile.create_table(&table_name, &["BIGINT"], &["a"])?;

    // When
    let prepared = conn.prepare(&table.sql_insert())?;
    let mut inserter = prepared.into_column_inserter(2, [BufferDesc::I64 { nullable: true }])?;
    inserter.set_num_rows(2);
    let mut view = inserter.column_mut(0).as_nullable_slice().unwrap();
    view.set_cell(0, Some(1i64));
    view.set_cell(1, Some(2));
    inserter.execute()?;

    // Then
    let actual = table.content_as_string(&conn);
    assert_eq!("1\n2", actual);

    Ok(())
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn send_long_data_binary_vec(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &[profile.blob_type])
        .unwrap();

    // Large vector with successive numbers. It's too large to send to the database in one go.
    let input: Vec<_> = (0..12000).map(|i| (i % 256) as u8).collect();

    let mut blob = BlobSlice::from_byte_slice(&input);

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    conn.execute(&insert, &mut blob.as_blob_param()).unwrap();

    // Query value just streamed into the DB and compare it with the input.
    let select = format!("SELECT a FROM {table_name}");
    let mut result = conn.execute(&select, ()).unwrap().unwrap();
    let mut row = result.next_row().unwrap().unwrap();
    let mut output = Vec::new();
    row.get_binary(1, &mut output).unwrap();

    assert_eq!(input, output);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn send_blob_as_part_of_tuplebinary_vec(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["INTEGER", profile.blob_type])
        .unwrap();
    // Large vector with successive numbers. It's too large to send to the database in one go.
    let input: Vec<_> = (0..12000).map(|i| (i % 256) as u8).collect();

    // When
    let mut blob = BlobSlice::from_byte_slice(&input);
    let insert = format!("INSERT INTO {table_name} (a,b) VALUES (?,?)");
    conn.execute(&insert, (&42i32, &mut blob.as_blob_param()))
        .unwrap();

    // Then
    // Query value just streamed into the DB and compare it with the input.
    let select = format!("SELECT a,b FROM {table_name}");
    let mut result = conn.execute(&select, ()).unwrap().unwrap();
    let mut row = result.next_row().unwrap().unwrap();
    let mut output_a: i32 = 0;
    let mut output_b = Vec::new();
    row.get_data(1, &mut output_a).unwrap();
    row.get_binary(2, &mut output_b).unwrap();

    assert_eq!(42, output_a);
    assert_eq!(input, output_b);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn send_long_data_string(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile.setup_empty_table(&table_name, &["Text"]).unwrap();

    // Large vector with successive numbers. It's too large to send to the database in one go.
    let input: String = (0..1200).map(|_| "abcdefghijklmnopqrstuvwxyz").collect();

    let mut blob = BlobSlice::from_text(&input);

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    conn.execute(&insert, &mut blob.as_blob_param()).unwrap();

    // Query value just streamed into the DB and compare it with the input.
    let select = format!("SELECT a FROM {table_name}");
    let mut result = conn.execute(&select, ()).unwrap().unwrap();
    let mut row = result.next_row().unwrap().unwrap();
    let mut output = Vec::new();
    row.get_text(1, &mut output).unwrap();
    let output = String::from_utf8(output).unwrap();

    assert_eq!(input, output);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] SQLite does not write anything to the database if there is no
// size hint given
#[test_case(POSTGRES; "PostgreSQL")]
fn send_long_data_binary_read(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &[profile.blob_type])
        .unwrap();

    // Large vector with successive numbers. It's too large to send to the database in one go.
    let input: Vec<_> = (0..12000).map(|i| (i % 256) as u8).collect();
    let read = io::Cursor::new(&input);

    let mut blob = BlobRead::with_upper_bound(read, 14000);

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    conn.execute(&insert, &mut blob.as_blob_param()).unwrap();

    // Query value just streamed into the DB and compare it with the input.
    let select = format!("SELECT a FROM {table_name}");
    let mut result = conn.execute(&select, ()).unwrap().unwrap();
    let mut row = result.next_row().unwrap().unwrap();
    let mut output = Vec::new();
    row.get_binary(1, &mut output).unwrap();

    assert_eq!(input, output);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn send_long_data_binary_file(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &[profile.blob_type])
        .unwrap();

    // Large vector with successive numbers. It's too large to send to the database in one go.
    let input: Vec<_> = (0..12000).map(|i| (i % 256) as u8).collect();

    let mut file = NamedTempFile::new().unwrap();
    file.write_all(&input).unwrap();

    let path = file.into_temp_path();

    let mut blob = BlobRead::from_path(&path).unwrap();

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");
    conn.execute(&insert, &mut blob.as_blob_param()).unwrap();

    // Query value just streamed into the DB and compare it with the input.
    let select = format!("SELECT a FROM {table_name}");
    let mut result = conn.execute(&select, ()).unwrap().unwrap();
    let mut row = result.next_row().unwrap().unwrap();
    let mut output = Vec::new();
    row.get_binary(1, &mut output).unwrap();

    assert_eq!(input, output);
}

/// Demonstrate how to strip abstractions and access raw functionality as exposed by `odbc-sys`.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn escape_hatch(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();

    let preallocated = conn.preallocate().unwrap();
    let mut statement = preallocated.into_statement();

    statement.reset_parameters().unwrap();

    unsafe {
        let select_utf8 = table.sql_all_ordered_by_id();
        // TableName does not exist, but we won't execute the query anyway
        let select = U16String::from_str(&select_utf8);
        let ret = sys::SQLPrepareW(
            statement.as_sys(),
            select.as_ptr(),
            select.len().try_into().unwrap(),
        );
        assert_eq!(ret, sys::SqlReturn::SUCCESS);
    }

    // If we use `.into_sys` we need to drop the handle manually
    let hstmt = statement.into_sys();
    unsafe {
        let ret = sys::SQLFreeHandle(sys::HandleType::Stmt, hstmt as sys::Handle);
        assert_eq!(ret, sys::SqlReturn::SUCCESS);
    }
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn varchar_null(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(10)"])
        .build(profile)
        .unwrap();

    let insert = format!("INSERT INTO {table_name} (a) VALUES (?)");

    conn.execute(&insert, &VarCharSlice::NULL).unwrap();

    assert_eq!("NULL", table.content_as_string(&conn))
}

/// Connect to database with connection string, and check the output connection string with
/// attributes complemented by the driver.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn get_full_connection_string(profile: &Profile) {
    let mut completed_connection_string = OutputStringBuffer::with_buffer_size(1024);
    environment()
        .unwrap()
        .driver_connect(
            profile.connection_string,
            &mut completed_connection_string,
            odbc_api::DriverCompleteOption::NoPrompt,
        )
        .unwrap();

    assert!(!completed_connection_string.is_truncated());

    let completed_connection_string = completed_connection_string.to_utf8();

    eprintln!("Completed Connection String: {completed_connection_string}");

    // Additional attributes should make the string larger.
    assert!(profile.connection_string.len() <= completed_connection_string.len());
}

/// We must be able to detect truncation in case we provide a buffer too small to hold the output
/// connection string
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] STATUS_STACK_BUFFER_OVERRUN
// #[test_case(SQLITE_3; "SQLite 3")] Does not write truncated connection string at all
#[test_case(POSTGRES; "PostgreSQL")]
fn get_full_connection_string_truncated(profile: &Profile) {
    let mut completed_connection_string = OutputStringBuffer::with_buffer_size(2);
    environment()
        .unwrap()
        .driver_connect(
            profile.connection_string,
            &mut completed_connection_string,
            odbc_api::DriverCompleteOption::NoPrompt,
        )
        .unwrap();

    eprintln!(
        "Output connection string: {}",
        completed_connection_string.to_utf8()
    );

    assert!(completed_connection_string.is_truncated());
}

/// We must be able to detect truncation in case we provide a buffer too small to hold the output
/// connection string
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn driver_connect_with_empty_out_connection_sring(profile: &Profile) {
    let mut completed_connection_string = OutputStringBuffer::empty();
    environment()
        .unwrap()
        .driver_connect(
            profile.connection_string,
            &mut completed_connection_string,
            odbc_api::DriverCompleteOption::NoPrompt,
        )
        .unwrap();

    assert!(completed_connection_string.is_truncated());
    assert!(completed_connection_string.to_utf8().is_empty());
}

#[test_case(MSSQL, "Microsoft SQL Server"; "Microsoft SQL Server")]
#[test_case(MARIADB, "MariaDB"; "Maria DB")]
#[test_case(SQLITE_3, "SQLite"; "SQLite 3")]
#[test_case(POSTGRES, "PostgreSQL"; "PostgreSQL")]
fn database_management_system_name(profile: &Profile, expected_name: &'static str) {
    let conn = profile.connection().unwrap();
    let actual_name = conn.database_management_system_name().unwrap();
    assert_eq!(expected_name, actual_name);
}

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

/// Demonstrating how to fill a vector of rows using this crate.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn fill_vec_of_rows(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(50)", "INTEGER"])
        .unwrap();
    let insert_sql = format!("INSERT INTO {table_name} (a,b) VALUES ('A', 1), ('B',2)");
    conn.execute(&insert_sql, ()).unwrap();

    // Now that the table is created and filled with some values lets query it and put its contents
    // into a `Vec`

    let query_sql = format!("SELECT a,b FROM {table_name}");
    let cursor = conn.execute(&query_sql, ()).unwrap().unwrap();
    let buf_desc = [
        BufferDesc::Text { max_str_len: 50 },
        BufferDesc::I32 { nullable: false },
    ];

    let buffer = ColumnarAnyBuffer::try_from_descs(1, buf_desc).unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();

    let mut actual = Vec::new();

    while let Some(batch) = cursor.fetch().unwrap() {
        // Extract first column known to contain text
        let col_a = batch.column(0).as_text_view().unwrap();

        // Extract second column known to contain non nullable i32
        let col_b = i32::as_slice(batch.column(1)).unwrap();

        for &b in col_b {
            let a = col_a
                .iter()
                .next()
                .unwrap()
                .map(|bytes| str::from_utf8(bytes).unwrap().to_owned());
            actual.push((a, b))
        }
    }

    assert_eq!(
        actual,
        [(Some("A".to_string()), 1), (Some("B".to_string()), 2)]
    )
}

/// Provoke return of NO_DATA from SQLExecute and SQLDirectExecute by deleting a non existing row.
/// The bindings must not panic, even though the result is not SQL_SUCCESS
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn no_data(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["INTEGER"])
        .unwrap();
    let sql = format!("DELETE FROM {table_name} WHERE id=5");
    // Assert no panic on direct execution
    conn.execute(&sql, ()).unwrap();
    // Assert no panic on prepared execution
    conn.prepare(&sql).unwrap().execute(()).unwrap();
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

/// Some drivers seem to have trouble binding buffers beyond `u16::MAX`. This has been seen failing
/// in the wild with SAP anywhere, but that ODBC driver is not part of this test suite.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_array_size_66536(profile: &Profile) {
    let table_name = table_name!();
    let conn = profile.setup_empty_table(&table_name, &["BIT"]).unwrap();
    let sql = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let row_set_buffer = ColumnarAnyBuffer::try_from_descs(
        u16::MAX as usize + 1,
        [BufferDesc::Bit { nullable: false }],
    )
    .unwrap();
    assert!(cursor.bind_buffer(row_set_buffer).is_ok())
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
#[should_panic(expected = "SQLFreeHandle failed with error code: -1")]
fn should_panic_if_connection_cannot_be_freed(profile: &Profile) {
    let conn = profile.connection().unwrap();

    // Since the types with their invariants in this crate helpfully prevent us from freeing a
    // connected handles, we have to abandon the saftey rails in order to provoke the error.
    let conn = conn.into_handle();

    // We drop the connection, but it is still connected. => This is a programming error, we want
    // the drop handler to panic.
    drop(conn);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
#[should_panic(expected = "original error")]
fn panic_in_drop_handlers_should_not_mask_original_error(profile: &Profile) {
    let conn = profile.connection().unwrap();

    // Since the types with their invariants in this crate helpfully prevent us from freeing a
    // connected handles, we have to abandon the saftey rails in order to provoke the error.
    let _conn = conn.into_handle();

    // If this error is propagated upwards, the above connections drop handler will be called and
    // fail. This tests wants to ensure the original error is not masked by that.
    panic!("original error")
}

/// Arrow uses the same binary format for the values of nullable slices, though null are represented
/// as bitmask. Make it possible for bindings to efficiently copy the values array out of a
/// nullable slice.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn memcopy_values_from_nullable_slice(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["INTEGER"])
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (42), (NULL), (5);"),
        (),
    )
    .unwrap();

    // When
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name}"), ())
        .unwrap() // Unwrap Result
        .unwrap(); // Unwrap Option, we know a select statement to produce a cursor.
    let buffer =
        ColumnarAnyBuffer::try_from_descs(3, [BufferDesc::I32 { nullable: true }]).unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let nullable_slice = batch.column(0).as_nullable_slice::<i32>().unwrap();
    let (values, indicators) = nullable_slice.raw_values();
    // Memcopy values.
    let values = values.to_vec();
    // Create array of bools indicating null values.
    let nulls: Vec<bool> = indicators
        .iter()
        .map(|&indicator| indicator == sys::NULL_DATA)
        .collect();

    // Then
    assert!(!nulls[0]);
    assert_eq!(values[0], 42);
    assert!(nulls[1]);
    // We explicitly don't give any guarantees about the value of #values[1]`.
    assert!(!nulls[2]);
    assert_eq!(values[2], 5);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn text_column_view_should_allow_for_filling_arrow_arrays(profile: &Profile) {
    // Given
    let table_name = "TextColumnViewShouldAllowForFillingArrowArrays";
    let conn = profile
        .setup_empty_table(table_name, &["VARCHAR(50)"])
        .unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {table_name} (a) VALUES \
                ('abcd'), \
                (NULL), \
                ('efghij'), \
                ('klm'), \
                ('npqrstu')"
        ),
        (),
    )
    .unwrap();

    // When
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name}"), ())
        .unwrap()
        .unwrap();

    let columnar_buffer =
        ColumnarAnyBuffer::try_from_descs(10, [BufferDesc::Text { max_str_len: 50 }]).unwrap();

    let mut cursor = cursor.bind_buffer(columnar_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let view = batch.column(0).as_text_view().unwrap();

    let mut valid = Vec::with_capacity(view.len());
    let mut offsets = Vec::with_capacity(view.len() + 1);

    let mut offset: usize = 0;
    for index in 0..view.len() {
        offset += view.content_length_at(index).unwrap_or(0)
    }

    let mut consequtives_values = Vec::with_capacity(offset);
    let raw_values_odbc = view.raw_value_buffer();
    offset = 0;
    for index in 0..view.len() {
        offsets.push(offset);
        if let Some(len) = view.content_length_at(index) {
            valid.push(true);
            offset += len;
            let start_index = index * (view.max_len() + 1);
            consequtives_values
                .extend_from_slice(&raw_values_odbc[start_index..(start_index + len)])
        } else {
            valid.push(false);
        }
    }

    // Then
    assert_eq!(valid, [true, false, true, true, true]);
    assert_eq!(offsets, [0, 4, 4, 10, 13]);
    assert_eq!(consequtives_values, b"abcdefghijklmnpqrstu");
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn detect_truncated_output_in_bulk_fetch(profile: &Profile) {
    // Given a text entry with a length of ten.
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(10)"])
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES ('0123456789')"),
        (),
    )
    .unwrap();

    // When fetching that field as part of a bulk, but with a buffer of only length 5.
    let buffer_description = BufferDesc::Text { max_str_len: 5 };
    let buffer = ColumnarAnyBuffer::try_from_descs(1, [buffer_description]).unwrap();
    let query = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&query, ()).unwrap().unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    assert!(matches!(
        cursor.fetch_with_truncation_check(true),
        Err(Error::TooLargeValueForBuffer {
            indicator: Some(10),
            buffer_index: 0,
        })
    ))
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn grow_batch_size_during_bulk_insert(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["INTEGER"])
        .unwrap();

    // When insert two batches with size one and two.
    let mut prepared = conn
        .prepare(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    let desc = BufferDesc::I32 { nullable: false };
    // The first batch is inserted with capacity 1
    let mut prebound = prepared.column_inserter(1, [desc]).unwrap();
    prebound.set_num_rows(1);
    let col = prebound.column_mut(0).as_slice::<i32>().unwrap();
    col[0] = 1;
    prebound.execute().unwrap();
    // Second batch is larger than the first and does not fit into the capacity. Only way to resize
    // is currently to destroy everything the ColumnarInserter, but luckily we only borrowed the
    // statement.
    let mut prebound = prepared.column_inserter(2, [desc]).unwrap();
    prebound.set_num_rows(2);
    let col = prebound.column_mut(0).as_slice::<i32>().unwrap();
    col[0] = 2;
    col[1] = 3;
    prebound.execute().unwrap();

    // Then
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("1\n2\n3", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_inserter_owning_connection(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["INTEGER"])
        .unwrap();

    // When insert two batches with size one and two.
    let mut prepared = conn
        .into_prepared(&format!("INSERT INTO {table_name} (a) VALUES (?)"))
        .unwrap();
    let desc = BufferDesc::I32 { nullable: false };
    // Insert a batch
    let mut prebound = prepared.column_inserter(1, [desc]).unwrap();
    prebound.set_num_rows(1);
    let col = prebound.column_mut(0).as_slice::<i32>().unwrap();
    col[0] = 1;
    prebound.execute().unwrap();

    // Then
    let conn = profile.connection().unwrap();
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("1", actual);
}

/// Fire an insert statement adding two rows and verify that the count of changed rows is 2.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_count_one_shot_query(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a) VALUES (1), (2)");

    // When
    let mut preallocated = conn.preallocate().unwrap();
    preallocated.execute(&insert, ()).unwrap();
    let row_count = preallocated.row_count().unwrap();

    // Then
    assert_eq!(Some(2), row_count);
}

/// Fire an insert statement adding two rows and verify that the count of changed rows is 2.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_count_prepared_insert(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    let insert = format!("INSERT INTO {table_name} (a) VALUES (?), (?)");

    // When
    let mut prepared = conn.prepare(&insert).unwrap();
    prepared.execute((&1, &2)).unwrap();
    let row_count = prepared.row_count().unwrap();

    // Then
    assert_eq!(Some(2), row_count);
}

#[test_case(MSSQL, None; "Microsoft SQL Server")]
#[test_case(MARIADB, Some(0); "Maria DB")]
#[test_case(SQLITE_3, Some(0); "SQLite 3")]
#[test_case(POSTGRES, Some(0); "PostgreSQL")]
fn row_count_create_table_preallocated(profile: &Profile, expectation: Option<usize>) {
    // Given a name for a table which does not exist
    let table_name = table_name!();
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name};"), ())
        .unwrap();

    // When
    let mut preallocated = conn.preallocate().unwrap();
    preallocated
        .execute(&format!("CREATE TABLE {table_name} (a INTEGER);"), ())
        .unwrap();
    let row_count = preallocated.row_count().unwrap();

    // Then
    assert_eq!(expectation, row_count);
}

#[test_case(MSSQL, Some(0); "Microsoft SQL Server")]
#[test_case(MARIADB, Some(0); "Maria DB")]
#[test_case(SQLITE_3, Some(0); "SQLite 3")]
#[test_case(POSTGRES, Some(0); "PostgreSQL")]
fn row_count_create_table_prepared(profile: &Profile, expectation: Option<usize>) {
    // Given a name for a table which does not exist
    let table_name = table_name!();
    let conn = profile.connection().unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {table_name};"), ())
        .unwrap();

    // When
    let mut prepared = conn
        .prepare(&format!("CREATE TABLE {table_name} (a INTEGER);"))
        .unwrap();
    prepared.execute(()).unwrap();
    let row_count = prepared.row_count().unwrap();

    // Then
    assert_eq!(expectation, row_count);
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
    conn.execute(&format!("DROP TABLE IF EXISTS {fk_table_name};"), ())
        .unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {pk_table_name};"), ())
        .unwrap();
    conn.execute(
        &format!("CREATE TABLE {pk_table_name} (id INTEGER, PRIMARY KEY(id));"),
        (),
    )
    .unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE {fk_table_name} (ext_id INTEGER, FOREIGN KEY (ext_id) REFERENCES \
            {pk_table_name}(id));"
        ),
        (),
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
    conn.execute(&format!("DROP TABLE IF EXISTS {fk_table_name};"), ())
        .unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {pk_table_name};"), ())
        .unwrap();
    conn.execute(
        &format!("CREATE TABLE {pk_table_name} (id INTEGER, PRIMARY KEY(id));"),
        (),
    )
    .unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE {fk_table_name} (ext_id INTEGER, FOREIGN KEY (ext_id) REFERENCES \
            {pk_table_name}(id));"
        ),
        (),
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

// The two failing drivers confuse buffer and character lengths with each other. It could not be
// worked around by allocating larger buffers.
// #[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")]

// SQLite just does not recognize the last letter
// #[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn describe_column_name_with_umlaut(profile: &Profile) {
    let table_name = table_name!();
    // Given a table with an umlaut in a column name
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .column_names(&["hällo𐐏"])
        .build(profile)
        .unwrap();

    // When executing a query with a result set containing that column and describing that column
    let mut result_set = conn
        .execute(&format!("SELECT hällo𐐏 FROM {table_name}"), ())
        .unwrap()
        .unwrap();
    let mut desc = ColumnDescription::default();
    result_set.describe_col(1, &mut desc).unwrap();
    let column_name = desc.name_to_string().unwrap();

    // Then
    assert_eq!("hällo𐐏", column_name);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Only allows one SQL Statement
// #[test_case(SQLITE_3; "SQLite 3")] Only allows one SQL Statement
#[test_case(POSTGRES; "PostgreSQL")]
fn execute_two_select_statements(profile: &Profile) {
    let conn = profile.connection().unwrap();

    let cursor = conn
        .execute("SELECT 1 AS A; SELECT 2 AS B;", ())
        .unwrap()
        .unwrap();

    let maybe_cursor = cursor.more_results().unwrap();
    assert!(maybe_cursor.is_some());
    let cursor = maybe_cursor.unwrap();
    let maybe_cursor = cursor.more_results().unwrap();
    assert!(maybe_cursor.is_none());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Only allows one SQL Statement
// #[test_case(SQLITE_3; "SQLite 3")] Only allows one SQL Statement
#[test_case(POSTGRES; "PostgreSQL")]
fn execute_select_insert_select(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();

    let first_cursor = conn
        .execute(
            &format!("SELECT 1 AS A; INSERT INTO {table_name} (a) VALUES (2); SELECT 3 AS B;"),
            (),
        )
        .unwrap()
        .unwrap();

    // The statement is not actually in a cursor state here, because the second statement is an
    // Insert statement. Everything works fine though, if we do not try to fetch any values. If we
    // fetch it would trigger a sequence error.
    let second_cursor = first_cursor.more_results().unwrap();
    assert!(second_cursor.is_some());
    let second_cursor = second_cursor.unwrap();

    let third_cursor = second_cursor.more_results().unwrap();
    assert!(third_cursor.is_some());
    let third_cursor = third_cursor.unwrap();

    let fourth_cursor = third_cursor.more_results().unwrap();
    assert!(fourth_cursor.is_none());
}

// #[test_case(MSSQL; "Microsoft SQL Server")] Without changing server configuration VARCHAR(50)
// does not seem to store things in UTF-8, but rather use an ASCII encoding which can not represent
// the chinese characters.
// Window hosted MARIA DB seems to also have trouble storying utf-8 in a varchar(50). Runs fine
// though with windows client and linux server
// #[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn chinese_text_argument(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    // When
    conn.execute(&insert_sql, &"您好".into_parameter()).unwrap();

    // Then
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
    let actual = {
        let buffer = ColumnarBuffer::<_>::new(vec![(1, TextColumn::<u16>::new(1, 50))]);
        let mut cursor = cursor.bind_buffer(buffer).unwrap();
        let batch = cursor.fetch().unwrap().unwrap();
        let utf16 = batch.column(0).get(0).unwrap();
        let utf16 = U16Str::from_slice(utf16);
        utf16.to_string().unwrap()
    };

    // Fetching non narrow should always work, but does not for PostgreSQL with narrow compilation
    // flag.
    #[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
    let actual = {
        let buffer = ColumnarBuffer::<_>::new(vec![(1, TextColumn::<u8>::new(1, 50))]);
        let mut cursor = cursor.bind_buffer(buffer).unwrap();
        let batch = cursor.fetch().unwrap().unwrap();
        batch.at_as_str(0, 0).unwrap().unwrap().to_string()
    };
    assert_eq!("您好", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] Postgres does not recognize NVARCHAR as a type
fn chinese_text_argument_nvarchar(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["NVARCHAR(50)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    // When
    let arg = U16String::from_str("您好"); // Narrow build will fail for MSSQL without this line.
    conn.execute(&insert_sql, &arg.into_parameter()).unwrap();

    // Then
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let buffer = ColumnarBuffer::<_>::new(vec![(1, TextColumn::<u16>::new(1, 50))]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let utf16 = batch.column(0).get(0).unwrap();
    let utf16 = U16Str::from_slice(utf16);
    let actual = utf16.to_string().unwrap();
    assert_eq!("您好", actual);
}

/// Inspired by <https://github.com/pacman82/odbc-api/issues/428>
/// Microsoft driver seems to report wrong sizes for the remaining strings if a text column can not
/// be fetched on first try.
// #[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
// #[test_case(POSTGRES; "PostgreSQL")] Fails on linux: Invalid string conversion occurred
fn cursor_get_text_from_text(profile: &Profile) {
    // Given a text column with a string larger than 255 characters. It also must contain non ASCII
    // characters.
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["TEXT"])
        .build(profile)
        .unwrap();
    // For this test we want to run into a scenario, there we can not fetch everything in the first
    // roundtrip, so we choose a text larger than 256 characters.
    let text = "€".repeat(300);
    let insert_sql = table.sql_insert();
    conn.execute(&insert_sql, &text.into_parameter()).unwrap();

    // When
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut buffer = Vec::new();
    row.get_text(1, &mut buffer).unwrap();

    // Then
    let actual = String::from_utf8(buffer).unwrap();
    assert_eq!("€".repeat(300), actual);
}

// This triggers a bug in the ODBC driver at least version 17 and 18
//
// In addition this bug does not seem to occur then talking to an MSSQL server hosted in a windows
// Enivornment. In our current CI test suite, if the client runs on windows so does the server, so
// we do not execute this test on windows.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[should_panic(
    expected = "SQLGetData has been unable to fetch all data, even though the capacity of the \
    target buffer has been adapted to hold the entire payload based on the indicator of the last \
    part. You may consider filing a bug with the ODBC driver you are using."
)]
#[cfg(not(target_os = "windows"))]
fn cursor_get_text_from_text_mssql(profile: &Profile) {
    // Given a text column with a string larger than 255 characters. It also must contain non ASCII
    // characters.
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["TEXT"])
        .build(profile)
        .unwrap();
    // For this test we want to run into a scenario, there we can not fetch everything in the first
    // roundtrip, so we choose a text larger than 256 characters.
    let text = "€".repeat(300);
    let insert_sql = table.sql_insert();
    conn.execute(&insert_sql, &text.into_parameter()).unwrap();

    // When
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    let mut buffer = Vec::new();
    row.get_text(1, &mut buffer).unwrap();

    // Microsoft driver is buggy in this situation, as it does not use the indicator to report the
    // true size of the string or the `NO_TOTAL`. We can at least test that a panic occurs and not
    // some endless loop or buffer overflow.
}

/// If we want to use two buffers alternating to fetch data (like in the concurrent use case in
/// the arrow-odbc downstream crate) we may want to generate a second row set buffer from an
/// existing one. For this it is useful if we can infer the capacity of the block cursor, without
/// unbinding it first.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_arrary_size_from_block_cursor(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();

    // When
    let capacity_used_to_create_buffer = 42;
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let buffer = ColumnarAnyBuffer::from_descs(
        capacity_used_to_create_buffer,
        [BufferDesc::I32 { nullable: true }],
    );
    let block_cursor = cursor.bind_buffer(buffer).unwrap();
    let capacity_reported_by_block_cursor = block_cursor.row_array_size();

    // Then
    assert_eq!(
        capacity_used_to_create_buffer,
        capacity_reported_by_block_cursor
    );
}

/// Learning test what display size drivers report for JSON columns
// #[test_case(MSSQL, "@json VARCHAR(max)", None; "Microsoft SQL Server")] // Unknown data type JSON
// #[test_case(MARIADB, Some(1073741823); "Maria DB")] // Different size reported if MariaDB runs on windows
#[test_case(SQLITE_3, Some(255); "SQLite 3")]
#[test_case(POSTGRES, Some(255); "PostgreSQL")]
fn json_column_display_size(profile: &Profile, expected_display_size: Option<usize>) {
    // Given a table with a column type which will return `NO_TOTAL`
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["JSON"])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();

    // When obtaining result set metadata
    let mut result_set = conn.execute(&query, ()).unwrap().unwrap();
    // First column is id with index `0`. Second index (`1`) referes to the JSON column
    let size = result_set
        .col_display_size(1)
        .unwrap()
        .map(NonZeroUsize::get);

    // Then it is equal to the recorded display size
    assert_eq!(expected_display_size, size);
}

/// Fetch decimal values in their text interpretation and transform them to their integer
/// representation.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn fetch_decimals_to_int(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DECIMAL(5,3)"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (12.345), (-12.345), (12), (12.3)"),
        (),
    )
    .unwrap();

    // When
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let row_set_buffer = TextRowSet::for_cursor(4, &mut cursor, None).unwrap();
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();
    let n = |i| batch.at_as_str(0, i).unwrap().unwrap().as_bytes();
    let n1 = decimal_text_to_i128(n(0), 3);
    let n2 = decimal_text_to_i128(n(1), 3);
    let n3 = decimal_text_to_i128(n(2), 3);
    let n4 = decimal_text_to_i128(n(3), 3);

    // Then
    assert_eq!(12345, n1);
    assert_eq!(-12345, n2);
    assert_eq!(12000, n3);
    assert_eq!(12300, n4);
}

/// Bulf fetch in a dedicated system thread. Usually so the application can process the last batch
/// while the next one is fetched.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_bulk_fetch_double_buffered(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(&format!("INSERT INTO {table_name} (a) VALUES (1), (2)"), ())
        .unwrap();

    // When
    let mut buffer_a = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let block_cursor = cursor.bind_buffer(buffer_b).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);

    let has_another_batch = concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    assert!(has_another_batch);
    assert_eq!(1, buffer_a.num_rows());
    assert_eq!(1i32, buffer_a.column(0).as_slice().unwrap()[0]);

    let has_another_batch = concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    assert!(has_another_batch);
    assert_eq!(1, buffer_a.num_rows());
    assert_eq!(2i32, buffer_a.column(0).as_slice().unwrap()[0]);

    let has_another_batch = concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    assert!(!has_another_batch);
}

/// Bulf fetch in a dedicated system thread. Usually so the application can process the last batch
/// while the next one is fetched.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_bulk_fetch_single_buffer(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(&format!("INSERT INTO {table_name} (a) VALUES (1), (2)"), ())
        .unwrap();

    // When
    let buffer = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let block_cursor = cursor.bind_buffer(buffer).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);

    let batch = concurrent_block_cursor.fetch().unwrap().unwrap();
    assert_eq!(1, batch.num_rows());
    assert_eq!(1i32, batch.column(0).as_slice().unwrap()[0]);
    concurrent_block_cursor.fill(batch);

    let batch = concurrent_block_cursor.fetch().unwrap().unwrap();
    assert_eq!(1, batch.num_rows());
    assert_eq!(2i32, batch.column(0).as_slice().unwrap()[0]);
    concurrent_block_cursor.fill(batch);

    let all_batches_consumed = concurrent_block_cursor.fetch().unwrap().is_none();
    assert!(all_batches_consumed);
}

/// Catch edge cases, there we stop the thread, while there are still batches to consume
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_bulk_fetch_fetch_one_batch(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(&format!("INSERT INTO {table_name} (a) VALUES (1), (2)"), ())
        .unwrap();

    // When
    let buffer = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let block_cursor = cursor.bind_buffer(buffer).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);
    let _ = concurrent_block_cursor.fetch().unwrap().unwrap();
    // Now instead of sending a buffer and fetching a next one, we interrupt the fetch thread while
    // it does not own a buffer.
    let cursor = concurrent_block_cursor.into_cursor().unwrap();
    // Now we can deterministically fetch the second batch in the main thread again. Since the fetch
    // thread has only ever seen one buffer, it could have only fetched one batch.

    let actual = cursor_to_string(cursor);
    assert_eq!("2", actual);
}

/// Bulk fetch in a dedicated system thread. Usually so the application can process the last batch
/// while the next one is fetched.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// SQLite, actually does not create a diagonstic record or is creating any kind of error in this
// case. It would just report the value to be zero. This has nothing todo with the fetch being
// concurrent though. To test the error handling, the other DBMs have to suffice
// #[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_bulk_fetch_with_invalid_buffer_type(profile: &Profile) {
    // Given an integer table with a NULL
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(&format!("INSERT INTO {table_name} (a) VALUES (NULL)"), ())
        .unwrap();

    // When fetching with a Columnar buffer not supporting nullable values
    let mut buffer_a = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();
    let block_cursor = cursor.bind_buffer(buffer_b).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);
    // This line provokes the first error, due to the invalid buffer.
    let result_one = concurrent_block_cursor.fetch_into(&mut buffer_a);
    let result_two = concurrent_block_cursor.fetch_into(&mut buffer_a);

    // Then
    assert!(result_one.is_err());
    // After the first error, we treat the stream same as we would treat a stream which is consumed.
    assert!(!result_two.unwrap());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
fn concurrent_fetch_of_multiple_result_sets(profile: &Profile) {
    // Given
    let conn = profile.connection().unwrap();
    let query = "SELECT 1 AS a; SELECT 2 AS b;";

    // When
    let mut buffer_a = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn.into_cursor(query, ()).unwrap().unwrap();
    let block_cursor = cursor.bind_buffer(buffer_b).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);
    // Consume first result set.
    concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    // Now continue with the same cursor to fetch the second
    let cursor = concurrent_block_cursor.into_cursor().unwrap();
    let cursor = cursor.more_results().unwrap().unwrap();
    let mut cursor = cursor.bind_buffer(buffer_a).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(2i32, batch.column(0).as_slice().unwrap()[0]);
}

/// This test covers a code path in which the thread dedicated to fething is not termintated by
/// running out of batches.
#[test_case(MSSQL; "Microsoft SQL Server")]
fn concurrent_fetch_skip_first_result_set(profile: &Profile) {
    // Given
    let conn = profile.connection().unwrap();
    let query = "SELECT 1 AS a; SELECT 2 AS b;";

    // When
    let buffer_a = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarAnyBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn.into_cursor(query, ()).unwrap().unwrap();
    let block_cursor = cursor.bind_buffer(buffer_b).unwrap();
    let concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);
    // Skip over first result set, without fetching any batches.
    // Now continue with the same cursor to fetch the second
    let cursor = concurrent_block_cursor.into_cursor().unwrap();
    let cursor = cursor.more_results().unwrap().unwrap();
    let mut cursor = cursor.bind_buffer(buffer_a).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(2i32, batch.column(0).as_slice().unwrap()[0]);
}

/// This tests checks if there is more than one attribute returned. We had a bug (see issue:
/// <https://github.com/pacman82/odbc-api/issues/511>) in which we used a buffer intended for zero
/// terminated strings to get the attributes. Yet the attributes use the `0` as a delimiter.
#[test]
fn list_all_driver_attributes() {
    // Given an ODBC environment with drivers installed
    let environment = &environment().unwrap();

    // When fetching driver infors
    let driver_infos = environment.drivers().unwrap();

    // At least one driver is expecetd to have more than one attribute.
    let maximum = driver_infos
        .iter()
        .map(|info| info.attributes.len())
        .max()
        .expect("At least one ODBC driver must be installed");
    assert!(maximum > 1);
}

#[test_case(MSSQL, true; "Microsoft SQL Server")]
#[test_case(MARIADB, false; "Maria DB")]
#[test_case(SQLITE_3, false; "SQLite 3")]
#[test_case(POSTGRES, false; "PostgreSQL")]
#[tokio::test]
async fn polling_preallocated_statement_execution(
    profile: &Profile,
    expected_to_support_polling: bool,
) {
    // Given a table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    let query = format!("INSERT INTO {table_name} (a) VALUES ('Hello, World!')");
    // We use this counter to check if the sleep function is actually invoked and the driver does
    // actually support asynchronous polling.
    let mut sleep_counter_spy = 0;
    let sleep = || {
        sleep_counter_spy += 1;
        tokio::time::sleep(Duration::from_millis(50))
    };

    // When
    let mut statement = conn.preallocate().unwrap().into_polling().unwrap();
    statement.execute(&query, (), sleep).await.unwrap();

    // Then
    let actual = table.content_as_string(&conn);
    assert_eq!("Hello, World!", actual);
    let used_polling = sleep_counter_spy != 0;
    assert_eq!(expected_to_support_polling, used_polling);
}

#[test_case(MSSQL, true; "Microsoft SQL Server")]
#[test_case(MARIADB, false; "Maria DB")]
#[test_case(SQLITE_3, false; "SQLite 3")]
#[test_case(POSTGRES, false; "PostgreSQL")]
#[tokio::test]
async fn async_bulk_fetch(profile: &Profile, expected_to_support_polling: bool) {
    // Given a table with a thousand records
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let mut inserter = prepared.into_text_inserter(1000, [50]).unwrap();
    for index in 0..1000 {
        inserter
            .append([Some(index.to_string().as_bytes())].iter().copied())
            .unwrap();
    }
    inserter.execute().unwrap();
    let query = table.sql_all_ordered_by_id();
    // We use this counter to check if the sleep function is actually invoked and the driver does
    // actually support asynchronous polling.
    let mut sleep_counter_spy = 0;
    let mut sleep = || {
        sleep_counter_spy += 1;
        tokio::time::sleep(Duration::from_millis(50))
    };

    // When
    let mut sum_rows_fetched = 0;
    let cursor = conn
        .execute_polling(&query, (), &mut sleep)
        .await
        .unwrap()
        .unwrap();
    // Fetching results in ten batches
    let buffer = TextRowSet::from_max_str_lens(100, [50usize]).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(buffer).unwrap();
    let mut maybe_batch = row_set_cursor.fetch(&mut sleep).await.unwrap();
    while let Some(batch) = maybe_batch {
        sum_rows_fetched += batch.num_rows();
        maybe_batch = row_set_cursor.fetch(&mut sleep).await.unwrap();
    }

    // Then
    assert_eq!(1000, sum_rows_fetched);
    let used_polling = sleep_counter_spy != 0;
    assert_eq!(expected_to_support_polling, used_polling);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_wise_bulk_query_using_tuple(profile: &Profile) {
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
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    let row_set_buffer = RowVec::<(i32, VarCharArray<50>)>::new(10);
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(2, batch.num_rows());
    assert_eq!(42, batch[0].0);
    assert_eq!("Hello, World!", batch[0].1.as_str().unwrap().unwrap());
    assert_eq!(5, batch[1].0);
    assert_eq!("Hallo, Welt!", batch[1].1.as_str().unwrap().unwrap());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_wise_bulk_query_nullable(profile: &Profile) {
    // Given a cursor
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42"), None]])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    let row_set_buffer = RowVec::<(Nullable<i32>,)>::new(10);
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(Some(&42), batch[0].0.as_opt());
    assert_eq!(None, batch[1].0.as_opt());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_wise_bulk_query_binary(profile: &Profile) {
    // Given a cursor
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(20)"])
        .values_by_column(&[&[Some("Hello, World!"), None]])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    let row_set_buffer = RowVec::<(VarBinaryArray<20>,)>::new(2);
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(b"Hello, World!", batch[0].0.as_bytes().unwrap());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn row_wise_bulk_query_wide_text(profile: &Profile) {
    // Given a cursor
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(20)"])
        .values_by_column(&[&[Some("Hello, World!"), None]])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    let row_set_buffer = RowVec::<(VarWCharArray<20>,)>::new(2);
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(
        "Hello, World!",
        batch[0].0.as_utf16().unwrap().to_string().unwrap()
    );
}

#[cfg(feature = "derive")]
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
        .execute(&table.sql_all_ordered_by_id(), ())
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

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn truncation_in_row_wise_bulk_buffer(profile: &Profile) {
    // Given a cursor
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .values_by_column(&[&[Some("Hello, World!")]])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    let mut row_set_buffer = RowVec::<(VarCharArray<10>,)>::new(10);

    let mut block_cursor = cursor.bind_buffer(&mut row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!("Hello, Wo", batch[0].0.as_str().unwrap().unwrap());
    drop(block_cursor);
    assert_eq!(
        TruncationInfo {
            indicator: Some(13),
            buffer_index: 0
        },
        (&(&mut row_set_buffer)).find_truncation().unwrap()
    )
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn fetch_fixed_type_row_wise(profile: &Profile) {
    // Given a cursor
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &42.into_parameter())
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    type RowSample = (i32,);

    let row_set_buffer = RowVec::<RowSample>::new(10);
    let mut block_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = block_cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(42, batch[0].0);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] SQLite just returns a zeroed out numeric struct
#[test_case(POSTGRES; "PostgreSQL")]
fn fetch_decimal_as_numeric_struct_using_get_data(profile: &Profile) {
    // Given a cursor over a result set with a decimal in its first column
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DECIMAL(5,3)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &(25.212).into_parameter())
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    // Binding numeric withouth this wrapper would bind it without the App Row Descriptor
    struct BindNumericAsAppRowDesc(Numeric);
    let mut target = BindNumericAsAppRowDesc(Numeric {
        precision: 0,
        scale: 0,
        sign: 0,
        val: Default::default(),
    });
    unsafe impl CData for BindNumericAsAppRowDesc {
        fn cdata_type(&self) -> CDataType {
            CDataType::Ard
        }
        fn indicator_ptr(&self) -> *const isize {
            std::ptr::null()
        }
        fn value_ptr(&self) -> *const std::ffi::c_void {
            &self.0 as *const Numeric as *const std::ffi::c_void
        }
        fn buffer_length(&self) -> isize {
            0
        }
    }
    unsafe impl CDataMut for BindNumericAsAppRowDesc {
        fn mut_indicator_ptr(&mut self) -> *mut isize {
            null_mut()
        }
        fn mut_value_ptr(&mut self) -> Pointer {
            &mut self.0 as *mut Numeric as *mut std::ffi::c_void
        }
    }
    unsafe {
        let mut stmt = cursor.into_stmt();

        let mut ard = stmt.application_row_descriptor().unwrap();

        ard.set_type(1, CDataType::Numeric).unwrap();
        ard.set_precision(1, 5).unwrap();
        ard.set_scale(1, 3).unwrap();

        stmt.fetch();

        stmt.get_data(1, &mut target);

        stmt.close_cursor();
    }

    // Then
    assert_eq!(5, target.0.precision);
    assert_eq!(3, target.0.scale);
    // 1 is positive, 0 is negative
    assert_eq!(1, target.0.sign);
    // Hex representation of 25212 is 627C
    // First character contains '7C' -> C ~ 12, 7 ~ 7; 12 + 16 * 7 = 124
    assert_eq!(124, target.0.val[0]);
    // Second character encodes '62' -> 2 + 16 * 6 = 98
    assert_eq!(98, target.0.val[1]);
    assert_eq!(0, target.0.val[2]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] Always filled with zero
#[test_case(POSTGRES; "PostgreSQL")]
fn fetch_decimal_as_numeric_struct_using_bind_col(profile: &Profile) {
    // Given a cursor over a result set with a decimal in its first column
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DECIMAL(5,3)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &(25.212).into_parameter())
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), ())
        .unwrap()
        .unwrap();

    // When
    // Binding numeric withouth this wrapper would bind it without the App Row Descriptor
    let mut target = Numeric {
        precision: 0,
        scale: 0,
        sign: 0,
        val: Default::default(),
    };
    unsafe {
        let mut stmt = cursor.into_stmt();

        // stmt.bind_col(1, &mut target);
        let mut ard = stmt.application_row_descriptor().unwrap();

        // Setting Field descriptors always seems to yield structs field with zeroes
        ard.set_type(1, CDataType::Numeric).unwrap();
        ard.set_precision(1, 5).unwrap();
        ard.set_scale(1, 3).unwrap();
        // Setting the dataptr directly on the ARD is required to make it work for MSSQL which does
        // seem to set the wrong pointer if just using bind col. Postgres and MariaDB would work as
        // intended with bind_col.
        // stmt.bind_col(1, &mut target);
        ard.set_data_ptr(1, &mut target as *mut Numeric as Pointer)
            .unwrap();

        stmt.fetch();

        stmt.close_cursor();
    }

    // Then
    assert_eq!(5, target.precision);
    assert_eq!(3, target.scale);
    // 1 is positive, 0 is negative
    assert_eq!(1, target.sign);
    // Hex representation of 25212 is 627C
    // First character contains '7C' -> C ~ 12, 7 ~ 7; 12 + 16 * 7 = 124
    assert_eq!(124, target.val[0]);
    // Second character encodes '62' -> 2 + 16 * 6 = 98
    assert_eq!(98, target.val[1]);
    assert_eq!(0, target.val[2]);
}

/// Learning test to see how scrolling cursors behave
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn scroll_cursor(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"one".into_parameter())
        .unwrap();
    conn.execute(&table.sql_insert(), &"two".into_parameter())
        .unwrap();
    conn.execute(&table.sql_insert(), &"three".into_parameter())
        .unwrap();
    let query = table.sql_all_ordered_by_id();

    // When
    let stmt = conn.preallocate().unwrap();
    let stmt = stmt.into_statement();
    let stmt_ptr = stmt.as_sys();
    let first;
    let second;
    let third;
    unsafe {
        // 3(UL) ~ SQL_CURSOR_STATIC
        let _ = odbc_sys::SQLSetStmtAttr(
            stmt_ptr,
            odbc_sys::StatementAttribute::CursorType,
            3 as Pointer,
            0,
        );
        // Bind row set buffer
        let mut stmt = Preallocated::new(stmt);
        let mut cursor = stmt.execute(&query, ()).unwrap().unwrap();
        let mut buffer = TextRowSet::for_cursor(1, &mut cursor, Some(10)).unwrap();
        let mut block_cursor = cursor.bind_buffer(&mut buffer).unwrap();
        // Move cursor two forward
        first = block_cursor
            .fetch()
            .unwrap()
            .unwrap()
            .at_as_str(0, 0)
            .unwrap()
            .unwrap()
            .to_owned();
        second = block_cursor
            .fetch()
            .unwrap()
            .unwrap()
            .at_as_str(0, 0)
            .unwrap()
            .unwrap()
            .to_owned();
        let _ = odbc_sys::SQLFetchScroll(stmt_ptr, sys::FetchOrientation::Absolute, 1);
        third = block_cursor
            .fetch()
            .unwrap()
            .unwrap()
            .at_as_str(0, 0)
            .unwrap()
            .unwrap()
            .to_owned();
    }

    // Then
    assert_eq!("one", first);
    assert_eq!("two", second);
    assert_eq!("two", third);
}

/// Learning test to see how downstream applications could recover from truncations during bulk
/// fetches
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn recover_from_truncation(profile: &Profile) {
    // Given a table
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(10)"])
        .build(profile)
        .unwrap();
    conn.execute(&table.sql_insert(), &"1".into_parameter())
        .unwrap();
    conn.execute(&table.sql_insert(), &"123456789".into_parameter())
        .unwrap();
    conn.execute(&table.sql_insert(), &"1".into_parameter())
        .unwrap();
    let query = table.sql_all_ordered_by_id();

    // When
    let stmt = conn.preallocate().unwrap();
    let stmt = stmt.into_statement();
    let stmt_ptr = stmt.as_sys();
    let untruncated;
    unsafe {
        // 3(UL) ~ SQL_CURSOR_STATIC
        let _ = odbc_sys::SQLSetStmtAttr(
            stmt_ptr,
            odbc_sys::StatementAttribute::CursorType,
            3 as Pointer,
            0,
        );
        // Bind row set buffer
        let mut stmt = Preallocated::new(stmt);
        let cursor = stmt.execute(&query, ()).unwrap().unwrap();
        let buffer = TextRowSet::from_max_str_lens(1, [1]).unwrap();
        let mut block_cursor = cursor.bind_buffer(buffer).unwrap();
        // Fetch first row set (one row). This would be truncated
        let _ = block_cursor.fetch().unwrap().unwrap();
        // Fetch second rowset with truncation
        let _ = block_cursor.fetch().unwrap().unwrap();
        // In order to recover we set the position back and bind a large enough buffer.

        // Set position back
        let _ = odbc_sys::SQLFetchScroll(stmt_ptr, sys::FetchOrientation::Prior, 0);

        // Bind large enough buffer
        let (cursor, _) = block_cursor.unbind().unwrap();
        let buffer = TextRowSet::from_max_str_lens(1, [10]).unwrap();
        let mut block_cursor = cursor.bind_buffer(buffer).unwrap();

        let batch = block_cursor.fetch().unwrap().unwrap();
        untruncated = batch.at_as_str(0, 0).unwrap().unwrap().to_owned();
    }

    // Then
    assert_eq!("123456789", untruncated);
}
