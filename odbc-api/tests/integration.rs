mod common;

use odbc_sys::{SqlDataType, Timestamp};
use test_case::test_case;

use common::{
    cursor_to_string, setup_empty_table, table_to_string, Profile, SingleColumnRowSetBuffer, ENV,
};

use odbc_api::{
    buffers::{
        AnyColumnView, AnyColumnViewMut, BufferDescription, BufferKind, ColumnarRowSet, Indicator,
        TextRowSet,
    },
    parameter::{VarBinaryArray, VarCharArray, VarCharSlice},
    ColumnDescription, Cursor, DataType, InputParameter, IntoParameter, Nullability, Nullable,
    U16String,
};
use std::{ffi::CString, iter, thread};

const MSSQL_CONNECTION: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

const MSSQL: &Profile = &Profile {
    connection_string: MSSQL_CONNECTION,
    index_type: "int IDENTITY(1,1)",
};

#[cfg(target_os = "windows")]
const SQLITE_3_CONNECTION: &str = "Driver={SQLite3 ODBC Driver};Database=sqlite-test.db";
#[cfg(not(target_os = "windows"))]
const SQLITE_3_CONNECTION: &str = "Driver={SQLite3};Database=sqlite-test.db";

const SQLITE_3: &Profile = &Profile {
    connection_string: SQLITE_3_CONNECTION,
    index_type: "int IDENTITY(1,1)",
};

#[cfg(target_os = "windows")]
const MARIADB_CONNECTION: &str = "Driver={MariaDB ODBC 3.1 Driver};\
    Server=localhost;DB=test_db;\
    UID=root;PWD=my-secret-pw;\
    Port=3306";

// Use 127.0.0.1 instead of localhost so the system uses the TCP/IP connector instead of the socket
// connector. Prevents error message: 'Can't connect to local MySQL server through socket'.
#[cfg(not(target_os = "windows"))]
const MARIADB_CONNECTION: &str = "Driver={/usr/lib/x86_64-linux-gnu/odbc/libmaodbc.so};\
    Server=127.0.0.1;DB=test_db;\
    UID=root;PWD=my-secret-pw;\
    Port=3306";

const MARIADB: &Profile = &Profile {
    connection_string: MARIADB_CONNECTION,
    index_type: "INTEGER AUTO_INCREMENT PRIMARY KEY",
};

/// Verify writer panics if too large elements are inserted into a binary column of ColumnarRowSet
/// buffer.
#[test]
#[should_panic]
fn insert_too_large_element_in_bin_column() {
    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Binary { length: 1 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));
    buffer.set_num_rows(1);
    if let AnyColumnViewMut::Binary(mut col) = buffer.column_mut(0) {
        col.write(iter::once(Some(&b"too large input."[..])))
    }
}

/// Verify writer panics if too large elements are inserted into a text column of ColumnarRowSet
/// buffer.
#[test]
#[should_panic]
fn insert_too_large_element_in_text_column() {
    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Text { max_str_len: 1 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));
    buffer.set_num_rows(1);
    if let AnyColumnViewMut::Text(mut col) = buffer.column_mut(0) {
        col.write(iter::once(Some(&b"too large input."[..])))
    }
}

#[test]
fn bogus_connection_string() {
    let conn = ENV.connect_with_connection_string("foobar");
    assert!(matches!(conn, Err(_)));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn connect_to_db(profile: &Profile) {
    let _conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
}

#[test]
fn describe_columns() {
    let conn = ENV
        .connect_with_connection_string(MSSQL.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        MSSQL.index_type,
        "DescribeColumns",
        &[
            "VARCHAR(255) NOT NULL",
            "INTEGER",
            "BINARY(12)",
            "VARBINARY(100)",
            "NCHAR(10)",
            "NUMERIC(3,2)",
            "DATETIME2",
            "TIME",
        ],
    )
    .unwrap();
    let sql = "SELECT a,b,c,d,e,f,g,h FROM DescribeColumns ORDER BY Id;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    assert_eq!(cursor.num_result_cols().unwrap(), 8);
    let mut actual = ColumnDescription::default();

    let desc = |name, data_type, nullability| ColumnDescription {
        name: U16String::from_str(name).into_vec(),
        data_type,
        nullability,
    };

    let kind = DataType::Varchar { length: 255 };
    let expected = desc("a", kind, Nullability::NoNulls);
    cursor.describe_col(1, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(1).unwrap());

    let kind = DataType::Integer;
    let expected = desc("b", kind, Nullability::Nullable);
    cursor.describe_col(2, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(2).unwrap());

    let kind = DataType::Binary { length: 12 };
    let expected = desc("c", kind, Nullability::Nullable);
    cursor.describe_col(3, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(3).unwrap());

    let kind = DataType::Varbinary { length: 100 };
    let expected = desc("d", kind, Nullability::Nullable);
    cursor.describe_col(4, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(4).unwrap());

    let kind = DataType::WChar { length: 10 };
    let expected = desc("e", kind, Nullability::Nullable);
    cursor.describe_col(5, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(5).unwrap());

    let kind = DataType::Numeric {
        precision: 3,
        scale: 2,
    };
    let expected = desc("f", kind, Nullability::Nullable);
    cursor.describe_col(6, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(6).unwrap());

    let kind = DataType::Timestamp { precision: 7 };
    let expected = desc("g", kind, Nullability::Nullable);
    cursor.describe_col(7, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(7).unwrap());

    let kind = DataType::Other {
        data_type: SqlDataType(-154),
        column_size: 16,
        decimal_digits: 7,
    };
    let expected = desc("h", kind, Nullability::Nullable);
    cursor.describe_col(8, &mut actual).unwrap();
    assert_eq!(expected, actual);
    assert_eq!(kind, cursor.col_data_type(8).unwrap());
}

#[test]
fn text_buffer() {
    let query = "SELECT title, year FROM Movies ORDER BY year;";
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let cursor = conn.execute(query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

#[test]
fn column_attributes() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title, year FROM Movies;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = Vec::new();

    cursor.col_name(1, &mut buf).unwrap();
    let buf = U16String::from_vec(buf);
    assert_eq!("title", buf.to_string().unwrap());

    let mut buf = buf.into_vec();
    cursor.col_name(2, &mut buf).unwrap();
    let name = U16String::from_vec(buf);
    assert_eq!("year", name.to_string().unwrap());
}

#[test]
fn prices() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT id,day,time,product,price FROM Sales ORDER BY id;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    // Test names
    let mut buf = Vec::new();

    let mut name = |column_number| {
        cursor.col_name(column_number, &mut buf).unwrap();
        std::char::decode_utf16(buf.iter().copied())
            .collect::<Result<String, _>>()
            .unwrap()
    };

    assert_eq!("id", name(1));
    assert_eq!("day", name(2));
    assert_eq!("time", name(3));
    assert_eq!("product", name(4));
    assert_eq!("price", name(5));

    // Test types

    assert_eq!(
        DataType::Decimal {
            precision: 10,
            scale: 2
        },
        cursor.col_data_type(5).unwrap()
    );

    // Test binding id int buffer
    let batch_size = 10;
    assert_eq!(DataType::Integer, cursor.col_data_type(1).unwrap());
    let id_buffer = SingleColumnRowSetBuffer::new(batch_size);
    let mut row_set_cursor = cursor.bind_buffer(id_buffer).unwrap();
    assert_eq!(&[1, 2, 3], row_set_cursor.fetch().unwrap().unwrap().get());
}

/// Bind a CHAR column to a character buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_char(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "BindChar";
    setup_empty_table(&conn, profile.index_type, table_name, &["CHAR(5)"]).unwrap();
    let insert_sql = format!("INSERT INTO {} (a) VALUES ('Hello');", table_name);
    conn.execute(&insert_sql, ()).unwrap();

    let sql = format!("SELECT a FROM {};", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = SingleColumnRowSetBuffer::with_text_column(1, 5);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(Some(&b"Hello"[..]), buf.value_at(0));
}

/// Bind a CHAR column to a wchar buffer
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_char_to_wchar(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "BindCharToWChar";
    setup_empty_table(&conn, profile.index_type, table_name, &["CHAR(5)"]).unwrap();
    let insert_sql = format!("INSERT INTO {} (a) VALUES ('Hello');", table_name);
    conn.execute(&insert_sql, ()).unwrap();

    let sql = format!("SELECT a FROM {};", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = SingleColumnRowSetBuffer::with_wide_text_column(1, 5);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(Some(U16String::from_str("Hello").as_ustr()), buf.ustr_at(0));
}

/// Binds a buffer which is too short to a fixed sized character type. This provokes an indicator of
/// `NO_TOTAL` on MSSQL.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn truncate_fixed_sized(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "TruncateFixedSized";
    setup_empty_table(&conn, profile.index_type, table_name, &["CHAR(5)"]).unwrap();
    let insert_sql = format!("INSERT INTO {} (a) VALUES ('Hello');", table_name);
    conn.execute(&insert_sql, ()).unwrap();

    let sql = format!("SELECT a FROM {};", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = SingleColumnRowSetBuffer::with_text_column(1, 3);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(Some(&b"Hel"[..]), buf.value_at(0));
}

/// Bind a VARCHAR column to a char buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_varchar(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "BindVarchar";
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(100)"]).unwrap();
    let insert_sql = format!("INSERT INTO {} (a) VALUES ('Hello, World!');", table_name);
    conn.execute(&insert_sql, ()).unwrap();

    let sql = format!("SELECT a FROM {};", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = SingleColumnRowSetBuffer::with_text_column(1, 100);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(Some(&b"Hello, World!"[..]), buf.value_at(0));
}

/// Bind a VARCHAR column to a wchar buffer
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_varchar_to_wchar(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "BindVarcharToWChar";
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(100)"]).unwrap();
    let insert_sql = format!("INSERT INTO {} (a) VALUES ('Hello, World!');", table_name);
    conn.execute(&insert_sql, ()).unwrap();

    let sql = format!("SELECT a FROM {};", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let mut buf = SingleColumnRowSetBuffer::with_wide_text_column(1, 100);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();
    drop(row_set_cursor);

    assert_eq!(
        Some(U16String::from_str("Hello, World!").as_ustr()),
        buf.ustr_at(0)
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_numeric_to_float(profile: &Profile) {
    // Setup table
    let table_name = "BindNumericToFloat";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["NUMERIC(3,2)"]).unwrap();
    let insert_sql = format!("INSERT INTO {} (a) VALUES (?);", table_name);
    conn.execute(&insert_sql, &1.23).unwrap();

    let sql = format!("SELECT a FROM {}", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let buf = SingleColumnRowSetBuffer::new(1);
    let mut row_set_cursor = cursor.bind_buffer(buf).unwrap();

    assert_eq!(&[1.23], row_set_cursor.fetch().unwrap().unwrap().get());
}

/// Bind a columnar buffer to a VARBINARY(10) column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] // Convert syntax is different
// #[test_case(SQLITE_3; "SQLite 3")]
fn columnar_fetch_varbinary(profile: &Profile) {
    // Setup
    let table_name = "ColumnarFetchVarbinary";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARBINARY(10)"]).unwrap();
    let insert_sql = format!(
        "INSERT INTO {} (a) Values \
        (CONVERT(Varbinary(10), 'Hello')),\
        (CONVERT(Varbinary(10), 'World')),\
        (NULL)",
        table_name
    );
    conn.execute(&insert_sql, ()).unwrap();

    // Retrieve values
    let cursor = conn
        .execute("SELECT a FROM ColumnarFetchVarbinary ORDER BY Id", ())
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(DataType::Varbinary { length: 10 }, data_type);
    let buffer_kind = BufferKind::from_data_type(data_type).unwrap();
    assert_eq!(BufferKind::Binary { length: 10 }, buffer_kind);
    let buffer_desc = BufferDescription {
        kind: buffer_kind,
        nullable: true,
    };
    let row_set_buffer = ColumnarRowSet::new(10, iter::once(buffer_desc));
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let col_view = batch.column(0);
    let mut col_it = if let AnyColumnView::Binary(col_it) = col_view {
        col_it
    } else {
        panic!("Column View expected to be binary")
    };
    assert_eq!(Some(&b"Hello"[..]), col_it.next().unwrap());
    assert_eq!(Some(&b"World"[..]), col_it.next().unwrap());
    assert_eq!(Some(None), col_it.next()); // Expecting NULL
    assert_eq!(None, col_it.next()); // Expecting iterator end.
}

/// Bind a columnar buffer to a BINARY(5) column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] // different convert syntax
// #[test_case(SQLITE_3; "SQLite 3")]
fn columnar_fetch_binary(profile: &Profile) {
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "ColumnarFetchBinary",
        &["BINARY(5)"],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO ColumnarFetchBinary (a) Values \
        (CONVERT(Binary(5), 'Hello')),\
        (CONVERT(Binary(5), 'World')),\
        (NULL)",
        (),
    )
    .unwrap();

    // Retrieve values
    let cursor = conn
        .execute("SELECT a FROM ColumnarFetchBinary ORDER BY Id", ())
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(DataType::Binary { length: 5 }, data_type);
    let buffer_kind = BufferKind::from_data_type(data_type).unwrap();
    assert_eq!(BufferKind::Binary { length: 5 }, buffer_kind);
    let buffer_desc = BufferDescription {
        kind: buffer_kind,
        nullable: true,
    };
    let row_set_buffer = ColumnarRowSet::new(10, iter::once(buffer_desc));
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let col_view = batch.column(0);
    let mut col_it = if let AnyColumnView::Binary(col_it) = col_view {
        col_it
    } else {
        panic!("Column View expected to be binary")
    };
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
    let table_name = "ColumnarFetchTimestamp";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["DATETIME2(3)"]).unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {} (a) Values \
        ({{ ts '2021-03-20 15:24:12.12' }}),\
        ({{ ts '2020-03-20 15:24:12' }}),\
        ({{ ts '1970-01-01 00:00:00' }}),\
        (NULL)",
            table_name
        ),
        (),
    )
    .unwrap();

    // Retrieve values
    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY Id", table_name), ())
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(DataType::Timestamp { precision: 3 }, data_type);
    let buffer_kind = BufferKind::from_data_type(data_type).unwrap();
    assert_eq!(BufferKind::Timestamp, buffer_kind);
    let buffer_desc = BufferDescription {
        kind: buffer_kind,
        nullable: true,
    };
    let row_set_buffer = ColumnarRowSet::new(10, iter::once(buffer_desc));
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let col_view = batch.column(0);
    let mut col_it = if let AnyColumnView::NullableTimestamp(col_it) = col_view {
        col_it
    } else {
        panic!("Column View expected to be binary")
    };
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
    let table_name = "ColumnarInsertTimestamp";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["DATETIME2"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Timestamp,
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));

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

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::NullableTimestamp(mut writer) = buffer.column_mut(0) {
        writer.write(input.iter().copied());
    } else {
        panic!("Expected timestamp column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "2020-03-20 16:13:54.0000000\n2021-03-20 16:13:54.1234567\nNULL";
    assert_eq!(expected, actual);
}

/// Insert values into a DATETIME2(3) column using a columnar buffer. Milliseconds precision is
/// different from the default precision 7 (100ns).
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_timestamp_ms(profile: &Profile) {
    let table_name = "ColmunarInsertTimestampMs";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["DATETIME2(3)"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Timestamp,
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));

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

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::NullableTimestamp(mut writer) = buffer.column_mut(0) {
        writer.write(input.iter().copied());
    } else {
        panic!("Expected timestamp column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY Id", table_name), ())
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
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "ColumnarInsertVarbinary",
        &["VARBINARY(13)"],
    )
    .unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        kind: BufferKind::Binary { length: 5 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(4, iter::once(desc));

    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::Binary(mut writer) = buffer.column_mut(0) {
        // Reset length to make room for `Hello, World!`.
        writer.set_max_len(13);
        assert_eq!(13, writer.max_len());
        writer.write(input.iter().copied());
    } else {
        panic!("Expected binary column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        "INSERT INTO ColumnarInsertVarbinary (a) VALUES (?)",
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute("SELECT a FROM ColumnarInsertVarbinary ORDER BY Id", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "48656C6C6F\n576F726C64\nNULL\n48656C6C6F2C20576F726C6421";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_varchar(profile: &Profile) {
    let table_name = "ColumnarInsertVarchar";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(13)"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        // Buffer size purposefully chosen too small, so we would get a panic if `set_max_len` would
        // not work.
        kind: BufferKind::Text { max_str_len: 5 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(4, iter::once(desc));

    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::Text(mut writer) = buffer.column_mut(0) {
        // Reset length to make room for `Hello, World!`.
        writer.set_max_len(13);
        assert_eq!(writer.max_len(), 13);
        writer.write(input.iter().copied());
    } else {
        panic!("Expected text column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY Id", table_name), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn adaptive_columnar_insert_varchar(profile: &Profile) {
    let table_name = "AdaptiveColumnarInsertVarchar";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(13)"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
        // encounter larger inputs.
        kind: BufferKind::Text { max_str_len: 1 },
        nullable: true,
    };

    // Input values to insert.
    let input = [
        Some(&b"Hi"[..]),
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];

    let mut buffer = ColumnarRowSet::new(input.len() as u32, iter::once(desc));

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::Text(mut writer) = buffer.column_mut(0) {
        for (index, &text) in input.iter().enumerate() {
            writer.append(index, text)
        }
    } else {
        panic!("Expected text column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY Id", table_name), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hi\nHello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(SQLITE_3; "SQLite 3")]
fn adaptive_columnar_insert_varbin(profile: &Profile) {
    let table_name = "AdaptiveColumnarInsertVarbin";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARBINARY(13)"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
        // encounter larger inputs.
        kind: BufferKind::Binary { length: 1 },
        nullable: true,
    };

    // Input values to insert.
    let input = [
        Some(&b"Hi"[..]),
        Some(&b"Hello"[..]),
        Some(&b"World"[..]),
        None,
        Some(&b"Hello, World!"[..]),
    ];

    let mut buffer = ColumnarRowSet::new(input.len() as u32, iter::once(desc));

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::Binary(mut writer) = buffer.column_mut(0) {
        for (index, &bytes) in input.iter().enumerate() {
            writer.append(index, bytes)
        }
    } else {
        panic!("Expected binary column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY Id", table_name), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "4869\n48656C6C6F\n576F726C64\nNULL\n48656C6C6F2C20576F726C6421";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_wide_varchar(profile: &Profile) {
    let table_name = "ColumnarInsertWideVarchar";
    // Setup
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["NVARCHAR(13)"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        // Buffer size purposefully chosen too small, so we would get a panic if `set_max_len` would
        // not work.
        kind: BufferKind::WText { max_str_len: 5 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));

    // Input values to insert. Note that the last element has > 5 chars and is going to trigger a
    // reallocation of the underlying buffer.
    let input = [
        Some(U16String::from_str("Hello")),
        Some(U16String::from_str("World")),
        None,
        Some(U16String::from_str("Hello, World!")),
    ];

    buffer.set_num_rows(input.len());
    if let AnyColumnViewMut::WText(mut writer) = buffer.column_mut(0) {
        // Reset length to make room for `Hello, World!`.
        writer.set_max_len(13);
        writer.write(
            input
                .iter()
                .map(|opt| opt.as_ref().map(|ustring| ustring.as_slice())),
        );
    } else {
        panic!("Expected text column writer");
    };

    // Bind buffer and insert values.
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &buffer,
    )
    .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY Id", table_name), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test]
fn bind_integer_parameter() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn.execute(sql, &1968).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

/// Learning test. Insert a string ending with \0. Not a terminating zero, but the payload ending
/// itself having zero as the last element.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] SQLite only cares for terminating zero, not the indicator
fn insert_string_ending_with_nul(profile: &Profile) {
    let table_name = "InsertStringEndingWithNul";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(10)"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES(?)", table_name);
    let param = "Hell\0";
    conn.execute(&sql, &param.into_parameter()).unwrap();

    let actual = table_to_string(&conn, table_name, &["a"]);
    assert_eq!("Hell\0", actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn prepared_statement(profile: &Profile) {
    // Setup
    let table_name = "PreparedStatement";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        table_name,
        &["VARCHAR(13)", "INTEGER"],
    )
    .unwrap();
    let insert = format!(
        "INSERT INTO {} (a,b) VALUES ('First', 1), ('Second', 2);",
        table_name
    );
    conn.execute(&insert, ()).unwrap();

    // Prepare the statement once
    let sql = format!("SELECT a FROM {} where b=?;", table_name);
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
fn preallocated(profile: &Profile) {
    // Prepare the statement once
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, "Preallocated", &["VARCHAR(10)"]).unwrap();
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
fn preallocation_soundness(profile: &Profile) {
    // Prepare the statement once
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "PreallocationSoundness",
        &["VARCHAR(10)"],
    )
    .unwrap();
    let mut prealloc = conn.preallocate().unwrap();

    {
        let res = prealloc
            .execute(
                "INSERT INTO PreallocationSoundness (a) VALUES ('Hello')",
                (),
            )
            .unwrap();
        assert!(res.is_none());
    }

    {
        let cursor = prealloc
            .execute("SELECT a FROM PreallocationSoundness ORDER BY id", ())
            .unwrap()
            .unwrap();
        let actual = cursor_to_string(cursor);
        let expected = "Hello";
        assert_eq!(expected, actual);
    }

    {
        let mut cursor = prealloc
            .execute("SELECT a FROM PreallocationSoundness ORDER BY id", ())
            .unwrap()
            .unwrap();

        // Fetch without binding buffers. If columns would still be bound we might see an invalid
        // memory access.
        let _row = cursor.next_row().unwrap().unwrap();
        assert!(cursor.next_row().unwrap().is_none());
    }
}

#[test]
fn integer_parameter_as_string() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn
        .execute(sql, &"1968".into_parameter())
        .unwrap()
        .unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test]
fn parameter_option_integer_some() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn
        .execute(sql, &Some(1968).into_parameter())
        .unwrap()
        .unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test]
fn parameter_option_integer_none() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn
        .execute(sql, &None::<i32>.into_parameter())
        .unwrap()
        .unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    assert!(cursor.fetch().unwrap().is_none());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] SQLite will work only if increasing length to VARCHAR(2).
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn non_ascii_char(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "NonAsciiChar";

    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(1)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES ('A'), ('Ü');", table_name),
        (),
    )
    .unwrap();

    let sql = format!("SELECT a FROM {} ORDER BY id;", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();
    let output = cursor_to_string(cursor);
    assert_eq!("A\nÜ", output);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn wchar(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "WChar";

    setup_empty_table(&conn, profile.index_type, table_name, &["NVARCHAR(1)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES ('A'), ('Ü');", table_name),
        (),
    )
    .unwrap();

    let sql = format!("SELECT a FROM {} ORDER BY id;", table_name);
    let cursor = conn.execute(&sql, ()).unwrap().unwrap();

    let desc = BufferDescription {
        nullable: false,
        kind: BufferKind::WText { max_str_len: 1 },
    };
    let row_set_buffer = ColumnarRowSet::new(2, iter::once(desc));
    let mut row_set_cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = row_set_cursor.fetch().unwrap().unwrap();
    let col = batch.column(0);
    let mut wtext_col = match col {
        AnyColumnView::WText(col) => col,
        _ => panic!("Unexpected column type"),
    };
    assert_eq!(U16String::from_str("A"), wtext_col.next().unwrap().unwrap());
    assert_eq!(U16String::from_str("Ü"), wtext_col.next().unwrap().unwrap());
    assert!(wtext_col.next().is_none());
    assert!(row_set_cursor.fetch().unwrap().is_none());
}

#[test]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn wchar_as_char() {
    let conn = ENV
        .connect_with_connection_string(MSSQL.connection_string)
        .unwrap();
    // NVARCHAR(2) <- NVARCHAR(1) would be enough to held the character, but we de not allocate
    // enough memory on the client side to hold the entire string.
    setup_empty_table(&conn, MSSQL.index_type, "WCharAsChar", &["NVARCHAR(1)"]).unwrap();

    conn.execute("INSERT INTO WCharAsChar (a) VALUES ('A'), ('Ü');", ())
        .unwrap();

    let sql = "SELECT a FROM WCharAsChar ORDER BY id;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();
    let output = cursor_to_string(cursor);
    assert_eq!("A\nÜ", output);
}

#[test]
fn two_parameters_in_tuple() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title FROM Movies where ? < year AND year < ?;";
    let cursor = conn.execute(sql, (&1960, &1970)).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn heterogenous_parameters_in_array(profile: &Profile) {
    let table_name = "heterogenous_parameters_in_array";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();

    // Setup table
    setup_empty_table(
        &conn,
        profile.index_type,
        table_name,
        &["INTEGER", "VARCHAR(13)"],
    )
    .unwrap();
    let insert_sql = format!(
        "INSERT INTO {} (a, b) VALUES (1, 'Hello'), (2, 'Hello'), (3, 'Hello'), (3, 'Hallo')",
        table_name
    );
    conn.execute(&insert_sql, ()).unwrap();

    // Execute test
    let query = format!("SELECT a,b FROM {} where  a > ? AND b = ?;", table_name);
    let params: [Box<dyn InputParameter>; 2] = [Box::new(2), Box::new("Hello".into_parameter())];
    let cursor = conn.execute(&query, &params[..]).unwrap().unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!("3,Hello", actual);
}

#[test]
fn column_names_iterator() {
    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    let sql = "SELECT title, year FROM Movies;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();
    let names: Vec<_> = cursor
        .column_names()
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();

    assert_eq!(&["title", "year"], names.as_slice());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bulk_insert_with_text_buffer(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "BulkInsertWithTextBuffer",
        &["VARCHAR(50)"],
    )
    .unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let mut prepared = conn
        .prepare("INSERT INTO BulkInsertWithTextBuffer (a) Values (?)")
        .unwrap();
    let mut params = TextRowSet::new(5, [50].iter().copied());
    params.append(["England"].iter().map(|s| Some(s.as_bytes())));
    params.append(["France"].iter().map(|s| Some(s.as_bytes())));
    params.append(["Germany"].iter().map(|s| Some(s.as_bytes())));

    prepared.execute(&params).unwrap();

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
fn bulk_insert_with_columnar_buffer(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "BulkInsertWithColumnarBuffer",
        &["VARCHAR(50)", "INTEGER"],
    )
    .unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let mut prepared = conn
        .prepare("INSERT INTO BulkInsertWithColumnarBuffer (a,b) Values (?,?)")
        .unwrap();
    let description = [
        BufferDescription {
            nullable: true,
            kind: BufferKind::Text { max_str_len: 50 },
        },
        BufferDescription {
            nullable: true,
            kind: BufferKind::I32,
        },
    ]
    .iter()
    .copied();
    let mut params = ColumnarRowSet::new(5, description);
    params.set_num_rows(3);
    let mut view_mut = params.column_mut(0);
    // Fill first column with text
    match &mut view_mut {
        AnyColumnViewMut::Text(col) => {
            let input = ["England", "France", "Germany"];
            col.write(input.iter().map(|&s| Some(s.as_bytes())))
        }
        _ => panic!("Unexpected column type"),
    }
    // Fill second column with integers
    let mut view_mut = params.column_mut(1);
    match &mut view_mut {
        AnyColumnViewMut::NullableI32(col) => {
            let input = [1, 2, 3];
            col.write(input.iter().map(|&i| Some(i)))
        }
        _ => panic!("Unexpected column type"),
    }

    prepared.execute(&params).unwrap();

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
fn send_connection(profile: &Profile) {
    let table_name = "SendConnection";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["INTEGER"]).unwrap();

    // Insert in one thread, query in another, using the same connection.
    let insert_sql = format!("INSERT INTO {} (a) VALUES (1),(2),(3)", table_name);
    conn.execute(&insert_sql, ()).unwrap();

    let conn = unsafe { conn.promote_to_send() };

    let handle = thread::spawn(move || table_to_string(&conn, table_name, &["a"]));

    let actual = handle.join().unwrap();
    assert_eq!("1\n2\n3", actual)
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn parameter_option_str(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "ParameterOptionStr",
        &["VARCHAR(50)"],
    )
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
fn parameter_option_byte_slice(profile: &Profile) {
    let table_name = "ParameterOptionByteSlice";

    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARBINARY(50)"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (?);", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();
    prepared.execute(&None::<&[u8]>.into_parameter()).unwrap();
    prepared
        .execute(&Some(&[1, 2, 3][..]).into_parameter())
        .unwrap();
    prepared.execute(&None::<&[u8]>.into_parameter()).unwrap();
    prepared
        .execute(&Some(vec![1, 2, 3]).into_parameter())
        .unwrap();

    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY id", table_name), ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "NULL\n010203\nNULL\n010203";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn parameter_varchar_512(profile: &Profile) {
    let table_name = "ParameterVarchar512";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(50)"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (?);", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();

    prepared.execute(&VarCharArray::<512>::NULL).unwrap();
    prepared
        .execute(&VarCharArray::<512>::new(b"Bernd"))
        .unwrap();

    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "NULL\nBernd";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Different string representation of binary data
// #[test_case(SQLITE_3; "SQLite 3")] Different string representation of binary data
fn parameter_varbinary_512(profile: &Profile) {
    let table_name = "ParameterVarbinary512";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARBINARY(50)"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (?);", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();

    prepared.execute(&VarBinaryArray::<512>::NULL).unwrap();
    prepared
        .execute(&VarBinaryArray::<512>::new(&[1, 2, 3]))
        .unwrap();

    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "NULL\n010203";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn parameter_cstr(profile: &Profile) {
    let table_name = "ParameterCStr";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(50)"]).unwrap();
    let sql = format!("INSERT INTO {} (a) VALUES (?);", table_name);
    let mut prepared = conn.prepare(&sql).unwrap();

    let param = CString::new("Hello, World!").unwrap();

    prepared.execute(&param).unwrap();
    prepared.execute(param.as_c_str()).unwrap();

    let actual = table_to_string(&conn, table_name, &["a"]);
    let expected = "Hello, World!\nHello, World!";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn read_into_columnar_buffer(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "ReadIntoColumnarBuffer",
        &["INTEGER", "VARCHAR(20)"],
    )
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
        BufferDescription {
            kind: BufferKind::I32,
            nullable: true,
        },
        BufferDescription {
            nullable: true,
            kind: BufferKind::Text { max_str_len: 20 },
        },
    ];
    let buffer = ColumnarRowSet::new(20, buffer_description.iter().copied());
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    // Assert existence of first batch
    let batch = cursor.fetch().unwrap().unwrap();
    match dbg!(batch.column(0)) {
        AnyColumnView::NullableI32(mut col) => assert_eq!(Some(&42), col.next().unwrap()),
        _ => panic!("Unexpected buffer type"),
    }
    match dbg!(batch.column(1)) {
        AnyColumnView::Text(mut col) => {
            assert_eq!(Some(&b"Hello, World!"[..]), col.next().unwrap())
        }
        _ => panic!("Unexpected buffer type"),
    }

    // Assert that there is no second batch.
    assert!(cursor.fetch().unwrap().is_none());
}

/// In use cases there the user supplies the query it may be necessary to ignore one column then
/// binding the buffers. This test constructs a result set with 3 columns and ignores the second
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn ignore_output_column(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "IgnoreOutputColumn",
        &["INTEGER", "INTEGER", "INTEGER"],
    )
    .unwrap();
    let cursor = conn
        .execute("SELECT a, b, c FROM IgnoreOutputColumn", ())
        .unwrap()
        .unwrap();

    let bd = BufferDescription {
        kind: BufferKind::I32,
        nullable: true,
    };
    let buffer = ColumnarRowSet::with_column_indices(20, [(1, bd), (3, bd)].iter().copied());
    let mut cursor = cursor.bind_buffer(buffer).unwrap();

    // Assert that there is no batch.
    assert!(cursor.fetch().unwrap().is_none());
}

#[test]
fn output_parameter() {
    use odbc_api::Out;

    let mut ret = Nullable::<i32>::null();
    let mut param = Nullable::<i32>::new(7);

    let conn = ENV
        .connect_with_connection_string(MSSQL_CONNECTION)
        .unwrap();
    conn.execute("{? = call TestParam(?)}", (Out(&mut ret), &mut param))
        .unwrap();

    // See magic numbers hardcoded in setup.sql
    assert_eq!(Some(99), ret.into_opt());
    assert_eq!(Some(7 + 5), param.into_opt());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn manual_commit_mode(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, "ManualCommitMode", &["INTEGER"]).unwrap();

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
fn unfinished_transaction(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "UnfinishedTransaction",
        &["INTEGER"],
    )
    .unwrap();

    // Manual commit mode needs to be explicitly enabled, since autocommit mode is default.
    conn.set_autocommit(false).unwrap();

    // Insert a value into the table.
    conn.execute("INSERT INTO UnfinishedTransaction (a) VALUES (5);", ())
        .unwrap();
}

/// Test behavior of strings with interior nul
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")]
fn interior_nul(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, "InteriorNul", &["VARCHAR(10)"]).unwrap();

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
    let expected = "a\0b";
    assert_eq!(expected, actual);
}

/// Use get_data to retrieve an integer
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn get_data_int(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, "GetDataInt", &["INTEGER"]).unwrap();

    conn.execute("INSERT INTO GetDataInt (a) VALUES (42)", ())
        .unwrap();

    let mut cursor = conn
        .execute("SELECT a FROM GetDataInt", ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = Nullable::<i32>::null();

    row.get_data(1, &mut actual).unwrap();
    assert_eq!(Some(42), actual.into_opt());

    // Cursor has reached its end
    assert!(cursor.next_row().unwrap().is_none())
}

/// Use get_data to retrieve a string
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn get_data_string(profile: &Profile) {
    let table_name = "GetDataString";

    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["Varchar(50)"]).unwrap();

    conn.execute(
        &format!(
            "INSERT INTO {} (a) VALUES ('Hello, World!'), (NULL)",
            table_name
        ),
        (),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY id", table_name), ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = VarCharArray::<32>::NULL;

    row.get_data(1, &mut actual).unwrap();
    assert_eq!(Some(&b"Hello, World!"[..]), actual.as_bytes());

    // second row
    row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut actual).unwrap();
    assert!(actual.as_bytes().is_none());

    // Cursor has reached its end
    assert!(cursor.next_row().unwrap().is_none())
}

/// Use get_data to retrieve a binary data
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn get_data_binary(profile: &Profile) {
    let table_name = "GetDataBinary";

    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["Varbinary(50)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?), (NULL)", table_name),
        &[1u8, 2, 3].into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY id", table_name), ())
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
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Does not support Varchar(max) syntax
// #[test_case(SQLITE_3; "SQLite 3")] Does not support Varchar(max) syntax
fn large_strings(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, "LargeStrings", &["Varchar(max)"]).unwrap();

    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();

    conn.execute(
        "INSERT INTO LargeStrings (a) VALUES (?)",
        &input.as_str().into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute("SELECT a FROM LargeStrings ORDER BY id", ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut buf = VarCharArray::<32>::NULL;
    let mut actual = String::new();

    loop {
        row.get_data(1, &mut buf).unwrap();
        actual += &std::str::from_utf8(buf.as_bytes().unwrap()).unwrap();
        if buf.is_complete() {
            break;
        }
    }

    assert_eq!(input, actual);
}

/// Test insertion and retrieving of large string values using get_text. Try to provoke
/// `SQL_NO_TOTAL` as a return value in the indicator buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] Does not support Varchar(max) syntax
// #[test_case(SQLITE_3; "SQLite 3")] Does not support Varchar(max) syntax
fn large_strings_get_text(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "LargeStringsGetText",
        &["Varchar(max)"],
    )
    .unwrap();

    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();

    conn.execute(
        "INSERT INTO LargeStringsGetText (a) VALUES (?)",
        &input.as_str().into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute("SELECT a FROM LargeStringsGetText ORDER BY id", ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = Vec::new();

    row.get_text(1, &mut actual).unwrap();

    assert_eq!(input, String::from_utf8(actual).unwrap());
}

/// Retrieving of short string values using get_data. This also helps to assert that we correctly
/// shorten the vectors length if the capacity of the originally passed in vector had been larger
/// than the retrieved string.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn short_strings_get_text(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        "ShortStringsGetText",
        &["Varchar(15)"],
    )
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
fn short_get_binary(profile: &Profile) {
    let table_name = "ShortGetBinary";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["Varbinary(15)"]).unwrap();

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &[1u8, 2, 3].into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY id", table_name), ())
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
// #[test_case(MARIADB; "Maria DB")] Does not support Varchar(max) syntax
// #[test_case(SQLITE_3; "SQLite 3")] Does not support Varchar(max) syntax
fn large_get_binary(profile: &Profile) {
    let table_name = "LargeGetBinary";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(&conn, profile.index_type, table_name, &["Varbinary(max)"]).unwrap();

    let input = vec![42; 2000];

    conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?)", table_name),
        &input.as_slice().into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY id", table_name), ())
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
fn capped_text_buffer(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "CappedTextBuffer";

    // Prepare table content
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(13)"]).unwrap();
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES ('Hello, World!');", table_name),
        (),
    )
    .unwrap();

    let cursor = conn
        .execute(&format!("SELECT a FROM {} ORDER BY id", table_name), ())
        .unwrap()
        .unwrap();

    let row_set_buffer = TextRowSet::for_cursor(1, &cursor, Some(5)).unwrap();
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
fn use_truncated_output_as_input(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "UseTruncatedOutputAsInput";

    // Prepare table content
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(13)"]).unwrap();
    conn.execute(
        &format!("INSERT INTO {} (a) VALUES ('Hello, World!');", table_name),
        (),
    )
    .unwrap();

    // Query 'Hello, World!' From the DB in a buffer with size 5. This should give us a Hello minus
    // the letter 'o' since we also need space for a terminating zero. => 'Hell'.
    let mut buf = VarCharArray::<5>::NULL;
    let query = format!("SELECT a FROM {}", table_name);
    let mut cursor = conn.execute(&query, ()).unwrap().unwrap();
    let mut row = cursor.next_row().unwrap().unwrap();
    row.get_data(1, &mut buf).unwrap();
    assert_eq!(b"Hell", buf.as_bytes().unwrap());
    assert_eq!(buf.is_complete(), false);
    drop(row);
    drop(cursor);

    let insert = format!("INSERT INTO {} (a) VALUES (?)", table_name);
    buf.hide_truncation();
    conn.execute(&insert, &buf).unwrap();

    let actual = table_to_string(&conn, table_name, &["a"]);
    assert_eq!("Hello, World!\nHell", actual);
}

/// Verify that the driver does not insert from invalid memory if inserting a truncated value
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn insert_truncated_value(profile: &Profile) {
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    let table_name = "InsertedTruncatedValue";

    // Prepare table content
    setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(13)"]).unwrap();

    let memory = "Hello\0INVALID MEMORY\0";
    // Contains hello plus terminating zero.
    let valid = &memory.as_bytes()[..6];
    // Truncated value.
    let parameter = VarCharSlice::from_buffer(valid, Indicator::Length(memory.len()));
    let result = conn.execute(
        &format!("INSERT INTO {} (a) VALUES (?);", table_name),
        &parameter,
    );

    match result {
        Err(e) => {
            // Failing is fine, especially with an error indicating truncation.
            eprintln!("{}", e)
        }
        Ok(None) => {
            // If this was successful we should make sure we did not insert 'INVALID MEMORY' into
            // the database. The better database drivers do not do this, and this could be seen as
            // wrong, but we are only interessted in unsafe behaviour.
            let actual = table_to_string(&conn, table_name, &["a"]);
            assert_eq!("Hello", actual)
        }
        _ => panic!("Unexpected cursor"),
    }
}

// #[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")]
// fn insert_truncated_var_char_array(profile: &Profile) {
//     let conn = ENV
//         .connect_with_connection_string(profile.connection_string)
//         .unwrap();
//     let table_name = "InsertedTruncatedVarCharArray";

//     // Prepare table content
//     setup_empty_table(&conn, profile.index_type, table_name, &["VARCHAR(50)"]).unwrap();

//     let memory = "Hello, World!INVALID MEMORY\0";
//     // Contains hello world.
//     let valid = &memory.as_bytes()[..13];
//     // Truncated value. Buffer can only hold 'Hello'
//     let parameter = VarCharArray::<5>::new(&valid);
//     let result = conn.execute(
//         &format!("INSERT INTO {} (a) VALUES (?);", table_name),
//         &parameter,
//     );

//     match result {
//         Err(e) => {
//             // Failing is fine, especially with an error indicating truncation.
//             eprintln!("{}", e)
//         }
//         Ok(None) => {
//             // If this was successful we should make sure we did not insert 'INVALID MEMORY' into
//             // the database. The better database drivers do not do this, and this could be seen as
//             // wrong, but we are only interessted in unsafe behaviour.
//             let actual = table_to_string(&conn, table_name, &["a"]);
//             assert_eq!("Hello", actual)
//         }
//         _ => panic!("Unexpected cursor"),
//     }
// }

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
fn arbitrary_input_parameters(profile: &Profile) {
    let table_name = "ArbitraryInputParameters";
    let conn = ENV
        .connect_with_connection_string(profile.connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
        profile.index_type,
        table_name,
        &["VARCHAR(20)", "INTEGER"],
    )
    .unwrap();

    let insert_statement = format!("INSERT INTO {} (a, b) VALUES (?, ?);", table_name);
    let param_a: Box<dyn InputParameter> = Box::new("Hello, World!".to_string().into_parameter());
    let param_b: Box<dyn InputParameter> = Box::new(42.into_parameter());
    let parameters = vec![param_a, param_b];

    conn.execute(&insert_statement, parameters.as_slice())
        .unwrap();

    let actual = table_to_string(&conn, table_name, &["a", "b"]);
    assert_eq!("Hello, World!,42", actual)
}

/// This test is inspired by a bug caused from a fetch statement generating a lot of diagnostic
/// messages.
#[test]
#[ignore = "Runs for a very long time"]
fn many_diagnostic_messages() {
    let table_name = "ManyDiagnosticMessages";
    let conn = ENV
        .connect_with_connection_string(MSSQL.connection_string)
        .unwrap();
    // In order to generate a lot of diagnostic messages with one function call, we try a bulk
    // insert for which each row generates a warning.
    // Setup table
    setup_empty_table(&conn, MSSQL.index_type, table_name, &["VARCHAR(2)"]).unwrap();

    // Incidentally our batch size is too large to be hold in an `i16`.
    let batch_size = 2 << 15;

    // Fill each row in the buffer with two letters.
    let mut buffer = TextRowSet::new(batch_size, iter::once(2));

    for _ in 0..batch_size {
        buffer.append([Some(&b"ab"[..])].iter().cloned());
    }

    let insert_sql = format!("INSERT INTO {} (a) VALUES (?)", table_name);
    conn.execute(&insert_sql, &buffer).unwrap();

    let query_sql = format!("SELECT a FROM {}", table_name);
    buffer = TextRowSet::new(batch_size, iter::once(1));
    let cursor = conn.execute(&query_sql, ()).unwrap().unwrap();
    let mut row_set_cursor = cursor.bind_buffer(buffer).unwrap();

    // This should cause the string to be truncated, since they are 2 letters wide, but there is
    // space for one. This should cause at least one warning per row.
    let _ = row_set_cursor.fetch();

    // We do not have an explicit assertion, we are just happy if no integer addition overflows.
}

/// Ensures access to driver and data source info is synchronized correctly when multiple threads
/// attempt to query it at the same time. First, we query the list of the known drivers and data
/// sources on the main thread. Then we spawn multiple threads that attempt to query these lists in
/// parallel. Finally we compare the lists to ensure they match the list we found on the main
/// thread.
#[test]
fn synchronized_access_to_driver_and_data_source_info() {
    let expected_drivers = ENV.drivers().unwrap();
    let expected_data_sources = ENV.data_sources().unwrap();

    const NUM_THREADS: usize = 5;
    let threads = iter::repeat(())
        .take(NUM_THREADS)
        .map(|_| {
            let expected_drivers = expected_drivers.clone();
            let expected_data_sources = expected_data_sources.clone();

            thread::spawn(move || {
                let drivers = ENV.drivers().unwrap();
                assert_eq!(expected_drivers, drivers);
                let data_sources_for_thread = ENV.data_sources().unwrap();
                assert_eq!(expected_data_sources, data_sources_for_thread);
            })
        })
        .collect::<Vec<_>>();

    for handle in threads {
        handle.join().unwrap();
    }
}
