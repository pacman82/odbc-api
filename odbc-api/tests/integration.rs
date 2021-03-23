mod common;

use odbc_sys::{SqlDataType, Timestamp};
use test_case::test_case;

use common::{cursor_to_string, setup_empty_table, SingleColumnRowSetBuffer, ENV};

use odbc_api::{
    buffers::{
        AnyColumnView, AnyColumnViewMut, BufferDescription, BufferKind, ColumnarRowSet, Indicator,
        TextRowSet,
    },
    parameter::VarChar32,
    ColumnDescription, Cursor, DataType, IntoParameter, Nullability, Nullable, U16String,
};
use std::{iter, thread};

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

#[cfg(target_os = "windows")]
const SQLITE_3: &str = "Driver={SQLite3 ODBC Driver};Database=sqlite-test.db";
#[cfg(not(target_os = "windows"))]
const SQLITE_3: &str = "Driver={SQLite3};Database=sqlite-test.db";

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

#[test]
fn connect_to_movies_db() {
    let _conn = ENV.connect_with_connection_string(MSSQL).unwrap();
}

#[test]
fn describe_columns() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(
        &conn,
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let cursor = conn.execute(query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

#[test]
fn column_attributes() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_char(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "BindChar";
    setup_empty_table(&conn, table_name, &["CHAR(5)"]).unwrap();
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

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_char_to_wchar(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "BindCharToWChar";
    setup_empty_table(&conn, table_name, &["CHAR(5)"]).unwrap();
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
#[test_case(SQLITE_3; "SQLite 3")]
fn truncate_fixed_sized(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "TruncateFixedSized";
    setup_empty_table(&conn, table_name, &["CHAR(5)"]).unwrap();
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

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_varchar(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "BindVarchar";
    setup_empty_table(&conn, table_name, &["VARCHAR(100)"]).unwrap();
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

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn bind_varchar_to_wchar(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "BindVarcharToWChar";
    setup_empty_table(&conn, table_name, &["VARCHAR(100)"]).unwrap();
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

#[test]
fn bind_numeric_to_float() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_numeric FROM AllTheTypes;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();
    let buf = SingleColumnRowSetBuffer::new(1);
    let mut row_set_cursor = cursor.bind_buffer(buf).unwrap();

    assert_eq!(&[1.23], row_set_cursor.fetch().unwrap().unwrap().get());
}

/// Bind a columnar buffer to a VARBINARY(10) column and fetch data.
#[test]
fn columnar_fetch_varbinary() {
    // Setup
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "ColumnarFetchVarbinary", &["VARBINARY(10)"]).unwrap();
    conn.execute(
        "INSERT INTO ColumnarFetchVarbinary (a) Values \
        (CONVERT(Varbinary(10), 'Hello')),\
        (CONVERT(Varbinary(10), 'World')),\
        (NULL)",
        (),
    )
    .unwrap();

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
#[test]
fn columnar_fetch_binary() {
    // Setup
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "ColumnarFetchBinary", &["BINARY(5)"]).unwrap();
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
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_fetch_timestamp(connection_string: &str) {
    let table_name = "ColumnarFetchTimestamp";
    // Setup
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2(3)"]).unwrap();
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
#[test]
fn columnar_insert_timestamp() {
    let table_name = "ColmunarInsertTimestamp";
    // Setup
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2"]).unwrap();

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
    let expected = "2020-03-20 16:13:54.0000000\n2021-03-20 16:13:54.1234567\nNULL";
    assert_eq!(expected, actual);
}

/// Insert values into a DATETIME2(3) column using a columnar buffer. Milliseconds precision is
/// different from the default precision 7 (100ns).
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_timestamp_ms(connection_string: &str) {
    let table_name = "ColmunarInsertTimestampMs";
    // Setup
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, table_name, &["DATETIME2(3)"]).unwrap();

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
#[test]
fn columnar_insert_varbinary() {
    // Setup
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "ColumnarInsertVarbinary", &["VARBINARY(13)"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_varchar(connection_string: &str) {
    let table_name = "ColumnarInsertVarchar";
    // Setup
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(13)"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn adaptive_columnar_insert_varchar(connection_string: &str) {
    let table_name = "AdaptiveColumnarInsertVarchar";
    // Setup
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, table_name, &["VARCHAR(13)"]).unwrap();

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
fn adaptive_columnar_insert_varbin(connection_string: &str) {
    let table_name = "AdaptiveColumnarInsertVarbin";
    // Setup
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, table_name, &["VARBINARY(13)"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn columnar_insert_wide_varchar(connection_string: &str) {
    let table_name = "ColumnarInsertWideVarchar";
    // Setup
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, table_name, &["NVARCHAR(13)"]).unwrap();

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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn.execute(sql, &1968).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test]
fn prepared_statement() {
    // Prepare the statement once
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let mut prepared = conn.prepare(sql).unwrap();

    // Execute it two times with different parameters
    {
        let cursor = prepared.execute(&1968).unwrap().unwrap();
        let title = cursor_to_string(cursor);
        assert_eq!("2001: A Space Odyssey", title);
    }

    {
        let cursor = prepared.execute(&1993).unwrap().unwrap();
        let title = cursor_to_string(cursor);
        assert_eq!("Jurassic Park", title);
    }
}

/// Reuse a preallocated handle, two times in a row.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn preallocated(connection_string: &str) {
    // Prepare the statement once
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "Preallocated", &["VARCHAR(10)"]).unwrap();
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
#[test_case(SQLITE_3; "SQLite 3")]
fn preallocation_soundness(connection_string: &str) {
    // Prepare the statement once
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "PreallocationSoundness", &["VARCHAR(10)"]).unwrap();
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
// #[test_case(SQLITE_3; "SQLite 3")] SQLite will work only if increasing length to VARCHAR(2).
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn char(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "Char";

    setup_empty_table(&conn, table_name, &["VARCHAR(1)"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn wchar(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "WChar";

    setup_empty_table(&conn, table_name, &["NVARCHAR(1)"]).unwrap();

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
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    // NVARCHAR(2) <- NVARCHAR(1) would be enough to held the character, but we de not allocate
    // enough memory on the client side to hold the entire string.
    setup_empty_table(&conn, "WCharAsChar", &["NVARCHAR(1)"]).unwrap();

    conn.execute("INSERT INTO WCharAsChar (a) VALUES ('A'), ('Ü');", ())
        .unwrap();

    let sql = "SELECT a FROM WCharAsChar ORDER BY id;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();
    let output = cursor_to_string(cursor);
    assert_eq!("A\nÜ", output);
}

#[test]
fn two_parameters_in_tuple() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where ? < year AND year < ?;";
    let cursor = conn.execute(sql, (&1960, &1970)).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor, None).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test]
fn column_names_iterator() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
#[test_case(SQLITE_3; "SQLite 3")]
fn bulk_insert_with_text_buffer(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "BulkInsertWithTextBuffer", &["VARCHAR(50)"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn bulk_insert_with_columnar_buffer(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
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

#[test]
fn send_connection() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let conn = unsafe { conn.promote_to_send() };

    let handle = thread::spawn(move || {
        conn.execute("SELECT title FROM Movies ORDER BY year", ())
            .unwrap()
            .unwrap();
    });

    handle.join().unwrap();
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn parameter_option_str(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "ParameterOptionStr", &["VARCHAR(50)"]).unwrap();
    let sql = "INSERT INTO ParameterOptionStr (a) VALUES (?);";
    let mut prepared = conn.prepare(sql).unwrap();
    prepared.execute(&None::<&str>.into_parameter()).unwrap();
    prepared.execute(&Some("Bernd").into_parameter()).unwrap();

    let cursor = conn
        .execute("SELECT a FROM ParameterOptionStr ORDER BY id", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "NULL\nBernd";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn parameter_varchar_512(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "ParameterVarchar512", &["VARCHAR(50)"]).unwrap();
    let sql = "INSERT INTO ParameterVarchar512 (a) VALUES (?);";
    let mut prepared = conn.prepare(sql).unwrap();

    prepared.execute(&VarChar32::copy_from_bytes(None)).unwrap();
    prepared
        .execute(&VarChar32::copy_from_bytes(Some(b"Bernd")))
        .unwrap();

    let cursor = conn
        .execute("SELECT a FROM ParameterVarchar512 ORDER BY id", ())
        .unwrap()
        .unwrap();

    let actual = cursor_to_string(cursor);
    let expected = "NULL\nBernd";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn read_into_columnar_buffer(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "ReadIntoColumnarBuffer", &["INTEGER", "VARCHAR(20)"]).unwrap();
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
#[test_case(SQLITE_3; "SQLite 3")]
fn ignore_output_column(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(
        &conn,
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

    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    conn.execute("{? = call TestParam(?)}", (Out(&mut ret), &mut param))
        .unwrap();

    // See magic numbers hardcoded in setup.sql
    assert_eq!(Some(99), ret.into_opt());
    assert_eq!(Some(7 + 5), param.into_opt());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn manual_commit_mode(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "ManualCommitMode", &["INTEGER"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn unfinished_transaction(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "UnfinishedTransaction", &["INTEGER"]).unwrap();

    // Manual commit mode needs to be explicitly enabled, since autocommit mode is default.
    conn.set_autocommit(false).unwrap();

    // Insert a value into the table.
    conn.execute("INSERT INTO UnfinishedTransaction (a) VALUES (5);", ())
        .unwrap();
}

/// Test behavior of strings with interior nul
#[test]
fn interior_nul() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "InteriorNul", &["VARCHAR(10)"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn get_data_int(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "GetDataInt", &["INTEGER"]).unwrap();

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
#[test_case(SQLITE_3; "SQLite 3")]
fn get_data_string(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "GetDataString", &["Varchar(50)"]).unwrap();

    conn.execute(
        "INSERT INTO GetDataString (a) VALUES ('Hello, World!'), (NULL)",
        (),
    )
    .unwrap();

    let mut cursor = conn
        .execute("SELECT a FROM GetDataString ORDER BY id", ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut actual = VarChar32::copy_from_bytes(None);

    row.get_data(1, &mut actual).unwrap();
    assert_eq!(Some(&b"Hello, World!"[..]), actual.as_bytes());

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
// #[test_case(SQLITE_3; "SQLite 3")] Does not support Varchar(max) syntax
fn large_strings(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "LargeStrings", &["Varchar(max)"]).unwrap();

    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();

    conn.execute(
        "INSERT INTO LargeStrings (a) VALUES (?)",
        &input.into_parameter(),
    )
    .unwrap();

    let mut cursor = conn
        .execute("SELECT a FROM LargeStrings ORDER BY id", ())
        .unwrap()
        .unwrap();

    let mut row = cursor.next_row().unwrap().unwrap();
    let mut buf = VarChar32::copy_from_bytes(None);
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

/// Test insertion and retrieving of large string values using get_data. Try to provoke
/// `SQL_NO_TOTAL` as a return value in the indicator buffer.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(SQLITE_3; "SQLite 3")] Does not support Varchar(max) syntax
fn large_strings_get_text(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "LargeStringsGetText", &["Varchar(max)"]).unwrap();

    let input = String::from_utf8(vec![b'a'; 2000]).unwrap();

    conn.execute(
        "INSERT INTO LargeStringsGetText (a) VALUES (?)",
        &input.into_parameter(),
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
#[test_case(SQLITE_3; "SQLite 3")]
fn short_strings_get_text(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    setup_empty_table(&conn, "ShortStringsGetText", &["Varchar(15)"]).unwrap();

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

    // Make inintial buffer larger than the string we want to fetch.
    let mut actual = Vec::with_capacity(100);

    row.get_text(1, &mut actual).unwrap();

    assert_eq!("Hello, World!", std::str::from_utf8(&actual).unwrap());
}

/// Demonstrates applying an upper limit to a text buffer and detecting truncation.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(SQLITE_3; "SQLite 3")]
fn capped_text_buffer(connection_string: &str) {
    let conn = ENV
        .connect_with_connection_string(connection_string)
        .unwrap();
    let table_name = "CappedTextBuffer";

    // Prepare table content
    setup_empty_table(&conn, table_name, &["VARCHAR(13)"]).unwrap();
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

/// This test is inspired by a bug caused from a fetch statement generating a lot of diagnostic
/// messages.
#[test]
#[ignore = "Runs for a very long time"]
fn many_diagnostic_messages() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    // In order to generate a lot of diagnostic messages with one function call, we try a bulk
    // insert for which each row generates a warning.
    // Setup table
    setup_empty_table(&conn, "ManyDiagnosticMessages", &["VARCHAR(2)"]).unwrap();

    // Incidentally our batch size is too large to be hold in an `i16`.
    let batch_size = 2 << 15;

    // Fill each row in the buffer with two letters.
    let mut buffer = TextRowSet::new(batch_size, iter::once(2));

    for _ in 0..batch_size {
        buffer.append([Some(&b"ab"[..])].iter().cloned());
    }

    conn.execute("INSERT INTO ManyDiagnosticMessages (a) VALUES (?)", &buffer)
        .unwrap();

    buffer = TextRowSet::new(batch_size, iter::once(1));
    let cursor = conn
        .execute("SELECT a FROM ManyDiagnosticMessages", ())
        .unwrap()
        .unwrap();
    let mut row_set_cursor = cursor.bind_buffer(buffer).unwrap();

    // This should cause the string to be truncated, since they are 2 letters wide, but there is
    // space for one. This should cause at least one warning per row.
    let _ = row_set_cursor.fetch();

    // We do not have an explicit assertion, we are just happy if no integer addition overflows.
}
