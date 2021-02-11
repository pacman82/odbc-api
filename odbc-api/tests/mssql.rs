mod common;

use common::{cursor_to_string, setup_empty_table, SingleColumnRowSetBuffer, ENV};

use odbc_api::{
    buffers::{
        AnyColumnView, AnyColumnViewMut, BufferDescription, BufferKind, ColumnarRowSet, TextRowSet,
    },
    ColumnDescription, Cursor, DataType, IntoParameter, Nullability, Nullable, U16String,
};
use std::{ffi::CStr, iter, thread};

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

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
        ],
    )
    .unwrap();
    let sql = "SELECT a,b,c,d FROM DescribeColumns ORDER BY Id;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    assert_eq!(cursor.num_result_cols().unwrap(), 4);
    let mut actual = ColumnDescription::default();

    let desc = |name, data_type, nullability| ColumnDescription {
        name: U16String::from_str(name).into_vec(),
        data_type,
        nullability,
    };

    let expected = desc("a", DataType::Varchar { length: 255 }, Nullability::NoNulls);
    cursor.describe_col(1, &mut actual).unwrap();
    assert_eq!(expected, actual);

    let expected = desc("b", DataType::Integer, Nullability::Nullable);
    cursor.describe_col(2, &mut actual).unwrap();
    assert_eq!(expected, actual);

    let expected = desc("c", DataType::Binary { length: 12 }, Nullability::Nullable);
    cursor.describe_col(3, &mut actual).unwrap();
    assert_eq!(expected, actual);

    let expected = desc("d", DataType::Varbinary { length: 100 }, Nullability::Nullable);
    cursor.describe_col(4, &mut actual).unwrap();
    assert_eq!(expected, actual);
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

#[test]
fn bind_char() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_char FROM AllTheTypes;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();
    let mut buf = SingleColumnRowSetBuffer::with_text_column(1, 5);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();

    assert_eq!(
        Some("abcde"),
        buf.value_at(0).map(|cstr| cstr.to_str().unwrap())
    );
}

#[test]
fn bind_varchar() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_varchar FROM AllTheTypes;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = SingleColumnRowSetBuffer::with_text_column(1, 100);
    let mut row_set_cursor = cursor.bind_buffer(&mut buf).unwrap();
    row_set_cursor.fetch().unwrap();

    assert_eq!(
        Some("Hello, World!"),
        buf.value_at(0).map(|cstr| cstr.to_str().unwrap())
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
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));

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

/// Insert values into a varbinary column using a columnar buffer
#[test]
fn columnar_insert_varchar() {
    // Setup
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "ColumnarInsertVarchar", &["VARCHAR(13)"]).unwrap();

    // Fill buffer with values
    let desc = BufferDescription {
        // Buffer size purposefully chosen too small, so we would get a panic if `set_max_len` would
        // not work.
        kind: BufferKind::Text { max_str_len: 5 },
        nullable: true,
    };
    let mut buffer = ColumnarRowSet::new(10, iter::once(desc));

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
        writer.write(input.iter().copied());
    } else {
        panic!("Expected text column writer");
    };

    // Bind buffer and insert values.
    conn.execute("INSERT INTO ColumnarInsertVarchar (a) VALUES (?)", &buffer)
        .unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute("SELECT a FROM ColumnarInsertVarchar ORDER BY Id", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Hello\nWorld\nNULL\nHello, World!";
    assert_eq!(expected, actual);
}

#[test]
fn all_types() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_char, my_numeric, my_varchar, my_float FROM AllTheTypes;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut cd = ColumnDescription::default();
    // Assert types
    cursor.describe_col(1, &mut cd).unwrap();
    assert_eq!(DataType::Char { length: 5 }, cd.data_type);
    cursor.describe_col(2, &mut cd).unwrap();
    assert_eq!(
        DataType::Numeric {
            precision: 3,
            scale: 2
        },
        cd.data_type
    );
    cursor.describe_col(3, &mut cd).unwrap();
    assert_eq!(DataType::Varchar { length: 100 }, cd.data_type);

    // mssql returns real if type is declared `FLOAT(3)` in transact SQL
    cursor.describe_col(4, &mut cd).unwrap();
    assert_eq!(DataType::Real, cd.data_type);
}

#[test]
fn bind_integer_parameter() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn.execute(sql, &1968).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
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
        let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
        let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();
        let batch = cursor.fetch().unwrap().unwrap();
        let title = batch.at_as_str(0, 0).unwrap().unwrap();
        assert_eq!("2001: A Space Odyssey", title);
    }

    {
        let cursor = prepared.execute(&1993).unwrap().unwrap();
        let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
        let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();
        let batch = cursor.fetch().unwrap().unwrap();
        let title = batch.at_as_str(0, 0).unwrap().unwrap();
        assert_eq!("Jurassic Park", title);
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
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
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
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
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
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    assert!(cursor.fetch().unwrap().is_none());
}

#[test]
#[cfg(not(target_os = "windows"))] // Windows does not use UTF-8 locale by default
fn char() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    // VARCHAR(2) <- VARCHAR(1) would be enough to held the character, but we de not allocate
    // enough memory on the client side to hold the entire string.
    setup_empty_table(&conn, "Char", &["VARCHAR(1)"]).unwrap();

    conn.execute("INSERT INTO CHAR (a) VALUES ('A'), ('Ü');", ())
        .unwrap();

    let sql = "SELECT a FROM Char ORDER BY id;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();
    let output = cursor_to_string(cursor);
    assert_eq!("A\nÜ", output);
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
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
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

#[test]
fn bulk_insert_with_text_buffer() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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

#[test]
fn bulk_insert_with_columnar_buffer() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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

#[test]
fn parameter_option_str() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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

#[test]
fn read_into_columnar_buffer() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
        AnyColumnView::Text(mut col) => assert_eq!(
            Some(CStr::from_bytes_with_nul(b"Hello, World!\0").unwrap()),
            col.next().unwrap()
        ),
        _ => panic!("Unexpected buffer type"),
    }

    // Assert that there is no second batch.
    assert!(cursor.fetch().unwrap().is_none());
}

/// In use cases there the user supplies the query it may be necessary to ignore one column then
/// binding the buffers. This test constructs a result set with 3 columns and ignores the second
#[test]
fn ignore_output_column() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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

#[test]
fn manual_commit_mode() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
#[test]
fn unfinished_transaction() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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

    conn.execute("INSERT INTO InteriorNul (a) VALUES (?);", &"a\0b".into_parameter()).unwrap();
    let cursor = conn.execute("SELECT CAST(A AS VARBINARY) FROM InteriorNul;", ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "610062";
    assert_eq!(expected, actual);

    let cursor = conn.execute("SELECT A FROM InteriorNul;", ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "a";
    assert_eq!(expected, actual);
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

// #[test]
// fn bind_numeric() {

//     // See:
//     // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/retrieve-numeric-data-sql-numeric-struct-kb222831?view=sql-server-ver15
//     // https://stackoverflow.com/questions/9177795/how-to-convert-sql-numeric-struct-to-double-and-string

//     let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
//     let sql = "SELECT my_numeric FROM AllTheTypes;";
//     let mut cursor = conn.exec_direct(sql).unwrap().unwrap();

//     let mut buf: Vec<Numeric> = vec![Numeric::default(); 1];

//     let mut ard = cursor.application_row_descriptor().unwrap();
//     unsafe {
//         ard.set_field_type(1, SqlDataType::NUMERIC).unwrap();
//         ard.set_field_scale(1, 2).unwrap();
//         ard.set_field_precision(1, 3).unwrap();
//     }

//     let bind_args = BindColParameters {
//         indicator: null_mut(),
//         target_length: size_of::<Numeric>() as i64,
//         target_type: CDataType::Numeric, // CDataType:ArdType
//         target_value: buf.as_mut_ptr() as Pointer,
//     };

//     unsafe {
//         cursor.set_row_array_size(1).unwrap();
//         cursor.bind_col(1, bind_args).unwrap();
//     }

//     cursor.fetch().unwrap();

//     assert_eq!(
//         Numeric {
//             precision: 0,
//             scale: 0,
//             sign: 0,
//             val: [0; 16]
//         },
//         buf[0]
//     );
// }
