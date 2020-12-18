mod common;

use common::{cursor_to_string, setup_empty_table, SingleColumnRowSetBuffer, ENV};

use odbc_api::{
    buffers::{AnyColumnView, BufferDescription, BufferKind, ColumnarRowSet, TextRowSet},
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
    let sql = "SELECT title, year FROM Movies ORDER BY year;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    assert_eq!(cursor.num_result_cols().unwrap(), 2);
    let mut cd = ColumnDescription::default();
    cursor.describe_col(1, &mut cd).unwrap();

    cursor.describe_col(1, &mut cd).unwrap();
    let name = U16String::from_str("title");

    // Expectation title column
    let title_desc = ColumnDescription {
        name: name.into_vec(),
        data_type: DataType::Varchar { length: 255 },
        nullability: Nullability::NoNulls,
    };

    assert_eq!(title_desc, cd);

    cursor.describe_col(2, &mut cd).unwrap();
    let name = U16String::from_str("year");

    // Expectation year column
    let year_desc = ColumnDescription {
        name: name.into_vec(),
        data_type: DataType::Integer,
        nullability: Nullability::Nullable,
    };

    assert_eq!(year_desc, cd);
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
fn two_paramters_in_tuple() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where ? < year AND year < ?;";
    let cursor = conn.execute(sql, (1960, 1970)).unwrap().unwrap();
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
fn bulk_insert() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "BulkInsert", &["VARCHAR(50)"]).unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let mut prepared = conn
        .prepare("INSERT INTO BulkInsert (a) Values (?)")
        .unwrap();
    let mut params = TextRowSet::new(5, [50].iter().copied());
    params.append(["England"].iter().map(|s| Some(s.as_bytes())));
    params.append(["France"].iter().map(|s| Some(s.as_bytes())));
    params.append(["Germany"].iter().map(|s| Some(s.as_bytes())));

    prepared.execute(&params).unwrap();

    // Assert that the table contains the rows that have just been inserted.
    let expected = "England\nFrance\nGermany";

    let cursor = conn
        .execute("SELECT a FROM BulkInsert ORDER BY id;", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);

    assert_eq!(expected, actual);
}

#[test]
fn send_connecion() {
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
fn use_columnar_buffer() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "UseColumnarRowSet", &["INTEGER", "VARCHAR(20)"]).unwrap();
    conn.execute(
        "INSERT INTO UseColumnarRowSet (a, b) VALUES (42, 'Hello, World!')",
        (),
    )
    .unwrap();

    // Get cursor querying table
    let cursor = conn
        .execute("SELECT a,b FROM UseColumnarRowSet ORDER BY id", ())
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
    use odbc_api::{sys::ParamType, Error, ParameterCollection};

    struct Out {
        ret: Nullable<i32>,
        param: Nullable<i32>,
    }

    unsafe impl ParameterCollection for &mut Out {
        fn parameter_set_size(&self) -> u32 {
            1
        }
        unsafe fn bind_parameters_to(
            self,
            stmt: &mut odbc_api::handles::Statement<'_>,
        ) -> Result<(), Error> {
            stmt.bind_parameter(1, ParamType::Output, &mut self.ret)?;
            stmt.bind_parameter(2, ParamType::Output, &mut self.param)?;
            Ok(())
        }
    }

    let mut out = Out {
        ret: Nullable::null(),
        param: Nullable::null(),
    };

    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    conn.execute("{? = call TestParam(?)}", &mut out).unwrap();

    // See magic numbers hardcoded in setup.sql
    assert_eq!(Some(99), out.ret.into_opt());
    assert_eq!(Some(88), out.param.into_opt());
}

/// This test is insipired by a bug caused from a fetch statement generating a lot of diagnostic
/// messages.
#[test]
#[ignore = "Runs for a very long time"]
fn many_diagnostic_messages() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    // In order to generate a lot of diagnostic messages with one function call, we try a bulk
    // insert for which each row generates a warning.
    // Setup table
    setup_empty_table(&conn, "ManyDiagnosticMessages", &["VARCHAR(2)"]).unwrap();

    // Incidentialy our batch size is too large to be hold in an `i16`.
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
