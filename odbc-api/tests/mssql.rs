mod common;

use common::{cursor_to_string, setup_empty_table, ENV};

use buffers::{OptF32Column, TextColumn};
use odbc_api::{
    buffers::{self, TextRowSet},
    sys::SqlDataType,
    ColumnDescription, Cursor, DataType, IntoParameter, Nullable, U16String,
};
use std::{convert::TryInto, thread};

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

#[test]
fn mssql_connect_to_movies_db() {
    let _conn = ENV.connect_with_connection_string(MSSQL).unwrap();
}

#[test]
fn mssql_describe_columns() {
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
        nullable: Nullable::NoNulls,
    };

    assert_eq!(title_desc, cd);

    cursor.describe_col(2, &mut cd).unwrap();
    let name = U16String::from_str("year");

    // Expectation year column
    let year_desc = ColumnDescription {
        name: name.into_vec(),
        data_type: DataType::Integer,
        nullable: Nullable::Nullable,
    };

    assert_eq!(year_desc, cd);
}

#[test]
fn mssql_text_buffer() {
    let query = "SELECT title, year FROM Movies ORDER BY year;";
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let cursor = conn.execute(query, ()).unwrap().unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

#[test]
fn mssql_column_attributes() {
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
fn mssql_prices() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT id,day,time,product,price FROM Sales ORDER BY id;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

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

    // Test binding id int buffer
    let batch_size = 10;
    assert_eq!(SqlDataType::INTEGER, cursor.col_concise_type(1).unwrap());
    let mut id_buffer: Vec<i32> = vec![0; batch_size];
    unsafe {
        cursor
            .set_row_array_size(batch_size.try_into().unwrap())
            .unwrap();

        // Bind id integer column
        cursor.bind_col(1, &mut id_buffer).unwrap();
        let mut num_rows_fetched = 0;
        cursor.set_num_rows_fetched(&mut num_rows_fetched).unwrap();

        cursor.fetch().unwrap();

        assert_eq!([1, 2, 3], id_buffer[0..num_rows_fetched as usize]);
        cursor.fetch().unwrap();
    }

    // Test types

    assert_eq!(SqlDataType::DECIMAL, cursor.col_concise_type(5).unwrap());
    assert_eq!(10, cursor.col_precision(5).unwrap());
    assert_eq!(2, cursor.col_scale(5).unwrap());
}

#[test]
fn mssql_bind_char() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_char FROM AllTheTypes;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = TextColumn::new(1, 5);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, &mut buf).unwrap();
        cursor.fetch().unwrap();

        assert_eq!(
            Some("abcde"),
            buf.value_at(0)
                .map(|bytes| std::str::from_utf8(bytes).unwrap())
        );
    }
}

#[test]
fn mssql_bind_varchar() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_varchar FROM AllTheTypes;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = TextColumn::new(1, 100);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, &mut buf).unwrap();
        cursor.fetch().unwrap();

        assert_eq!(
            Some("Hello, World!"),
            buf.value_at(0)
                .map(|bytes| std::str::from_utf8(bytes).unwrap())
        );
    }
}

#[test]
fn mssql_bind_numeric_to_float() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_numeric FROM AllTheTypes;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = OptF32Column::new(1);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, &mut buf).unwrap();
        cursor.fetch().unwrap();
    }

    unsafe {
        assert_eq!(Some(&1.23), buf.value_at(0));
    }
}

#[test]
fn mssql_all_types() {
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
fn mssql_bind_integer_parameter() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn.execute(sql, 1968).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test]
fn mssql_prepared_statement() {
    // Prepare the statement once
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let mut prepared = conn.prepare(sql).unwrap();

    // Execute it two times with different parameters
    {
        let cursor = prepared.execute(1968).unwrap().unwrap();
        let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
        let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();
        let batch = cursor.fetch().unwrap().unwrap();
        let title = batch.at_as_str(0, 0).unwrap().unwrap();
        assert_eq!("2001: A Space Odyssey", title);
    }

    {
        let cursor = prepared.execute(1993).unwrap().unwrap();
        let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
        let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();
        let batch = cursor.fetch().unwrap().unwrap();
        let title = batch.at_as_str(0, 0).unwrap().unwrap();
        assert_eq!("Jurassic Park", title);
    }
}

#[test]
fn mssql_integer_parameter_as_string() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn.execute(sql, "1968".into_parameter()).unwrap().unwrap();
    let mut buffer = TextRowSet::for_cursor(1, &cursor).unwrap();
    let mut cursor = cursor.bind_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

#[test]
fn mssql_two_paramters_in_tuple() {
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
fn mssql_column_names_iterator() {
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
fn mssql_bulk_insert() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "BulkInsert", "Country", "VARCHAR(50)").unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let mut prepared = conn
        .prepare("INSERT INTO BulkInsert (Country) Values (?)")
        .unwrap();
    let mut params = TextRowSet::new(5, [50].iter().copied());
    params.append(["England"].iter().map(|s| Some(s.as_bytes())));
    params.append(["France"].iter().map(|s| Some(s.as_bytes())));
    params.append(["Germany"].iter().map(|s| Some(s.as_bytes())));

    prepared.execute(&params).unwrap();

    // Assert that the table contains the rows that have just been inserted.
    let expected = "England\nFrance\nGermany";

    let cursor = conn
        .execute("SELECT country FROM BulkInsert ORDER BY id;", ())
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
fn mssql_parameter_option_str() {
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    setup_empty_table(&conn, "ParameterOptionStr", "name", "VARCHAR(50)").unwrap();
    let sql = "INSERT INTO ParameterOptionStr (name) VALUES (?);";
    let mut prepared = conn.prepare(sql).unwrap();
    prepared.execute(None::<&str>.into_parameter()).unwrap();
    prepared.execute(Some("Bernd").into_parameter()).unwrap();

    let cursor = conn
        .execute("SELECT name FROM ParameterOptionStr ORDER BY id", ())
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "NULL\nBernd";
    assert_eq!(expected, actual);
}

// #[test]
// fn mssql_bind_numeric() {

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
