use buffers::{ColumnBuffer, OptF32Column, TextColumn};
use lazy_static::lazy_static;
use odbc_api::{
    buffers::{self, TextRowSet},
    sys::SqlDataType,
    ColumnDescription, Cursor, DataType, Environment, IntoParameter, Nullable, U16String,
};
use std::convert::TryInto;

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    static ref ENV: Environment = unsafe { Environment::new().unwrap() };
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
fn mssql_describe_columns() {
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title, year FROM Movies ORDER BY year;";
    let cursor = conn.execute(sql, ()).unwrap().unwrap();

    let batch_size = 2;
    let mut buffer = buffers::TextRowSet::for_cursor(batch_size, &cursor).unwrap();
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer).unwrap();
    let mut row_set = row_set_cursor.fetch().unwrap().unwrap();
    assert_eq!(row_set.at_as_str(0, 0).unwrap().unwrap(), "Interstellar");
    assert!(row_set.at_as_str(1, 0).unwrap().is_none());
    assert_eq!(
        row_set.at_as_str(0, 1).unwrap().unwrap(),
        "2001: A Space Odyssey"
    );
    assert_eq!(row_set.at_as_str(1, 1).unwrap().unwrap(), "1968");
    row_set = row_set_cursor.fetch().unwrap().unwrap();
    assert_eq!(row_set.at_as_str(0, 0).unwrap().unwrap(), "Jurassic Park");
    assert_eq!(row_set.at_as_str(1, 0).unwrap().unwrap(), "1993");
}

#[test]
fn mssql_column_attributes() {
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
        cursor.bind_col(1, id_buffer.bind_arguments()).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_char FROM AllTheTypes;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = TextColumn::new(1, 5);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, buf.bind_arguments()).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_varchar FROM AllTheTypes;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = TextColumn::new(1, 100);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, buf.bind_arguments()).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_numeric FROM AllTheTypes;";
    let mut cursor = conn.execute(sql, ()).unwrap().unwrap();

    let mut buf = OptF32Column::new(1);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, buf.bind_arguments()).unwrap();
        cursor.fetch().unwrap();
    }

    unsafe {
        assert_eq!(Some(&1.23), buf.value_at(0));
    }
}

#[test]
fn mssql_all_types() {
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
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
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    // Assert empty table
    conn.execute("DROP TABLE IF EXISTS BulkInsert", ()).unwrap();
    conn.execute(
        r#"CREATE TABLE BulkInsert (
            Id int IDENTITY(1,1),
            Country varchar(50)
        );"#,
        (),
    )
    .unwrap();

    let mut prepared = conn
        .prepare("INSERT INTO BulkInsert (Country) Values (?)")
        .unwrap();
    let mut params = TextRowSet::new(5, [50].iter().copied());
    params.append(["England"].iter().map(|s| Some(s.as_bytes())));
    params.append(["France"].iter().map(|s| Some(s.as_bytes())));
    params.append(["Germany"].iter().map(|s| Some(s.as_bytes())));

    prepared.execute(&params).unwrap();
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
