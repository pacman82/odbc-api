use buffers::{ColumnBuffer, OptF32Column, TextColumn};
use lazy_static::lazy_static;
use odbc_api::{
    buffers::{self, FixedSizedCType, TextRowSet},
    handles::Statement,
    sys::{ParamType, Pointer, SqlDataType},
    ColumnDescription, Cursor, DataType, Environment, Error, Nullable, Parameters, U16String,
};
use std::ptr::null_mut;
use std::{convert::TryInto, sync::Mutex};

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
// Therefore synchronize test execution.
lazy_static! {
    static ref SERIALIZE: Mutex<()> = Mutex::new(());
}

fn init() -> &'static Mutex<()> {
    // Set environment to something like:
    // RUST_LOG=odbc-api=info cargo test
    let _ = env_logger::builder().is_test(true).try_init();
    &SERIALIZE
}

#[test]
fn bogus_connection_string() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };
    let conn = env.connect_with_connection_string("foobar");
    assert!(matches!(conn, Err(_)));
}

#[test]
fn connect_to_movies_db() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };
    let _conn = env.connect_with_connection_string(MSSQL).unwrap();
}

#[test]
fn mssql_describe_columns() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title, year FROM Movies ORDER BY year;";
    let cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

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
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title, year FROM Movies ORDER BY year;";
    let cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

    let batch_size = 2;
    let mut buffer = buffers::TextRowSet::new(batch_size, &cursor).unwrap();
    let mut row_set_cursor = cursor.bind_row_set_buffer(&mut buffer).unwrap();
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
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title, year FROM Movies;";
    let cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

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
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT id,day,time,product,price FROM Sales ORDER BY id;";
    let mut cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

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
    }

    cursor.fetch().unwrap();

    // Test types

    assert_eq!(SqlDataType::DECIMAL, cursor.col_concise_type(5).unwrap());
    assert_eq!(10, cursor.col_precision(5).unwrap());
    assert_eq!(2, cursor.col_scale(5).unwrap());
}

#[test]
fn mssql_bind_char() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_char FROM AllTheTypes;";
    let mut cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

    let mut buf = TextColumn::new(1, 5);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, buf.bind_arguments()).unwrap();
    }

    cursor.fetch().unwrap();

    unsafe {
        assert_eq!(
            Some("abcde"),
            buf.value_at(0)
                .map(|bytes| std::str::from_utf8(bytes).unwrap())
        );
    }
}

#[test]
fn mssql_bind_varchar() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_varchar FROM AllTheTypes;";
    let mut cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

    let mut buf = TextColumn::new(1, 100);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, buf.bind_arguments()).unwrap();
    }

    cursor.fetch().unwrap();

    unsafe {
        assert_eq!(
            Some("Hello, World!"),
            buf.value_at(0)
                .map(|bytes| std::str::from_utf8(bytes).unwrap())
        );
    }
}

#[test]
fn mssql_bind_numeric_to_float() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_numeric FROM AllTheTypes;";
    let mut cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

    let mut buf = OptF32Column::new(1);
    unsafe {
        cursor.set_row_array_size(1).unwrap();
        cursor.bind_col(1, buf.bind_arguments()).unwrap();
    }

    cursor.fetch().unwrap();

    unsafe {
        assert_eq!(Some(&1.23), buf.value_at(0));
    }
}

#[test]
fn mssql_all_types() {
    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT my_char, my_numeric, my_varchar, my_float FROM AllTheTypes;";
    let cursor = conn.exec_direct(sql, ()).unwrap().unwrap();

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
    struct YearParam(i32);

    unsafe impl Parameters for YearParam {
        unsafe fn bind_input(&self, stmt: &mut Statement) -> Result<(), Error> {
            stmt.bind_parameter(
                1,
                ParamType::Input,
                i32::C_DATA_TYPE,
                DataType::Integer,
                &self.0 as *const i32 as *mut i32 as Pointer,
                0,
                null_mut(),
            )
            .unwrap();
            Ok(())
        }
    }

    let _lock = init().lock();
    let env = unsafe { Environment::new().unwrap() };

    let mut conn = env.connect_with_connection_string(MSSQL).unwrap();
    let sql = "SELECT title FROM Movies where year=?;";
    let cursor = conn.exec_direct(sql, YearParam(1968)).unwrap().unwrap();
    let mut buffer = TextRowSet::new(1, &cursor).unwrap();
    let mut cursor = cursor.bind_row_set_buffer(&mut buffer).unwrap();

    let batch = cursor.fetch().unwrap().unwrap();
    let title = batch.at_as_str(0, 0).unwrap().unwrap();

    assert_eq!("2001: A Space Odyssey", title);
}

// #[test]
// fn mssql_bind_numeric() {

//     // See:
//     // https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/retrieve-numeric-data-sql-numeric-struct-kb222831?view=sql-server-ver15
//     // https://stackoverflow.com/questions/9177795/how-to-convert-sql-numeric-struct-to-double-and-string

//     let _ = init().lock();
//     let env = unsafe { Environment::new().unwrap() };

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
