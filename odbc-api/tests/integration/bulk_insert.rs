use odbc_api::{
    BindParamDesc, Connection, DataType, InOrder, InputParameterMapping, IntoParameter, U16String,
    buffers::{AnySliceMut, BufferDesc, Item, TextColumn},
    parameter::WithDataType,
    sys::{NULL_DATA, Numeric, Timestamp},
};

use stdext::function_name;
use test_case::test_case;
use widestring::Utf16String;

use crate::common::{Given, MARIADB, MSSQL, POSTGRES, Profile, SQLITE_3, cursor_to_string};

/// If inserting text with more than 4000 characters, under windows we bind it as WVARCHAR, which
/// may be limited to 4000 characters or something similar.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_long_strings(profile: &Profile) {
    // Given a text with more than 4000 characters and an VARCHAR(MAX) column
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(5000)"])
        .build(profile)
        .unwrap();
    let text = "a".repeat(5000);

    // When we insert the text as a parameter
    let result = conn.execute(&table.sql_insert(), &text.into_parameter(), None);

    // Then we expect the insert to succeed
    assert!(result.is_ok());
    assert_eq!("a".repeat(5000), table.content_as_string(&conn));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_long_strings_as_wchar(profile: &Profile) {
    // Given a text with more than 4000 characters and an VARCHAR(MAX) column
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(5000)"])
        .build(profile)
        .unwrap();
    let text = Utf16String::from_str(&"a".repeat(5000));

    // When we "bulk" insert the text as a parameter
    let mut inserter = conn
        .prepare(&table.sql_insert())
        .unwrap()
        .into_column_inserter(1, [BindParamDesc::wide_text(5000)])
        .unwrap();
    inserter
        .column_mut(0)
        .as_w_text_view()
        .unwrap()
        .set_cell(0, Some(text.as_slice()));
    inserter.set_num_rows(1);
    inserter.execute().unwrap();

    // Then we expect the insert to succeed
    // assert!(result.is_ok());
    assert_eq!("a".repeat(5000), table.content_as_string(&conn));
}

/// SQL Server's VARCHAR is limited to 8000 bytes; VARCHAR(MAX) is used for values > 8000
#[test_case(MSSQL, "VARCHAR(MAX)"; "Microsoft SQL Server")]
#[test_case(MARIADB, "VARCHAR(8001)"; "Maria DB")]
#[test_case(SQLITE_3, "VARCHAR(8001)"; "SQLite 3")]
#[test_case(POSTGRES, "VARCHAR(8001)"; "PostgreSQL")]
fn long_strings_with_more_than_8000_bytes(profile: &Profile, column_type: &str) {
    // Given a table with VARCHAR > 8000
    let table_name = table_name!();
    let column_types = [column_type];
    let (conn, table) = Given::new(&table_name)
        .column_types(&column_types)
        .build(profile)
        .unwrap();
    let text = "a".repeat(8001);

    // When we bulk insert a string longer than 8000 bytes
    let mut inserter = conn
        .prepare(&table.sql_insert())
        .unwrap()
        .into_column_inserter(1, [BindParamDesc::text(8001)])
        .unwrap();
    inserter
        .column_mut(0)
        .as_text_view()
        .unwrap()
        .set_cell(0, Some(text.as_bytes()));
    inserter.set_num_rows(1);
    inserter.execute().unwrap();

    // Then we expect the insert to succeed
    assert_eq!(text, table.content_as_string(&conn));
}

/// Insert values into a DATETIME2 column using a columnar buffer
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
// #[test_case(SQLITE_3; "SQLite 3")] // default precision of 3 instead 7
fn columnar_insert_timestamp(profile: &Profile) {
    let table_name = table_name!();
    // Setup
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DATETIME2"])
        .build(profile)
        .unwrap();

    // Fill buffer with values
    let desc = BindParamDesc::timestamp(true, 7);
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
    let nullable = true;
    let desc = BindParamDesc::i32(nullable);
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
    let desc = BindParamDesc::timestamp(true, 3);
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
            fraction: 123000000,
        }),
        None,
    ];

    prebound.set_num_rows(input.len());
    let mut writer = prebound.column_mut(0).as_nullable_slice().unwrap();
    writer.write(input.iter().copied());

    prebound.execute().unwrap();

    // Query values and compare with expectation
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
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
    let desc = BindParamDesc::binary(5);
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
        .execute(&table.sql_all_ordered_by_id(), (), None)
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
    // Buffer size purposefully chosen too small, so we would get a panic if `set_max_len` would not
    // work.
    let max_str_len = 5;
    let desc = BindParamDesc::text(max_str_len);
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
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
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
    let parameter_buffers = vec![WithDataType::new(
        TextColumn::try_new(4, 5).unwrap(),
        DataType::Integer,
    )];
    // Safety: all values in the buffer are safe for insertion
    let index_mapping = InOrder::new(parameter_buffers.len());
    let mut prebound = unsafe {
        prepared.unchecked_bind_columnar_array_parameters(parameter_buffers, index_mapping)
    }
    .unwrap();
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

// #[test_case(MSSQL; "Microsoft SQL Server")] Numeric value out of range. We would likely need to
// edit the APD to support a scale different from zero.
#[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] Unsupported parameter type
#[test_case(POSTGRES; "PostgreSQL")]
fn columnar_insert_numeric_using_numeric_buffer(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DECIMAL(5,3)"])
        .build(profile)
        .unwrap();
    let stmt = conn.prepare(&table.sql_insert()).unwrap();

    // When
    let desc = BindParamDesc {
        buffer_desc: BufferDesc::Numeric,
        data_type: DataType::Numeric {
            precision: 5,
            scale: 3,
        },
    };

    let mut inserter = stmt.into_column_inserter(3, [desc]).unwrap();
    let AnySliceMut::Numeric(slice) = inserter.column_mut(0) else {
        panic!("Expected numeric column");
    };
    slice[0] = Numeric {
        precision: 5,
        scale: 3,
        sign: 1,
        val: 12345u128.to_le_bytes(),
    };
    slice[1] = Numeric {
        precision: 5,
        scale: 3,
        sign: 1,
        val: 23456u128.to_le_bytes(),
    };
    slice[2] = Numeric {
        precision: 5,
        scale: 3,
        sign: 1,
        val: 34567u128.to_le_bytes(),
    };
    inserter.set_num_rows(3);
    inserter.execute().unwrap();

    // Then
    let content = table.content_as_string(&conn);
    assert_eq!("12.345\n23.456\n34.567", content);
}

/// Currently all DBMS under test would allow inserting decimals as VARCHAR and perform the
/// conversation themselves implicitly. However
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn columnar_insert_decimal(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DECIMAL(5,3)"])
        .build(profile)
        .unwrap();

    // When
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let descriptions = [BindParamDesc {
        // DECIMAL(5,3) display size: precision + 2 (sign + decimal point) = 7
        buffer_desc: BufferDesc::Text { max_str_len: 7 },
        data_type: DataType::Decimal {
            precision: 5,
            scale: 3,
        },
    }];
    let index_mapping = InOrder::new(descriptions.len());
    let mut inserter = prepared
        .into_column_inserter_with_mapping(4, descriptions, index_mapping)
        .unwrap();

    inserter.set_num_rows(4);
    let mut col_view = inserter.column_mut(0).as_text_view().unwrap();
    col_view.set_cell(0, Some(b"12.345"));
    col_view.set_cell(1, Some(b"23.456"));
    col_view.set_cell(2, None);
    col_view.set_cell(3, Some(b"34.567"));

    inserter.execute().unwrap();

    // Then
    let content = table.content_as_string(&conn);
    assert_eq!("12.345\n23.456\nNULL\n34.567", content);
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

    // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
    // encounter larger inputs.
    let max_str_len = 1;
    let desc = BindParamDesc::text(max_str_len);
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
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
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

    // Buffer size purposefully chosen too small, so we need to increase the buffer size if we
    // encounter larger inputs.
    let max_bytes = 1;
    let desc = BindParamDesc::binary(max_bytes);
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
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    let expected = "4869\n48656C6C6F\n576F726C64\nNULL\n48656C6C6F2C20576F726C6421";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn with_varying_buffer_sizes(profile: &Profile) {
    // Given a table with an INTEGER column `a`` and a prepared statement to insert into it.
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .build(profile)
        .unwrap();
    let prepared = conn.prepare(&table.sql_insert()).unwrap();

    // When we create a columnar inserter with a batch size of 1 and insert a single value.
    let mut inserter = prepared
        .into_column_inserter(1, [BindParamDesc::i32(false)])
        .unwrap();
    inserter.set_num_rows(1);
    inserter.column_mut(0).as_slice::<i32>().unwrap()[0] = 1;
    inserter.execute().unwrap();
    // And we resize the buffer to two size and insert two more values.
    let mapping = InOrder::new(1);
    let mut inserter = inserter.resize(2, mapping).unwrap();
    inserter.set_num_rows(2);
    inserter.column_mut(0).as_slice::<i32>().unwrap()[0] = 2;
    inserter.column_mut(0).as_slice::<i32>().unwrap()[1] = 3;
    inserter.execute().unwrap();

    // Then we expect the table to contain the values 1, 2, and 3.
    let actual = table.content_as_string(&conn);
    let expected = "1\n2\n3";
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
    let max_str_len = 20;
    let desc = BindParamDesc::wide_text(max_str_len);
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
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
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
fn bulk_insert_with_text_buffer(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    // When
    // Fill a text buffer with three rows, and insert them into the database.
    let prepared = conn.prepare(&insert_sql).unwrap();
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
    let actual = table.content_as_string(&conn);
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_with_columnar_buffer(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)", "INTEGER"])
        .build(profile)
        .unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let description = [BindParamDesc::text(50), BindParamDesc::i32(true)];

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
    let actual = table.content_as_string(&conn);

    assert_eq!(expected, actual);
}

/// Use into_column_inserter to insert values into multiple columns from a single buffer. This
/// usecase appeard during implementing the `exec` subcommand of `odbc2parquet`. If we want to be
/// mindful of the memory usage in case the same parquet column file maps to multiple placeholders.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_two_columns_from_one_buffer(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "INTEGER"])
        .build(profile)
        .unwrap();

    // Fill a text buffer with three rows, and insert them into the database.
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let description = [BindParamDesc {
        buffer_desc: BufferDesc::I32 { nullable: false },
        data_type: DataType::Integer,
    }];

    struct MyMapping;
    impl InputParameterMapping for MyMapping {
        fn parameter_index_to_column_index(&self, _paramteter_index: u16) -> usize {
            0
        }
        fn num_parameters(&self) -> usize {
            2
        }
    }

    let mut prebound = prepared
        .into_column_inserter_with_mapping(5, description, MyMapping)
        .unwrap();

    prebound.set_num_rows(3);
    // Fill ther column with integers
    let col_view = prebound.column_mut(0).as_slice().unwrap();
    col_view[0] = 42;
    col_view[1] = 5;
    col_view[2] = 7;

    prebound.execute().unwrap();

    // Assert that each column now contains the data we just inserted.
    let expected = "42,42\n5,5\n7,7";
    let actual = table.content_as_string(&conn);

    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bulk_insert_with_multiple_batches(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)", "INTEGER"])
        .build(profile)
        .unwrap();

    // When

    // First batch

    // Fill a buffer with three rows, and insert them into the database.
    let prepared = conn.prepare(&table.sql_insert()).unwrap();
    let description = [BindParamDesc::text(50), BindParamDesc::i32(true)];
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
    let actual = table.content_as_string(&conn);

    assert_eq!(expected, actual);
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
    let mut inserter = prepared.into_column_inserter(2, [BindParamDesc::i64(true)])?;
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
    let desc = BindParamDesc::i32(false);
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
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), (), None)
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
    let desc = BindParamDesc::i32(false);
    // Insert a batch
    let mut prebound = prepared.column_inserter(1, [desc]).unwrap();
    prebound.set_num_rows(1);
    let col = prebound.column_mut(0).as_slice::<i32>().unwrap();
    col[0] = 1;
    prebound.execute().unwrap();

    // Then
    let conn = profile.connection().unwrap();
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY id"), (), None)
        .unwrap()
        .unwrap();
    let actual = cursor_to_string(cursor);
    assert_eq!("1", actual);
}

/// Inserts a Vector of integers using a generic implementation
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn insert_vec_column_using_generic_code(profile: &Profile) {
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "BIGINT", "FLOAT(53)"])
        .build(profile)
        .unwrap();
    let insert_sql = table.sql_insert();

    fn insert_tuple_vec<A: Item, B: Item, C: Item>(
        conn: &Connection<'_>,
        insert_sql: &str,
        source: &[(A, B, C)],
    ) {
        let mut prepared = conn.prepare(insert_sql).unwrap();
        // Number of rows submitted in one round trip
        let capacity = source.len();
        // We do not need a nullable buffer since elements of source are not optional
        let descriptions = [
            A::bind_param_desc(false),
            B::bind_param_desc(false),
            C::bind_param_desc(false),
        ];
        let mut inserter = prepared.column_inserter(capacity, descriptions).unwrap();
        // We send everything in one go.
        inserter.set_num_rows(source.len());
        // Now let's copy the row based tuple into the columnar structure
        for (index, (a, b, c)) in source.iter().enumerate() {
            inserter.column_mut(0).as_slice::<A>().unwrap()[index] = *a;
            inserter.column_mut(1).as_slice::<B>().unwrap()[index] = *b;
            inserter.column_mut(2).as_slice::<C>().unwrap()[index] = *c;
        }
        inserter.execute().unwrap();
    }
    insert_tuple_vec(
        &conn,
        &insert_sql,
        &[(1i32, 1i64, 0.5f64), (2, 2, 0.25), (3, 3, 0.125)],
    );

    let actual = table.content_as_string(&conn);
    assert_eq!("1,1,0.5\n2,2,0.25\n3,3,0.125", actual);
}
