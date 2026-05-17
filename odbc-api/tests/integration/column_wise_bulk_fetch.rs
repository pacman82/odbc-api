use odbc_api::{
    Bit, ConcurrentBlockCursor, Cursor as _, DataType, Error, IntoParameter,
    ResultSetMetadata as _,
    buffers::{BufferDesc, ColumnarAnyBuffer, ColumnarDynBuffer, TextRowSet},
    sys::{Date, NULL_DATA, Numeric, Time, Timestamp},
};

use std::{iter, num::NonZeroUsize, time::Duration};

use stdext::function_name;
use test_case::test_case;

use crate::common::{Given, MARIADB, MSSQL, POSTGRES, Profile, SQLITE_3, cursor_to_string};

/// Fetch text from data source using the TextBuffer type
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn text_row_set(profile: &Profile) {
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
        None,
    )
    .unwrap();

    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();
    // Cursor to string helper utilizes the text buffer
    let actual = cursor_to_string(cursor);
    let expected = "Interstellar,NULL\n2001: A Space Odyssey,1968\nJurassic Park,1993";
    assert_eq!(expected, actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn text(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .values_by_column(&[&[Some("Hello, World!")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let batch_size = 5;
    let buffer = ColumnarDynBuffer::from_descs(batch_size, [BufferDesc::Text { max_str_len: 256 }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_text().unwrap().get(0);

    assert_eq!(Some(b"Hello, World!".as_slice()), actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn wide_text(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["VARCHAR(50)"])
        .values_by_column(&[&[Some("Hello, World!")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let batch_size = 5;
    let buffer =
        ColumnarDynBuffer::from_descs(batch_size, [BufferDesc::WText { max_str_len: 256 }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_wide_text().unwrap().get(0);

    assert!(actual.is_some());
    let actual = actual.unwrap();
    assert_eq!("Hello, World!", String::from_utf16(actual).unwrap());
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bin_from_text(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        // VARBINARY does not exist for every database system. Since this test is about the buffer,
        // we just use VARCHAR and fetch it as binary rather than text
        .column_types(&["VARCHAR(50)"])
        .values_by_column(&[&[Some("Hello, World!")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let batch_size = 5;
    let buffer = ColumnarDynBuffer::from_descs(batch_size, [BufferDesc::Binary { max_bytes: 256 }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_binary().unwrap().get(0);
    assert_eq!(Some(b"Hello, World!".as_slice()), actual);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn time(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["TIME"])
        .values_by_column(&[&[Some("12:34:56")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::Time { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<Time>().unwrap();
    assert_eq!(
        Time {
            hour: 12,
            minute: 34,
            second: 56
        },
        actual[0]
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn time_any_buffer(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["TIME"])
        .values_by_column(&[&[Some("12:34:56")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarAnyBuffer::from_descs(1, [BufferDesc::Time { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let column = batch.column(0).as_slice::<Time>().unwrap();
    assert_eq!(
        Time {
            hour: 12,
            minute: 34,
            second: 56
        },
        column[0]
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn nullable_time(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["TIME"])
        .values_by_column(&[&[Some("12:34:56"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::Time { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<Time>().unwrap();
    assert_eq!(
        Some(&Time {
            hour: 12,
            minute: 34,
            second: 56
        }),
        actual.get(0)
    );
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn nullable_time_any_buffer(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["TIME"])
        .values_by_column(&[&[Some("12:34:56"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarAnyBuffer::from_descs(2, [BufferDesc::Time { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let column = batch.column(0).as_nullable_slice::<Time>().unwrap();
    assert_eq!(
        Some(&Time {
            hour: 12,
            minute: 34,
            second: 56
        }),
        column.get(0)
    );
    assert_eq!(None, column.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn f32(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["REAL"])
        .values_by_column(&[&[Some("12.3")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::F32 { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<f32>().unwrap();
    assert_eq!(12.3, actual[0]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn f32_nullable(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["REAL"])
        .values_by_column(&[&[Some("12.3"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::F32 { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<f32>().unwrap();
    assert_eq!(Some(&12.3), actual.get(0));
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn i8(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::I8 { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<i8>().unwrap();
    assert_eq!(42, actual[0]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn i8_nullable(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::I8 { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<i8>().unwrap();
    assert_eq!(Some(&42), actual.get(0));
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn i16(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::I16 { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<i16>().unwrap();
    assert_eq!(42, actual[0]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn i16_nullable(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::I16 { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<i16>().unwrap();
    assert_eq!(Some(&42), actual.get(0));
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn u8(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::U8 { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<u8>().unwrap();
    assert_eq!(42, actual[0]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn u8_nullable(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::U8 { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<u8>().unwrap();
    assert_eq!(Some(&42), actual.get(0));
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bit(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["BIT"])
        .values_by_column(&[&[Some("1")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::Bit { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<Bit>().unwrap();
    assert_eq!(Bit::from_bool(true), actual[0]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn bit_nullable(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["BIT"])
        .values_by_column(&[&[Some("1"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::Bit { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<Bit>().unwrap();
    assert_eq!(Some(&Bit::from_bool(true)), actual.get(0));
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn i64(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::I64 { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_slice::<i64>().unwrap();
    assert_eq!(42, actual[0]);
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn i64_nullable(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER"])
        .values_by_column(&[&[Some("42"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::I64 { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let actual = batch.column(0).as_nullable_slice::<i64>().unwrap();
    assert_eq!(Some(&42), actual.get(0));
    assert_eq!(None, actual.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn date(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DATE"])
        .values_by_column(&[&[Some("2025-05-23")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::Date { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let column = batch.column(0).as_slice::<Date>().unwrap();
    assert_eq!(
        Date {
            year: 2025,
            month: 5,
            day: 23
        },
        column[0]
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn date_any_buffecr(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DATE"])
        .values_by_column(&[&[Some("2025-05-23")]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarAnyBuffer::from_descs(1, [BufferDesc::Date { nullable: false }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let column = batch.column(0).as_slice::<Date>().unwrap();
    assert_eq!(
        Date {
            year: 2025,
            month: 5,
            day: 23
        },
        column[0]
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn nullable_date(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DATE"])
        .values_by_column(&[&[Some("2025-05-23"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(2, [BufferDesc::Date { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let column = batch.column(0).as_nullable_slice::<Date>().unwrap();
    assert_eq!(
        Some(&Date {
            year: 2025,
            month: 5,
            day: 23
        }),
        column.get(0)
    );
    assert_eq!(None, column.get(1));
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn nullable_date_any_buffer(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["DATE"])
        .values_by_column(&[&[Some("2025-05-23"), None]])
        .build(profile)
        .unwrap();
    let query = table.sql_all_ordered_by_id();
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();

    // When
    let buffer = ColumnarAnyBuffer::from_descs(2, [BufferDesc::Date { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let maybe_batch = cursor.fetch().unwrap();

    // Then
    let batch = maybe_batch.unwrap();
    let column = batch.column(0).as_nullable_slice::<Date>().unwrap();
    assert_eq!(
        Some(&Date {
            year: 2025,
            month: 5,
            day: 23
        }),
        column.get(0)
    );
    assert_eq!(None, column.get(1));
}

/// Bind a columnar buffer to a VARBINARY(10) column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] // Convert syntax is different
// #[test_case(SQLITE_3; "SQLite 3")]
fn from_varbinary(profile: &Profile) {
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
    conn.execute(&insert_sql, (), None).unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), (), None)
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
    assert_eq!(BufferDesc::Binary { max_bytes: 10 }, buffer_desc);
    let row_set_buffer = ColumnarDynBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let mut col_it = batch.column(0).as_binary().unwrap().iter();

    assert_eq!(Some(&b"Hello"[..]), col_it.next().unwrap());
    assert_eq!(Some(&b"World"[..]), col_it.next().unwrap());
    assert_eq!(Some(None), col_it.next()); // Expecting NULL
    assert_eq!(None, col_it.next()); // Expecting iterator end.
}

/// Bind a columnar buffer to a BINARY(5) column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] // different convert syntax
// #[test_case(SQLITE_3; "SQLite 3")]
fn from_fixed_size_binary(profile: &Profile) {
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
        None,
    )
    .unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&table.sql_all_ordered_by_id(), (), None)
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
    assert_eq!(BufferDesc::Binary { max_bytes: 5 }, buffer_desc);
    let row_set_buffer = ColumnarDynBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let mut col_it = batch.column(0).as_binary().unwrap().iter();
    assert_eq!(Some(&b"Hello"[..]), col_it.next().unwrap());
    assert_eq!(Some(&b"World"[..]), col_it.next().unwrap());
    assert_eq!(Some(None), col_it.next()); // Expecting NULL
    assert_eq!(None, col_it.next()); // Expecting iterator end.
}

/// Bind a columnar buffer to a DATETIME2 column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
#[test_case(SQLITE_3; "SQLite 3")]
fn datetime_2_into_timestamp(profile: &Profile) {
    let table_name = table_name!();
    let (conn, _table) = Given::new(&table_name)
        .column_types(&["DATETIME2(3) NOT NULL"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!(
            "INSERT INTO {table_name} (a) Values \
        ({{ ts '2021-03-20 15:24:12.12' }}),\
        ({{ ts '2020-03-20 15:24:12' }}),\
        ({{ ts '1970-01-01 00:00:00' }})"
        ),
        (),
        None,
    )
    .unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(DataType::Timestamp { precision: 3 }, data_type);
    let buffer_desc = BufferDesc::from_data_type(data_type, false).unwrap();
    assert_eq!(BufferDesc::Timestamp { nullable: false }, buffer_desc);
    let row_set_buffer = ColumnarDynBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
    let mut cursor = cursor.bind_buffer(row_set_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let col = batch.column(0).as_slice().unwrap();
    assert_eq!(
        &[
            Timestamp {
                year: 2021,
                month: 3,
                day: 20,
                hour: 15,
                minute: 24,
                second: 12,
                fraction: 120_000_000,
            },
            Timestamp {
                year: 2020,
                month: 3,
                day: 20,
                hour: 15,
                minute: 24,
                second: 12,
                fraction: 0,
            },
            Timestamp {
                year: 1970,
                month: 1,
                day: 1,
                hour: 0,
                minute: 0,
                second: 0,
                fraction: 0,
            }
        ],
        col
    );
}

/// Bind a columnar buffer to a DATETIME2 column and fetch data.
#[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")] No DATEIME2 type
#[test_case(SQLITE_3; "SQLite 3")]
fn datetime_2_into_nullable_timestamp(profile: &Profile) {
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
        None,
    )
    .unwrap();

    // Retrieve values
    let mut cursor = conn
        .execute(&format!("SELECT a FROM {table_name} ORDER BY Id"), (), None)
        .unwrap()
        .unwrap();
    let data_type = cursor.col_data_type(1).unwrap();
    assert_eq!(DataType::Timestamp { precision: 3 }, data_type);
    let buffer_desc = BufferDesc::from_data_type(data_type, true).unwrap();
    assert_eq!(BufferDesc::Timestamp { nullable: true }, buffer_desc);
    let row_set_buffer = ColumnarDynBuffer::try_from_descs(10, iter::once(buffer_desc)).unwrap();
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

// Maria DB and MSSQL will fetch this with scale 0 and precision 38.
// #[test_case(MSSQL; "Microsoft SQL Server")]
// #[test_case(MARIADB; "Maria DB")]
// #[test_case(SQLITE_3; "SQLite 3")] Unsupported parameter type
#[test_case(POSTGRES; "PostgreSQL")]
fn from_numeric_into_nullable_numeric(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["NUMERIC(5,3) NOT NULL"])
        .values_by_column(&[&[Some("12.345"), Some("23.456"), Some("34.567")]])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), (), None)
        .unwrap()
        .unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(3, [BufferDesc::Numeric]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    // Then
    let numeric = batch.column(0).as_slice::<Numeric>().unwrap();
    assert_eq!(
        Numeric {
            precision: 5,
            scale: 3,
            sign: 1,
            val: 12345u128.to_le_bytes()
        },
        numeric[0]
    );
    assert_eq!(
        Numeric {
            precision: 5,
            scale: 3,
            sign: 1,
            val: 23456u128.to_le_bytes()
        },
        numeric[1]
    );
    assert_eq!(
        Numeric {
            precision: 5,
            scale: 3,
            sign: 1,
            val: 34567u128.to_le_bytes()
        },
        numeric[2]
    );
}

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn multiple_columns(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INTEGER", "VARCHAR(20)"])
        .values_by_column(&[&[Some("42")], &[Some("Hello, World!")]])
        .build(profile)
        .unwrap();
    let cursor = conn
        .execute(&table.sql_all_ordered_by_id(), (), None)
        .unwrap()
        .unwrap();

    // When
    let buffer_description = [
        BufferDesc::I32 { nullable: true },
        BufferDesc::Text { max_str_len: 20 },
    ];
    let buffer = ColumnarDynBuffer::try_from_descs(20, buffer_description).unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    // Then
    let mut col = batch.column(0).as_nullable_slice().unwrap();
    assert_eq!(Some(&42), col.next().unwrap());
    assert_eq!(
        Some(&b"Hello, World!"[..]),
        batch.column(1).as_text().unwrap().get(0)
    );
    // Assert that there is no second batch.
    assert!(cursor.fetch().unwrap().is_none());
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
    let cursor = conn.execute(&sql, (), None).unwrap().unwrap();
    let row_set_buffer = ColumnarDynBuffer::try_from_descs(
        u16::MAX as usize + 1,
        [BufferDesc::Bit { nullable: false }],
    )
    .unwrap();
    assert!(cursor.bind_buffer(row_set_buffer).is_ok())
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
        None,
    )
    .unwrap();

    // When
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name}"), (), None)
        .unwrap() // Unwrap Result
        .unwrap(); // Unwrap Option, we know a select statement to produce a cursor.
    let buffer = ColumnarDynBuffer::from_descs(3, [BufferDesc::I32 { nullable: true }]);
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let nullable_slice = batch.column(0).as_nullable_slice::<i32>().unwrap();
    let (values, indicators) = nullable_slice.raw_values();
    // Memcopy values.
    let values = values.to_vec();
    // Create array of bools indicating null values.
    let nulls: Vec<bool> = indicators
        .iter()
        .map(|&indicator| indicator == NULL_DATA)
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
fn text_view_allows_for_filling_arrow_arrays(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(50)"])
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
        None,
    )
    .unwrap();

    // When
    let cursor = conn
        .execute(&format!("SELECT a FROM {table_name}"), (), None)
        .unwrap()
        .unwrap();

    let columnar_buffer =
        ColumnarDynBuffer::try_from_descs(10, [BufferDesc::Text { max_str_len: 50 }]).unwrap();

    let mut cursor = cursor.bind_buffer(columnar_buffer).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();
    let view = batch.column(0).as_text().unwrap();

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
fn detect_truncated_output(profile: &Profile) {
    // Given a text entry with a length of ten.
    let table_name = table_name!();
    let conn = profile
        .setup_empty_table(&table_name, &["VARCHAR(10)"])
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES ('0123456789')"),
        (),
        None,
    )
    .unwrap();

    // When fetching that field as part of a bulk, but with a buffer of only length 5.
    let buffer_description = BufferDesc::Text { max_str_len: 5 };
    let buffer = ColumnarDynBuffer::try_from_descs(1, [buffer_description]).unwrap();
    let query = format!("SELECT a FROM {table_name}");
    let cursor = conn.execute(&query, (), None).unwrap().unwrap();
    let mut cursor = cursor.bind_buffer(buffer).unwrap();
    assert!(matches!(
        cursor.fetch_with_truncation_check(true),
        Err(Error::TooLargeValueForBuffer {
            indicator: Some(10),
            buffer_index: 0,
        })
    ))
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
        .execute(&table.sql_all_ordered_by_id(), (), None)
        .unwrap()
        .unwrap();
    let buffer = ColumnarDynBuffer::from_descs(
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

/// Bulk fetch in a dedicated system thread. Usually so the application can process the last batch
/// while the next one is fetched.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_double_buffered(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (1), (2)"),
        (),
        None,
    )
    .unwrap();

    // When
    let mut buffer_a = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), (), None)
        .unwrap()
        .unwrap();
    let block_cursor = cursor.bind_buffer(buffer_b).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);

    let has_another_batch = concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    assert!(has_another_batch);
    assert_eq!(1, buffer_a.num_rows());
    assert_eq!(1i32, buffer_a.column(0).as_slice::<i32>().unwrap()[0]);

    let has_another_batch = concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    assert!(has_another_batch);
    assert_eq!(1, buffer_a.num_rows());
    assert_eq!(2i32, buffer_a.column(0).as_slice::<i32>().unwrap()[0]);

    let has_another_batch = concurrent_block_cursor.fetch_into(&mut buffer_a).unwrap();
    assert!(!has_another_batch);
}

/// Bulf fetch in a dedicated system thread. Usually so the application can process the last batch
/// while the next one is fetched.
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_single_buffer(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (1), (2)"),
        (),
        None,
    )
    .unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), (), None)
        .unwrap()
        .unwrap();
    let block_cursor = cursor.bind_buffer(buffer).unwrap();
    let mut concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);

    let batch = concurrent_block_cursor.fetch().unwrap().unwrap();
    assert_eq!(1, batch.num_rows());
    assert_eq!(1i32, batch.column(0).as_slice::<i32>().unwrap()[0]);
    concurrent_block_cursor.fill(batch);

    let batch = concurrent_block_cursor.fetch().unwrap().unwrap();
    assert_eq!(1, batch.num_rows());
    assert_eq!(2i32, batch.column(0).as_slice::<i32>().unwrap()[0]);
    concurrent_block_cursor.fill(batch);

    let all_batches_consumed = concurrent_block_cursor.fetch().unwrap().is_none();
    assert!(all_batches_consumed);
}

/// Catch edge cases, there we stop the thread, while there are still batches to consume
#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
#[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_fetch_of_one_batch(profile: &Profile) {
    // Given
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (1), (2)"),
        (),
        None,
    )
    .unwrap();

    // When
    let buffer = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), (), None)
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

#[test_case(MSSQL; "Microsoft SQL Server")]
#[test_case(MARIADB; "Maria DB")]
// SQLite, actually does not create a diagonstic record or is creating any kind of error in this
// case. It would just report the value to be zero. This has nothing todo with the fetch being
// concurrent though. To test the error handling, the other DBMs have to suffice
// #[test_case(SQLITE_3; "SQLite 3")]
#[test_case(POSTGRES; "PostgreSQL")]
fn concurrent_with_invalid_buffer_type(profile: &Profile) {
    // Given an integer table with a NULL
    let table_name = table_name!();
    let (conn, table) = Given::new(&table_name)
        .column_types(&["INT"])
        .build(profile)
        .unwrap();
    conn.execute(
        &format!("INSERT INTO {table_name} (a) VALUES (NULL)"),
        (),
        None,
    )
    .unwrap();

    // When fetching with a Columnar buffer not supporting nullable values
    let mut buffer_a = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn
        .into_cursor(&table.sql_all_ordered_by_id(), (), None)
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
    let mut buffer_a = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn.into_cursor(query, (), None).unwrap().unwrap();
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
    assert_eq!(2i32, batch.column(0).as_slice::<i32>().unwrap()[0]);
}

/// This test covers a code path in which the thread dedicated to fething is not termintated by
/// running out of batches.
#[test_case(MSSQL; "Microsoft SQL Server")]
fn concurrent_fetch_skip_first_result_set(profile: &Profile) {
    // Given
    let conn = profile.connection().unwrap();
    let query = "SELECT 1 AS a; SELECT 2 AS b;";

    // When
    let buffer_a = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let buffer_b = ColumnarDynBuffer::from_descs(1, [BufferDesc::I32 { nullable: false }]);
    let cursor = conn.into_cursor(query, (), None).unwrap().unwrap();
    let block_cursor = cursor.bind_buffer(buffer_b).unwrap();
    let concurrent_block_cursor = ConcurrentBlockCursor::from_block_cursor(block_cursor);
    // Skip over first result set, without fetching any batches.
    // Now continue with the same cursor to fetch the second
    let cursor = concurrent_block_cursor.into_cursor().unwrap();
    let cursor = cursor.more_results().unwrap().unwrap();
    let mut cursor = cursor.bind_buffer(buffer_a).unwrap();
    let batch = cursor.fetch().unwrap().unwrap();

    // Then
    assert_eq!(2i32, batch.column(0).as_slice::<i32>().unwrap()[0]);
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
