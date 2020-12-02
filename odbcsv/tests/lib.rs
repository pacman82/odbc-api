use std::{fs::File, io::Read};

use assert_cmd::{assert::Assert, Command};
use lazy_static::lazy_static;
use odbc_api::Environment;

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    static ref ENV: Environment = unsafe { Environment::new().unwrap() };
}

/// Test helper using two commands to roundtrip csv to and from a data source.
///
/// # Parameters
///
/// * `csv`: csv used in the roundtrip. Table schema is currently hardcoded.
/// * `table_name`: Each test must use its unique table name, to avoid race conditions with other
///   tests.
fn roundtrip(csv: &'static str, table_name: &str) -> Assert {
    // Setup table for test. We use the table name only in this test.
    let conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    conn.execute(&format!("DROP TABLE IF EXISTS {}", table_name), ())
        .unwrap();
    conn.execute(
        &format!(
            "CREATE TABLE {} (country VARCHAR(255), population BIGINT);",
            table_name
        ),
        (),
    )
    .unwrap();

    // Insert csv
    Command::cargo_bin("odbcsv")
        .unwrap()
        .args(&["-vvvv", "insert", "--connection-string", MSSQL, table_name])
        .write_stdin(csv)
        .assert()
        .success();

    // Query csv
    Command::cargo_bin("odbcsv")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            &format!(
                "SELECT country, population FROM {} ORDER BY population;",
                table_name
            ),
        ])
        .assert()
        .stdout(csv)
}

#[test]
fn query_mssql() {
    let csv = "title,year\n\
        Jurassic Park,1993\n\
        2001: A Space Odyssey,1968\n\
        Interstellar,\n\
    ";

    Command::cargo_bin("odbcsv")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            "SELECT title, year from Movies",
        ])
        .assert()
        .success()
        .stdout(csv);
}

#[test]
fn placeholders() {
    let csv = "title\n\
        2001: A Space Odyssey\n\
    ";

    Command::cargo_bin("odbcsv")
        .unwrap()
        .args(&[
            "-vvvv",
            "query",
            "--connection-string",
            MSSQL,
            "SELECT title from Movies where year > ? and year < ? ",
            "1960",
            "1970",
        ])
        .assert()
        .success()
        .stdout(csv);
}

#[test]
fn insert() {
    let csv = "country,population\n\
        Norway,5380000\n\
        Germany,83500000\n\
        USA,329000000\n\
    ";

    roundtrip(csv, "odbcsv_insert").success();
}

#[test]
fn insert_empty_document() {
    let csv = "country,population\n";

    roundtrip(csv, "odbcsv_empty_document").success();
}

#[test]
fn insert_batch_size_one() {
    let csv = "country,population\n\
        Norway,5380000\n\
        Germany,83500000\n\
        USA,329000000\n\
    ";

    roundtrip(csv, "odbcsv_insert_batch_size_one").success();
}

#[test]
fn insert_with_nulls() {
    let csv = "country,population\n\
        Norway,\n\
        ,83500000\n\
        USA,329000000\n\
    ";

    roundtrip(csv, "odbcsv_insert_with_nulls").success();
}

/// An "optional" for list-drivers command. It checks for the existence of a "list-drivers.txt". If
/// so it compares the output of the `list-drivers` command with the file content. This setup is
/// intended to provide a test for dev container or CI setups there the installed drivers are
/// controlled by this repository, but gracefully skip, if we run natively on a developer machine
/// with a different set of drivers installed.
#[test]
fn list_drivers() {
    if let Ok(mut expectation) = File::open("tests/list-drivers.txt") {
        let mut buf = String::new();
        expectation.read_to_string(&mut buf).unwrap();

        Command::cargo_bin("odbcsv")
            .unwrap()
            .args(&["-vvvv", "list-drivers"])
            .assert()
            .success()
            .stdout(buf);
    }
}
