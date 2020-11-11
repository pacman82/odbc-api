use assert_cmd::Command;
use lazy_static::lazy_static;
use odbc_api::Environment;

const MSSQL: &str =
    "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;";

// Rust by default executes tests in parallel. Yet only one environment is allowed at a time.
lazy_static! {
    static ref ENV: Environment = unsafe { Environment::new().unwrap() };
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

    // Setup table for test. We use the table name only in this test.
    let mut conn = ENV.connect_with_connection_string(MSSQL).unwrap();
    conn.execute("DROP TABLE IF EXISTS odbcsv_insert;", ())
        .unwrap();
    conn.execute(
        "CREATE TABLE odbcsv_insert (country VARCHAR(255), population BIGINT);",
        (),
    )
    .unwrap();

    // Insert csv
    Command::cargo_bin("odbcsv")
        .unwrap()
        .args(&[
            "-vvvv",
            "insert",
            "--connection-string",
            MSSQL,
            "odbcsv_insert",
        ])
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
            "SELECT country, population FROM odbcsv_insert ORDER BY population;",
        ])
        .assert()
        .stdout(csv)
        .success();
}
