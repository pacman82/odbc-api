use assert_cmd::Command;

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
            "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;",
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
            "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;",
            "SELECT title from Movies where year > ? and year < ? ",
            "1960",
            "1970"
        ])
        .assert()
        .success()
        .stdout(csv);
}
