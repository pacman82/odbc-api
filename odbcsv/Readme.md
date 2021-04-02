# ODBCSV

Query an ODBC data source and output the result as CSV or to insert from CSV into an ODBC data source.

## Installation

Several installation options are available.

### Download prebuilt binaries

You can download the latest binaries here from the `odbc-api` GitHub release: <https://github.com/pacman82/odbc-api/releases/latest>.

### Install using cargo

```shell
cargo install odbcsv
```

## Usage

### Querying an Microsoft SQL Database and storing the result in a file

```shell
odbcsv query \
--output query.csv \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" \
"SELECT title, year from Movies"
```

### Specify user and password independently

```shell
odbcsv query \
--output query.csv \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;" \
--user "SA" \
--password "<YourStrong@Passw0rd>" \
"SELECT title, year from Movies"
```

Alternatively you may also specify the `ODBC_USER` and `ODBC_PASSWORD` environment variable.

### Query using data source name

```bash
odbcsv query \
--output query.csv \
--dsn my_db \
--password "<YourStrong@Passw0rd>" \
--user "SA" \
"SELECT * FROM Birthdays"
```

### Use parameters in query

```shell
odbcsv query \
--output query.csv \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" \
"SELECT * FROM Birthdays WHERE year > ? and year < ?" \
1990 2010
```

### Insert data from CSV to database

```shell
odbcsv insert \
--input birthdays.csv \
--connection-string "Driver={ODBC Driver 17 for SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" \
Birthdays \
```

Use `--help` to see all options.
