# ODBCSV

Query an ODBC data source and output the result as CSV.

## Usage

```shell
odbcsv --output query.csv --connection-string "Driver={SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" "SELECT title, year from Movies"
```

Use `--help` to see all options.

## Installation

```shell
cargo install odbcsv
```
