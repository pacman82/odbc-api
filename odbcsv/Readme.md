# ODBCSV

Query an ODBC data source and output the result as CSV.

## Usage

```shell
odbcsv --output query.csv --connection-string "Driver={SQL Server};Server=localhost;UID=SA;PWD=<YourStrong@Passw0rd>;" "SELECT title, year from Movies"
```

Use `--help` to see all options.

## Installation

Several installation options are available.

### Download prebuild binaries

You can download the latest binaries here from the `odbc-api` GitHub release: <https://github.com/pacman82/odbc-api/releases/latest>.

### Install using cargo

```shell
cargo install odbcsv
```
