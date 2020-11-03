# Changelog

## 0.2.1

* Binary GitHub Releases

## 0.2.0

* Default bulk size increased to 5000 rows.
* Support for positional parameters in SQL query
* Support for opening connections with Data Source Name (`dsn`) instead of connection string.

## 0.1.7

* Updated Dependencies

## 0.1.6

* Column aliases are for headlines are no more reliably retrieved, even for ODBC data sources with drivers not reporting the column name length.

## 0.1.5

* Use column attributes instead of describe column to deduce names.

## 0.1.4

* Updated `odbc-api` version. This fixes a bug there data might be truncated.

## 0.1.3

* Updated metadata
* Updated dependencies

## 0.1.2

Use column display size to determine column buffer size.

## 0.1.1

Varchar typed columns have their size more accurately buffered.

## 0.1.0

Initial release
