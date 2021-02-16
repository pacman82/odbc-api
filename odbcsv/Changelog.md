# Changelog

## Next

* Insert now logs batch number and rows

## 0.3.13

* Update dependencies

## 0.3.12

* Fix: Interior nuls in the values of a VARCHAR columns cause no longer a panic.

## 0.3.11

* Updated dependencies

## 0.3.10

* Updated dependencies

## 0.3.9

* Updated dependencies

## 0.3.8

* Updated dependencies

## 0.3.7

* Fix: Allocated buffer sizes, now account for multi byte characters.

## 0.3.6

* Fix: There has been an integer overflow causing a panic if an ODBC API call generated more than 2 ^ 15 warnings at once.

## 0.3.5

* Add command `list-data-sources`

## 0.3.4

* Updated dependencies

## 0.3.3

* Add command `list-drivers`, to list drivers and attributes.

## 0.3.2

* Updated dependencies

## 0.3.1

* Fix: Commandline argument shorthand for `--input` is now `-i` instead of `o`.

## 0.3.0

* Command line interface now takes a subcommand.

## 0.2.5

* Runtime is now statically linked for windows executables.

## 0.2.4

* Updated dependencies

## 0.2.3

* Updated dependencies

## 0.2.2

* Fix release of 64 bit Windows binary

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
