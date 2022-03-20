# Changelog

## 0.3.65

* Updated dependencies

## 0.3.64

* Updated dependencies

## 0.3.63

* Updated dependencies

## 0.3.62

* Updated dependencies

## 0.3.61

* Updated dependencies

## 0.3.60

* Updated dependencies

## 0.3.59

* Updated dependencies

## 0.3.58

* Updated dependencies
* Fix: Out of memory then listing columns for a MariaDB source on Windows

## 0.3.57

* Updated dependencies

## 0.3.56

* Updated dependencies

## 0.3.55

* Updated dependencies

## 0.3.54

* Passwords containing `+` are now also escaped, if passed via the `--password` parameter.

## 0.3.53

* Updated dependencies

## 0.3.52

* Updated dependencies

## 0.3.51

* Fixed an issue which caused the wrong text to be rendered in the help message.

## 0.3.50

* Introduced new subcommand `fetch`. It replaces `query` and allows for reading the SQL query text from a file.

## 0.3.49

* Updated dependencies

## 0.3.48

* Updated dependencies

## 0.3.47

* Add new subcommand `list-columns`.

## 0.3.46

* Add new subcommand `list-tables`.

## 0.3.45

* Updated dependencies

## 0.3.44

* Updated dependencies

## 0.3.43

* Updated dependencies

## 0.3.42

* Updated dependencies

## 0.3.41

* Updated dependencies

## 0.3.40

* Updated dependencies

## 0.3.39

* Updated dependencies

## 0.3.38

* Updated dependencies

## 0.3.37

* Updated dependencies

## 0.3.36

* Updated depnedencies

## 0.3.35

* Updated dependencies

## 0.3.34

* Updated dependencies

## 0.3.33

* Updated dependencies

## 0.3.32

* Updated dependencies

## 0.3.31

* Updated dependencies

## 0.3.30

* Update dependencies

## 0.3.29

* Update dependencies

## 0.3.28

* Update dependencies

## 0.3.27

* Update dependencies

## 0.3.26

* Windows version only: Add `--prompt` flag to allow for completion of connection string via GUI.

## 0.3.25

* Update dependencies

## 0.3.24

* Command line parameters `user` and `password` will no longer be ignored then passed together with a connection string. Instead their values will be appended as `UID` and `PWD` attributes at the end.

## 0.3.23

* Update dependencies

## 0.3.22

* Update dependencies

## 0.3.21

* Update dependencies

## 0.3.20

* Update dependencies

## 0.3.19

* Update dependencies

## 0.3.18

* Update dependencies

## 0.3.17

* Update dependencies
* Fix: A panic could happen during insert due to an out of bounds access to an input buffer.

## 0.3.16

* Update dependencies

## 0.3.15

* New optional parameter `max-str-len` can be used to limit memory consumption.

## 0.3.14

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
