# Changelog

## 0.8.1 (next)

## 0.8.0

* Basic support for binding parameters.
* Basic support for prepared queries.
* Due to the new possibility of binding parameters `Statement::exec_direct` and `Statement::execute` must now be considered unsafe. 
* `BindColParameters` renamed to `BindColArgs`.

## 0.7.0

* `Environment::connect` now takes `&str` instead of `&U16Str`.
* Adds `Environment::connect_utf16`.

## 0.6.2

* Logs all diagnostics in case driver returns multiple errors.

## 0.6.1

* Adds `Bit`, `OptBitColumn`, `OptI8Column`, `OptU8Column`.

## 0.6.0

* Extend `ColumnDescription` enum with `Bigint`, `Tinyint` and `Bit`.

## 0.5.0

* Introduced `DataType` into the `ColumnDescription` struct.
* Renaming: Drops `Buffer` suffix from `OptDateColumn`, `OptF32Column`, `OptF64Column`, `OptFixedSizedColumn`, `OptI32Column`, `OptI64Column`, `OptTimestampColumn`.

## 0.4.0

* Adds buffer implementation for fixed sized types

## 0.3.1

* Implements `FixedCSizedCType` for `odbc_sys::Time`.

## 0.3.0

* Renamed `cursor::col_data_type` to `cursor::col_type`.
* Adds `cursor::col_concise_type`.

## 0.2.3

* Adds `cursor::col_precision`.
* Adds `cursor::col_scale`.
* Adds `cursor::col_name`.

## 0.2.2

* Add trait `buffers::FixSizedCType`.

## 0.2.1

* Fix: `buffers::TextColumn` now correctly adjusts the target length by +1 for terminating zero.

## 0.2.0

Adds `buffers::TextRowSet`.

## 0.1.2

* Adds `col_display_size`, `col_data_type` and `col_octet_length` to `Cursor`.

## 0.1.1

* Adds convenience methods, which take UTF-8 in public interface.
* Adds `ColmunDescription::could_be_null`.
* Adds `Cursor::is_unsigned`.

## 0.1.0

Initial release
