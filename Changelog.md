# Changelog

## 0.16.0

* `Cursor::fetch` is now safe.
* Introduced `Connection::preallocate` to allow for efficient prealloaction of statement handles in interactive clients.
* Renamed `Connection::exec_direct_utf16` into `Connection::execute_utf16`.

## 0.15.0

* Text column buffers throught the API now use `&[u8]` to represent queried strings instead of `CStr` to account for the possibility of interior nuls.

## 0.14.0

* Fix: `BufferKind::from_data_type` now uses `display_size` rather than `column_size` then mapping `Numeric` and `Decimal` types to `Text` buffers.
* `BufferKind::from_data_type` now maps `DataType::Time` with `precision` > 0 to `BufferKind::Text` in order to avoid loosing fractional seconds. 
* Renamed `DataType::Tinyint` to `DataType::TinyInt` in order to be consistent with `DataType::SmallInt`.
* Renamed `DataType::Bigint` to `DataType::BigInt` in order to be consistent with `DataType::SmallInt`.
* Removed Application Row Descriptor handle. Likely to be reintroduced later though.
* `ColumnarRowSet` now has support for variadic binary data types.
* Add support for binary data types.

## 0.13.2

* Add support for transactions.

## 0.13.1

* Add support for bulk inserts to `ColumnarRowSet`.
* New section about inserting values in the guide.

## 0.13.0

* Add support for output parameters in stored procedures.
  * Input parameters now must be bound by reference.
  * Input/output parameters are bound by mutable reference.
* `Input` has been renamed to `HasDataType`.
* `Nullable` has been renamed to `Nullability`.
* Update to odbc-sys 0.17.0.

## 0.12.0

* Add `ColumnarRowSet::with_column_indices`, to enable binding only some columns of a cursor object.
* Struct `DataSourceInfo` is now public
* Associated function of `Cursor` `col_type` and `col_concise_type` have been removed in favor of `col_data_type`.
* Add `DataType::utf8_len`.
* Fix: `TextRowSet::for_cursor` now allocates 4 times larger buffers for character data, in order to have enough space for larger utf-8 characters.

## 0.11.1

* Fix: There has been an integer overflow causing a panic if an ODBC API call generated more than 2 ^ 15 warnings at once.
* Fix: `BufferKind::from_data_type` now always uses the integer type which fits best, for Decimal and Numeric types with scale `0`.

## 0.11.0

* Removed `Cursor::set_row_bind_type`.
* Removed `Cursor::set_row_array_size`.
* Removed `Cursor::set_num_rows_fetched`.
* `RowSetBuffer` trait now requires an implementation of `bind_type`.
* `RowSetBuffer` trait now requires an implementation of `row_array_size`.
* `RowSetBuffer` trait now requires an implementation of `mut_num_rows_fetched`.
* Add `Environment::data_sources`.
* Add `Environment::user_data_sources`.
* Add `Environment::system_data_sources`.
* `TextRowSet` now returns `CStr` instead of `&[u8]`.

## 0.10.3

* Removed `Connection::promote_send_and_sync` because it is unclear that diagnostic messages would
  always be pulled by the correct thread.
* Adds `Environment::drivers` to list available ODBC drivers.

## 0.10.2

* Implement `IntoParameter` for `Option<&str>`.

## 0.10.1

* `Connection::execute` takes now `&self` instead of `&mut self`.
* Support promoting `Connection` to `Send` or `Sync`.

## 0.10.0

* Adds method `Perpared::describe_param()`.
* `handle::Statement::bind_parameter` changes into `handle::Statement::bind_input_parameter`, in order take a `* const`.
* `VarCharParam` renamed into `VarChar`.
* `TextRowSet::new` renamed into `TextRowSet::for_cursor`.
* trait `ParameterCollection` has a new method `parameter_set_size`.
* `Parameter` trait is now inherting from `Input`.
* `ColumnBuffer` trait is replaced by `CDataMut`.
* Extended guide with section about parameter binding.

## 0.9.1

* `Environment` is now `Send` and `Sync`.
* Fix: Executing statements not generating results now does no longer create cursors.
* Fix: `Prepared::execute` now returns `Result<Option<CursorImpl<_>>, Error>` instead of `Result<CursorImpl<_>, Error>`.
* Fix: `Cursor::execute` is now `unsafe`.
* Fix: `Cursor::fetch` is now `unsafe`.

## 0.9.0

* `Cursor::bind_row_set_buffer` is now `Cursor::bind_buffer`.
* `Connection::exec_direct` is now `Connection::execute`.
* Tuple support for passing input parameters.

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
