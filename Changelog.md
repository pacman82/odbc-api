# Changelog

## 0.22.1

* Support for passing binary data as parameter.
  * `VarBinarySlice` - immutable borrowed parameter.
  * `VarBinarySliceMut`] - mutable borrowed input / output parameter
  * `VarBinaryArray` - stack allocated owned input / output parameter
  * `VarBinaryBox` - heap allocated owned input / output parameter
  * `&[u8]` now implements `IntoParameter`.
  * `Vec<u8>` now implements `IntoParameter`.
* Introduced `CursorRow::get_binary` to fetch arbitrary large blobs into a `Vec<u8>`.

* Fix: `VarCharArray::new` is now placing a terminating zero at the end in case of truncation. As statet in the documentation.

## 0.22.0

* Add `Environment::driver_connect` which forwards more functionality from `SqlDriverConnect`. In particular it is now possible (on windows systems) to complete connection strings via the dialog provided by the ODBC driver and driver manager.
* `Error` has new variant: `Error::AbortedConnectionStringCompletion`. Which occurs if the GUI for completion is closed before returning any data.

Thanks to @grovesNL for the contribution.

## 0.21.0

* `Environment::drivers` now takes `&self` instead of `&mut self`.
* `Environment::data_sources` now takes `&self` instead of `&mut self`.
* `Environment::user_data_sources` now takes `&self` instead of `&mut self`.
* `Environment::system_data_sources` now takes `&self` instead of `&mut self`.
* `handles::Environment::drivers_buffer_len` now is `unsafe` and takes `&self` instead of `&mut self`.
* `handles::Environment::drivers_buffer_fill` now is `unsafe` and takes `&self` instead of `&mut self`.
* `handles::Environment::data_source_buffer_len` now is `unsafe` and takes `&self` instead of `&mut self`.
* `handles::Environment::data_source_buffer_fill` now is `unsafe` and takes `&self` instead of `&mut self`.

Thanks to @grovesNL for the contribution.

## 0.20.1

* Test are now also executed against Maria DB
* Add `BufferDescription::bytes_per_row`.
* Add function `escape_attribute_value`.

## 0.20.0

* Required at least Rust 1.51.0 to compile.
* `IntoParameter` has been implement for `String`. This is the most likely thing to break your code as calling `.into_parameter()` would have worked on `String` before, but would have invoked the implementation on `str`. Should you want the old behaviour just call `my_string.as_str().into_parameter()`.
* Replaced `VarCharRef` with `VarCharSlice`.
* Replaced `VarChar32` and `VarChar512` with `VarCharArray<LENGTH>`.
* Replaced `VarCharMut` with `VarCharSliceMut`.
* `VarChar::copy_from_bytes` has been removed. Try constructing with `new`, `from_buffer` or using `NULL` instead.
* `VarChar::indicator()` now returns an `Indicator` instead of an `isize`.
* Replaced `VarCharRef::null()` with `VarCharSlice::NULL`.
* Support `CString` and `CStr` as input parameters.

## 0.19.6

* `Box<dyn InputParameter>` now implements `InputParameter`.

## 0.19.5

* Fix: `BinColumnWriter::append` erroneously inserted a terminating zero at the end of the value.

## 0.19.4

* Fix: Timestamps buffers are now bound with a default precision of 7 (100ns) in order to avoid an 'invalid precision' error than inserting (occurred with MSSQL). Previously the precision had been 9 (1ns).

## 0.19.3

Increased support for adaptive buffering during inserts using `ColumnarRowSet`:

* Add `TextColumnWriter::append`.
* Add `TextColumnWriter::max_len`.
* Add `TextColumnWriter::rebind`.
* Add `BinColumnWriter::max_len`.
* Add `BinColumnWriter::rebind`.
* Add `BinColumnWriter::append`.

Support for directly writing formatted text with fixed length to the underlying buffers.

* Add `TextColumn::set_mut`.
* Add `TextColumnWriter::set_mut`.

## 0.19.2

* Add `TextColumnWriter::set_value`.

## 0.19.1

* Fix: `Statement::col_data_type` now returns `DataType::WChar` instead of `DataType::Other` then appropriate.
* Fix: An out of bounds panic with columnar inserts from text buffer, if the last value of the buffer had maximum length in a buffer with the maximum number of rows filled (yes, of by one error).

## 0.19.0

* Add variant `DataType::WChar` for fixed sized UTF-16 character data.
* Add method `DataType::utf16_len`.

## 0.18.0

* `CursorRow::get_text` now returns `bool` with `false` indicating a `NULL` field.
* `TextColumn` is now generic over its character type. The type alias `CharColumn` for
  `TextColumn<u8>` can serve as a drop in replacement for the previous iteration of `TextColumn`.
* `AnyColumnBuffer` has a new variant called `WText` holding a UTF-16 `TextColumn<u16>` or `WCharColumn`.
* `TextRowSet::for_cursor` has a new parameter `max_str_len` to designate the upper limit for column buffer size.
* Add `TextRowSet::indicator_at` to allow checking for truncation of strings.

## 0.17.0

* Introduces `Cursor::next_row` to fetch data without binding buffers first.
* Allow retrieving arbitrarily large strings from the data source.
* Removed `RowSetCursor::unbind` due to soundness issue, if fetching continues without any buffers bound.
* Hides internal interface of `Cursor` trait behind `Cursor::stmt`.
* `RowSetCursor` now implements `Drop`.
* `Statement::set_num_rows_fetched` now takes an option to allow unbinding a previously set reference.
* Trait `OutputParameter` has been renamed to `Output`.
* Trait `FixedSizedCType` has been renamed to `Pod`.

## 0.16.0

* `Cursor::fetch` is now safe.
* Introduced `Connection::preallocate` to allow for efficient preallocation of statement handles in interactive clients.
* Renamed `Connection::exec_direct_utf16` into `Connection::execute_utf16`.

## 0.15.0

* Text column buffers throughout the API now use `&[u8]` to represent queried strings instead of `CStr` to account for the possibility of interior nuls.

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

* Adds method `Prepared::describe_param()`.
* `handle::Statement::bind_parameter` changes into `handle::Statement::bind_input_parameter`, in order take a `* const`.
* `VarCharParam` renamed into `VarChar`.
* `TextRowSet::new` renamed into `TextRowSet::for_cursor`.
* trait `ParameterCollection` has a new method `parameter_set_size`.
* `Parameter` trait is now inheriting from `Input`.
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
* Adds `ColumnDescription::could_be_null`.
* Adds `Cursor::is_unsigned`.

## 0.1.0

Initial release
