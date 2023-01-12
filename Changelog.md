# Changelog

## 0.54.0

* Changed checking of truncated values after fetch. The new method checks the indicator buffer, rather than relying on diagnostics. This means `Error::TooManyDiagnostics` can no longer occurr.

## 0.53.0

* Better error messages if checking for truncation fails due to too many diagnostics
* Removed deprecated
  * `BufferDescription`
  * `BufferKind`
* Removed deprecated names
  * `AnyColumnBuffer`
  * `AnyColumnSliceMut`
  * `AnyColumnView`
  * `RowSetCursor`
  * `RowSetCursorPolling`

## 0.52.4

* Improved support for fetching parameter descriptions of prepared statements:
  * Introduced method `Prepared::num_params`
  * Introduced method `Prepared::parameter_descriptions`

## 0.52.3

* Introduced method `Prepared::column_inserter` to replace `Prepared::any_column_inserter`.
* Introduced method `Prepared::into_column_inserter` to replace `Prepared::into_any_column_inserter`.

## 0.52.2

* Introduced associated method `Item::buffer_desc` to replace `Item::BUFFER_KIND`.

## 0.52.1

* `BufferKind` and `BufferDescription` are now unified to `BufferDesc`.
* Renamed `RowSetCursor` into `BlockCursor`.
* Fix: `ColumnarBulkInserter::new` now resets all parameters on the statement handle in case it fails. This prevents accessing the bound parameters on a borrowed statement handle after the constructor failed, at which point their valididty is no longer guaranteed.

## 0.52.0

* `sys::Timestamp` is now directly supported with `get_data`.
* `InputParameter` is now implying the new `CElement` trait.

## 0.51.0

* Removed `ColumnProjection` trait.
* `ColumnBuffer` now has generic associated type `View` instead of inheriting from `ColumnProjection`.
* `WithDataType` can now also be used to annotate the Relational type on `Timestamp`.
* `WithDataType<Timestamp>` now implements `OutputParameter`.
* Minimum Rust version is 1.65.0

## 0.50.3

* Fix: There has been an issue with fetching text values using `get_text` from a `db2` System. `db2` is not part of the test suite (yet?), so not entirely sure that it is truly gone yet, but chances are its fixed.
* Implement `ColumnarBuffer` for `Vec<T>`.

## 0.50.2

* `TextBufferRowSet::for_cursor` now applies the upper bound column size limits also to buffers if the metadata indicates a size of `0`. This allows e.g. fetching values from Microsoft SQL Server columns with type `VARCHAR(max)`.

## 0.50.1

* Updated documentation for `BufferDescription` and `DataType`.

## 0.50.0

* Fetching diagnostic records now starts with a message buffer size of 512 instead of 0 in order to recduce calls to `SQLGetDiagRec`.

### Breaking

* `SqlResult` has now new variants `SqlResult::NoData` and `SqlResult::NeedData`.
* `SqlResult::into_result_with_trunaction_check` has been replaced by `SqlResult::into_result_with`.
* `handles::Environment::driver_connect` now returns `SqlResult<()>` instead of `SqlResult<bool>`.
* `handles::Environment::drivers_buffer_fill` now returns `SqlResult<()>` instead of `SqlResult<bool>`.
* `handles::Environment::drivers_buffer_len` now returns `SqlResult<(i16, i16)>` instead of `SqlResult<Option<(i16, i16)>>`.
* `handles::Environment::data_source_buffer_len` now returns `SqlResult<(i16, i16)>` instead of `SqlResult<Option<(i16, i16)>>`.
* `handles::Connection::execute` now returns now returns `SqlResult<()>` instead of `SqlResult<bool>`.
* `handles::Connection::exec_direct` now returns now returns `SqlResult<()>` instead of `SqlResult<bool>`.
* `handles::Statement::fetch` now returns now returns `SqlResult<()>` instead of `SqlResult<bool>`.
* `handles::Statement::put_binary_batch` now returns now returns `SqlResult<()>` instead of `SqlResult<bool>`.

### Non Breaking

* Reserve an extra word (2 Bytes) when querying Catalog length in order to compensate for an of by one error in PostgreSQL driver when using wide character encoding. The issue would have resulted in the last letter being replaced with `\0` in previous versions.

## 0.49.0

* No breaking changes. Messed up setting version. Should have been `0.48.1` 😅.
* Asynchronous execution of one shot queries using `Connection::execute_polling`.
* Asynchronous bulk fetching of results using `RowSetCursorPolling`.

## 0.48.0

* Both `Preallocated::row_count` and `Prepared::row_count` now return `Option<usize>` instead of `isize`.

## 0.47.0

* Support getting number of rows affected by `INSERT`, `UPDATE` or `DELETE`.
  * New method `Preallocated::row_count`
  * New method `Prepared::row_count`
* `TextRowSet::from_max_str_lens` now takes `IntoIterator<Item=usize>` instead of `Iterator<Item=usize>`.
* trait method `RowSetBuffer::bind_to_cursor` has been renamed to `bind_columns_to_cursor` and now takes a `StatementRef` instead of a `Cursor`. This change is unlikely to affect user code, as so far I know all downstream crates use the provided `RowSetBuffer` implementations and do not implement their own versions.

## 0.46.0

* Minimal support for asynchronous code in the `handle` module.
* `SqlResult` has a new variant `SqlResult::StillExecuting`.
* New function `Statement::set_async_enable` can be used to enable polling for statements.
* Functions returning `Option<SqlResult<_>>` now return `SqlResult<Option<_>>` or `SqlResult<bool>` in order to allow for top level asynchronous code to build on the same abstraction as the synchronous code.

Since most users would not engage with the `unsafe` functions of the `handle` module their code should be unaffected.

## 0.45.1

* `TextRowSet::for_cursor` now only performs a faliable allocation, if no upper bound for string length is specified. This has been changed to remidy performance regressions, but still have the faliable allocation in situation there it could likely occurr.

## 0.45.0

* Add `Connection::into_prepared` to allow for prepared queries to take ownership of the connection.
* `Prepared::describe_param` now requires an exclusive reference
* Remove `Prebound`.

## 0.44.3

* Add `NullableSliceMut::set_cell`

## 0.44.2

* Add `Bit::from_bool`.

## 0.44.1

* Add `ColumnarBulkInserter::capacity`.

## 0.44.0

* All methods on the `ResultSetMetaData` trait now require exclusive (`&mut`) references.
* The trait `BorrowMutStatement` has been replaced by `AsStatementRef`.
* `ColumnarBulkInserter` is now generic over statement ownership.
* It is now possible to create a bulk inserter which just borrows a prepared statement, rather than taking ownership of it. Use `Prepared::any_column_inserter` to do so. This is useful for dynamically growing the capacity of the bound array parameters. So far users were forced to create a new `ColumnarBulkInserter` to do so. This is still true, but creating a new `ColumnarBulkInserter` now longer implies that a new prepared statement has to be created.

## 0.43.0

* `BoundInputSlice`, `TextColumnSliceMut` and `BinColumnSliceMut` now only track the lifetime of the statement. The do no longer need to track the lifetime of the `Connection` anymore.

## 0.42.0

* Removed `TextColumn::set_max_len` as `resize_max_str_len` can do everything the former can and
  does it better.
* `ColumnBuffer` and `TextRowSet` can no longer be used to insert data into the database. It can now only be used to receive data. This is due to different invariants which must hold true for input and output buffers. The new `ColumnarBulkInserter` which can be constructed using `into_any_column_inserter` or `into_text_inserter` on a prepared statment closes this safety hole. It also allows for faster insertion into a database in case you transmit the data in several batches, because it does not require the user to rebind the parameter buffers in safe code. This significantly reduces the overhead for each batch.

## 0.41.0

* Refactored traits for passing parameters to `Connection::execute`. Should not break user syntax, though.
* Allow using fixed size paramters like `i32` directly as output parameter without wrapping them into a `Nullable` first.
* Renamed `TextColumnWriter::rebind` into `TextColumnWriter::resize_max_str`
* Fast insertion into database using `TextRowSet` without rebinding parameter buffers for individual batches.

## 0.40.2

* Introduce alias for `ColumnarAnyBuffer` for `ColumnarBuffer<AnyColumnBuffer>`.

## 0.40.1

* Introduce feature `iodbc`. This features enables all features required for this crate to work with the IODBC driver manager.

  * `narrow` ODBC function calls
  * Use of `odbc_version_3_5` symbols and declaration of ODBC API version 3.0.
  * Linking of upstream `odbc-sys` vs `iodbc` as opposed to plain `odbc`.

* Introduce `ColumnDescription::new` to allow for intantiating `ColumnDescription` in test cases without caring about wether `SqlChar` is `u8` or `u16`.

## 0.40.0

* The fallibale allocations introduced for `buffer_from_description` had performance implications. Therfore the fallibale allocations are available to users of this library to `try_buffer_from_description`. `buffer_for_description` is now panicing again in case there is not enough memory available to allocate the buffers.

## 0.39.1

* Fixed an issue introduced in `0.39.1` there the terminating zero at the end of diagnostic messages has not been removed correctly.

## 0.39.0

* Support for checking for truncation after bulk fetch with: `RowSetCursor::fetch_with_truncation_check`.

## 0.38.0

* Add column buffer index to allocation errors to provide more context

## 0.37.0

* Add fallibale allocation of binary columns.
* Add fallibale allocation of text columns.

## 0.36.2

* Add `AnyColumnViewMut::as_text_view`.

## 0.36.1

* If setting the ODBC version fails with `HY092` (Invalid attribute value), the user will now see an error message stating that likely the ODBC driver manager should be updated as it does not seem to support ODBC version 3.80. This gives the user a better starting point to fix the problem than just the diagnostic record.

## 0.36.0

* Introduce `AnyColumnView::Text` and `AnyColumnView::WText` now holds a `TextColumnView` instead of a `TextColumnIt`. To get the iterator you can just call `iter()` on the `TextColumnView`.
* Introduce `AnyColumnView::Binary` now holds a `BinColumnView` instead of a `BinColumnIt`.
* Add `AnyColumnView::as_text_view`.
* Add `AnyColumnView::as_w_text_view`.
* Add `AnyColumnView::as_slice`.
* Add `AnyColumnView::as_nullable_slice`.

The `TextColumnView` has been added in order to allow for faster bindings to arrow. As it gives access to the raw value buffer.

## 0.35.2

* Allow direct access to raw buffers of `NullableSliceMut`.

## 0.35.1

* Allow direct access to raw buffers of `NullableSlice`.
* `ColumnWithIndicator::iter` is no longer `unsafe`
* `TextColumn::indicator_at` is no longer `unsafe`
* `TextColumn::value_at` is no longer `unsafe`
* `BinColumn::value_at` is no longer `unsafe`.
* `BinColumn::iter` is no longer `unsafe`.

Thanks to @jorgecarleitao for providing all the input about safety and discovering performance\
bottlenecks in the buffer interfaces.

## 0.35.0

* Better error for Oracle users, then trying to fetch data into 64 Bit Buffers. Added variant `OracleOdbcDriverDoesNotSupport64Bit` to error type in order to achieve this.

## 0.34.1

* Introduce feature `odbc_version_3_5`. Decalaring ODBC version 3.80 is still default behaviour. The flag has mainly been introduced to achieve compatibility with iODBC.

## 0.34.0

* Introduces `narrow` feature, allowing to compile this library against narrow ODBC function calls. This is optional now, but may become default behaviour in the future on non-windows platforms (because those typically have UTF-8 locale by default.)
* Removed public higher level methods directly dealing with utf16. Similar functions using unicode remain in the API. This has been done in order better support building against narrow ODBC function calls on some platforms while keeping complexity in check.
* `ResultSetMetadata::col_name` now returns `Result<String, Error>`.
* `Environment::driver_connect` now takes `&mut OutputStringBuffer` instead of `Option<&mut OutputStringBuffer>`. Use `OutputStringBuffer::empty()` instead of `None`.
* Removed `Connection::tables` now takes `&str` instead of `Option<&str>` arguments. Use `""` instead of none
* Removed `Environment::connect_utf16`.
* Removed `Preallocated::execute_utf16`.
* Removed `Connection::execute_utf16`.
* Removed `Connection::prepare_utf16`.
* Removed `OutputStringBuffer::ucstr`.
* Removed `Connection::fetch_current_catalog`.
* Removed `Connection::fetch_database_management_system_name`.
* updated to `odbc-sys 0.21.2`

## 0.33.2

* No changes (Release is currently tied to odbcsv)

## 0.33.1

* No changes (Release is currently tied to odbcsv)

## 0.33.0

* Uses Rust Edition 2021
* Mutable references `&mut T` are no longer implicitly bound as input / output Parameters, but must be wrapped with `InOut` instead. This has been done to make the decision between `Out` and `InOut` explicit, and also help to avoid binding parameters as `InOut` by accident. Shared references `&T` are still implicitly bound as input parameters however.
* Renamed `Parameter` to `ParameterRef`
* Renamed `Parameter::bind_parameter` to `ParameterRef::bind_to`
* Renamed `Output` to `OutputParameter`
* Renamed `TextColumn::rebind` to `TextColumn::resize_max_str`.
* `InputParameter` is now longer exported in the top level namespace, but only as `parameter::InputParameter`.
* `ParameterRefCollection::bind_to_statement` now takes `&mut self` rather than `self`.
* `ColumnarRowSet` and `TextRowSetBuffer` are now `ColumnarBuffer`, which is generic over columns.
* `ColumnarRowSet::new` is now `buffer_from_descriptions`.
* Removed `SqlResult::log_diagnostics`.

## 0.32.0

* Removed `unsafe` from trait `HasDataType`.
* Introduced `Prepared::into_statement` and `CursorImpl::new` to enable usecases not covered by safe abstractions yet.

## 0.31.2

* `escape_attribute_value` now also escapes `+` signs. This has been introduced because an authentication vs a PostgreSQL failed, if a password containing `+` has not been escaped.

## 0.31.1

* Fix: Some drivers (i.e. SqLite) report the size of column names in characters instead of bytes. This could cause truncation of names provided by `colum_names` if the supplied buffers were to short. Now the correct provided buffer is always resized to be large enough to hold the entire name, even for these drivers.

## 0.31.0

In order to avoid exposing the dependency to `raw-window-handle` in the public interface the way this library opens prompts for connecetion on windows platform has changed:

* Breaking change: `Environment::driver_connect` now automatically creates a message only window for the user on non-windows platform. `DriverCompleteOption` is now a simple C-Like enumeration.
* Introduced `Environment::driver_connect_with_hwnd` to allow power users to provide their own window handles.

## 0.30.1

* Minor improvements to documentation.

## 0.30.0

* Allows for fetching result set metadata from `Prepared` statements.
  * Introduced trait `ResultSetMetadata`. Which is implemented by both `Prepared` and `Cursor`.

## 0.29.3

* Make `StatementConnection` public

## 0.29.2

* Add `into_cursor` to Connection

## 0.29.1

* Improved documentation for `Connection::columns`.

## 0.29.0

* Removed generic lifetime argument from `CursorImpl`.
* Add `Connection::tables`.
* Add `Preallocated::tables`.

## 0.28.3

* After failing to get any race conditions with it on either Windows or Linux. `Environment::new` is now considered to be safe.

## 0.28.2

* Reexport crate `force_send_sync`.

## 0.28.1

* Fix: `handles::Environment` is now longer declared `Sync`. Due to the interior mutability regarding error states.
* Fix: A lock has been added to `Environment` to ensure diagnostics for allocation errors always are handled by the correct thread.

## 0.28.0

* Add `is_empty` to `NullableSlice` and `NullableSliceMut`.
* Remove `Statement::col_data_type`. Method is still available via the `Cursor` trait, though.
* Replace `Error` with `SqlReturn` in `handles` module to not make any decisions about handling diagnostics in that layer.

## 0.27.3

* Add implementation for `ExactSizeIterator` for `TextColumnIt` and `BinColumnIt`.

## 0.27.2

* Add `NullableSlice::len`.

## 0.27.1

* Fix: `DataType::Float` is now a struct variant holding a precision value which is either `24` for 32Bit Floating points or `53` for 64Bit Floating point numbers.

## 0.27.0

* `Connection::columns` and `Preallocated::columns` now return a `Result<CursorImpl, Error>` instead of a `Result<Option<CursorImpl>, Error>` since `columns` always returns a Cursor.
* These functions now return a `Result<u16, Error>` instead of a `Result<usize, Error>`:
  * `Connection::max_catalog_name_len`
  * `Connection::max_schema_name_len`
  * `Connection::max_column_name_len`
* Renamed `OptIt` to `NullableSlice`.
* Renamed `OptWriter` to `NullableSliceMut`.
* Added `buffer::Item` trait to make it more convinient to extract a slice or a `NullableSlice` from `AnyColumnView`.

## 0.26.1

* `handles::Nullablility` now implements `Copy`.
* `Nullable<T>` now implements `Copy` if `T` does.

## 0.26.0

* Add `Error::InvalidRowArraySize` which is returned instead of `Error::Diagnostics` if invalid attribute is returned during setting row set array size.

## 0.25.0

* `Error::Diagnostics` is now a struct variant and also contains the name of the ODBC function call which caused the error.
* Renamed `Error::OdbcApiVersionUnsupported` to `UnsupportedOdbcApiVersion` to achieve consistent word order.
* Fix: Row set array size is now specified in `usize` rather than `u32`.
* Fix: Bind type is now specified in `usize` rather than `u32`.
* Fix: Parameter set size is now specified in `usize` rather than `u32`.

## 0.24.1

* Fix: Code example blobs
* Fix: An issue intruduced in `0.24.0` there a searched update or delete statement that does not affect any rows at the data source could cause a panic.

## 0.24.0

* Add Support for sending long data.
* Add variant `DataType::LongVarChar`.
* Add variant `DataType::LongVarBinary`.
* Add variant `Error::FailedReadingInput`.
* trait `HasDataType` no longer implies trait `CData`.

## 0.23.2

* Adds `Connection::columns`
* Adds `Connection::columns_buffer_description`

These wrap the ODBC `SqlColumns` function and represent anoter way to obtain schema information.

Thanks to @grovesNL for the contribution.

## 0.23.1

* Add support for Connection Pooling
* Add methods for exposing metadata to SQLColumns based queries (Thanks again to @grovesNL).

## 0.23.0

* Better error message in case ODBC Api version is set on older version of unixODBC.
  * Introduced `OdbcApiVersionUnsupported` variant to Error type.
  * Changed ODBC state representation in diagnostic to narrow ASCII characters.

## 0.22.3

* Fix: `VarCharBox::null()` had been bound as `VARCHAR(0)`. This caused an error with Access and
  older versions of Microsoft SQL Server ODBC Driver. To fix this `VarCharBox::null()` is now bound
  as `VARCHAR(1)`. Thanks to @grovesNL for finding and investigating the issue.

  Analoge issues also fixed for:

  * `VarBinarySlice::NULL`
  * `VarBinaryBox::null()`

## 0.22.2

* Fix: `VarCharSlice::NULL` had been bound as `VARCHAR(0)`. This caused an error with Access and
  older versions of Microsoft SQL Server ODBC Driver. To fix this `VarCharSlice::NULL` is now bound
  as `VARCHAR(1)`. Thanks to @grovesNL for finding and investigating the issue.
* Support access to raw `odbc-sys` functionality. This is done to support use cases for which a safe
  abstraction is not yet available (like streaming large values into the database).
  * Add `preallocated::into_statement`.
  * Add `StatementImpl::into_sys`.
  * Add `StatementImpl::as_sys`.
  * Add `Statement::param_data`

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
