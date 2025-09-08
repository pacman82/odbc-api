# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [18.0.1](https://github.com/pacman82/odbc-api/compare/v18.0.0...v18.0.1) - 2025-09-08

### Other

- update to odbc-sys 0.27
- use next_blob_param helper also in `execute`

## [18.0.0](https://github.com/pacman82/odbc-api/compare/v17.0.0...v18.0.0) - 2025-09-08

### Added

- Allow for async row streams to be Send
- `StatementImpl` and `StatementRef` are now `Send`

### Fixed

- [**breaking**] `Statement::num_result_cols` now takes a `&mut self` instead of

### Other

- Update to odbc-sys 26
- Fix broken reference
- Minor improvemnts to doc comments
- *(deps)* bump log from 0.4.27 to 0.4.28
- *(deps)* bump tempfile from 3.20.0 to 3.21.0
- *(deps)* bump thiserror from 2.0.15 to 2.0.16
- *(deps)* bump thiserror from 2.0.14 to 2.0.15
- *(deps)* bump proc-macro2 from 1.0.97 to 1.0.101
- *(deps)* bump syn from 2.0.105 to 2.0.106

## [17.0.0](https://github.com/pacman82/odbc-api/compare/v16.0.0...v17.0.0) - 2025-08-18

### Added

- ConnectionTransition::into_preallocated
- It is now possible to create `Prepared` statements owning a `SharedConnection` due to `SharedConnection` implementing  `ConnectionTransition`
- It is now possible to create `Prepared` statements owning a `Arc<Connection>` due to `Arc<Connection>` implementing  `ConnectionTransition`

### Other

- [**breaking**] Repalce `odbc_api::shared_connection_into_cursor` with `ConnectionTransition::into_cursor`
- [**breaking**] `Prepared::into_statement` is now named `into_handle`.
- `Connection::execute_arc` is replaced by `ConnectionTransition::into_cursor`
- [**breaking**] `ConnectionAndError` is now a type alias to `FailedStateTransition`
- [**breaking**] Removed `Connection::into_sys`

## [16.0.0](https://github.com/pacman82/odbc-api/compare/v15.0.0...v16.0.0) - 2025-08-16

### Added

- `shared_connection_into_cursor` allows cursors to share ownership
- Add type alias SyncConnection

### Other

- Simplify `execute_arc`
- [**breaking**] Rename `ConnectionOwner` to `StatementParent`.
- Improved documentation around Send for Connections

## [15.0.0](https://github.com/pacman82/odbc-api/compare/v14.3.0...v15.0.0) - 2025-08-14

### Added

- Cursors can now have shared ownership over `Connection`s using `execute_arc`.

### Fixed

- All of `Statement`s method now require `&mut self`

### Other

- execute_with_parameters now takes a statement instead of a lazy_statement
- [**breaking**] Introducing ConnectionOwner trait in order to decuple statement_connection from Connection
- [**breaking**] move StatementConnection into handles submodule
- [**breaking**] AsHandle is now named AnyHandle
- *(deps)* bump anyhow from 1.0.98 to 1.0.99
- *(deps)* bump thiserror from 2.0.12 to 2.0.14
- StatementConnection takes Connection as generic argument,
- formatting
- *(deps)* bump syn from 2.0.104 to 2.0.105
- *(deps)* bump proc-macro2 from 1.0.96 to 1.0.97
- *(deps)* bump proc-macro2 from 1.0.95 to 1.0.96

## [14.3.0](https://github.com/pacman82/odbc-api/compare/v14.2.1...v14.3.0) - 2025-08-10

### Added

- Check for option value changed during bulk fetch
- Add SqlResult::into_result_without_logging
- Introduce DiagnosticStream
- Introduce `SqlResult::has_diagnostics`

### Other

- fix mismatched lifetime elision syntax
- fix clippy lints
- [**breaking**] Replace `SqlResult::into_result_option` with
- [**breaking**] Replaced SqlResult::into_result_with with
- *(deps)* bump tokio from 1.47.0 to 1.47.1
- Revert version change
- reset version to 14.2.0 so release-plz, picks it up again

## [14.2.1](https://github.com/pacman82/odbc-api/compare/v14.2.0...v14.2.1) - 2025-07-31

### Other

- Render documentation of derive feature on docs.rs
- *(deps)* bump winit from 0.30.11 to 0.30.12
- *(deps)* bump tokio from 1.46.1 to 1.47.0
- *(deps)* bump criterion from 0.6.0 to 0.7.0
# Changelog

## [14.2.0](https://github.com/pacman82/odbc-api/compare/v14.1.0...v14.2.0) - 2025-07-23

### Added

- Fetch row_array_size from statement handle

### Other

- formatting (mostly)
- Mention Fetch derive macro in RowVec doc comment

## [14.1.0](https://github.com/pacman82/odbc-api/compare/v14.0.2...v14.1.0) - 2025-07-20

### Added

- Add resize for ColumnarBulkInserter
- AnyBuffer is resize
- ColumnWithIndicator is Resize
- implement Resize for Vec
- Implement resize for wide text column buffers
- Implement Resize for Text and Binary column buffers

### Fixed

- Typo in parameter module documentation

### Other

- *(deps)* bump tokio from 1.46.0 to 1.46.1
- *(deps)* bump tokio from 1.45.1 to 1.46.0

## [14.0.2](https://github.com/pacman82/odbc-api/compare/v14.0.1...v14.0.2) - 2025-06-29

### Other

- Link `InputParameter` documentation to parameter module level documentation.

## [14.0.1](https://github.com/pacman82/odbc-api/compare/v14.0.0...v14.0.1) - 2025-06-29

### Other

- Improved discoverability of supported input parameters
- All types which are CElement and HasDataType are now InputParameter

## [14.0.0](https://github.com/pacman82/odbc-api/compare/v13.1.0...v14.0.0) - 2025-06-26

### Added

- Support for saving memory through binding one input parameter two multiple placeholders, in cases there these placeholders are supposed to be replaced withe the same values. In order to achieve this the methods `Prepared::into_columnar_inserter_with_mapping` and `Prepared::column_inserter_with_mapping` hane been added.

### Other

- [**breaking**] `Prepared::unchecked_bind_columnar_array_parameters` now takes an explicit mapping
- [**breaking**] `ColumnarBulkInserter::new` now takes an explicit mapping

## [13.1.0](https://github.com/pacman82/odbc-api/compare/v13.0.1...v13.1.0) - 2025-06-25

### Added

- Add `NullableSlice::get` to access n-th element without iterating

### Other

- Mention that get can panic for TextColumnView::get
- *(deps)* bump env_logger from 0.11.6 to 0.11.8
- Explain why Connection is not `Sync`
- *(deps)* bump tokio from 1.45.0 to 1.45.1
- bulk fetch dates
- use non-deprecated black box from standard library
- *(deps)* bump syn from 2.0.103 to 2.0.104
- *(deps)* bump syn from 2.0.102 to 2.0.103
- *(deps)* bump syn from 2.0.101 to 2.0.102
- unit test expansion of derive Fetch macro

## [13.0.1](https://github.com/pacman82/odbc-api/compare/v13.0.0...v13.0.1) - 2025-05-21

### Other

- document how to fetch nullable values with get_data
- *(deps)* bump winit from 0.30.10 to 0.30.11
- *(deps)* bump tempfile from 3.19.1 to 3.20.0
- *(deps)* bump criterion from 0.5.1 to 0.6.0

## [13.0.0](https://github.com/pacman82/odbc-api/compare/v12.2.0...v13.0.0) - 2025-05-18

### Added

- [**breaking**] Move prompting for connection string to "prompt" feature ([#720](https://github.com/pacman82/odbc-api/pull/720))

### Other

- Explaining the `prompt` feature.
- learning test for concise type reported for time
- *(deps)* bump tokio from 1.44.2 to 1.45.0
- Add 2Byte Character in UTF16 to umlaut test
- umlaut in column name
- fix test Column Description for date
- Column Description for date

## [12.2.0](https://github.com/pacman82/odbc-api/compare/v12.1.0...v12.2.0) - 2025-05-01

### Added

- Adds `Nullability::could_be_nullable`

## [12.1.0](https://github.com/pacman82/odbc-api/compare/v12.0.2...v12.1.0) - 2025-05-01

### Added

- `ResultSetMetadata::col_nullability` can now be used to fetch information about the nullability of a result set column.

### Other

- Conscise data types for timestamps of all tested DBMs
- *(deps)* bump winit from 0.30.9 to 0.30.10
- learning test for concise type of Datetime2
- Update comments on DataType Other
- *(deps)* bump syn from 2.0.100 to 2.0.101

## [12.0.2](https://github.com/pacman82/odbc-api/compare/v12.0.1...v12.0.2) - 2025-04-27

### Fixed

- `DataType::utf8_len` and `DataType::utf16_len` now also account for special characters if `DataType` is `LongVarchar` or `WLongVarchar`
- `SqlDataType::EXT_W_LONG_VARCHAR` is now mapped to `DataType::WLongVarchar` rather than `DataType::Other`.

Together these fixes should enable downstream crates like `arrow-odbc` to allocate large enough buffers for fetching strings with special characters even large relational types like `VACHAR(1000)` are used with PostgreSQL.

### Other

- Fix test assertions for non-windows platforms
- Import transmute from mem rather than intrinsic namespace
- *(deps)* bump anyhow from 1.0.97 to 1.0.98
- Added MacPorts

## [12.0.1](https://github.com/pacman82/odbc-api/compare/v12.0.0...v12.0.1) - 2025-04-10

### Fixed

- `decimal_text_to_i*Bits*` Avoid panics in cases the scale in the text representation exceeds the passed scale

### Other

- Test formatting
- *(deps)* bump tokio from 1.44.1 to 1.44.2

## [12.0.0](https://github.com/pacman82/odbc-api/compare/v11.1.1...v12.0.0) - 2025-04-05

### Added

- Bulk inserting WChar now automatically uses 'DataType::WLongVarchar' for long strings
- [**breaking**] Using strings longer than 4000 characters as parameters with MSSQL.

### Fixed

- build error for narrow feature

### Other

- *(deps)* bump log from 0.4.26 to 0.4.27
- *(deps)* bump tempfile from 3.19.0 to 3.19.1
- *(deps)* bump widestring from 1.1.0 to 1.2.0
- *(deps)* bump tempfile from 3.18.0 to 3.19.0
- *(deps)* bump tokio from 1.44.0 to 1.44.1
- *(deps)* bump tokio from 1.43.0 to 1.44.0
- *(deps)* bump thiserror from 2.0.11 to 2.0.12
- *(deps)* bump tempfile from 3.17.1 to 3.18.0
- *(deps)* bump anyhow from 1.0.96 to 1.0.97
- lints doc comments
- [**breaking**] Update to edition 2024
- *(deps)* bump log from 0.4.25 to 0.4.26
- *(deps)* bump anyhow from 1.0.95 to 1.0.96
- *(deps)* bump quote from 1.0.39 to 1.0.40
- *(deps)* bump syn from 2.0.99 to 2.0.100
- *(deps)* bump syn from 2.0.98 to 2.0.99
- *(deps)* bump quote from 1.0.38 to 1.0.39

## [11.1.1](https://github.com/pacman82/odbc-api/compare/v11.1.0...v11.1.1) - 2025-02-19

### Fixed

- Return type of decimal_text_to_i32 is now indeed i32 and no longer i64

## [11.1.0](https://github.com/pacman82/odbc-api/compare/v11.0.0...v11.1.0) - 2025-02-19

### Added

- Add conversion functions decimal_text_to_i64, decimal_text_to_i32

### Other

- *(deps)* bump tempfile from 3.17.0 to 3.17.1
- *(deps)* bump tempfile from 3.16.0 to 3.17.0

## [11.0.0](https://github.com/pacman82/odbc-api/compare/v10.2.0...v11.0.0) - 2025-02-16

### Added

- [**breaking**] Query timeout for into_cursor
- [**breaking**] Introduce query timeout to Connection::execute
- Add Connection::into_preallocated

### Other

- Add missing timeout parameter to failing tests
- [**breaking**] PreallocatedPolling is generic over handle type
- All but into_polling of Preallocateds implementation is now generic over statment handle
- [**breaking**] Preallocated is now dependend on statement handle type, rather than lifetime
- More usecase centric documentation for set_query_timeout_sec

## [10.2.0](https://github.com/pacman82/odbc-api/compare/v10.1.1...v10.2.0) - 2025-02-15

### Added

- Support for query timeouts. `Preallocated::set_query_timeout_sec` and `Prepared::set_query_timeout_sec` can be used to trigger a timeout error, if the first data from that statement exceeds the time limit. This requires driver support.

### Other

- query timeout
- *(deps)* bump winit from 0.30.8 to 0.30.9
- *(deps)* bump tempfile from 3.15.0 to 3.16.0
- *(deps)* bump log from 0.4.22 to 0.4.25
- *(deps)* bump thiserror from 2.0.10 to 2.0.11
- *(deps)* bump tokio from 1.42.0 to 1.43.0
- *(deps)* bump thiserror from 2.0.9 to 2.0.10
- *(deps)* bump winit from 0.30.7 to 0.30.8
- *(deps)* bump tempfile from 3.14.0 to 3.15.0
- *(deps)* bump syn from 2.0.96 to 2.0.98
- *(deps)* bump syn from 2.0.95 to 2.0.96
- *(deps)* bump syn from 2.0.94 to 2.0.95

## [10.1.1](https://github.com/pacman82/odbc-api/compare/v10.1.0...v10.1.1) - 2025-01-03

### Other

- Improve panic message on invalid indicator values

## [10.1.0](https://github.com/pacman82/odbc-api/compare/v10.0.0...v10.1.0) - 2025-01-02

### Added

- autodetect homebrew library path during build

### Other

- *(deps)* bump thiserror from 2.0.8 to 2.0.9
- *(deps)* bump anyhow from 1.0.94 to 1.0.95
- *(deps)* bump winit from 0.30.5 to 0.30.7
- *(deps)* bump env_logger from 0.11.5 to 0.11.6
- *(deps)* bump thiserror from 2.0.7 to 2.0.8
- *(deps)* bump thiserror from 2.0.6 to 2.0.7
- *(deps)* bump thiserror from 2.0.4 to 2.0.6
- *(deps)* bump thiserror from 2.0.3 to 2.0.4
- *(deps)* bump anyhow from 1.0.93 to 1.0.94
- *(deps)* bump tokio from 1.41.1 to 1.42.0
- *(deps)* bump syn from 2.0.93 to 2.0.94
- *(deps)* bump syn from 2.0.92 to 2.0.93
- *(deps)* bump syn from 2.0.91 to 2.0.92
- *(deps)* bump quote from 1.0.37 to 1.0.38
- *(deps)* bump syn from 2.0.90 to 2.0.91
- *(deps)* bump syn from 2.0.89 to 2.0.90

## [10.0.0](https://github.com/pacman82/odbc-api/compare/v9.0.0...v10.0.0) - 2024-11-24

### Added

- [**breaking**] `odbc-api` will now use narrow function calls by default on non-windows systems. This assumes that the ODBC driver on that platform uses UTF-8 encoding. This is usually the case as many Linux systems use an UTF-8 locale. Outside of windows the wide function calls are usually less battle tested on drivers. Downstream artefacts like `arrow-odbc` and `odbc2parquet` therefore have been compiling with the `narrow` flag on non-windows systems for quite a while now to opt into the behavior which most likely works by default. However if somebody had a non-windows platform with a non-utf 8 local and a driver which actually could use UTF-16, he could not set appropriate compiler flags to revert the `narrow` feature added by these crates. Since the default behavior for each platform is now triggered by `odbc-api` itself, downstream artefacts can overwrite it both ways. E.g. using the `narrow` flag on windows to get some speed, if they know their target platform has a UTF-8 local configured. Or the other way around using the `wide` flag on a Linux system to e.g. handle special character in column names, which only seem to work with the wide variants of the drivers. So if you used `odbc-api` previously on Linux without any flags it did by default use the `wide` function calls. It now uses the `narrow` ones by default. If you want the old behavior just specify the `wide` feature flag. For windows users nothing breaks.

### Other

- *(deps)* bump thiserror from 2.0.0 to 2.0.3
- *(deps)* bump csv from 1.3.0 to 1.3.1
- *(deps)* bump tempfile from 3.13.0 to 3.14.0
- *(deps)* bump tokio from 1.41.0 to 1.41.1
- *(deps)* bump thiserror from 1.0.68 to 2.0.0
- *(deps)* bump anyhow from 1.0.92 to 1.0.93
- *(deps)* bump thiserror from 1.0.66 to 1.0.68
- *(deps)* bump thiserror from 1.0.65 to 1.0.66
- *(deps)* bump anyhow from 1.0.91 to 1.0.92
- *(deps)* bump syn from 2.0.87 to 2.0.89
- *(deps)* bump syn from 2.0.86 to 2.0.87
- *(deps)* bump syn from 2.0.85 to 2.0.86

## 9.0.0

* `ConcurrentBlockCursor` is now generic over the buffer type

## 8.1.4

* Fix failed release 8.1.3 due to out of sync version for derive package

## 8.1.3

* Fix: Failed attempts to rollback transactions during `Drop` of a connection will no longer panic.

## 8.1.2

* Fix: `Connection::execute_polling` did not enable asynchronus execution on the statement handle. This means that the operations on that cursor were actually blocking even if the driver does support polling.

## 8.1.1

* Fix release. Release from main branch

## 8.1.0

* `ConnectionAndError` now implements `std::error::Error`.

## 8.0.1-2

* Fix: Fix version of `odbc-api-derive` crate

## 8.0.0

* `Connection::into_cursor` now allows you to recover the `Connection` in case of failure. For this the return type has been changed from `Error` to `ConnectionAndError`. `Error` implements `From<ConnectionAndError>`, so converting it to Error via `?` is frictionless.

## 7.2.4

* Improved documentation

## 7.2.3

* Update dependencies

## 7.2.2

* Fix: Add metadata for `odbc-api-derive` crate

## 7.2.1

* Fix: Release pipeline for derive feature

## 7.2.0

* Adds derive feature for deriving custom implementations of `FetchRow`.

## 7.1.0

* Adds support for row wise bulk fetching via `RowVec`.

## 7.0.0

* Removed `Quirks`. Detection of driver problems via dbms name is not relyable enough.
* Support setting packet size, via `ConnectionOptions::packet_size`.

## 6.0.2

* Fixes an issue with `decimal_text_to_i128`, which caused a negative value to be treated as positive, if it had been below a magnitude of one. Thanks to @timkpaine for reporting and fixing the issue.

## 6.0.1

* Fixes an issue with `Environment::drivers`, which caused it to have at most one driver attribute in the `DriverInfo` attributes.

## 6.0.0

* `ConcurrentBlockCursor::from_block_cursor` is now infalliable.
* `Quirks::indicators_returned_from_bulk_fetch_are_memory_garbage` will be now set for every database name which starts with `DB2`.

## 5.1.0

* Adds `ConcurrentBlockCursor` to ease fetching large result sets in a dedicated system thread.

## 5.0.0

* removed reexport of `force_send_sync`. It is no longer used within `odbc-api` this should have been removed together with the methods which were promoting Connections to `Send`, but has been overlooked.
* Adds `decimal_text_to_i128` a useful function for downstream applications including `odbc2parquet` and `arrow-odbc`.

## 4.1.0

* Introducing `Quirks` to document knoweldege about behaviour from drivers which is different from the ODBC specification. This can be helpful for generic applications in order to support databases if they want to invest in workarounds for bugs or percularities of ODBC drivers.

## 4.0.1

* Additional `debug` log messages around allocating and deallocating handles.

## 4.0.0

* `ResultSetMetadata::col_display_size` now returns `Option<NonZeroUsize>` instead of `isize`.
* `ResultSetMetadata::col_octet_length` now returns `Option<NonZeroUsize>`, instead of `isize`.
* `ResultSetMetadata::utf8_display_sizes` now iterates over `Option<NonZeroUsize>`, instead of `usize`.
* `DataType` now stores the length of variadic types as `Option<NonZeroUsize`> insteaf of `usize`.
* `DataType::display_size` now returns `Option<NonZeroUSize>` instead of `Option<usize>`.
* `DataType::utf8_len` now returns `Option<NonZeroUSize>` instead of `Option<usize>`.
* `DataType::utf16_len` now returns `Option<NonZeroUSize>` instead of `Option<usize>`
* `DataType::column_size` now returns `Option<NonZeroUSize>` instead of `usize`
* `BufferDesc::from_data_type` now returns `None` for variadic types without upper bound instead of a zero sized buffer.
* `Indicator::value_len` now is named `Indicator::length`.
* Provide buffer index for truncation errors

## 3.0.1

* Setting only `odbc_version_3_5` without deactivating default features now compiles again. It is still recommended though to use the `odbc_version_3_5` flag only in combination with `--no-default-features` since `odbc_version_3_8` is a default feature.

## 3.0.0

* `ParameterDescription::nullable` has been renamed to `ParameterDescription::nullability`
* Remove deprecated function `Connection::promote_to_send`.
* `ErrorUnsupportedOdbcVersion` is now returned if driver manager emits a diagnostic record using with status `S1009` then setting ODBC version. That status code used to mean "Invalid Attribute" back in the days of ODBC 2.x.

## 2.2.0

* Adds `BlockCursor::row_array_size`, in order to infer maximum batch size without unbinding the row set buffer first. This allows downstream `arrow-odbc` for a faster implementation then creating a double buffered concurrent stream of batches directly from a block cursor.

## 2.1.0

It is now better possible to use `odbc-api` with multithreading in completly safe code.

* `Connection` is now `Send`.
* `StatementConnection` is now `Send`.

According to the standard this is guaranteed to be safe. By now this has also worked in practice long enough and with enough different drivers, that this can be considered safe, not only by the ODBC standard, but also in practice.

## 2.0.0

* Variant `Error::TooLargeValueForBuffer` now has a member `indicator`, indicating the size of the complete value in the DBMS. This has been introduced to help users decide how to pick a maximum string size in bulk fetches, in the presence of large variadic columns.

## 1.0.2

* Fix: `Cursor::fetch_with_truncation_check` did erroneously report truncation errors for `NULL` fields, if the query also triggered any ODBC diagnostic. This has been fixed in the previous release for variadic binary column buffers but a similar failure had been missed for variadic text columns.

## 1.0.1

* Fix: `Cursor::fetch_with_truncation_check` did erroneously report truncation errors for `NULL` fields, if the query also triggered any ODBC diagnostic. This remained undedected for a while, because in every situation that there is a truncation, there must also be a diagnostic according to the ODBC standard. However the reverse is not true. In this situation a `NULL` had been erronously treated the same as a `NO_TOTAL` indicator and a false positive has been reported to a user claiming that there is a truncation, there in fact is none.

## 1.0.0

* Introduced `CursorRow::get_wide_text` in order to fetch large UTF-16 values into a buffer.
* The `IntoParameter` implementation for `str` slices and `Strings` will now be converting them to `VarWCharBox` if the `narrow` compile time feature is **not** activated. This has been done in order to allow for easier writing of portable code between Windows and non-Windows platforms. Usually you would like to activate the `narrow` feature on non-windows platform there a UTF-8 default locale can be assumed. On Windows you want utilize UTF-16 encoding since it is the only reliable way to to transfer non ASCII characters independent from the systems locale. With this change on both platforms one can now simply write:

```rust
// Uses UTF-16 if `narrow` is not set. Otherwise directly binds the UTF-8 silce.
conn.execute(&insert_sql, &"Frühstück".into_parameter()).unwrap();
```

```rust
// Guaranteed to use UTF-16 independent of compiliation flags.
let arg = U16String::from_str("Frühstück");
conn.execute(&insert_sql, &arg.into_parameter()).unwrap();
```

```rust
// Guaranteed to use UTF-8 independent of compiliation flags. This relies on the ODBC driver to use
// UTF-8 encoding.
let arg = Narrow("Frühstück");
conn.execute(&insert_sql, &arg.into_parameter()).unwrap();
```

## 0.57.1

* Corrected typos in documentation. Thanks to @zachbateman

## 0.57.0

* Support for `U16String` and `Option<U16Str>` as input parameter via `IntoParameter` trait.
* Added `VarWCharSlice`, `VarWCharBox`, `VarWCharSliceMut`, `VarWCharArray`.

## 0.56.2

* Support `U16Str` and `Option<U16Str>` as input parameter via `IntoParameter` trait.

## 0.56.1

* Fix: `Statement::complete_async` is now annotated to be only available in ODBC version 3.8. This missing annotation prevented compilation then specifying ODBC version 3.5.

## 0.56.0

* `CursorRow::get_variadic` is now private as it was always intended to be a private implemenatation detail.
* `Statement::set_num_rows_fetched` has been split into `Statement::set_num_rows_fetched` and `Statement::unset_num_rows_fetched` in order to reduce the amount of unsafe code a bit.
* Adds `Cursor::more_results` to support statements which return multiple result sets.

## 0.55.0

* Added support for login timeout, i.e. the user can now explicitly specify a time in seconds after which a login attempt to a datasource should fail. This triggered two breaking changes:

* `Environment::connect_with_connection_string` now takes an additional argument `ConnectionOptions`.
* `Environment::connect` now takes an additional argument `ConnectionOptions`.

Migration is trivial, by supplying `ConnectionsOptions::default()` as the last argument.

## 0.54.3

* Added catalog function `foreign_keys` to `Connection` and `Preallocated`.

## 0.54.2

* `CursorRow::get_text` or `Cursor::get_binary` now panics if the `SQLGetData` implementation of the ODBC driver reports inconsistent indicator for the same cell. Previously it could have looped forever.

## 0.54.1

* Introduced common implementation `VarCell` for `VarBinary` and `VarChar`.

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
* Fix: An out of bounds panic with columnar inserts from text buffer, if the last value of the buffer had maximum length in a buffer with the maximum number of rows filled (yes, off by one error).

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
