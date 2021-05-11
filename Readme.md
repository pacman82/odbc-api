# ODBC-API

[![Docs](https://docs.rs/odbc-api/badge.svg)](https://docs.rs/odbc-api/)
[![MIT licensed](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/pacman82/odbc-api/blob/master/LICENSE)
[![Published](http://meritbadge.herokuapp.com/odbc-api)](https://crates.io/crates/odbc-api)
[![Coverage Status](https://coveralls.io/repos/github/pacman82/odbc-api/badge.svg?branch=master)](https://coveralls.io/github/pacman82/odbc-api?branch=master)

Rust ODBC bindings. ODBC (Open Database Connectivity) is an open standard to connect to a variaty of data sources. Most data sources offer ODBC drivers. This crate is currently tested against:

* Microsoft SQL Server
* MariaDB
* SQLite

Current ODBC Version is `3.80`.

This crate is build on top of the `odbc-sys` ffi bindings, which provide definitions of the ODBC C Interface, but do not build any kind of abstraction on top of it.

## Usage

To build this library you need to link against the `odbc` library of your systems ODBC driver manager. It should be automatically detected by the build. On Windows systems it is preinstalled. On Linux and OS-X [unix-odbc](http://www.unixodbc.org/) must be installed. To create a Connections to a data source, its ODBC drivers must also be installed.

Check the [guide](https://docs.rs/odbc-api/latest/odbc_api/guide/index.html) for code examples and a tour of the features.

## Features

* [x] Connect using either Data Source names (DSN) or connection strings
* [x] Support for logging ODBC diagnostics and warnings (via `log` crate).
* [x] Provide nice errors and log diagnostic output.
* [x] Support for columnar bulk inserts.
* [x] Support for columnar bulk queries.
* [ ] Support for rowise bulk inserts.
* [ ] Support for rowise bulk queries.
* [x] Support for Output parameters of stored procedures.
* [x] Support prepared and 'one shot' queries.
* [x] Transactions
* [x] Pass parameters to queries
* [ ] Support for async
* [x] Support for Multithreading
* [ ] Support for inserting arbitrary large binary / text data in stream
* [x] Support for fetching arbitrary large text / binary data in stream
* [ ] Support for driver aware connection pooling
