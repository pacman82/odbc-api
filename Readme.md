# ODBC-API

ODBC (Open Database Connectivity) bindings for Rust.

## Usage

Check the [guide](https://docs.rs/odbc-api/latest/odbc_api/guide/index.html) for programming examples.

## Motivation

Supports writing ODBC Application in Rust. Prior work in this area has been done:

* `odbc-sys`: Bare bindings to the ODBC C Library. Declares C-Function signatures and handles linking. All other libraries mentioned are based on this one.
* `odbc-safe`: (Mostly) safe wrapper around `odbc-sys`. Apart from protecting against unsafe behaviour, this library also tries to prevent invalid use of the ODBC API by modelling state transitions.
* `odbc-rs`: Higher level and more idiomatic bindings. Wraps `odbc-safe`.

So why `odbc-api`? This is a somewhat less ambitious and more opinionated rewrite of `odbc-safe`. I never achieved a somewhat feature complete state with `odbc-safe`. This is in part due to the fact that I had significantly less time for Open Source, but also due to the fact that ODBCs way of borrowing and sharing buffers is very much at odds with Rusts type system, introducing significant complexity in an unopinionated general purpose wrapper. Also, there are a lot of possible sate transitions. So for many use cases I found myself going back to the bare `odbc-sys`. On the other hand many reoccurring high level scenarios could have quite nice safe abstractions, if you are a bit opinionated about the design.

## Local test setup

Running local tests currently requires:

* docker and docker compose.
* [Microsoft ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15).

With docker and the SQL Driver installed run

```shell
docker-compose up
```

in order to start the database. After the database is setup and ready to go run

```
cargo test
```

to run all tests in the workspace, which should now succeed.


## Design decisions

Here are some of the tradeoffs I made in this library to make my life (and hopefully that of the users a bit easier).

### Use of the Wide ODBC methods returning UTF-16

Idiomatic Rust uses UTF-8. Sadly the narrow C-Methods in ODBC do not always imply the use of UTF-8, which can cause some mess with encodings (think about a Windows system with a Japanese local). Although the use of the wide methods may require the additional encoding, decoding and allocation of strings it works consistently on all platforms and configuration. Also application performance does rarely depend on how fast we process the connection string. Correct handling of the buffers for the payload is far more important. That being said. This library is not a zero cost abstraction. In some setups you may very well be able to assume that you can directly use UTF-8 and this API does introduce some overhead on such systems.

### Warnings are logged

ODBC calls produce a Result. If successful they may also produce any number of warnings. These are logged away using the `log` crate with the `warn` severity. Rust type systems would have been powerful enough to express the presence of warnings in the Return type, yet I did not want to loose out on simply using `?`-operator for error propagation. Also I never wanted to do something with these warnings besides logging them. Some overhead is introduced, as the messages for these warnings are allocated, even if you do not register a backend for the `log` crate at all.

## State

Currently this library enables you to efficiently query data and retrieve the results in blocks rather than line row by row, making efficient use of how odbc manages and bind buffers. This is as far as I know the only ODBC rust wrapper which does this.
