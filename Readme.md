# ODBC-API

ODBC (Open Database Connectivity) bindings for Rust.

[![Docs](https://docs.rs/odbc-api/badge.svg)](https://docs.rs/odbc-api/)
[![MIT licensed](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/pacman82/odbc-api/blob/master/LICENSE)
[![Published](http://meritbadge.herokuapp.com/odbc-api)](https://crates.io/crates/odbc-api)

## Usage

Check the [guide](https://docs.rs/odbc-api/latest/odbc_api/guide/index.html) for code examples and a tour of the features.

## Features

- [x] Connect using either Data Source names (DSN) or connection strings
- [x] Provide nice erros and log diagnostic output.
- [x] Support for columnwise bulk inserts.
- [x] Support for columnwise bulk queries.
- [ ] Support for rowise bulk inserts.
- [ ] Support for rowise bulk queries.
- [ ] Provide general purpose implementations of buffers to bind, which can be used in safe code without caveats.
- [x] Support prepared and 'one shot' queries.
- [x] Pass parameters to queries
- [ ] Support for async
- [ ] Support for Multithreading

## Motivation

Supports writing ODBC Application in Rust. Prior work in this area has been done:

* `odbc-sys`: Bare bindings to the ODBC C Library. Declares C-Function signatures and handles linking. All other libraries mentioned are based on this one.
* `odbc-safe`: (Mostly) safe wrapper around `odbc-sys`. Apart from protecting against unsafe behaviour, this library also tries to prevent invalid use of the ODBC API by modelling state transitions.
* `odbc-rs`: Higher level and more idiomatic bindings. Wraps `odbc-safe`.

So why `odbc-api`? This is a somewhat less ambitious and more opinionated rewrite of `odbc-safe`. `odbc-safe` aimed to model all the possible state transitions offered by `odbc`. This turned out to be both, incredibly challenging to implement and maintain, and cumbersome to use. ODBCs way of sharing mutable buffers is very much at odds with Rusts type system, introducing significant complexity in an unopinionated general purpose wrapper. This library leaves the low level odbc abstractions as unsafe as they are, but aims to provide idiomatic interfaces for the usual high level use cases.

## Design decisions

Here are some of the tradeoffs I made in this library to make my life (and hopefully that of the users a bit easier).

### Commiting to one version of the ODBC Api

The underlying `odbc-sys` crate does not limit you to a specific ODBC version. This crate however uses ODBC version `3.8` fixed.

### Use of the Wide ODBC methods returning UTF-16

Idiomatic Rust uses UTF-8. Sadly the narrow C-Methods in ODBC do not always imply the use of UTF-8, which can cause some mess with encodings (think about a Windows system with a Japanese local). Although the use of the wide methods may require the additional encoding, decoding and allocation of strings it works consistently on all platforms and configuration. Also application performance does rarely depend on how fast we process the connection string. Correct handling of the buffers for the payload is far more important. That being said. This library is not a zero cost abstraction. In some setups you may very well be able to assume that you can directly use UTF-8 and this API does introduce some overhead on such systems.

### Warnings are logged

ODBC calls produce a Result. If successful they may also produce any number of warnings. These are logged away using the `log` crate with the `warn` severity. Rust type systems would have been powerful enough to express the presence of warnings in the Return type, yet I did not want to loose out on simply using `?`-operator for error propagation. Also I never wanted to do something with these warnings besides logging them. Some overhead is introduced, as the messages for these warnings are allocated, even if you do not register a backend for the `log` crate at all.

