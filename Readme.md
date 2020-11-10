# ODBC-API

ODBC (Open Database Connectivity) bindings for Rust.

## Usage

```rust
use anyhow::Error;
use odbc_api::{buffers::TextRowSet, Cursor, Environment, IntoParameter};
use std::{
    io::{stdout, Write},
    path::PathBuf,
};
/// Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
/// faster than fetching individual rows.
const BATCH_SIZE: u32 = 100000;

fn main() -> Result<(), Error> {
    // Write csv to standard out
    let out = stdout();
    let mut writer = csv::Writer::from_writer(out);
    // We know this is going to be the only ODBC environment in the entire process, so this is
    // safe.
    let environment = unsafe { Environment::new() }?;
    // Connect using a DSN. Alternativly we could have used a connection string
    let mut connection = environment.connect(
        "DataSourceName",
        "Username",
        "Password",
    )?;
    // Execute a one of query without any parameters.
    match connection.execute("SELECT * FROM TableName", ())? {
        Some(cursor) => {
            // Write the column names to stdout
            let mut headline : Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
            writer.write_record(headline)?;

            let mut buffers = TextRowSet::new(BATCH_SIZE, &cursor)?;
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
            while let Some(batch) = row_set_cursor.fetch()? {
                for row_index in 0..batch.num_rows() {
                    let record = (0..batch.num_cols())
                        .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]));
                    writer.write_record(record)?;
                }
            }
        }
        None => {
            eprintln!(
                "Query came back empty. No output has been created."
            );
        }
    }
    Ok(())
}
```

Check the [guide](https://docs.rs/odbc-api/latest/odbc_api/guide/index.html) for more examples and a tour of the features.

## Features

- [x] Connect using either Data Source names (DSN) or connection strings
- [x] Provide nice erros and log diagnostic output.
- [x] Efficiently query data and retrieve the results in blocks rather than line row by row, making efficient use of how odbc manages and bind buffers. This is as far as I know the only ODBC rust wrapper which does this.
- [ ] Provide general purpose implementations of buffers to bind, which can be used in safe code without caveats.
- [x] Support prepared and 'one shot' queries.
- [x] Pass parameters to queries
- [ ] Support for async
- [ ] Support for Multithreading

## Local test setup

Running local tests currently requires:

* Docker and Docker compose.

With docker and the SQL Driver installed run:

```shell
docker-compose up
```

This starts two containers called `odbc-api_dev` and `odbc-api_mssql`. You can use the `dev` container to build your code and execute tests in case you do not want to install the required ODBC drivers and/or Rust toolchain on your local machine.

Otherwise you can install these requirements from here:

* [Microsoft ODBC Driver 17 for SQL Server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver15).
* <https://rustup.rs/>

The `mssql` container runs a Microsoft SQL Server used for answering the test queries. We can execute the tests in Rust typical fashion using:

```
cargo test
```

to run all tests in the workspace, which should now succeed.

### Visual Studio Code

Should you use Visual Studio Code with the Remote Developmont extension, it will pick up the `.devcontainer` configuration and everything should be setup for you.

## Motivation

Supports writing ODBC Application in Rust. Prior work in this area has been done:

* `odbc-sys`: Bare bindings to the ODBC C Library. Declares C-Function signatures and handles linking. All other libraries mentioned are based on this one.
* `odbc-safe`: (Mostly) safe wrapper around `odbc-sys`. Apart from protecting against unsafe behaviour, this library also tries to prevent invalid use of the ODBC API by modelling state transitions.
* `odbc-rs`: Higher level and more idiomatic bindings. Wraps `odbc-safe`.

So why `odbc-api`? This is a somewhat less ambitious and more opinionated rewrite of `odbc-safe`. `odbc-safe` aimed to model all the possible state transitions offered by `odbc`. This turned out to be both, incredibly challenging to implement and maintain, and cumbersome to use. ODBCs way of sharing mutable buffers is very much at odds with Rusts type system, introducing significant complexity in an unopinionated general purpose wrapper. This library leaves the low level odbc abstractions as unsafe as they are, but aims to provide idiomatic interfaces for the usual high level use cases.

## Design decisions

Here are some of the tradeoffs I made in this library to make my life (and hopefully that of the users a bit easier).

### Use of the Wide ODBC methods returning UTF-16

Idiomatic Rust uses UTF-8. Sadly the narrow C-Methods in ODBC do not always imply the use of UTF-8, which can cause some mess with encodings (think about a Windows system with a Japanese local). Although the use of the wide methods may require the additional encoding, decoding and allocation of strings it works consistently on all platforms and configuration. Also application performance does rarely depend on how fast we process the connection string. Correct handling of the buffers for the payload is far more important. That being said. This library is not a zero cost abstraction. In some setups you may very well be able to assume that you can directly use UTF-8 and this API does introduce some overhead on such systems.

### Warnings are logged

ODBC calls produce a Result. If successful they may also produce any number of warnings. These are logged away using the `log` crate with the `warn` severity. Rust type systems would have been powerful enough to express the presence of warnings in the Return type, yet I did not want to loose out on simply using `?`-operator for error propagation. Also I never wanted to do something with these warnings besides logging them. Some overhead is introduced, as the messages for these warnings are allocated, even if you do not register a backend for the `log` crate at all.

