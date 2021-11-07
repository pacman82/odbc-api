/*!
# Introduction to `odbc-api` (documentation only)

## About ODBC

ODBC is an open standard which allows you to connect to various data sources. Mostly these data
sources are databases, but ODBC drivers are also available for various file types like Excel or
CSV.

Your application does not link against a driver, but will link against an ODBC driver manager
which must be installed on the system you intend to run the application. On modern Windows
Platforms ODBC is always installed, on OS-X or Linux distributions a driver manager like
[unixODBC](http://www.unixodbc.org/) must be installed.

To connect to a data source a driver for the specific data source in question must be installed.
On windows you can type 'ODBC Data Sources' into the search box to start a little GUI which
shows you the various drivers and preconfigured data sources on your system.

This however is not a guide on how to configure and setup ODBC. This is a guide on how to use
the Rust bindings for applications which want to utilize ODBC data sources.

## Quickstart

```no_run
//! A program executing a query and printing the result as csv to standard out. Requires
//! `anyhow` and `csv` crate.

use anyhow::Error;
use odbc_api::{buffers::TextRowSet, Cursor, Environment, ResultSetMetadata};
use std::{
    ffi::CStr,
    io::{stdout, Write},
    path::PathBuf,
};

/// Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
/// faster than fetching individual rows.
const BATCH_SIZE: usize = 5000;

fn main() -> Result<(), Error> {
    // Write csv to standard out
    let out = stdout();
    let mut writer = csv::Writer::from_writer(out);

    // If you do not do anything fancy it is recommended to have only one Environment in the
    // entire process.
    let environment = Environment::new()?;

    // Connect using a DSN. Alternatively we could have used a connection string
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

            // Use schema in cursor to initialize a text buffer large enough to hold the largest
            // possible strings for each column up to an upper limit of 4KiB.
            let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &cursor, Some(4096))?;
            // Bind the buffer to the cursor. It is now being filled with every call to fetch.
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            // Iterate over batches
            while let Some(batch) = row_set_cursor.fetch()? {
                // Within a batch, iterate over every row
                for row_index in 0..batch.num_rows() {
                    // Within a row iterate over every column
                    let record = (0..batch.num_cols()).map(|col_index| {
                        batch
                            .at(col_index, row_index)
                            .unwrap_or(&[])
                    });
                    // Writes row as csv
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

## 32 Bit and 64 Bit considerations.

To consider whether you want to work with 32 Bit or 64 Bit data sources is especially important
for windows users, as driver managers (and possibly drivers) may both exist at the same time
in the same system.

In any case, depending on the platform part of your target triple either 32 Bit or 64 Bit
drivers are going to work, but not both. On a private windows machine (even on a modern 64 Bit
Windows) it is not unusual to find lots of 32 Bit drivers installed on the system, but none for
64 Bits. So for windows users it is worth thinking about not using the default toolchain which
is likely 64 Bits and to switch to a 32 Bit one. On other platforms you are usually fine
sticking with 64 Bits, as there are not usually any drivers preinstalled anyway, 64 Bit or
otherwise.

No code changes are required, so you can also just build both if you want to.

## Connecting to a data source

### Setting up the ODBC Environment

To connect with a data source we need a connection. To create a connection we need an ODBC
environment.

```no_run
use odbc_api::Environment;

let env = Environment::new()?;

# Ok::<(), odbc_api::Error>(())
```

This is it. Our ODBC Environment is ready for action. We can use it to list data sources and
drivers, but most importantly we can use it to create connections.

These bindings currently support two ways of creating a connections:

### Connect using a connection string

Connection strings do not require that the data source is preconfigured by the driver manager
this makes them very flexible.

```no_run
use odbc_api::Environment;

let env = Environment::new()?;

let connection_string = "
    Driver={ODBC Driver 17 for SQL Server};\
    Server=localhost;\
    UID=SA;\
    PWD=<YourStrong@Passw0rd>;\
";

let mut conn = env.connect_with_connection_string(connection_string)?;
# Ok::<(), odbc_api::Error>(())
```

There is a syntax to these connection strings, but few people go through the trouble to learn
it. Most common strategy is to google one that works for with your data source. The connection
borrows the environment, so you will get a compiler error, if your environment goes out of scope
before the connection does.

> You can list the available drivers using [`crate::Environment::drivers`].

### Connect using a Data Source Name (DSN)

Should a data source be known by the driver manager we can access it using its name and
credentials. This is more convenient for the user or application developer, but requires a
configuration of the ODBC driver manager. Think of it as shifting work from users to
administrators.

```no_run
use odbc_api::Environment;

let env = Environment::new()?;

let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
# Ok::<(), odbc_api::Error>(())
```

How to configure such data sources is not the scope of this guide, and depends on the driver
manager in question.

> You can list the available data sources using [`crate::Environment::data_sources`].

### Connection pooling

ODBC specifies an interface to enable the driver manager to enable connection pooling for your
application. It is of by default, but if you use ODBC to connect to your data source instead of
implementing it in your application, or importing a library you may simply enable it in ODBC
instead.
Connection Pooling is governed by two attributes. The most important one is the connection
pooling scheme which is `Off` by default. It must be set even before you create your ODBC
environment. It is global mutable state on the process level. Setting it in Rust is therefore
unsafe.

The other one is changed via [`crate::Environment::set_connection_pooling_matching`]. It governs
how a connection is chosen from the pool. It defaults to strict which means the `Connection`
you get from the pool will have exactly the attributes specified in the connection string.

Here is an example of how to create an ODBC environment with connection pooling.

```
use lazy_static::lazy_static;
use odbc_api::{Environment, sys::{AttrConnectionPooling, AttrCpMatch}};

lazy_static! {
    pub static ref ENV: Environment = {
        // Enable connection pooling. Let driver decide whether the attributes of two connection
        // are similar enough to change the attributes of a pooled one, to fit the requested
        // connection, or if it is cheaper to create a new Connection from scratch.
        // See <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/driver-aware-connection-pooling>
        //
        // Safety: This call changes global mutable space in the underlying ODBC driver manager.
        unsafe {
            Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
        }
        let mut env = Environment::new().unwrap();
        // Strict is the default, and is set here to be explicit about it.
        env.set_connection_pooling_matching(AttrCpMatch::Strict).unwrap();
        env
    };
}
```

## Executing SQL statements

### Executing a single SQL statement

With our ODBC connection all set up and ready to go, we can execute an SQL query:

```no_run
use odbc_api::Environment;

let env = Environment::new()?;

let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
    // Use cursor to process query results.
}
# Ok::<(), odbc_api::Error>(())
```

The first parameter of `execute` is the SQL statement text. The second parameter is used to pass
arguments of the SQL Statements itself (more on that later). Ours has none, so we use `()` to
not bind any arguments to the statement. You can learn all about passing parameters from the
[`parameter module level documentation`](`crate::parameter`). It may feature an example for
your use case.

Note that the result of the operation is an `Option`. This reflects that not every statement
returns a [`Cursor`](crate::Cursor). `INSERT` statements usually do not, but even `SELECT`
queries which would return zero rows can depending on the driver return either an empty cursor
or no cursor at all. Should a cursor exists, it must be consumed or closed. The `drop` handler
of Cursor will close it for us. If the `Option` is `None` there is nothing to close, so is all
taken care of, nice.

### Executing a many SQL statements in sequence

Execution of a statement, its bound parameters, its buffers and result set, are all managed by
ODBC using a statement handle. If you call [`crate::Connection::execute`] a new one is allocated
each time and the resulting cursor takes ownership of it (giving you an easier time with the
borrow checker). In a use case there you want to execute multiple SQL Statements over the same
connection in sequence, you may want to reuse an already allocated statement handle. This is
what the [`crate::Preallocated`] struct is for. Please note that if you want to execute the same
query with different parameters you can have potentially even better performance by utilizing
prepared Statements ([`crate::Prepared`]).

```
use odbc_api::{Connection, Error};
use std::io::{self, stdin, Read};

fn interactive(conn: &Connection) -> io::Result<()>{
    let mut statement = conn.preallocate().unwrap();
    let mut query = String::new();
    stdin().read_line(&mut query)?;
    while !query.is_empty() {
        match statement.execute(&query, ()) {
            Err(e) => println!("{}", e),
            Ok(None) => println!("No results set generated."),
            Ok(Some(cursor)) => {
                // ...print cursor contents...
            }
        }
        stdin().read_line(&mut query)?;
    }
    Ok(())
}
```

### Executing prepared queries

Should your use case require you to execute the same query several times with different
parameters, prepared queries are the way to go. These give the ODBC driver a chance to store
cache the access plan associated with your SQL statement. It is not unlike compiling your
program once and executing it several times.

```
use odbc_api::{Connection, Error, IntoParameter};
use std::io::{self, stdin, Read};

fn interactive(conn: &Connection) -> io::Result<()>{
    let mut prepared = conn.prepare("SELECT * FROM Movies WHERE title=?;").unwrap();
    let mut title = String::new();
    stdin().read_line(&mut title)?;
    while !title.is_empty() {
        match prepared.execute(&title.as_str().into_parameter()) {
            Err(e) => println!("{}", e),
            // Most drivers would return a result set even if no Movie with the title is found,
            // the result set would just be empty. Well, most drivers.
            Ok(None) => println!("No results set generated."),
            Ok(Some(cursor)) => {
                // ...print cursor contents...
            }
        }
        stdin().read_line(&mut title)?;
    }
    Ok(())
}
```

## Fetching results

The most efficient way to query results is not query an ODBC data source row by row, but to
ask for a whole bulk of rows at once. The ODBC driver and driver manager will then fill these
row sets into buffers which have been previously bound. This is also the most efficient way to
query a single row many times for many queries, if the application can reuse the bound buffer.
This crate allows you to provide your own buffers by implementing the [`crate::RowSetBuffer`]
trait. That however requires `unsafe` code.

This crate also provides two implementation of the [`crate::RowSetBuffer`] trait, ready to be
used in safe code:

* [`crate::buffers::TextRowSet`]
* [`crate::buffers::ColumnarRowSet`]

### Fetching results row by row without binding buffers upfront.

Fetching data without binding buffers, may imply lots of round trips to the data source (one by
row, or even by field). Also the driver has no upfront knowledge what C-Type your value should
be represented as, preventing further optimizations. Usually it is the slowest possible way to
siphon data out of a data source. That being said, it is a convenient programming model, as the
developer does not need to prepare and allocate the buffers beforehand. It is also a good way to
retrieve really large single values out of a data source (like one large text file).

```
use odbc_api::{Connection, Error, IntoParameter, Cursor};

fn get_large_text(name: &str, conn: &mut Connection<'_>) -> Result<Option<String>, Error> {
    let mut cursor = conn
        .execute("SELECT content FROM LargeFiles WHERE name=?", &name.into_parameter())?
        .expect("Assume select statement creates cursor");
    if let Some(mut row) = cursor.next_row()? {
        let mut buf = Vec::new();
        row.get_text(1, &mut buf)?;
        let ret = String::from_utf8(buf).unwrap();
        Ok(Some(ret))
    } else {
        Ok(None)
    }
}
```

### Fetching results column wise with `ColumnarRowSet`.

Consider querying a table with two columns `year` and `name`.

```no_run
use odbc_api::{
    Environment, Cursor,
    buffers::{AnyColumnView, ColumnarRowSet, BufferDescription, BufferKind, Item},
};

let env = Environment::new()?;

let batch_size = 1000; // Maximum number of rows in each row set
let buffer_description = [
    // We know year to be a Nullable SMALLINT
    BufferDescription {
        kind: BufferKind::I16,
        nullable: true,
    },
    // and name to be a required VARCHAR
    BufferDescription {
        kind: BufferKind::Text { max_str_len: 255 },
        nullable: false,
    }
];
let mut buffer = ColumnarRowSet::new(batch_size, buffer_description.iter().copied());

let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
    // Bind buffer to cursor. We bind the buffer as a mutable reference here, which makes it
    // easier to reuse for other queries, but we could have taken ownership.
    let mut row_set_cursor = cursor.bind_buffer(&mut buffer)?;
    // Loop over row sets
    while let Some(row_set) = row_set_cursor.fetch()? {
        // Process years in row set
        let year_col = row_set.column(0);
        for year in i16::as_nullable_slice(year_col)
            .expect("Year column buffer expected to be nullable Int")
        {
            // Iterate over `Option<i16>` with it ..
        }
        // Process names in row set
        match row_set.column(1) {
            AnyColumnView::Text(it) => {
                // Iterate over `Option<&CStr> with it ..
            }
            _ => panic!("Name column buffer expected to be text")
        }
    }
}
# Ok::<(), odbc_api::Error>(())
```

This second examples changes two things, we do not know the schema in advance and use the
SQL DataType to determine the best fit for the buffers. Also we want to do everything in a
function and return a `Cursor` with an already bound buffer. This approach is best if you have
few and very long query, so the overhead of allocating buffers is negligible and you want to
have an easier time with the borrow checker.

```no_run
use odbc_api::{
    Connection, RowSetCursor, Error, Cursor, Nullability, ResultSetMetadata,
    buffers::{ColumnarRowSet, BufferDescription, BufferKind}
};

fn get_birthdays<'a>(conn: &'a mut Connection)
    -> Result<RowSetCursor<impl Cursor + 'a, ColumnarRowSet>, Error>
{
    let cursor = conn.execute("SELECT year, name FROM Birthdays;", ())?.unwrap();
    let mut column_description = Default::default();
    let buffer_description : Vec<_> = (0..cursor.num_result_cols()?).map(|index| {
        cursor.describe_col(index as u16 + 1, &mut column_description)?;
        Ok(BufferDescription {
            nullable: matches!(column_description.nullability, Nullability::Unknown | Nullability::Nullable),
            // Use reasonable sized text, in case we do not know the buffer type.
            kind: BufferKind::from_data_type(column_description.data_type)
                .unwrap_or(BufferKind::Text { max_str_len: 255 })
        })
    }).collect::<Result<_, Error>>()?;

    // Row set size of 5000 rows.
    let buffer = ColumnarRowSet::new(5000, buffer_description.into_iter());
    // Bind buffer and take ownership over it.
    cursor.bind_buffer(buffer)
}
```

## Inserting values into a table

### Inserting a single row into a table

Inserting a single row can be done by executing a statement and binding the fields as parameters
in a tuple.

```no_run
use odbc_api::{Connection, Error, IntoParameter};

fn insert_birth_year(conn: &Connection, name: &str, year: i16) -> Result<(), Error>{
    conn.execute(
        "INSERT INTO Birthdays (name, year) VALUES (?, ?)",
        (&name.into_parameter(), &year)
    )?;
    Ok(())
}
```

### Columnar bulk inserts

Inserting values row by row can introduce a lot of overhead. ODBC allows you to perform either
row or column wise bulk inserts. Especially in pipelines for data science you may already have
buffers in a columnar layout at hand. [`crate::buffers::ColumnarRowSet`] can be used for bulk
inserts.

```no_run
use odbc_api::{
    Connection, Error, IntoParameter,
    buffers::{ColumnarRowSet, BufferDescription, BufferKind, AnyColumnViewMut, Item}
};

fn insert_birth_years(conn: &Connection, names: &[&str], years: &[i16]) -> Result<(), Error> {

    // All columns must have equal length.
    assert_eq!(names.len(), years.len());

    // Create a columnar buffer which fits the input parameters.
    let buffer_description = [
        BufferDescription {
            kind: BufferKind::Text { max_str_len: 255 },
            nullable: false,
        },
        BufferDescription {
            kind: BufferKind::I16,
            nullable: false,
        },
    ];
    let mut buffer = ColumnarRowSet::new(
        names.len(),
        buffer_description.iter().copied()
    );

    // Fill the buffer with values column by column
    match buffer.column_mut(0) {
        AnyColumnViewMut::Text(mut col) => {
            col.write(names.iter().map(|s| Some(s.as_bytes())))
        }
        _ => panic!("We know the name column to hold text.")
    }

    let col = i16::as_slice_mut(buffer.column_mut(1))
        .expect("We know the year column to hold i16.");
    col.copy_from_slice(years);

    conn.execute(
        "INSERT INTO Birthdays (name, year) VALUES (?, ?)",
        &buffer
    )?;
    Ok(())
}
```
*/
