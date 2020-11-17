//! Introduction to `odbc-api` (documentation only)
//!
//! # About ODBC
//!
//! ODBC is an open standard which allows you to connect to various data sources. Mostly these data
//! sources are databases, but ODBC drivers are also available for various file types like Excel or
//! CSV.
//!
//! Your application does not does not link against a driver, but will link against an ODBC driver
//! manager which must be installed on the system you intend to run the application. On modern
//! Windows Platforms ODBC is always installed, on OS-X or Linux distributions a driver manager like
//! [unixODBC](http://www.unixodbc.org/) must be installed by whomever manages the system.
//!
//! To connect to a data source a driver for the specific data source in question must be installed.
//! On windows you can type 'ODBC Data Sources' into the search box to start a little GUI which
//! shows you the various drivers and preconfigured data sources on your system.
//!
//! This however is not a guide on how to configure and setup ODBC. This is a guide on how to use
//! the Rust bindings for applications which want to utilize ODBC data sources.
//!
//! # Quickstart
//!
//! ```no_run
//! //! A program executing a query and printing the result as csv to standard out. Requires
//! //! `anyhow` and `csv` crate.
//!
//! use anyhow::Error;
//! use odbc_api::{buffers::TextRowSet, Cursor, Environment};
//! use std::{
//!     io::{stdout, Write},
//!     path::PathBuf,
//! };
//!
//! /// Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
//! /// faster than fetching individual rows.
//! const BATCH_SIZE: u32 = 100000;
//!
//! fn main() -> Result<(), Error> {
//!     // Write csv to standard out
//!     let out = stdout();
//!     let mut writer = csv::Writer::from_writer(out);
//!
//!     // We know this is going to be the only ODBC environment in the entire process, so this is
//!     // safe.
//!     let environment = unsafe { Environment::new() }?;
//!
//!     // Connect using a DSN. Alternatively we could have used a connection string
//!     let mut connection = environment.connect(
//!         "DataSourceName",
//!         "Username",
//!         "Password",
//!     )?;
//!
//!     // Execute a one of query without any parameters.
//!     match connection.execute("SELECT * FROM TableName", ())? {
//!         Some(cursor) => {
//!             // Write the column names to stdout
//!             let mut headline : Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
//!             writer.write_record(headline)?;
//!
//!             // Use schema in cursor to initialize a text buffer large enough to hold the largest
//!             // possible strings for each column.
//!             let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &cursor)?;
//!             // Bind the buffer to the cursor. It is now being filled with every call to fetch.
//!             let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;
//!
//!             // Iterate over batches
//!             while let Some(batch) = row_set_cursor.fetch()? {
//!                 // Within a batch, iterate over every row
//!                 for row_index in 0..batch.num_rows() {
//!                     // Within a row iterate over every column
//!                     let record = (0..batch.num_cols())
//!                         .map(|col_index| batch.at(col_index, row_index).unwrap_or(&[]));
//!                     // Writes row as csv
//!                     writer.write_record(record)?;
//!                 }
//!             }
//!         }
//!         None => {
//!             eprintln!(
//!                 "Query came back empty. No output has been created."
//!             );
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # 32 Bit and 64 Bit considerations.
//!
//! To consider wether you want to work with 32 Bit or 64 Bit data sources is especially important
//! for windows users, as driver managers (and possibly drivers) may both exist at the same time
//! in the same system.
//!
//! In any case, depending on the platform part of your target tripple either 32 Bit or 64 Bit
//! drivers are going to work, but not both. On a private windows machine (even on a modern 64 Bit
//! Windows) it is not unusual to find lots of 32 Bit drivers installed on the system, but none for
//! 64 Bits. So for windows users it is worth thinking about not using the default toolchain which
//! is likely 64 Bits and to switch to a 32 Bit one. On other platforms you are usually fine
//! sticking with 64 Bits, as there are not usually any drivers preinstalled anyway, 64 Bit or
//! otherwise.
//!
//! No code changes are required, so you can also just build both if you want to.
//!
//! # Connect to a data source
//!
//! ## Setting up the ODBC Environment
//!
//! To connect with a data source we need a connection. To create a connection we need an ODBC
//! environment.
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
//! // making this call safe.
//! unsafe {
//!     let env = Environment::new()?;
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! Oh dear! Aren't we of to a bad start. First step in using this API and already a piece of unsafe
//! code. Well we are talking with a C API those contract explicitly demands that there MUST be at
//! most one ODBC Environment in the entire process. This requirement can only be verified in
//! application code. If you write a library you MUST NOT wrap the creation of an ODBC environment
//! in a safe function call. If another libray would do the same and an application were to use
//! both of these, it might create two environments in safe code and thus causing undefined
//! behaviour, which is clearly a violation of Rusts safety guarantees. On the other hand in
//! application code it is pretty easy to get right. You call it, and you call it only once.
//!
//! Apart from that. This is it. Our ODBC Environment is ready for action.
//!
//! These bindings currently support two ways of creating a connections:
//!
//! ## Connect using a connection string
//!
//! Connection strings do not require that the data source is preconfigured by the driver manager
//! this makes them very flexible.
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
//! // making this call safe.
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let connection_string = "
//!     Driver={ODBC Driver 17 for SQL Server};\
//!     Server=localhost;\
//!     UID=SA;\
//!     PWD=<YourStrong@Passw0rd>;\
//! ";
//!
//! let mut conn = env.connect_with_connection_string(connection_string)?;
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! There is a syntax to these connection strings, but few people go through the trouble to learn
//! it. Most common strategy is to google one that works for with your data source. The connection
//! borrows the environment, so you will get a compiler error, if your environment goes out of scope
//! before the connection does.
//!
//! ## Connect using a Data Source Name (DSN)
//!
//! Should a data source be known by the driver manager we can access it using its name and
//! credentials. This is more convinient for the user or application developer, but requires a
//! configuration of the ODBC driver manager. Think of it as shifting work from users to
//! administrators.
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
//! // making this call safe.
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! How to configure such data sources is not the scope of this guide, and depends on the driver
//! manager in question.
//!
//! ## Lifetime considerations for Connections
//!
//! An ODBC connection MUST NOT outlive the ODBC environment. This is modeled as the connection
//! borrowing the environment. It is a shared borrow, to allow for more than one connection per
//! environment. This way the compiler will catch programming errors early. The most popular among
//! them seems to be returning a `Connection` from a function which also creates the environment.
//!
//! # Executing a statement
//!
//! With our ODBC connection all set up and ready to go, we can execute an SQL query:
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays;", ())? {
//!     // Use cursor to process query results.
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! The first parameter of `execute` is the SQL statement text. The second parameter is used to pass
//! arguments of the SQL Statements itself (more on that later). Ours has none, so we use `()` to
//! not bind any arguments to the statement.
//! Note that not every statement returns a `Cursor`. `INSERT` statements usually do not, but even
//! `SELECT` queries which would return zero rows can depending on the driver return an empty cursor
//! or no cursor at all. If a cursor exists, it must be consumed or closed. The `drop` handler of
//! Cursor will close it, so it is all taken care of.
//!
//! ## Passing parameters to a statement
//!
//! ODBC allows you to bind parameters to positional placeholders. In the simples case it looks like
//! this:
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! let year = 1980;
//! if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", year)? {
//!     // Use cursor to process query results.
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! ### Annotating a Parameter with an SQL DataType
//!
//! In the last example we used a bit of domain knowledge about the query and provided it with an
//! `i32`. All types implementing the `Parameter` trait can be used. Each `Parameter` type comes
//! with a default SQL Type as which it is bound. In the last example this spared us from specifing
//! that we bind `year` as an SQL `INTEGER` (because `INTEGER` is default for `i32`). If we want to,
//! we can specify the SQL type independent from the Rust type we are binding, by wrapping it in
//! `WithDataType`.
//!
//! ```no_run
//! use odbc_api::{Environment, parameter::WithDataType, DataType};
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! let year = WithDataType{
//!    value: 1980,
//!    data_type: DataType::Varchar {length: 4}
//! };
//! if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", year)? {
//!     // Use cursor to process query results.
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! In that case it is likely that the driver manager converts our anotated year into a string which
//! is most likely being converted back into an integer by the driver. All this converting can be 
//! confusing, but it is helpful if we do not know what types the parameters actually have (i.e. the
//! query could have been entered by the user on the command line.). There is also an option to
//! query the parameter types beforhand, but my advice is not trust the information blindly if you
//! cannot test this with your driver beforehand.
//!
//! ### Passing a fixed number of parameters
//!
//! To pass multiple but a fixed number of parameters to a query you can use tuples.
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! let too_old = 1980;
//! let too_young = 2000;
//! if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE ? < year < ?;", (too_old, too_young))? {
//!     // Use cursor to congratulate only persons in the right age group...
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! ### Passing an abitrary number of parameters
//!
//! Not always do we know the number of required parameters at compile time. This might be the case
//! if the query itself is generated from user input. Luckily slices of parameters are supported, too.
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! let params = [1980, 2000];
//! if let Some(cursor) = conn.execute(
//!     "SELECT year, name FROM Birthdays WHERE ? < year < ?;",
//!     &params[..])?
//! {
//!     // Use cursor to process query results.
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! ### Passing the type you absolutly think should work as a parameter, but does not
//!
//! Sadly not every type can be safely bound as something the ODBC C-API understands. Most
//! prominent maybe are Rust string slices (`&str`), as they just refuse to have terminating zero
//! at their end.
//!
//! ```no_run
//! use odbc_api::Environment;
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! // conn.execute("SELECT year FROM Birthdays WHERE name=?;", "Bernd")?; // <- compiler error.
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! Alas, not all is lost. We can still make use of the `IntoParameter` trait to convert it into
//! something that works.
//!
//! ```no_run
//! use odbc_api::{Environment, IntoParameter};
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//! if let Some(cursor) = conn.execute(
//!     "SELECT year FROM Birthdays WHERE name=?;",
//!     "Bernd".into_parameter())?
//! {
//!     // Use cursor to process query results.
//! };
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! There you go. Conversion in this case is not too expensive either, just a stack allocated
//! integer. Wait, the type you wanted to use, but that I have conviniently not chosen in this
//! example still does not work? Well, in that case dear reader, please open an issue or a pull
//! request. `IntoParameter` can also be implemented in safe code as you only have to produce a type
//! which implements `Parameter` and not require any unsafe binding code with pointers. So it is a
//! good extension point for supporting your crates custom types.