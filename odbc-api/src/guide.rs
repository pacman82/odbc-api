//! Introduction to `odbc-api` (documentation only)
//!
//! # About ODBC
//!
//! ODBC is an open standard which allows you to connect to various data sources without your
//! application having to explicitly be written for them or for it to be explicitly linked to their
//! drivers during build time. Mostly these data sources are databases, but ODBC drivers are also
//! available for various file types like Excel or CSV.
//!
//! While your application does not does not link against a driver it will talk to an ODBC driver
//! manager which must be installed on the system you intend to run the application. On modern
//! Windows Platforms ODBC is always preinstalled, on OS-X or Linux distributions a driver manager
//! like unix-odbc must be installed.
//!
//! To actually connect to a data source a driver must be installed for the ODBC driver manager to
//! manage. On windows you can type 'ODBC Data Sources' into the search box to start a little GUI
//! which shows you the various drivers and preconfigured data sources on your system.
//!
//! This however is not a guide on how to configure and setup ODBC. This is a guide on how to use
//! the Rust bindings for applications which want to utilize ODBC data sources.
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
//! # Setting up the ODBC Environment
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
//! code. Well we are talking with a C API those contract explicitly demands that there may be only
//! one ODBC Environment in the entire process. The only way to know that this call is safe is in
//! application code. If you write a library you MUST NOT wrap the creation of an ODBC environment
//! in a safe function call. If another libray would do the same and an application were to use
//! both of these, it might create two environments in safe code and thus causing undefined
//! behaviour, which is clearly a violation of Rusts safety guarantees.
//!
//! Apart from that. This is it. Our ODBC Environment is ready for action.
//!
//! # Connect to a data source
//!
//! These bindings currently support two ways of connecting to an ODBC data source. Let us start
//! with the most flexible one.
//!
//! ## Connect using a connection string
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
//! The data source name serves as a key into a map of preconfigured configuration options. This
//! makes invoking them from an application very easy.
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
//! # use odbc_api::Environment;
//!
//! # let env = unsafe {
//! #    Environment::new()?
//! # };
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
