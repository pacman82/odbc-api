//! Introduction to `odbc-api` (documentation only)
//!
//! # About ODBC
//!
//! ODBC is an open standard which allows you to connect to various data sources without your
//! application having to explicitly written for them or for it to be explicitly linked to drivers
//! during build time. Mostly these data sources are databases, but ODBC drivers are also available
//! for various file types like Excel or CSV. For reasons of simplicity we will speak of Databases
//! throughout this tutorial.
//!
//! While your application does not does not link against a driver it will talk to an ODBC driver
//! manager which must be installed on the system you intend to run the application. On modern
//! Windows Platform ODBC is always preinstalled, on OS-X or Linux distributions a driver manager
//! like unix-odbc must be installed by the administrators.
//!
//! To actually connect to a data source a driver must be installed for the ODBC driver manage to
//! manage. On windows you can type 'ODBC Data Sources' into the search box to start a little GUI
//! which shows you the various drivers and preconfigured data sources on your system.
//!
//! This however is not a guide on how to configure and setup ODBC. This is a guide on how to use
//! the Rust bindings for applications which want to utilize ODBC data sources.
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
//! Oh dear! Aren't we of to a bad start. First example of how to use this API and already a piece
//! unsafe code. Well we are talking with a C API those contracts explicitly demands that there may
//! be only one ODBC Environment in the entire process. The only way to know that this is call is
//! safe is in application code. If you write a library you MUST NOT wrap the creation of an ODBC
//! environment in a safe function call. If another libray would do the same and an application
//! were to use both of these, it might create two environments in safe code and thus causing
//! undefined behaviour, which is clearly a violation of Rusts safety guarantees.
//!
//! Apart from that. That's it. Our ODBC Environment is ready for action.