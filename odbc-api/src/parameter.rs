//! # Passing parameters to statement
//!
//! ## In a nutshell
//!
//! * `()` -> No parameter
//! * `&a` -> Single input parameter
//! * `InOut(&mut a)` -> Input Output parameter
//! * `Out(&mut a)` -> Output parameter
//! * `(a,b,c)` -> Fixed number of parameters
//! * `&[a]` -> Arbitrary number of parameters
//! * `&mut BlobParam` -> Stream long input parameters.
//! * `Box<dyn InputParameter>` -> Arbitrary input parameter
//! * `&[Box<dyn InputParameter>]` -> Arbitrary number of arbitrary input parameters
//! * `a.into_parameter()` -> Convert idiomatic Rust type into something bindable by ODBC.
//!
//! ## Passing a single parameter
//!
//! ODBC allows you to bind parameters to positional placeholders. In the simples case it looks like
//! this:
//!
//! ```no_run
//! use odbc_api::{Environment, ConnectionOptions};
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
//! let year = 1980;
//! if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", &year)? {
//!     // Use cursor to process query results.
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! All types implementing the `Parameter` trait can be used.
//!
//! ## Annotating a parameter with an explicit SQL DataType
//!
//! In the last example we used a bit of domain knowledge about the query and provided it with an
//! `i32`. Each `Parameter` type comes with a default SQL Type as which it is bound. In the last
//! example this spared us from specifing that we bind `year` as an SQL `INTEGER` (because `INTEGER`
//! is default for `i32`). If we want to, we can specify the SQL type independent from the Rust type
//! we are binding, by wrapping it in `WithDataType`.
//!
//! ```no_run
//! use odbc_api::{Environment, ConnectionOptions, DataType, parameter::WithDataType};
//! use std::num::NonZeroUsize;
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
//! let year = WithDataType{
//!    value: 1980,
//!    data_type: DataType::Varchar {length: NonZeroUsize::new(4) }
//! };
//! if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", &year)? {
//!     // Use cursor to process query results.
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! In that case it is likely that the driver manager converts our annotated year into a string
//! which is most likely being converted back into an integer by the driver. All this converting can
//! be confusing, but it is helpful if we do not know what types the parameters actually have (i.e.
//! the query could have been entered by the user on the command line.). There is also an option to
//! query the parameter types beforehand, but my advice is not trust the information blindly if you
//! cannot test this with your driver beforehand.
//!
//! ## Passing a fixed number of parameters
//!
//! To pass multiple but a fixed number of parameters to a query you can use tuples.
//!
//! ```no_run
//! use odbc_api::{Environment, ConnectionOptions};
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
//! let too_old = 1980;
//! let too_young = 2000;
//! if let Some(cursor) = conn.execute(
//!     "SELECT year, name FROM Birthdays WHERE ? < year < ?;",
//!     (&too_old, &too_young),
//! )? {
//!     // Use cursor to congratulate only persons in the right age group...
//! }
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! ## Passing an arbitrary number of parameters
//!
//! Not always do we know the number of required parameters at compile time. This might be the case
//! if the query itself is generated from user input. Luckily slices of parameters are supported, too.
//!
//! ```no_run
//! use odbc_api::{Environment, ConnectionOptions};
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
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
//! ## Passing an input parameters parsed from the command line
//!
//! In case you want to read parameters from the command line you can also let ODBC do the work of
//! converting the text input into something more suitable.
//!
//! ```
//! use odbc_api::{Connection, IntoParameter, Error, parameter::VarCharSlice};
//!
//! fn execute_arbitrary_command(connection: &Connection, query: &str, parameters: &[&str])
//!     -> Result<(), Error>
//! {
//!     // Convert all strings to `VarCharSlice` and bind them as `VarChar`. Let ODBC convert them
//!     // into something better matching the types required be the query.
//!     let params: Vec<_> = parameters
//!         .iter()
//!         .map(|param| param.into_parameter())
//!         .collect();
//!
//!     // Execute the query as a one off, and pass the parameters. String parameters are parsed and
//!     // converted into something more suitable by the data source itself.
//!     connection.execute(&query, params.as_slice())?;
//!     Ok(())
//! }
//! ```
//!
//! Should you have more type information the type available, but only at runtime can also bind an
//! array of `[Box<dyn InputParameter]`.
//!
//! ## Output and Input/Output parameters
//!
//! Mutable references are treated as input/output parameters. To use a parameter purely as an
//! output parameter you may wrapt it into out. Consider a Mircosoft SQL Server with the following
//! stored procedure:
//!
//! ```mssql
//! CREATE PROCEDURE TestParam
//! @OutParm int OUTPUT
//! AS
//! SELECT @OutParm = @OutParm + 5
//! RETURN 99
//! GO
//! ```
//!
//! We bind the return value as the first output parameter. The second parameter is an input/output
//! bound as a mutable reference.
//!
//! ```no_run
//! use odbc_api::{Environment, ConnectionOptions, Out, InOut, Nullable};
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
//!
//! let mut ret = Nullable::<i32>::null();
//! let mut param = Nullable::<i32>::new(7);
//!
//! conn.execute(
//!     "{? = call TestParam(?)}",
//!     (Out(&mut ret), InOut(&mut param)))?;
//!
//! assert_eq!(Some(99), ret.into_opt());
//! assert_eq!(Some(7 + 5), param.into_opt());
//!
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! ## Sending long data
//!
//! Many ODBC drivers have size limits of how big parameters can be. Apart from that you may not
//! want to allocate really large buffers in your application in order to keep a small memory
//! footprint. Luckily ODBC also supports streaming data to the database batch by batch at statement
//! execution time. To support this, this crate offers the [`BlobParam`], which can be bound as a
//! mutable reference. An instance of [`BlobParam`] is usually created by calling
//! [`Blob::as_blob_param`] from a wrapper implenting [`Blob`].
//!
//! ### Inserting long binary data from a file.
//!
//! [`BlobRead::from_path`] is the most convinient way to turn a file path into a [`Blob`]
//! parameter. The following example also demonstrates that the streamed blob parameter can be
//! combined with regular input parmeters like `id`.
//!
//! ```
//! use std::{error::Error, path::Path};
//! use odbc_api::{Connection, parameter::{Blob, BlobRead}, IntoParameter};
//!
//! fn insert_image_to_db(
//!     conn: &Connection<'_>,
//!     id: &str,
//!     image_path: &Path) -> Result<(), Box<dyn Error>>
//! {
//!     let mut blob = BlobRead::from_path(&image_path)?;
//!
//!     let sql = "INSERT INTO Images (id, image_data) VALUES (?, ?)";
//!     let parameters = (&id.into_parameter(), &mut blob.as_blob_param());
//!     conn.execute(sql, parameters)?;
//!     Ok(())
//! }
//! ```
//!
//! ### Inserting long binary data from any `io::BufRead`.
//!
//! This is more flexible than inserting just from files. Note however that files provide metadata
//! about the length of the data, which `io::BufRead` does not. This is not an issue for most
//! drivers, but some can perform optimization if they know the size in advance. In the tests
//! SQLite has shown a bug to only insert empty data if no size hint has been provided.
//!
//! ```
//! use std::io::BufRead;
//! use odbc_api::{Connection, parameter::{Blob, BlobRead}, IntoParameter, Error};
//!
//! fn insert_image_to_db(
//!     conn: &Connection<'_>,
//!     id: &str,
//!     image_data: impl BufRead) -> Result<(), Error>
//! {
//!     const MAX_IMAGE_SIZE: usize = 4 * 1024 * 1024;
//!     let mut blob = BlobRead::with_upper_bound(image_data, MAX_IMAGE_SIZE);
//!
//!     let sql = "INSERT INTO Images (id, image_data) VALUES (?, ?)";
//!     let parameters = (&id.into_parameter(), &mut blob.as_blob_param());
//!     conn.execute(sql, parameters)?;
//!     Ok(())
//! }
//! ```
//!
//! ### Inserting long strings
//!
//! This example insert `title` as a normal input parameter but streams the potentially much longer
//! `String` in `text` to the database as a large text blob. This allows to circumvent the size
//! restrictions for `String` arguments of many drivers (usually around 4 or 8 KiB).
//!
//! ```
//! use odbc_api::{Connection, parameter::{Blob, BlobSlice}, IntoParameter, Error};
//!
//! fn insert_book(
//!     conn: &Connection<'_>,
//!     title: &str,
//!     text: &str
//! ) -> Result<(), Error>
//! {
//!     let mut blob = BlobSlice::from_text(text);
//!
//!     let insert = "INSERT INTO Books (title, text) VALUES (?,?)";
//!     let parameters = (&title.into_parameter(), &mut blob.as_blob_param());
//!     conn.execute(&insert, parameters)?;
//!     Ok(())
//! }
//! ```
//!
//! ### Inserting long binary data from `&[u8]`.
//!
//! ```
//! use odbc_api::{Connection, parameter::{Blob, BlobSlice}, IntoParameter, Error};
//!
//! fn insert_image(
//!     conn: &Connection<'_>,
//!     id: &str,
//!     image_data: &[u8]
//! ) -> Result<(), Error>
//! {
//!     let mut blob = BlobSlice::from_byte_slice(image_data);
//!
//!     let insert = "INSERT INTO Images (id, image_data) VALUES (?,?)";
//!     let parameters = (&id.into_parameter(), &mut blob.as_blob_param());
//!     conn.execute(&insert, parameters)?;
//!     Ok(())
//! }
//! ```
//!
//! ## Passing the type you absolutely think should work, but does not.
//!
//! Sadly not every type can be safely bound as something the ODBC C-API understands. Most prominent
//! among those is a Rust string slice (`&str`).
//!
//! ```no_run
//! use odbc_api::{Environment, ConnectionOptions};
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
//! // conn.execute("SELECT year FROM Birthdays WHERE name=?;", "Bernd")?; // <- compiler error.
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! Alas, not all is lost. We can still make use of the [`crate::IntoParameter`] trait to convert it
//! into something that works.
//!
//! ```no_run
//! use odbc_api::{Environment, IntoParameter, ConnectionOptions};
//!
//! let env = Environment::new()?;
//!
//! let mut conn = env.connect(
//!     "YourDatabase", "SA", "My@Test@Password1",
//!     ConnectionOptions::default()
//! )?;
//! if let Some(cursor) = conn.execute(
//!     "SELECT year FROM Birthdays WHERE name=?;",
//!     &"Bernd".into_parameter())?
//! {
//!     // Use cursor to process query results.
//! };
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! Conversion for `&str` is not too expensive either. Just an integer more on the stack. Wait, the
//! type you wanted to use, but that I have conveniently not chosen in this example still does not
//! work? Well, in that case please open an issue or a pull request. [`crate::IntoParameter`] can usually be
//! implemented entirely in safe code, and is a suitable spot to enable support for your custom
//! types.
mod blob;
mod c_string;
mod varcell;

pub use self::{
    blob::{Blob, BlobParam, BlobRead, BlobSlice},
    varcell::{
        Binary, Text, VarBinary, VarBinaryArray, VarBinaryBox, VarBinarySlice, VarBinarySliceMut,
        VarCell, VarChar, VarCharArray, VarCharBox, VarCharSlice, VarCharSliceMut, VarKind,
        VarWCharArray, VarWCharBox, VarWCharSlice, VarWCharSliceMut, WideText,
    },
};

use std::ffi::c_void;

use odbc_sys::CDataType;

use crate::{
    fixed_sized::Pod,
    handles::{CData, CDataMut, HasDataType},
    DataType,
};

/// A CData representing a single value rather than an entire buffer of a range of values.
///
/// # Safety
///
/// Considerations for implementers
///
/// Callers must be able to rely on all pointers being valid, i.e. the "range" is not empty.
///
/// Since the indicator provided by implementation is used to indicate the length of the value in
/// the buffer, care must be taken to prevent out of bounds access in case the implementation also
/// is used as an output parameter, and contains truncated values (i.e. the indicator is longer than
/// the buffer and the value within).
pub unsafe trait CElement: CData {
    /// Must panic if the parameter is not complete. I.e. the indicator of a variable length
    /// parameter indicates a value larger than what is present in the value buffer.
    ///
    /// This is used to prevent using truncacted values as input buffers, which could cause
    /// inserting invalid memory with drivers which just copy values for the length of the indicator
    /// buffer without checking the length of the target buffer first. The ODBC standard is
    /// inconclusive wether the driver has to check for this or not. So we need to check this. We
    /// can not manifest this as an invariant expressed by a type for all cases, due to the
    /// existence of input/output parameters.
    fn assert_completness(&self);
}

/// Can be used to fill in a field value indicated by a placeholder (`?`) then executing an SQL
/// statement.
pub trait InputParameter: HasDataType + CElement {}

impl<T> InputParameter for T where T: CElement + HasDataType {}

/// # Safety
///
/// Guarantees that there is space in the output buffer for at least one element.
pub unsafe trait OutputParameter: CDataMut + HasDataType {}

/// Wraps a mutable reference. Use this wrapper in order to indicate that a mutable reference should
/// be bound as an input / output parameter.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, Out, InOut, Nullable, ConnectionOptions};
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default()
/// )?;
///
/// let mut ret = Nullable::<i32>::null();
/// let mut param = Nullable::new(7);
///
/// conn.execute(
///     "{? = call TestParam(?)}",
///     (Out(&mut ret), InOut(&mut param)))?;
///
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub struct InOut<'a, T>(pub &'a mut T);

/// Use this to warp a mutable reference to an [`OutputParameter`]. This will cause the argument to
/// be considered an output parameter only. Without this wrapper it would be considered an input
/// parameter. You can use [`InOut`] if you want to indicate that the argument is an input and an
/// output parameter.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, Out, InOut, Nullable, ConnectionOptions};
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default(),
/// )?;
///
/// let mut ret = Nullable::<i32>::null();
/// let mut param = Nullable::new(7);
///
/// conn.execute(
///     "{? = call TestParam(?)}",
///     (Out(&mut ret), InOut(&mut param)))?;
///
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub struct Out<'a, T>(pub &'a mut T);

/// Annotates an instance of an inner type with an SQL Data type in order to indicate how it should
/// be bound as a parameter to an SQL Statement.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, ConnectionOptions, DataType, parameter::WithDataType};
/// use std::num::NonZeroUsize;
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default()
/// )?;
/// // Bind year as VARCHAR(4) rather than integer.
/// let year = WithDataType{
///    value: 1980,
///    data_type: DataType::Varchar {length: NonZeroUsize::new(4)}
/// };
/// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", &year)? {
///     // Use cursor to process query results.
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
///
/// Can also be used to wrap [`crate::sys::Timestamp`] so they implement [`OutputParameter`].
///
/// ```no_run
/// # use odbc_api::{
/// #    Connection, Cursor, DataType, parameter::WithDataType, IntoParameter, sys::Timestamp
/// # };
/// # fn given(cursor: &mut impl Cursor, connection: Connection<'_>) {
/// let mut ts = WithDataType {
///     value: Timestamp::default(),
///     data_type: DataType::Timestamp { precision: 0 },
/// };
/// connection.execute(
///     "INSERT INTO Posts (text, timestamps) VALUES (?,?)",
///     (&"Hello".into_parameter(), &ts.into_parameter())
/// );
/// # }
/// ```
#[derive(Debug)]
pub struct WithDataType<T> {
    /// Value to wrap with a Data Type. Should implement [`crate::handles::CData`], to be useful.
    pub value: T,
    /// The SQL type this value is supposed to map onto. What exactly happens with this information
    /// is up to the ODBC driver in use.
    pub data_type: DataType,
}

unsafe impl<T> CData for WithDataType<T>
where
    T: CData,
{
    fn cdata_type(&self) -> CDataType {
        self.value.cdata_type()
    }

    fn indicator_ptr(&self) -> *const isize {
        self.value.indicator_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.value.value_ptr()
    }

    fn buffer_length(&self) -> isize {
        self.value.buffer_length()
    }
}

unsafe impl<T> CDataMut for WithDataType<T>
where
    T: CDataMut,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        self.value.mut_indicator_ptr()
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.value.mut_value_ptr()
    }
}

impl<T> HasDataType for WithDataType<T> {
    fn data_type(&self) -> DataType {
        self.data_type
    }
}

unsafe impl<T> CElement for WithDataType<T>
where
    T: CElement,
{
    fn assert_completness(&self) {
        self.value.assert_completness()
    }
}
unsafe impl<T> OutputParameter for WithDataType<T> where T: Pod {}

// Allow for input parameters whose type is only known at runtime.
unsafe impl CData for Box<dyn InputParameter> {
    fn cdata_type(&self) -> CDataType {
        self.as_ref().cdata_type()
    }

    fn indicator_ptr(&self) -> *const isize {
        self.as_ref().indicator_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.as_ref().value_ptr()
    }

    fn buffer_length(&self) -> isize {
        self.as_ref().buffer_length()
    }
}

impl HasDataType for Box<dyn InputParameter> {
    fn data_type(&self) -> DataType {
        self.as_ref().data_type()
    }
}
unsafe impl CElement for Box<dyn InputParameter> {
    fn assert_completness(&self) {
        self.as_ref().assert_completness()
    }
}
