//! # Passing parameters to statement
//!
//! ## In a nutshell
//!
//! * `()` -> No parameter
//! * `&a` -> Single input parameter
//! * `&mut a` -> Input Output parameter
//! * `Out(&mut a)` -> Output parameter
//! * `(&a,&b,&c)` -> Fixed number of parameters
//! * `&[a]` -> Arbitrary number of parameters
//! * `Box<dyn InputParameter>` -> Aribtrary input parameter
//! * `&[Box<dyn InputParameter>]` -> Aribtrary number of arbitrary input parameters
//! * `a.into_parameter()` -> Convert idiomatic Rust type into something bindable by ODBC.
//!
//! ## Passing a single parameter
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
//! use odbc_api::Environment;
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
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
//! use odbc_api::{Environment, Out, Nullable};
//!
//! let env = unsafe {
//!     Environment::new()?
//! };
//!
//! let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
//!
//! let mut ret = Nullable::<i32>::null();
//! let mut param = Nullable::<i32>::new(7);
//!
//! conn.execute(
//!     "{? = call TestParam(?)}",
//!     (Out(&mut ret), &mut param))?;
//!
//! assert_eq!(Some(99), ret.into_opt());
//! assert_eq!(Some(7 + 5), param.into_opt());
//!
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! ## Passing the type you absolutely think should work, but does not.
//!
//! Sadly not every type can be safely bound as something the ODBC C-API understands. Most prominent
//! among those is a Rust string slice (`&str`).
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
//! Alas, not all is lost. We can still make use of the [`crate::IntoParameter`] trait to convert it
//! into something that works.
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
mod varbin;
mod varchar;

pub use self::{
    blob::{Blob, BlobParam, BlobSlice},
    varbin::{VarBinary, VarBinaryArray, VarBinaryBox, VarBinarySlice, VarBinarySliceMut},
    varchar::{VarChar, VarCharArray, VarCharBox, VarCharSlice, VarCharSliceMut},
};

use std::ffi::c_void;

use odbc_sys::CDataType;

use crate::{
    handles::{CData, CDataMut, HasDataType, Statement, StatementImpl},
    DataType, Error,
};

/// Use implementations of this type as arguments to SQL Statements.
///
/// # Safety
///
/// Considerations for implementers
///
/// Extend the [`crate::handles::HasDataType`] trait with the guarantee, that the bound parameter
/// buffer contains at least one element.
///
/// Since the indicator provided by implementation is used to indicate the length of the value in
/// the buffer, care must be taken to prevent out of bounds access in case the implementation also
/// is used as an output parameter, and contains truncated values (i.e. the indicator is longer than
/// the buffer and the value within).
pub unsafe trait InputParameter: HasDataType + CData {}

/// Guarantees that there is space in the output buffer for at least one element.
pub unsafe trait Output: CDataMut + HasDataType {}

/// Implementers of this trait can be used as individual parameters of in a
/// [`crate::ParameterCollection`]. They can be bound as either input parameters, output parameters
/// or both.
pub unsafe trait Parameter {
    /// Bind the parameter in question to a specific `parameter_number`.
    ///
    /// # Safety
    ///
    /// Since the parameter is now bound to `stmt` callers must take care that it is ensured that
    /// the parameter remains valid while it is bound. If the parameter is bound as an output
    /// parameter it must also be ensured that it is exclusively referenced by statement.
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl<'_>,
    ) -> Result<(), Error>;
}

/// Bind immutable references as input parameters.
unsafe impl<T: ?Sized> Parameter for &T
where
    T: InputParameter,
{
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl<'_>,
    ) -> Result<(), Error> {
        stmt.bind_input_parameter(parameter_number, self)
    }
}

/// Bind mutable references as input/output parameter.
unsafe impl<T> Parameter for &mut T
where
    T: Output,
{
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl<'_>,
    ) -> Result<(), Error> {
        stmt.bind_parameter(parameter_number, odbc_sys::ParamType::InputOutput, self)
    }
}

/// Wraps a mutable reference. Use this wrapper in order to indicate that a mutable reference should
/// be bound as an output parameter only, rather than an input / output parameter.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, Out, Nullable};
///
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
///
/// let mut ret = Nullable::<i32>::null();
/// let mut param = Nullable::<i32>::new(7);
///
/// conn.execute(
///     "{? = call TestParam(?)}",
///     (Out(&mut ret), &mut param))?;
///
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub struct Out<'a, T>(pub &'a mut T);

/// Mutable references wrapped in `Out` are bound as output parameters.
unsafe impl<'a, T> Parameter for Out<'a, T>
where
    T: Output,
{
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl<'_>,
    ) -> Result<(), Error> {
        stmt.bind_parameter(parameter_number, odbc_sys::ParamType::Output, self.0)
    }
}

/// Annotates an instance of an inner type with an SQL Data type in order to indicate how it should
/// be bound as a parameter to an SQL Statement.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, parameter::WithDataType, DataType};
///
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
/// // Bind year as VARCHAR(4) rather than integer.
/// let year = WithDataType{
///    value: 1980,
///    data_type: DataType::Varchar {length: 4}
/// };
/// if let Some(cursor) = conn.execute("SELECT year, name FROM Birthdays WHERE year > ?;", &year)? {
///     // Use cursor to process query results.
/// }
/// # Ok::<(), odbc_api::Error>(())
/// ```
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

unsafe impl<T> HasDataType for WithDataType<T>
where
    T: HasDataType,
{
    fn data_type(&self) -> DataType {
        self.data_type
    }
}

unsafe impl<T> InputParameter for WithDataType<T> where T: InputParameter {}

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

unsafe impl HasDataType for Box<dyn InputParameter> {
    fn data_type(&self) -> DataType {
        self.as_ref().data_type()
    }
}

unsafe impl InputParameter for Box<dyn InputParameter> {}
