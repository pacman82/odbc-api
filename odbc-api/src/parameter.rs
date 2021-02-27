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
//! * a.into_parameter() -> Convert idiomatic Rust type into something bindable by ODBC.
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
//! In that case it is likely that the driver manager converts our anotated year into a string which
//! is most likely being converted back into an integer by the driver. All this converting can be
//! confusing, but it is helpful if we do not know what types the parameters actually have (i.e. the
//! query could have been entered by the user on the command line.). There is also an option to
//! query the parameter types beforhand, but my advice is not trust the information blindly if you
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
//! ## Passing an abitrary number of parameters
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
//! ## Passing the type you absolutly think should work, but does not.
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
//! Alas, not all is lost. We can still make use of the [`crate::IntoParameter`] trait to convert it into
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
//!     &"Bernd".into_parameter())?
//! {
//!     // Use cursor to process query results.
//! };
//! # Ok::<(), odbc_api::Error>(())
//! ```
//!
//! Conversion for `&str` is not too expensive either. Just an integer more on the stack. Wait, the
//! type you wanted to use, but that I have conviniently not chosen in this example still does not
//! work? Well, in that case please open an issue or a pull request. [`crate::IntoParameter`] can usually be
//! implemented entirely in safe code, and is a suitable spot to enable support for your custom
//! types.

use std::{cmp::min, convert::TryInto, ffi::c_void};

use odbc_sys::{CDataType, NULL_DATA};

use crate::{
    handles::{CData, CDataMut, HasDataType, Statement, StatementImpl},
    DataType, Error,
};

/// Extend the [`crate::handles::HasDataType`] trait with the guarantee, that the bound parameter buffer
/// contains at least one element.
pub unsafe trait InputParameter: HasDataType {}

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
    /// parameter it must also be ensured that it is exclusivly referenced by statement.
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl,
    ) -> Result<(), Error>;
}

/// Bind immutable references as input parameters.
unsafe impl<T> Parameter for &T
where
    T: InputParameter,
{
    unsafe fn bind_parameter(
        self,
        parameter_number: u16,
        stmt: &mut StatementImpl,
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
        stmt: &mut StatementImpl,
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
        stmt: &mut StatementImpl,
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

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
///
/// This type is created if `into_parameter` of the `IntoParameter` trait is called on a `&str`.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, IntoParameter};
///
/// let env = unsafe {
///     Environment::new()?
/// };
///
/// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
/// if let Some(cursor) = conn.execute(
///     "SELECT year FROM Birthdays WHERE name=?;",
///     &"Bernd".into_parameter())?
/// {
///     // Use cursor to process query results.
/// };
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub struct VarChar<'a> {
    bytes: &'a [u8],
    /// Will be set to value.len() by constructor.
    length: isize,
}

impl<'a> VarChar<'a> {
    /// Constructs a new VarChar containing the text in the specified buffer.
    pub fn new(value: &'a [u8]) -> Self {
        VarChar {
            bytes: value,
            length: value.len().try_into().unwrap(),
        }
    }

    /// Constructs a new VarChar representing the NULL value.
    pub fn null() -> Self {
        VarChar {
            bytes: &[],
            length: NULL_DATA,
        }
    }
}

unsafe impl CData for VarChar<'_> {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.length
    }

    fn value_ptr(&self) -> *const c_void {
        self.bytes.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        0
    }
}

unsafe impl HasDataType for VarChar<'_> {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.bytes.len(),
        }
    }
}

unsafe impl InputParameter for VarChar<'_> {}

/// A stack allocated VarChar type able to hold strings up to a length of 512 bytes (including the
/// terminating zero).
///
/// Due to its memory layout this type can be bound either as a single parameter, or as an element
/// of a rowise output, but not be used in columnar parameter arrays or output buffers.
pub struct VarChar512 {
    buffer: [u8; 512],
    indicator: isize,
}

impl VarChar512 {
    /// Length of the allocated buffer in bytes including terminating zero.
    const BUFFER_LEN: usize = 512;

    /// Constructs a new instance of VarChar512. `None` is used as a representation of `NULL` and
    /// values in some with more than 512 - 1 bytes will cause a panic (-1 because of the byte
    /// required for the terminating zero).
    pub fn new(bytes: Option<&[u8]>) -> Self {
        if let Some(bytes) = bytes {
            let mut buffer = [0; Self::BUFFER_LEN];
            if bytes.len() > Self::BUFFER_LEN - 1 {
                panic!("Value is to large to be stored in a VarChar512");
            }
            buffer[..bytes.len()].copy_from_slice(bytes);
            let indicator = bytes.len() as isize;
            VarChar512 { buffer, indicator }
        } else {
            VarChar512 {
                buffer: [0; Self::BUFFER_LEN],
                indicator: NULL_DATA,
            }
        }
    }

    /// Returns the binary representation of the string, excluding the terminating zero.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if self.indicator == NULL_DATA {
            None
        } else {
            let length = min(512 - 1, self.indicator.try_into().unwrap());
            Some(&self.buffer[..length])
        }
    }
}

unsafe impl CData for VarChar512 {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.indicator as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.buffer.as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        // This is the maximum buffer length, but it is NOT the length of an instance of VarChar512
        // due to the missing size of the indicator value. As such the buffer length can not be used
        // to correctly index a columnar buffer of VarChar512.
        512
    }
}

unsafe impl HasDataType for VarChar512 {
    fn data_type(&self) -> DataType {
        // Buffer length minus 1 for terminating zero
        DataType::Varchar { length: 512 - 1 }
    }
}

unsafe impl CDataMut for VarChar512 {
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        &mut self.indicator as *mut isize
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.buffer.as_mut_ptr() as *mut c_void
    }
}

unsafe impl Output for VarChar512 {}
unsafe impl InputParameter for VarChar512 {}

#[cfg(test)]
mod tests {
    use super::VarChar512;


    #[test]
    #[should_panic]
    fn construct_to_large_varchar_512() {
        VarChar512::new(Some(&vec![b'a'; 512]));
    }
}