use super::{
    as_handle::AsHandle, drop_handle, error::IntoResult, logging::log_diagnostics, Connection,
    Error,
};
use log::debug;
use odbc_sys::{
    AttrOdbcVersion, EnvironmentAttribute, FetchOrientation, HDbc, HEnv, Handle, HandleType,
    SQLAllocHandle, SQLDriversW, SQLSetEnvAttr, SqlReturn,
};
use std::{convert::TryInto, ptr::null_mut};

/// An `Environment` is a global context, in which to access data.
///
/// Associated with an `Environment` is any information that is global in nature, such as:
///
/// * The `Environment`'s state
/// * The current environment-level diagnostics
/// * The handles of connections currently allocated on the environment
/// * The current stetting of each environment attribute
#[derive(Debug)]
pub struct Environment {
    /// Invariant: Should always point to a valid ODBC Environment
    handle: HEnv,
}

/// See: <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading?view=sql-server-ver15>
unsafe impl Send for Environment {}
/// See: <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading?view=sql-server-ver15>
unsafe impl Sync for Environment {}

unsafe impl AsHandle for Environment {
    fn as_handle(&self) -> Handle {
        self.handle as Handle
    }

    fn handle_type(&self) -> HandleType {
        HandleType::Env
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        unsafe {
            drop_handle(self.handle as Handle, HandleType::Env);
        }
    }
}

impl Environment {
    /// An allocated ODBC Environment handle
    ///
    /// # Safety
    ///
    /// There may only be one Odbc environment in any process at any time. Take care using this
    /// function in unit tests, as these run in parallel by default in Rust. Also no library should
    /// probably wrap the creation of an odbc environment into a safe function call. This is because
    /// using two of these "safe" libraries at the same time in different parts of your program may
    /// lead to race condition thus violating Rust's safety guarantees.
    ///
    /// Creating one environment in your binary is safe however.
    pub unsafe fn new() -> Result<Self, Error> {
        let mut handle = null_mut();
        let (handle, info) = match SQLAllocHandle(HandleType::Env, null_mut(), &mut handle) {
            // We can't provide nay diagnostics, as we don't have
            SqlReturn::ERROR => return Err(Error::NoDiagnostics),
            SqlReturn::SUCCESS => (handle, false),
            SqlReturn::SUCCESS_WITH_INFO => (handle, true),
            other => panic!(
                "Unexpected Return value for allocating ODBC Environment: {:?}",
                other
            ),
        };

        debug!("ODBC Environment created.");

        let env = Environment {
            handle: handle as HEnv,
        };
        if info {
            log_diagnostics(&env);
        }
        Ok(env)
    }

    /// Declares which Version of the ODBC API we want to use. This is the first thing that should
    /// be done with any ODBC environment.
    pub fn declare_version(&self, version: AttrOdbcVersion) -> Result<(), Error> {
        unsafe {
            SQLSetEnvAttr(
                self.handle,
                EnvironmentAttribute::OdbcVersion,
                version.into(),
                0,
            )
            .into_result(self)
        }
    }

    /// Allocate a new connection handle. The `Connection` must not outlive the `Environment`.
    pub fn allocate_connection(&self) -> Result<Connection, Error> {
        let mut handle = null_mut();
        unsafe {
            SQLAllocHandle(HandleType::Dbc, self.as_handle(), &mut handle).into_result(self)?;
            Ok(Connection::new(handle as HDbc))
        }
    }

    /// Provides access to the raw ODBC environment handle.
    pub fn as_raw(&self) -> HEnv {
        self.handle
    }

    /// List drivers descriptions and driver attribute keywords.
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines wether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or wether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`]).
    /// * `buffer_description`: In case `true` is returned this buffer is filled with the
    ///   description of the driver.
    /// * `buffer_attributes`: In case `true` is returned this buffer is filled with a list of
    ///   key value attributes. E.g.: `"key1=value1\0key2=value2\0\0"`.
    ///
    /// See [SQLDrivers][1]
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqldrivers-function
    pub fn drivers_buffer_fill(
        &mut self,
        direction: FetchOrientation,
        buffer_description: &mut Vec<u16>,
        buffer_attributes: &mut Vec<u16>,
    ) -> Result<bool, Error> {

        // Use full capacity
        buffer_description.resize(buffer_description.capacity(), 0);
        buffer_attributes.resize(buffer_attributes.capacity(), 0);

        unsafe {
            match SQLDriversW(
                self.handle,
                direction,
                buffer_description.as_mut_ptr(),
                buffer_description.len().try_into().unwrap(),
                null_mut(),
                buffer_attributes.as_mut_ptr(),
                buffer_attributes.len().try_into().unwrap(),
                null_mut(),
            ) {
                SqlReturn::NO_DATA => Ok(false),
                other => {
                    other.into_result(self)?;
                    Ok(true)
                }
            }
        }
    }

    /// Use together with [`drivers_buffer_fill`] to list drivers descriptions and driver attribute
    /// keywords.
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines wether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or wether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`]).
    ///
    /// # Return
    ///
    /// Returns `(driver description length, attribute length)`. Length is in characters minus
    /// terminating zero.
    ///
    /// See [SQLDrivers][1]
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqldrivers-function
    pub fn drivers_buffer_len(
        &mut self,
        direction: FetchOrientation,
    ) -> Result<Option<(i16, i16)>, Error> {
        // Lengths in characters minus terminating zero
        let mut length_description: i16 = 0;
        let mut length_attributes: i16 = 0;
        unsafe {
            // Determine required buffer size
            match SQLDriversW(
                self.handle,
                direction,
                null_mut(),
                0,
                &mut length_description,
                null_mut(),
                0,
                &mut length_attributes,
            ) {
                SqlReturn::NO_DATA => return Ok(None),
                other => other.into_result(self)?,
            }
            Ok(Some((length_description, length_attributes)))
        }
    }
}