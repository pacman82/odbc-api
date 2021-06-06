use super::{
    as_handle::AsHandle, drop_handle, error::IntoResult, logging::log_diagnostics, Connection,
    Error, State,
};
use log::debug;
use odbc_sys::{
    AttrOdbcVersion, EnvironmentAttribute, FetchOrientation, HDbc, HEnv, Handle, HandleType,
    SQLAllocHandle, SQLDataSourcesW, SQLDriversW, SQLSetEnvAttr, SqlReturn,
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
    /// Call this method to enable connection pooling for ODBC before creating the ODBC environment.
    ///
    /// See:
    /// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/driver-manager-connection-pooling>
    ///
    /// # Safety
    ///
    /// > An ODBC driver must be fully thread-safe, and connections must not have thread affinity to
    /// > support connection pooling. This means the driver is able to handle a call on any thread
    /// > at any time and is able to connect on one thread, to use the connection on another thread,
    /// > and to disconnect on a third thread.
    pub unsafe fn set_connection_pooling(
        cp_mode: odbc_sys::AttrConnectionPooling,
    ) -> Result<(), Error> {
        match SQLSetEnvAttr(
            null_mut(),
            odbc_sys::EnvironmentAttribute::ConnectionPooling,
            cp_mode.into(),
            0,
        ) {
            SqlReturn::ERROR => return Err(Error::NoDiagnostics),
            SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => Ok(()),
            other => panic!(
                "Unexpected Return value ('{:?}') for SQLSetEnvAttr then trying to set connection \
                pooling to {:?}", other, cp_mode
            ),
        }
    }

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
            let result = SQLSetEnvAttr(
                self.handle,
                EnvironmentAttribute::OdbcVersion,
                version.into(),
                0,
            )
            .into_result(self);

            // Translate invalid attribute into a more meaningful error, provided the additional
            // context that we know we tried to set version number.
            result.map_err(|error| {
                if let Error::Diagnostics(record) = error {
                    if record.state == State::INVALID_STATE_TRANSACTION {
                        Error::OdbcApiVersionUnsupported(record)
                    } else {
                        Error::Diagnostics(record)
                    }
                } else {
                    error
                }
            })
        }
    }

    /// Allocate a new connection handle. The `Connection` must not outlive the `Environment`.
    pub fn allocate_connection(&self) -> Result<Connection<'_>, Error> {
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
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over driver information at a time.
    /// Method changes environment state. This method would be safe to call via an exclusive `&mut`
    /// reference, yet that would restrict usecases. E.g. requesting information would only be
    /// possible before connections borrow a reference.
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines whether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or whether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`]).
    /// * `buffer_description`: In case `true` is returned this buffer is filled with the
    ///   description of the driver.
    /// * `buffer_attributes`: In case `true` is returned this buffer is filled with a list of
    ///   key value attributes. E.g.: `"key1=value1\0key2=value2\0\0"`.
    ///
    ///  Use [`Environment::drivers_buffer_len`] to determine buffer lengths.
    ///
    /// See [SQLDrivers][1]
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqldrivers-function
    pub unsafe fn drivers_buffer_fill(
        &self,
        direction: FetchOrientation,
        buffer_description: &mut Vec<u16>,
        buffer_attributes: &mut Vec<u16>,
    ) -> Result<bool, Error> {
        // Use full capacity
        buffer_description.resize(buffer_description.capacity(), 0);
        buffer_attributes.resize(buffer_attributes.capacity(), 0);

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

    /// Use together with [`Environment::drivers_buffer_fill`] to list drivers descriptions and driver attribute
    /// keywords.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over driver information at a time.
    /// Method changes environment state. This method would be safe to call via an exclusive `&mut`
    /// reference, yet that would restrict usecases. E.g. requesting information would only be
    /// possible before connections borrow a reference.
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines whether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or whether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`]).
    ///
    /// # Return
    ///
    /// `(driver description length, attribute length)`. Length is in characters minus terminating
    /// terminating zero.
    ///
    /// See [SQLDrivers][1]
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqldrivers-function
    pub unsafe fn drivers_buffer_len(
        &self,
        direction: FetchOrientation,
    ) -> Result<Option<(i16, i16)>, Error> {
        // Lengths in characters minus terminating zero
        let mut length_description: i16 = 0;
        let mut length_attributes: i16 = 0;
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

    /// Use together with [`Environment::data_source_buffer_fill`] to list drivers descriptions and
    /// driver attribute keywords.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over data source information at a
    /// time. Method changes environment state. This method would be safe to call via an exclusive
    /// `&mut` reference, yet that would restrict usecases. E.g. requesting information would only
    /// be possible before connections borrow a reference.
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines whether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or whether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`], [`FetchOrientation::FirstSystem`],
    ///   [`FetchOrientation::FirstUser`]).
    ///
    /// # Return
    ///
    /// `(server name length,  description length)`. Length is in characters minus terminating zero.
    pub unsafe fn data_source_buffer_len(
        &self,
        direction: FetchOrientation,
    ) -> Result<Option<(i16, i16)>, Error> {
        // Lengths in characters minus terminating zero
        let mut length_name: i16 = 0;
        let mut length_description: i16 = 0;
        // Determine required buffer size
        match odbc_sys::SQLDataSourcesW(
            self.handle,
            direction,
            null_mut(),
            0,
            &mut length_name,
            null_mut(),
            0,
            &mut length_description,
        ) {
            SqlReturn::NO_DATA => return Ok(None),
            other => other.into_result(self)?,
        }
        Ok(Some((length_name, length_description)))
    }

    /// List drivers descriptions and driver attribute keywords.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over data source information at a
    /// time. Method changes environment state. This method would be safe to call via an exclusive
    /// `&mut` reference, yet that would restrict usecases. E.g. requesting information would only
    /// be possible before connections borrow a reference.
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines whether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or whether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`], [`FetchOrientation::FirstSystem`],
    ///   [`FetchOrientation::FirstUser`]).
    /// * `buffer_name`: In case `true` is returned this buffer is filled with the name of the
    ///   datasource.
    /// * `buffer_description`: In case `true` is returned this buffer is filled with a description
    ///   of the datasource (i.e. Driver name).
    ///
    ///  Use [`Environment::data_source_buffer_len`] to determine buffer lengths.
    pub unsafe fn data_source_buffer_fill(
        &self,
        direction: FetchOrientation,
        buffer_name: &mut Vec<u16>,
        buffer_description: &mut Vec<u16>,
    ) -> Result<bool, Error> {
        // Use full capacity
        buffer_name.resize(buffer_name.capacity(), 0);
        buffer_description.resize(buffer_description.capacity(), 0);

        match SQLDataSourcesW(
            self.handle,
            direction,
            buffer_name.as_mut_ptr(),
            buffer_name.len().try_into().unwrap(),
            null_mut(),
            buffer_description.as_mut_ptr(),
            buffer_description.len().try_into().unwrap(),
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
