use super::{
    as_handle::AsHandle,
    drop_handle,
    sql_char::SqlChar,
    sql_result::{ExtSqlReturn, SqlResult},
    Connection,
};
use log::debug;
use odbc_sys::{
    AttrCpMatch, AttrOdbcVersion, EnvironmentAttribute, FetchOrientation, HDbc, HEnv, Handle,
    HandleType, SQLAllocHandle, SQLSetEnvAttr,
};
use std::ptr::null_mut;

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use odbc_sys::{SQLDataSources as sql_data_sources, SQLDrivers as sql_drivers};

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
use odbc_sys::{SQLDataSourcesW as sql_data_sources, SQLDriversW as sql_drivers};

/// An `Environment` is a global context, in which to access data.
///
/// Associated with an `Environment` is any information that is global in nature, such as:
///
/// * The `Environment`'s state
/// * The current environment-level diagnostics
/// * The handles of connections currently allocated on the environment
/// * The current setting of each environment attribute
#[derive(Debug)]
pub struct Environment {
    /// Invariant: Should always point to a valid ODBC Environment
    handle: HEnv,
}

/// See: <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/multithreading>
unsafe impl Send for Environment {}

// We are not declaring Environment as Sync due to its interior mutability with regards to iterator
// state and error handilng

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
    /// Enable or disable (default) connection pooling for ODBC connections. Call this function
    /// before creating the ODBC environment for which you want to enable connection pooling.
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
    pub unsafe fn set_connection_pooling(scheme: odbc_sys::AttrConnectionPooling) -> SqlResult<()> {
        SQLSetEnvAttr(
            null_mut(),
            odbc_sys::EnvironmentAttribute::ConnectionPooling,
            scheme.into(),
            odbc_sys::IS_INTEGER,
        )
        .into_sql_result("SQLSetEnvAttr")
    }

    pub fn set_connection_pooling_matching(&mut self, matching: AttrCpMatch) -> SqlResult<()> {
        unsafe {
            SQLSetEnvAttr(
                self.handle,
                odbc_sys::EnvironmentAttribute::CpMatch,
                matching.into(),
                odbc_sys::IS_INTEGER,
            )
        }
        .into_sql_result("SQLSetEnvAttr")
    }

    /// An allocated ODBC Environment handle
    pub fn new() -> SqlResult<Self> {
        // After running a lot of unit tests in parallel on both linux and windows architectures and
        // never seeing a race condition related to this I deem this safe. In the past I feared
        // concurrent construction of multiple Environments might race on shared state. Mostly due
        // to <https://github.com/Koka/odbc-rs/issues/29> and
        // <http://old.vk.pp.ru/docs/sybase-any/interfaces/00000034.htm>. Since however I since
        // however official sources imply it is ok for an application to have multiple environments
        // and I did not get it to race ever on my machine.
        unsafe {
            let mut handle = null_mut();
            let result: SqlResult<()> = SQLAllocHandle(HandleType::Env, null_mut(), &mut handle)
                .into_sql_result("SQLAllocHandle");
            result.on_success(|| Environment {
                handle: handle as HEnv,
            })
        }
    }

    /// Declares which Version of the ODBC API we want to use. This is the first thing that should
    /// be done with any ODBC environment.
    pub fn declare_version(&self, version: AttrOdbcVersion) -> SqlResult<()> {
        unsafe {
            SQLSetEnvAttr(
                self.handle,
                EnvironmentAttribute::OdbcVersion,
                version.into(),
                0,
            )
            .into_sql_result("SQLSetEnvAttr")
        }
    }

    /// Allocate a new connection handle. The `Connection` must not outlive the `Environment`.
    pub fn allocate_connection(&self) -> SqlResult<Connection<'_>> {
        let mut handle = null_mut();
        unsafe {
            SQLAllocHandle(HandleType::Dbc, self.as_handle(), &mut handle)
                .into_sql_result("SQLAllocHandle")
                .on_success(|| {
                    let handle = handle as HDbc;
                    debug!("SQLAllocHandle allocated connection (Dbc) handle '{handle:?}'");
                    Connection::new(handle)
                })
        }
    }

    /// Provides access to the raw ODBC environment handle.
    pub fn as_raw(&self) -> HEnv {
        self.handle
    }

    /// List drivers descriptions and driver attribute keywords. Returns `NoData` to indicate the
    /// end of the list.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over driver information at a time.
    /// Method changes environment state. This method would be safe to call via an exclusive `&mut`
    /// reference, yet that would restrict use cases. E.g. requesting information would only be
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
        buffer_description: &mut [SqlChar],
        buffer_attributes: &mut [SqlChar],
    ) -> SqlResult<()> {
        sql_drivers(
            self.handle,
            direction,
            buffer_description.as_mut_ptr(),
            buffer_description.len().try_into().unwrap(),
            null_mut(),
            buffer_attributes.as_mut_ptr(),
            buffer_attributes.len().try_into().unwrap(),
            null_mut(),
        )
        .into_sql_result("SQLDrivers")
    }

    /// Use together with [`Environment::drivers_buffer_fill`] to list drivers descriptions and
    /// driver attribute keywords.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over driver information at a time.
    /// Method changes environment state. This method would be safe to call via an exclusive `&mut`
    /// reference, yet that would restrict use cases. E.g. requesting information would only be
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
    pub unsafe fn drivers_buffer_len(&self, direction: FetchOrientation) -> SqlResult<(i16, i16)> {
        // Lengths in characters minus terminating zero
        let mut length_description: i16 = 0;
        let mut length_attributes: i16 = 0;
        // Determine required buffer size
        sql_drivers(
            self.handle,
            direction,
            null_mut(),
            0,
            &mut length_description,
            null_mut(),
            0,
            &mut length_attributes,
        )
        .into_sql_result("SQLDrivers")
        .on_success(|| (length_description, length_attributes))
    }

    /// Use together with [`Environment::data_source_buffer_fill`] to list drivers descriptions and
    /// driver attribute keywords.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over data source information at a
    /// time. Method changes environment state. This method would be safe to call via an exclusive
    /// `&mut` reference, yet that would restrict use cases. E.g. requesting information would only
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
    ) -> SqlResult<(i16, i16)> {
        // Lengths in characters minus terminating zero
        let mut length_name: i16 = 0;
        let mut length_description: i16 = 0;
        // Determine required buffer size
        sql_data_sources(
            self.handle,
            direction,
            null_mut(),
            0,
            &mut length_name,
            null_mut(),
            0,
            &mut length_description,
        )
        .into_sql_result("SQLDataSources")
        .on_success(|| (length_name, length_description))
    }

    /// List drivers descriptions and driver attribute keywords.
    ///
    /// # Safety
    ///
    /// Callers need to make sure only one thread is iterating over data source information at a
    /// time. Method changes environment state. This method would be safe to call via an exclusive
    /// `&mut` reference, yet that would restrict use cases. E.g. requesting information would only
    /// be possible before connections borrow a reference. [`SqlResult::NoData`]
    ///
    /// # Parameters
    ///
    /// * `direction`: Determines whether the Driver Manager fetches the next driver in the list
    ///   ([`FetchOrientation::Next`]) or whether the search starts from the beginning of the list
    ///   ([`FetchOrientation::First`], [`FetchOrientation::FirstSystem`],
    ///   [`FetchOrientation::FirstUser`]).
    /// * `buffer_name`: 0illed with the name of the datasource if available
    /// * `buffer_description`: Filled with a description of the datasource (i.e. Driver name).
    ///
    ///  Use [`Environment::data_source_buffer_len`] to determine buffer lengths.
    pub unsafe fn data_source_buffer_fill(
        &self,
        direction: FetchOrientation,
        buffer_name: &mut [SqlChar],
        buffer_description: &mut [SqlChar],
    ) -> SqlResult<()> {
        sql_data_sources(
            self.handle,
            direction,
            buffer_name.as_mut_ptr(),
            buffer_name.len().try_into().unwrap(),
            null_mut(),
            buffer_description.as_mut_ptr(),
            buffer_description.len().try_into().unwrap(),
            null_mut(),
        )
        .into_sql_result("SQLDataSources")
    }
}
