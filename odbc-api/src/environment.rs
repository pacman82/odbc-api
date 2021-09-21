use std::{cmp::max, collections::HashMap, ptr::null_mut, sync::Mutex};

use crate::{
    handles::{self, log_diagnostics, OutputStringBuffer, SqlResult, State},
    Connection, Error,
};
use log::debug;
use odbc_sys::{AttrCpMatch, AttrOdbcVersion, FetchOrientation, HWnd};
use widestring::{U16CStr, U16Str, U16String};

// Currently only windows driver manager supports prompt.
#[cfg(target_os = "windows")]
use raw_window_handle::{HasRawWindowHandle, RawWindowHandle};

/// An ODBC 3.8 environment.
///
/// Associated with an `Environment` is any information that is global in nature, such as:
///
/// * The `Environment`'s state
/// * The current environment-level diagnostics
/// * The handles of connections currently allocated on the environment
/// * The current stetting of each environment attribute
///
/// Creating the environment is the first applications do, then interacting with an ODBC driver
/// manager. There must only be one environment in the entire process.
pub struct Environment {
    environment: handles::Environment,
    /// ODBC environments use interior mutability to maintain iterator state then iterating over
    /// driver and / or data source information. The environment is otherwise protected by interior
    /// synchronization mechanism, yet in order to be able to access to iterate over information
    /// using a shared reference we need to protect the interior iteration state with a mutex of its
    /// own.
    info_iterator_state: Mutex<()>,
}

impl Environment {
    /// Enable or disable (default) connection pooling for ODBC connections. Call this function
    /// before creating the ODBC environment for which you want to enable connection pooling.
    ///
    /// ODBC specifies an interface to enable the driver manager to enable connection pooling for
    /// your application. It is of by default, but if you use ODBC to connect to your data source
    /// instead of implementing it in your application, or importing a library you may simply enable
    /// it in ODBC instead.
    /// Connection Pooling is governed by two attributes. The most important one is the connection
    /// pooling scheme which is `Off` by default. It must be set even before you create your ODBC
    /// environment. It is global mutable state on the process level. Setting it in Rust is therefore
    /// unsafe.
    ///
    /// The other one is changed via [`Self::set_connection_pooling_matching`]. It governs how a
    /// connection is choosen from the pool. It defaults to strict which means the `Connection` you
    /// get from the pool will have exactly the attributes specified in the connection string.
    ///
    /// See:
    /// <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/driver-manager-connection-pooling>
    ///
    /// # Example
    ///
    /// ```
    /// use lazy_static::lazy_static;
    /// use odbc_api::{Environment, sys::{AttrConnectionPooling, AttrCpMatch}};
    ///
    /// lazy_static! {
    ///     pub static ref ENV: Environment = unsafe {
    ///         // Enable connection pooling. Let driver decide wether the attributes of two connection
    ///         // are similar enough to change the attributes of a pooled one, to fit the requested
    ///         // connection, or if it is cheaper to create a new Connection from scratch.
    ///         // See <https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/driver-aware-connection-pooling>
    ///         Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
    ///         let mut env = Environment::new().unwrap();
    ///         // Strict is the default, and is set here to be explicit about it.
    ///         env.set_connection_pooling_matching(AttrCpMatch::Strict).unwrap();
    ///         env
    ///     };
    /// }
    /// ```
    ///
    /// # Safety
    ///
    /// > An ODBC driver must be fully thread-safe, and connections must not have thread affinity to
    /// > support connection pooling. This means the driver is able to handle a call on any thread
    /// > at any time and is able to connect on one thread, to use the connection on another thread,
    /// > and to disconnect on a third thread.
    pub unsafe fn set_connection_pooling(
        scheme: odbc_sys::AttrConnectionPooling,
    ) -> Result<(), Error> {
        match handles::Environment::set_connection_pooling(scheme) {
            SqlResult::Error { .. } => Err(Error::NoDiagnostics),
            SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => Ok(()),
        }
    }

    /// Determines how a connection is chosen from a connection pool. When [`Self::connect`],
    /// [`Self::connect_with_connection_string`] or [`Self::driver_connect`] is called, the Driver
    /// Manager determines which connection is reused from the pool. The Driver Manager tries to
    /// match the connection options in the call and the connection attributes set by the
    /// application to the keywords and connection attributes of the connections in the pool. The
    /// value of this attribute determines the level of precision of the matching criteria.
    ///
    /// The following values are used to set the value of this attribute:
    ///
    /// * [`crate::sys::AttrCpMatch::Strict`] = Only connections that exactly match the connection
    /// options in the call and the connection attributes set by the application are reused. This is
    /// the default.
    /// * [`crate::sys::AttrCpMatch::Relaxed`] = Connections with matching connection string \
    /// keywords can be used. Keywords must match, but not all connection attributes must match.
    pub fn set_connection_pooling_matching(&mut self, matching: AttrCpMatch) -> Result<(), Error> {
        match self.environment.set_connection_pooling_matching(matching) {
            SqlResult::Error { .. } => Err(Error::NoDiagnostics),
            SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => Ok(()),
        }
    }

    /// Entry point into this API. Allocates a new ODBC Environment and declares to the driver
    /// manager that the Application wants to use ODBC version 3.8.
    ///
    /// # Safety
    ///
    /// There may only be one ODBC environment in any process at any time. Take care using this
    /// function in unit tests, as these run in parallel by default in Rust. Also no library should
    /// probably wrap the creation of an odbc environment into a safe function call. This is because
    /// using two of these "safe" libraries at the same time in different parts of your program may
    /// lead to race condition thus violating Rust's safety guarantees.
    ///
    /// Creating one environment in your binary is safe however.
    pub unsafe fn new() -> Result<Self, Error> {
        let result = handles::Environment::new();

        let environment = match result {
            SqlResult::Success(env) => env,
            SqlResult::SuccessWithInfo(env) => {
                log_diagnostics(&env);
                env
            }
            SqlResult::Error { .. } => return Err(Error::NoDiagnostics),
        };

        debug!("ODBC Environment created.");

        let result = environment
            .declare_version(AttrOdbcVersion::Odbc3_80)
            .into_result(&environment);

        // Translate invalid attribute into a more meaningful error, provided the additional
        // context that we know we tried to set version number.
        result.map_err(|error| {
            if let Error::Diagnostics { record, function } = error {
                if record.state == State::INVALID_STATE_TRANSACTION {
                    Error::UnsupportedOdbcApiVersion(record)
                } else {
                    Error::Diagnostics { record, function }
                }
            } else {
                error
            }
        })?;

        Ok(Self {
            environment,
            info_iterator_state: Mutex::new(()),
        })
    }

    /// Allocates a connection handle and establishes connections to a driver and a data source.
    ///
    /// * See [Connecting with SQLConnect][1]
    /// * See [SQLConnectFunction][2]
    ///
    /// # Arguments
    ///
    /// * `data_source_name` - Data source name. The data might be located on the same computer as
    /// the program, or on another computer somewhere on a network.
    /// * `user` - User identifier.
    /// * `pwd` - Authentication string (typically the password).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
    /// // making this call safe.
    /// let env = unsafe {
    ///     Environment::new()?
    /// };
    ///
    /// let mut conn = env.connect("YourDatabase", "SA", "<YourStrong@Passw0rd>")?;
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqlconnect-function
    /// [2]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqlconnect-function
    pub fn connect(
        &self,
        data_source_name: &str,
        user: &str,
        pwd: &str,
    ) -> Result<Connection<'_>, Error> {
        let data_source_name = U16String::from_str(data_source_name);
        let user = U16String::from_str(user);
        let pwd = U16String::from_str(pwd);
        self.connect_utf16(&data_source_name, &user, &pwd)
    }

    /// Allocates a connection handle and establishes connections to a driver and a data source.
    ///
    /// * See [Connecting with SQLConnect][1]
    /// * See [SQLConnectFunction][2]
    ///
    /// # Arguments
    ///
    /// * `data_source_name` - Data source name. The data might be located on the same computer as
    /// the program, or on another computer somewhere on a network.
    /// * `user` - User identifier.
    /// * `pwd` - Authentication string (typically the password).
    ///
    /// [1]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqlconnect-function
    /// [2]: https://docs.microsoft.com/sql/odbc/reference/syntax/sqlconnect-function
    pub fn connect_utf16(
        &self,
        data_source_name: &U16Str,
        user: &U16Str,
        pwd: &U16Str,
    ) -> Result<Connection<'_>, Error> {
        let mut connection = self
            .environment
            .allocate_connection()
            .into_result(&self.environment)?;
        connection
            .connect(data_source_name, user, pwd)
            .into_result(&connection)?;
        Ok(Connection::new(connection))
    }

    /// Allocates a connection handle and establishes connections to a driver and a data source.
    ///
    /// An alternative to `connect`. It supports data sources that require more connection
    /// information than the three arguments in `connect` and data sources that are not defined in
    /// the system information.
    ///
    /// To find out your connection string try: <https://www.connectionstrings.com/>
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// // I herby solemnly swear that this is the only ODBC environment in the entire process, thus
    /// // making this call safe.
    /// let env = unsafe {
    ///     Environment::new()?
    /// };
    ///
    /// let connection_string = "
    ///     Driver={ODBC Driver 17 for SQL Server};\
    ///     Server=localhost;\
    ///     UID=SA;\
    ///     PWD=<YourStrong@Passw0rd>;\
    /// ";
    ///
    /// let mut conn = env.connect_with_connection_string(connection_string)?;
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn connect_with_connection_string(
        &self,
        connection_string: &str,
    ) -> Result<Connection<'_>, Error> {
        let connection_string = U16String::from_str(connection_string);
        self.connect_with_connection_string_utf16(&connection_string)
    }

    /// Allocates a connection handle and establishes connections to a driver and a data source.
    ///
    /// An alternative to `connect`. It supports data sources that require more connection
    /// information than the three arguments in `connect` and data sources that are not defined in
    /// the system information.
    ///
    /// To find out your connection string try: <https://www.connectionstrings.com/>
    pub fn connect_with_connection_string_utf16(
        &self,
        connection_string: &U16Str,
    ) -> Result<Connection<'_>, Error> {
        let mut connection = self
            .environment
            .allocate_connection()
            .into_result(&self.environment)?;
        connection
            .connect_with_connection_string(connection_string)
            .into_result(&connection)?;
        Ok(Connection::new(connection))
    }

    /// Allocates a connection handle and establishes connections to a driver and a data source.
    ///
    /// An alternative to `connect` and `connect_with_connection_string`. This method can be
    /// provided with an incomplete or even empty connection string. If any additional information
    /// is required, the driver manager/driver will attempt to create a prompt to allow the user to
    /// provide the additional information.
    ///
    /// If the connection is successful, the complete connection string (including any information
    /// provided by the user through a prompt) is returned.
    ///
    /// # Parameters
    ///
    /// * `connection_string`: Connection string.
    /// * `completed_connection_string`: Output buffer with the complete connection string. It is
    ///   recommended to choose a buffer with at least `1024` bytes length. **Note**: Some driver
    ///   implementation have poor error handling in case the provided buffer is too small. At the
    ///   time of this writing:
    ///   * Maria DB crashes with STATUS_TACK_BUFFER_OVERRUN
    ///   * SQLite does not change the output buffer at all and does not indicate truncation.
    /// * `driver_completion`: Specifies how and if the driver manager uses a prompt to complete
    ///   the provided connection string.
    ///
    /// # Examples
    ///
    /// In the first example, we intentionally provide a blank connection string so the user will be
    /// prompted to select a data source to use. Note that this functionality is only available on
    /// windows.
    ///
    /// ```no_run
    /// # #[cfg(target_os = "widows")]
    /// # fn f(parent_window: impl raw_window_handle::HasRawWindowHandle)
    /// #   -> Result<(), odbc_api::Error> {
    /// use odbc_api::{Environment, handles::OutputStringBuffer, DriverCompleteOption};
    ///
    /// // I hereby solemnly swear that this is the only ODBC environment in the entire process,
    /// // thus making this call safe.
    /// let env = unsafe {
    ///     Environment::new()?
    /// };
    ///
    /// let mut output_buffer = OutputStringBuffer::with_buffer_size(1024);
    /// let connection = env.driver_connect(
    ///     "",
    ///     Some(&mut output_buffer),
    ///     DriverCompleteOption::Prompt(&parent_window),
    /// )?;
    ///
    /// // Check that the output buffer has been large enough to hold the entire connection string.
    /// assert!(!output_buffer.is_truncated());
    ///
    /// // Now `connection_string` will contain the data source selected by the user.
    /// let connection_string = output_buffer.to_utf8();
    /// # Ok(()) }
    /// ```
    ///
    /// In the following examples we specify a DSN that requires login credentials, but the DSN does
    /// not provide those credentials. Instead, the user will be prompted for a UID and PWD. The
    /// returned `connection_string` will contain the `UID` and `PWD` provided by the user. Note
    /// that this functionality is currently only available on windows targets.
    ///
    /// ```
    /// # use odbc_api::DriverCompleteOption;
    /// # #[cfg(target_os = "windows")]
    /// # fn f(
    /// #    parent_window: impl raw_window_handle::HasRawWindowHandle,
    /// #    mut output_buffer: odbc_api::handles::OutputStringBuffer,
    /// #    env: odbc_api::Environment,
    /// # ) -> Result<(), odbc_api::Error> {
    /// let without_uid_or_pwd = "DSN=SomeSharedDatabase;";
    /// let connection = env.driver_connect(
    ///     &without_uid_or_pwd,
    ///     Some(&mut output_buffer),
    ///     DriverCompleteOption::Complete(&parent_window),
    /// )?;
    /// let connection_string = output_buffer.to_utf8();
    ///
    /// // Now `connection_string` might be something like
    /// // `DSN=SomeSharedDatabase;UID=SA;PWD=<YourStrong@Passw0rd>;`
    /// # Ok(()) }
    /// ```
    ///
    /// In this case, we use a DSN that is already sufficient and does not require a prompt. Because
    /// a prompt is not needed, `window` is also not required. The returned `connection_string` will
    /// be mostly the same as `already_sufficient` but the driver may append some extra attributes.
    ///
    /// ```
    /// # use odbc_api::DriverCompleteOption;
    /// # fn f(
    /// #    mut output_buffer: odbc_api::handles::OutputStringBuffer,
    /// #    env: odbc_api::Environment,
    /// # ) -> Result<(), odbc_api::Error> {
    /// let already_sufficient = "DSN=MicrosoftAccessFile;";
    /// let connection = env.driver_connect(
    ///    &already_sufficient,
    ///    Some(&mut output_buffer),
    ///    DriverCompleteOption::NoPrompt,
    /// )?;
    /// let connection_string = output_buffer.to_utf8();
    ///
    /// // Now `connection_string` might be something like
    /// // `DSN=MicrosoftAccessFile;DBQ=C:\Db\Example.accdb;DriverId=25;FIL=MS Access;MaxBufferSize=2048;`
    /// # Ok(()) }
    /// ```
    pub fn driver_connect(
        &self,
        connection_string: &str,
        completed_connection_string: Option<&mut OutputStringBuffer>,
        driver_completion: DriverCompleteOption<'_>,
    ) -> Result<Connection<'_>, Error> {
        let mut connection = self
            .environment
            .allocate_connection()
            .into_result(&self.environment)?;
        let connection_string = U16String::from_str(connection_string);

        unsafe {
            connection.driver_connect(
                &connection_string,
                driver_completion.parent_window(),
                completed_connection_string,
                driver_completion.as_sys(),
            )
        }
        .map(|res| res.into_result(&connection))
        .unwrap_or(Err(Error::AbortedConnectionStringCompletion))?;
        Ok(Connection::new(connection))
    }

    /// Get information about available drivers. Only 32 or 64 Bit drivers will be listed, depending
    /// on wether you are building a 32 Bit or 64 Bit application.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let env = unsafe { Environment::new () }?;
    /// for driver_info in env.drivers()? {
    ///     println!("{:#?}", driver_info);
    /// }
    ///
    /// # Ok::<_, odbc_api::Error>(())
    /// ```
    pub fn drivers(&self) -> Result<Vec<DriverInfo>, Error> {
        let mut driver_info = Vec::new();

        // Since we have exclusive ownership of the environment handle and we take the lock, we can
        // guarantee that this method is currently the only one changing the state of the internal
        // iterators of the environment.
        let _lock = self.info_iterator_state.lock().unwrap();
        unsafe {
            // Find required buffer size to avoid truncation.
            let (mut desc_len, mut attr_len) = if let Some(res) = self
                .environment
                // Start with first so we are independent of state
                .drivers_buffer_len(FetchOrientation::First)
            {
                res.into_result(&self.environment)?
            } else {
                // No drivers present
                return Ok(Vec::new());
            };

            // If there are let's loop over the rest
            while let Some(res) = self.environment.drivers_buffer_len(FetchOrientation::Next) {
                let (candidate_desc_len, candidate_attr_len) =
                    res.into_result(&self.environment)?;
                desc_len = max(candidate_desc_len, desc_len);
                attr_len = max(candidate_attr_len, attr_len);
            }

            // Allocate +1 character extra for terminating zero
            let mut desc_buf = vec![0; desc_len as usize + 1];
            let mut attr_buf = vec![0; attr_len as usize + 1];

            while self
                .environment
                .drivers_buffer_fill(FetchOrientation::Next, &mut desc_buf, &mut attr_buf)
                .map(|res| res.into_result(&self.environment))
                .transpose()?
                .is_some()
            {
                let description = U16CStr::from_slice_with_nul(&desc_buf).unwrap();
                let attributes = U16CStr::from_slice_with_nul(&attr_buf).unwrap();

                let description = description.to_string().unwrap();
                let attributes = attributes.to_string().unwrap();
                let attributes = attributes_iter(&attributes).collect();

                driver_info.push(DriverInfo {
                    description,
                    attributes,
                });
            }
        }

        Ok(driver_info)
    }

    /// User and system data sources
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let env = unsafe { Environment::new () }?;
    /// for data_source in env.data_sources()? {
    ///     println!("{:#?}", data_source);
    /// }
    ///
    /// # Ok::<_, odbc_api::Error>(())
    /// ```
    pub fn data_sources(&self) -> Result<Vec<DataSourceInfo>, Error> {
        self.data_sources_impl(FetchOrientation::First)
    }

    /// Only system data sources
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let env = unsafe { Environment::new () }?;
    /// for data_source in env.system_data_sources()? {
    ///     println!("{:#?}", data_source);
    /// }
    ///
    /// # Ok::<_, odbc_api::Error>(())
    /// ```
    pub fn system_data_sources(&self) -> Result<Vec<DataSourceInfo>, Error> {
        self.data_sources_impl(FetchOrientation::FirstSystem)
    }

    /// Only user data sources
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let mut env = unsafe { Environment::new () }?;
    /// for data_source in env.user_data_sources()? {
    ///     println!("{:#?}", data_source);
    /// }
    ///
    /// # Ok::<_, odbc_api::Error>(())
    /// ```
    pub fn user_data_sources(&self) -> Result<Vec<DataSourceInfo>, Error> {
        self.data_sources_impl(FetchOrientation::FirstUser)
    }

    fn data_sources_impl(&self, direction: FetchOrientation) -> Result<Vec<DataSourceInfo>, Error> {
        let mut data_source_info = Vec::new();

        // Since we have exclusive ownership of the environment handle and we take the lock, we can
        // guarantee that this method is currently the only one changing the state of the internal
        // iterators of the environment.
        let _lock = self.info_iterator_state.lock().unwrap();
        unsafe {
            // Find required buffer size to avoid truncation.
            let (mut server_name_len, mut driver_len) =
                if let Some(res) = self.environment.data_source_buffer_len(direction) {
                    res.into_result(&self.environment)?
                } else {
                    // No drivers present
                    return Ok(Vec::new());
                };

            // If there are let's loop over the rest
            while let Some((candidate_name_len, candidate_decs_len)) = self
                .environment
                .drivers_buffer_len(FetchOrientation::Next)
                .map(|res| res.into_result(&self.environment))
                .transpose()?
            {
                server_name_len = max(candidate_name_len, server_name_len);
                driver_len = max(candidate_decs_len, driver_len);
            }

            // Allocate +1 character extra for terminating zero
            let mut server_name_buf = vec![0; server_name_len as usize + 1];
            let mut driver_buf = vec![0; driver_len as usize + 1];

            let mut not_empty = self
                .environment
                .data_source_buffer_fill(direction, &mut server_name_buf, &mut driver_buf)
                .map(|res| res.into_result(&self.environment))
                .transpose()?
                .is_some();

            while not_empty {
                let server_name = U16CStr::from_slice_with_nul(&server_name_buf).unwrap();
                let driver = U16CStr::from_slice_with_nul(&driver_buf).unwrap();

                let server_name = server_name.to_string().unwrap();
                let driver = driver.to_string().unwrap();

                data_source_info.push(DataSourceInfo {
                    server_name,
                    driver,
                });
                not_empty = self
                    .environment
                    .data_source_buffer_fill(
                        FetchOrientation::Next,
                        &mut server_name_buf,
                        &mut driver_buf,
                    )
                    .map(|res| res.into_result(&self.environment))
                    .transpose()?
                    .is_some()
            }
        }

        Ok(data_source_info)
    }
}

#[cfg(not(target_os = "windows"))]
#[doc(hidden)]
pub struct NonExhaustive<'a>(std::marker::PhantomData<&'a i32>);

/// Specifies how the driver and driver manager complete the incoming connection string. See
/// [`Environment::driver_connect`].
pub enum DriverCompleteOption<'a> {
    /// Do not show a prompt to the user. This implies that the connection string, must already
    /// provide all information needed to Connect to the data source, otherwise the operation fails.
    /// This is the only variant on non windows platforms
    NoPrompt,
    /// Always show a prompt to the user.
    #[cfg(target_os = "windows")]
    Prompt(&'a dyn HasRawWindowHandle),
    /// Only show a prompt to the user if the information in the connection string is not sufficient
    /// to connect to the data source.
    #[cfg(target_os = "windows")]
    Complete(&'a dyn HasRawWindowHandle),
    /// Like complete, but the user may not change any information already provided within the
    /// connection string.
    #[cfg(target_os = "windows")]
    CompleteRequired(&'a dyn HasRawWindowHandle),
    // Solves two problems for us.
    //
    // 1. We do not need a breaking change if we decide to add prompt support for non windows
    //    platforms
    // 2. Provides us with a place for the 'a lifetime so the borrow checker won't complain.
    #[cfg(not(target_os = "windows"))]
    #[doc(hidden)]
    __NonExhaustive(NonExhaustive<'a>),
}

impl<'a> DriverCompleteOption<'a> {
    #[cfg(target_os = "windows")]
    pub fn parent_window(&self) -> HWnd {
        let has_raw_window_handle = match self {
            DriverCompleteOption::NoPrompt => return null_mut(),
            DriverCompleteOption::Prompt(hrwh)
            | DriverCompleteOption::Complete(hrwh)
            | DriverCompleteOption::CompleteRequired(hrwh) => hrwh,
        };
        match has_raw_window_handle.raw_window_handle() {
            RawWindowHandle::Windows(handle) => handle.hwnd,
            _ => null_mut(),
        }
    }

    #[cfg(not(target_os = "windows"))]
    pub fn parent_window(&self) -> HWnd {
        null_mut()
    }

    #[cfg(target_os = "windows")]
    pub fn as_sys(&self) -> odbc_sys::DriverConnectOption {
        match self {
            DriverCompleteOption::NoPrompt => odbc_sys::DriverConnectOption::NoPrompt,
            DriverCompleteOption::Prompt(_) => odbc_sys::DriverConnectOption::Prompt,
            DriverCompleteOption::Complete(_) => odbc_sys::DriverConnectOption::Complete,
            DriverCompleteOption::CompleteRequired(_) => {
                odbc_sys::DriverConnectOption::CompleteRequired
            }
        }
    }

    #[cfg(not(target_os = "windows"))]
    pub fn as_sys(&self) -> odbc_sys::DriverConnectOption {
        odbc_sys::DriverConnectOption::NoPrompt
    }
}

/// Struct holding information available on a driver. Can be obtained via [`Environment::drivers`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DriverInfo {
    /// Name of the ODBC driver
    pub description: String,
    /// Attributes values of the driver by key
    pub attributes: HashMap<String, String>,
}

/// Holds name and description of a datasource
///
/// Can be obtained via [`Environment::data_sources`]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSourceInfo {
    /// Name of the data source
    pub server_name: String,
    /// Description of the data source
    pub driver: String,
}

/// Called by drivers to pares list of attributes
///
/// Key value pairs are separated by `\0`. Key and value are separated by `=`
fn attributes_iter(attributes: &str) -> impl Iterator<Item = (String, String)> + '_ {
    attributes
        .split('\0')
        .take_while(|kv_str| *kv_str != String::new())
        .map(|kv_str| {
            let mut iter = kv_str.split('=');
            let key = iter.next().unwrap();
            let value = iter.next().unwrap();
            (key.to_string(), value.to_string())
        })
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn parse_attributes() {
        let buffer = "APILevel=2\0ConnectFunctions=YYY\0CPTimeout=60\0DriverODBCVer=03.\
                      50\0FileUsage=0\0SQLLevel=1\0UsageCount=1\0\0";
        let attributes: HashMap<_, _> = attributes_iter(buffer).collect();
        assert_eq!(attributes["APILevel"], "2");
        assert_eq!(attributes["ConnectFunctions"], "YYY");
        assert_eq!(attributes["CPTimeout"], "60");
        assert_eq!(attributes["DriverODBCVer"], "03.50");
        assert_eq!(attributes["FileUsage"], "0");
        assert_eq!(attributes["SQLLevel"], "1");
        assert_eq!(attributes["UsageCount"], "1");
    }
}
