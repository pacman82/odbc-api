use std::{
    cmp::max,
    collections::HashMap,
    ptr::null_mut,
    sync::{Mutex, OnceLock},
};

use crate::{
    connection::ConnectionOptions,
    error::ExtendResult,
    handles::{
        self, log_diagnostics, slice_to_utf8, OutputStringBuffer, SqlChar, SqlResult, SqlText,
        State, SzBuffer,
    },
    Connection, DriverCompleteOption, Error,
};
use log::debug;
use odbc_sys::{AttrCpMatch, AttrOdbcVersion, FetchOrientation, HWnd};

#[cfg(target_os = "windows")]
// Currently only windows driver manager supports prompt.
use winit::{
    application::ApplicationHandler,
    event::WindowEvent,
    event_loop::{ActiveEventLoop, EventLoop},
    platform::run_on_demand::EventLoopExtRunOnDemand,
    window::{Window, WindowId},
};

#[cfg(not(feature = "odbc_version_3_5"))]
const ODBC_API_VERSION: AttrOdbcVersion = AttrOdbcVersion::Odbc3_80;

#[cfg(feature = "odbc_version_3_5")]
const ODBC_API_VERSION: AttrOdbcVersion = AttrOdbcVersion::Odbc3;

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
#[derive(Debug)]
pub struct Environment {
    environment: handles::Environment,
    /// ODBC environments use interior mutability to maintain iterator state then iterating over
    /// driver and / or data source information. The environment is otherwise protected by interior
    /// synchronization mechanism, yet in order to be able to access to iterate over information
    /// using a shared reference we need to protect the interior iteration state with a mutex of its
    /// own.
    /// The environment is also mutable with regards to Errors, which are accessed over the handle.
    /// If multiple fallible operations are executed in parallel, we need the mutex to ensure the
    /// errors are fetched by the correct thread.
    internal_state: Mutex<()>,
}

unsafe impl Sync for Environment {}

impl Environment {
    /// Enable or disable (default) connection pooling for ODBC connections. Call this function
    /// before creating the ODBC environment for which you want to enable connection pooling.
    ///
    /// ODBC specifies an interface to enable the driver manager to enable connection pooling for
    /// your application. It is off by default, but if you use ODBC to connect to your data source
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
    /// ```no_run
    /// use odbc_api::{Environment, sys::{AttrConnectionPooling, AttrCpMatch}};
    ///
    /// /// Create an environment with connection pooling enabled.
    /// let env = unsafe {
    ///     Environment::set_connection_pooling(AttrConnectionPooling::DriverAware).unwrap();
    ///     let mut env = Environment::new().unwrap();
    ///     // Strict is the default, and is set here to be explicit about it.
    ///     env.set_connection_pooling_matching(AttrCpMatch::Strict).unwrap();
    ///     env
    /// };
    /// ```
    ///
    /// # Safety
    ///
    /// > An ODBC driver must be fully thread-safe, and connections must not have thread affinity to
    /// > support connection pooling. This means the driver is able to handle a call on any thread
    /// > at any time and is able to connect on one thread, to use the connection on another thread,
    /// > and to disconnect on a third thread.
    ///
    /// Also note that this is changes global mutable state for the entire process. As such it is
    /// vulnerable to race conditions if called from more than one place in your application. It is
    /// recommened to call this in the beginning, before creating any connection.
    pub unsafe fn set_connection_pooling(
        scheme: odbc_sys::AttrConnectionPooling,
    ) -> Result<(), Error> {
        match handles::Environment::set_connection_pooling(scheme) {
            SqlResult::Error { .. } => Err(Error::FailedSettingConnectionPooling),
            SqlResult::Success(()) | SqlResult::SuccessWithInfo(()) => Ok(()),
            other => {
                panic!("Unexpected return value `{other:?}`.")
            }
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
    ///   options in the call and the connection attributes set by the application are reused. This
    ///   is the default.
    /// * [`crate::sys::AttrCpMatch::Relaxed`] = Connections with matching connection string \
    ///   keywords can be used. Keywords must match, but not all connection attributes must match.
    pub fn set_connection_pooling_matching(&mut self, matching: AttrCpMatch) -> Result<(), Error> {
        self.environment
            .set_connection_pooling_matching(matching)
            .into_result(&self.environment)
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
    pub fn new() -> Result<Self, Error> {
        let result = handles::Environment::new();

        let environment = match result {
            SqlResult::Success(env) => env,
            SqlResult::SuccessWithInfo(env) => {
                log_diagnostics(&env);
                env
            }
            SqlResult::Error { .. } => return Err(Error::FailedAllocatingEnvironment),
            other => panic!("Unexpected return value '{other:?}'"),
        };

        debug!("ODBC Environment created.");

        debug!("Setting ODBC API version to {ODBC_API_VERSION:?}");
        let result = environment
            .declare_version(ODBC_API_VERSION)
            .into_result(&environment);

        // Status code S1009 has been seen with unixODBC 2.3.1. S1009 meant (among other things)
        // invalid attribute. If we see this then we try to declare the ODBC version it is of course
        // likely that the driver manager only knows ODBC 2.x.
        // See: <https://learn.microsoft.com/sql/odbc/reference/develop-app/sqlstate-mappings>
        const ODBC_2_INVALID_ATTRIBUTE: State = State(*b"S1009");

        // Translate invalid attribute into a more meaningful error, provided the additional
        // context that we know we tried to set version number.
        result.provide_context_for_diagnostic(|record, function| match record.state {
            // INVALID_STATE_TRANSACTION has been seen with some really old version of unixODBC on
            // a CentOS used to build manylinux wheels, with the preinstalled ODBC version.
            // INVALID_ATTRIBUTE_VALUE is the correct status code to emit for a driver manager if it
            // does not know the version and has been seen with an unknown version of unixODBC on an
            // Oracle Linux.
            ODBC_2_INVALID_ATTRIBUTE
            | State::INVALID_STATE_TRANSACTION
            | State::INVALID_ATTRIBUTE_VALUE => Error::UnsupportedOdbcApiVersion(record),
            _ => Error::Diagnostics { record, function },
        })?;

        Ok(Self {
            environment,
            internal_state: Mutex::new(()),
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
    ///   the program, or on another computer somewhere on a network.
    /// * `user` - User identifier.
    /// * `pwd` - Authentication string (typically the password).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::{Environment, ConnectionOptions};
    ///
    /// let env = Environment::new()?;
    ///
    /// let mut conn = env.connect(
    ///     "YourDatabase", "SA", "My@Test@Password1",
    ///     ConnectionOptions::default()
    /// )?;
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
        options: ConnectionOptions,
    ) -> Result<Connection<'_>, Error> {
        let data_source_name = SqlText::new(data_source_name);
        let user = SqlText::new(user);
        let pwd = SqlText::new(pwd);

        let mut connection = self.allocate_connection()?;

        options.apply(&connection)?;

        connection
            .connect(&data_source_name, &user, &pwd)
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
    /// use odbc_api::{ConnectionOptions, Environment};
    ///
    /// let env = Environment::new()?;
    ///
    /// let connection_string = "
    ///     Driver={ODBC Driver 18 for SQL Server};\
    ///     Server=localhost;\
    ///     UID=SA;\
    ///     PWD=My@Test@Password1;\
    /// ";
    ///
    /// let mut conn = env.connect_with_connection_string(
    ///     connection_string,
    ///     ConnectionOptions::default()
    /// )?;
    /// # Ok::<(), odbc_api::Error>(())
    /// ```
    pub fn connect_with_connection_string(
        &self,
        connection_string: &str,
        options: ConnectionOptions,
    ) -> Result<Connection<'_>, Error> {
        let connection_string = SqlText::new(connection_string);
        let mut connection = self.allocate_connection()?;

        options.apply(&connection)?;

        connection
            .connect_with_connection_string(&connection_string)
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
    ///   the provided connection string. For arguments other than
    ///   [`crate::DriverCompleteOption::NoPrompt`] this method is going to create a message only
    ///   parent window for you on windows. On other platform this method is going to panic. In case
    ///   you want to provide your own parent window please use [`Self::driver_connect_with_hwnd`].
    ///
    /// # Examples
    ///
    /// In the first example, we intentionally provide a blank connection string so the user will be
    /// prompted to select a data source to use. Note that this functionality is only available on
    /// windows.
    ///
    /// ```no_run
    /// use odbc_api::{Environment, handles::OutputStringBuffer, DriverCompleteOption};
    ///
    /// let env = Environment::new()?;
    ///
    /// let mut output_buffer = OutputStringBuffer::with_buffer_size(1024);
    /// let connection = env.driver_connect(
    ///     "",
    ///     &mut output_buffer,
    ///     DriverCompleteOption::Prompt,
    /// )?;
    ///
    /// // Check that the output buffer has been large enough to hold the entire connection string.
    /// assert!(!output_buffer.is_truncated());
    ///
    /// // Now `connection_string` will contain the data source selected by the user.
    /// let connection_string = output_buffer.to_utf8();
    /// # Ok::<_,odbc_api::Error>(())
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
    /// #    mut output_buffer: odbc_api::handles::OutputStringBuffer,
    /// #    env: odbc_api::Environment,
    /// # ) -> Result<(), odbc_api::Error> {
    /// let without_uid_or_pwd = "DSN=SomeSharedDatabase;";
    /// let connection = env.driver_connect(
    ///     &without_uid_or_pwd,
    ///     &mut output_buffer,
    ///     DriverCompleteOption::Complete,
    /// )?;
    /// let connection_string = output_buffer.to_utf8();
    ///
    /// // Now `connection_string` might be something like
    /// // `DSN=SomeSharedDatabase;UID=SA;PWD=My@Test@Password1;`
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
    ///    &mut output_buffer,
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
        completed_connection_string: &mut OutputStringBuffer,
        driver_completion: DriverCompleteOption,
    ) -> Result<Connection<'_>, Error> {
        let mut driver_connect = |hwnd: HWnd| unsafe {
            self.driver_connect_with_hwnd(
                connection_string,
                completed_connection_string,
                driver_completion,
                hwnd,
            )
        };

        match driver_completion {
            DriverCompleteOption::NoPrompt => (),
            #[cfg(target_os = "windows")]
            _ => {
                // We need a parent window, let's provide a message only window.
                let mut window_app = MessageOnlyWindowEventHandler {
                    run_prompt_dialog: Some(driver_connect),
                    result: None,
                };
                let mut event_loop = EventLoop::new().unwrap();
                event_loop.run_app_on_demand(&mut window_app).unwrap();
                return window_app.result.unwrap();
            }
            #[cfg(not(target_os = "windows"))]
            _ => panic!("Prompt is not supported for non-windows systems."),
        };
        let hwnd = null_mut();
        driver_connect(hwnd)
    }

    /// Allows to call driver connect with a user supplied HWnd. Same as [`Self::driver_connect`],
    /// but with the possibility to provide your own parent window handle in case you want to show
    /// a prompt to the user.
    ///
    /// # Safety
    ///
    /// `parent_window` must be a valid window handle, to a window type supported by the ODBC driver
    /// manager. On windows this is a plain window handle, which is of course understood by the
    /// windows built in ODBC driver manager. Other working combinations are unknown to the author.
    pub unsafe fn driver_connect_with_hwnd(
        &self,
        connection_string: &str,
        completed_connection_string: &mut OutputStringBuffer,
        driver_completion: DriverCompleteOption,
        parent_window: HWnd,
    ) -> Result<Connection<'_>, Error> {
        let mut connection = self.allocate_connection()?;
        let connection_string = SqlText::new(connection_string);

        let connection_string_is_complete = connection
            .driver_connect(
                &connection_string,
                parent_window,
                completed_connection_string,
                driver_completion.as_sys(),
            )
            .into_result_bool(&connection)?;
        if !connection_string_is_complete {
            return Err(Error::AbortedConnectionStringCompletion);
        }
        Ok(Connection::new(connection))
    }

    /// Get information about available drivers. Only 32 or 64 Bit drivers will be listed, depending
    /// on whether you are building a 32 Bit or 64 Bit application.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let env = Environment::new ()?;
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
        let _lock = self.internal_state.lock().unwrap();
        unsafe {
            // Find required buffer size to avoid truncation.
            let (mut desc_len, mut attr_len) = if let Some(res) = self
                .environment
                // Start with first so we are independent of state
                .drivers_buffer_len(FetchOrientation::First)
                .into_result_option(&self.environment)?
            {
                res
            } else {
                // No drivers present
                return Ok(Vec::new());
            };

            // If there are, let's loop over the remaining drivers
            while let Some((candidate_desc_len, candidate_attr_len)) = self
                .environment
                .drivers_buffer_len(FetchOrientation::Next)
                .into_result_option(&self.environment)?
            {
                desc_len = max(candidate_desc_len, desc_len);
                attr_len = max(candidate_attr_len, attr_len);
            }

            // Allocate +1 character extra for terminating zero
            let mut desc_buf = SzBuffer::with_capacity(desc_len as usize);
            // Do **not** use nul terminated buffer, as nul is used to delimit key value pairs of
            // attributes.
            let mut attr_buf: Vec<SqlChar> = vec![0; attr_len as usize];

            while self
                .environment
                .drivers_buffer_fill(FetchOrientation::Next, desc_buf.mut_buf(), &mut attr_buf)
                .into_result_bool(&self.environment)?
            {
                let description = desc_buf.to_utf8();
                let attributes =
                    slice_to_utf8(&attr_buf).expect("Attributes must be interpretable as UTF-8");

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
    /// let env = Environment::new()?;
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
    /// let env = Environment::new ()?;
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
        let _lock = self.internal_state.lock().unwrap();
        unsafe {
            // Find required buffer size to avoid truncation.
            let (mut server_name_len, mut driver_len) = if let Some(res) = self
                .environment
                .data_source_buffer_len(direction)
                .into_result_option(&self.environment)?
            {
                res
            } else {
                // No drivers present
                return Ok(Vec::new());
            };

            // If there are let's loop over the rest
            while let Some((candidate_name_len, candidate_decs_len)) = self
                .environment
                .drivers_buffer_len(FetchOrientation::Next)
                .into_result_option(&self.environment)?
            {
                server_name_len = max(candidate_name_len, server_name_len);
                driver_len = max(candidate_decs_len, driver_len);
            }

            let mut server_name_buf = SzBuffer::with_capacity(server_name_len as usize);
            let mut driver_buf = SzBuffer::with_capacity(driver_len as usize);

            let mut not_empty = self
                .environment
                .data_source_buffer_fill(direction, server_name_buf.mut_buf(), driver_buf.mut_buf())
                .into_result_bool(&self.environment)?;

            while not_empty {
                let server_name = server_name_buf.to_utf8();
                let driver = driver_buf.to_utf8();

                data_source_info.push(DataSourceInfo {
                    server_name,
                    driver,
                });
                not_empty = self
                    .environment
                    .data_source_buffer_fill(
                        FetchOrientation::Next,
                        server_name_buf.mut_buf(),
                        driver_buf.mut_buf(),
                    )
                    .into_result_bool(&self.environment)?;
            }
        }

        Ok(data_source_info)
    }

    fn allocate_connection(&self) -> Result<handles::Connection, Error> {
        // Hold lock diagnostics errors are consumed in this thread.
        let _lock = self.internal_state.lock().unwrap();
        self.environment
            .allocate_connection()
            .into_result(&self.environment)
    }
}

/// An ODBC [`Environment`] with static lifetime. This function always returns a reference to the
/// same instance. The environment is constructed then the function is called for the first time.
/// Every time after the initial construction this function must succeed.
///
/// Useful if your application uses ODBC for the entirety of its lifetime, since using a static
/// lifetime means there is one less lifetime you and the borrow checker need to worry about. If
/// your application only wants to use odbc for part of its runtime, you may want to use
/// [`Environment`] directly in order to explicitly free its associated resources earlier. No matter
/// the application, it is recommended to only have one [`Environment`] per process.
pub fn environment() -> Result<&'static Environment, Error> {
    static ENV: OnceLock<Environment> = OnceLock::new();
    if let Some(env) = ENV.get() {
        // Environment already initialized, nothing to do, but to return it.
        Ok(env)
    } else {
        // ODBC Environment not initialized yet. Let's do so and return it.
        let env = Environment::new()?;
        let env = ENV.get_or_init(|| env);
        Ok(env)
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

/// Message loop for prompt dialog. Used by [`Environment::driver_connect`].
#[cfg(target_os = "windows")]
struct MessageOnlyWindowEventHandler<'a, F> {
    run_prompt_dialog: Option<F>,
    result: Option<Result<Connection<'a>, Error>>,
}

#[cfg(target_os = "windows")]
impl<'a, F> ApplicationHandler for MessageOnlyWindowEventHandler<'a, F>
where
    F: FnOnce(HWnd) -> Result<Connection<'a>, Error>,
{
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        let parent_window = event_loop
            .create_window(Window::default_attributes().with_visible(false))
            .unwrap();

        use winit::raw_window_handle::{HasWindowHandle, RawWindowHandle, Win32WindowHandle};

        let hwnd = match parent_window.window_handle().unwrap().as_raw() {
            RawWindowHandle::Win32(Win32WindowHandle { hwnd, .. }) => hwnd.get() as HWnd,
            _ => panic!("ODBC Prompt is only supported on window platforms"),
        };

        if let Some(run_dialog) = self.run_prompt_dialog.take() {
            self.result = Some(run_dialog(hwnd))
        }
        event_loop.exit();
    }

    fn window_event(&mut self, _event_loop: &ActiveEventLoop, _id: WindowId, _event: WindowEvent) {}
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
mod tests {

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

    #[cfg(not(target_os = "windows"))]
    #[test]
    #[should_panic(expected = "Prompt is not supported for non-windows systems.")]
    fn driver_connect_with_prompt_panics_under_linux() {
        let env = Environment::new().unwrap();
        let mut out = OutputStringBuffer::empty();
        env.driver_connect("", &mut out, DriverCompleteOption::Prompt)
            .unwrap();
    }
}
