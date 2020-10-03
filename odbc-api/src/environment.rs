use crate::{handles, Connection, Error};
use odbc_sys::AttrOdbcVersion;
use widestring::{U16Str, U16String};

/// An ODBC 3.8 environment.
///
/// Associated with an `Environment` is any information that is global in nature, such as:
///
/// * The `Environment`'s state
/// * The current environment-level diagnostics
/// * The handles of connections currently allocated on the environment
/// * The current stetting of each environment attribute
pub struct Environment {
    environment: handles::Environment,
}

impl Environment {
    /// Allocates a new ODBC Environment and declares that the Application wants to use ODBC version
    /// 3.8.
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
        let environment = crate::handles::Environment::new()?;
        environment.declare_version(AttrOdbcVersion::Odbc3_80)?;
        Ok(Self { environment })
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
    pub fn connect(
        &self,
        data_source_name: &str,
        user: &str,
        pwd: &str,
    ) -> Result<Connection, Error> {
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
    ) -> Result<Connection, Error> {
        let mut connection = self.environment.allocate_connection()?;
        connection.connect(data_source_name, user, pwd)?;
        Ok(Connection::new(connection))
    }

    /// Allocates a connection handle and establishes connections to a driver and a data source.
    ///
    /// An alternative to `connect`. It supports data sources that require more connection
    /// information than the three arguments in `connect` and data sources that are not defined in
    /// the system information.
    ///
    /// To find out your connection string try: <https://www.connectionstrings.com/>
    pub fn connect_with_connection_string(
        &self,
        connection_string: &str,
    ) -> Result<Connection, Error> {
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
    ) -> Result<Connection, Error> {
        let mut connection = self.environment.allocate_connection()?;
        connection.connect_with_connection_string(connection_string)?;
        Ok(Connection::new(connection))
    }
}
