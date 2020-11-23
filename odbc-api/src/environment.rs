use std::{cmp::max, collections::HashMap};

use crate::{handles, Connection, Error};
use odbc_sys::{AttrOdbcVersion, FetchOrientation};
use widestring::{U16CStr, U16Str, U16String};

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
}

impl Environment {
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

    /// Get information about available drivers. Only 32 or 64 Bit drivers will be listed, depending
    /// on wether you are building a 32 Bit or 64 Bit application.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use odbc_api::Environment;
    ///
    /// let mut env = unsafe { Environment::new () }?;
    /// for driver_info in env.drivers()? {
    ///     println!("{:#?}", driver_info);
    /// }
    ///
    /// # Ok::<_, odbc_api::Error>(())
    /// ```
    pub fn drivers(&mut self) -> Result<Vec<DriverInfo>, Error> {
        // Find required buffer size to avoid truncation.

        // Start with first so we are independent of state
        let (mut desc_len, mut attr_len) = if let Some(v) = self
            .environment
            .drivers_buffer_len(FetchOrientation::First)?
        {
            v
        } else {
            // No drivers present
            return Ok(Vec::new());
        };

        // If there are let's loop over the rest
        while let Some((cand_desc_len, cand_attr_len)) = self
            .environment
            .drivers_buffer_len(FetchOrientation::Next)?
        {
            desc_len = max(cand_desc_len, desc_len);
            attr_len = max(cand_attr_len, attr_len);
        }

        // Allocate +1 character extra for terminating zero
        let mut desc_buf = vec![0; desc_len as usize + 1];
        let mut attr_buf = vec![0; attr_len as usize + 1];

        let mut driver_infos = Vec::new();
        while self.environment.drivers_buffer_fill(
            FetchOrientation::Next,
            &mut desc_buf,
            &mut attr_buf,
        )? {
            let description = U16CStr::from_slice_with_nul(&desc_buf).unwrap();
            let attributes = U16CStr::from_slice_with_nul(&attr_buf).unwrap();

            let description = description.to_string().unwrap();
            let attributes = attributes.to_string().unwrap();
            let attributes = attributes_iter(&attributes).collect();

            driver_infos.push(DriverInfo {
                description,
                attributes,
            });
        }

        Ok(driver_infos)
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
