use crate::{
    Error,
    handles::{Diagnostics, SetConnectionAttribute},
};

/// Options to be passed then opening a connection to a datasource.
#[derive(Default, Clone, Copy)]
pub struct ConnectionOptions {
    /// Number of seconds to wait for a login request to complete before returning to the
    /// application. The default is driver-dependent. If `0` the timeout is disabled and a
    /// connection attempt will wait indefinitely.
    ///
    /// If the specified timeout exceeds the maximum login timeout in the data source, the driver
    /// substitutes that value and uses the maximum login timeout instead.
    ///
    /// This corresponds to the `SQL_ATTR_LOGIN_TIMEOUT` attribute in the ODBC specification.
    ///
    /// See:
    /// <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function>
    pub login_timeout_sec: Option<u32>,
    /// Packet size in bytes. Not all drivers support this option.
    pub packet_size: Option<u32>,
}

/// Apply connection attributes to a connection handle
///
/// Application which desire to set driver specific attributes can implement this trait
/// and pass its implementation to
/// [`crate::Environment::connect_with_connection_string_and_driver_specific_options`] in order to
/// apply them.
pub trait ApplyConnectionAttributes {
    /// Set the attributes corresponding to the connection options to an allocated connection
    /// handle. Usually you would rather provide the options then creating the connection with e.g.
    /// [`crate::Environment::connect_with_connection_string`] rather than calling this method
    /// yourself.
    fn apply(&self, handle: &(impl SetConnectionAttribute + Diagnostics)) -> Result<(), Error>;
}

impl ApplyConnectionAttributes for ConnectionOptions {
    fn apply(&self, handle: &(impl SetConnectionAttribute + Diagnostics)) -> Result<(), Error> {
        if let Some(timeout) = self.login_timeout_sec {
            handle.set_login_timeout_sec(timeout).into_result(handle)?;
        }
        if let Some(packet_size) = self.packet_size {
            handle.set_packet_size(packet_size).into_result(handle)?;
        }
        Ok(())
    }
}
