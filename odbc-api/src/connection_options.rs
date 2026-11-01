use std::marker::PhantomData;

use odbc_sys::{ConnectionAttribute, IS_POINTER, Pointer, SsAccessToken};

use crate::{Error, handles};

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

impl ConnectionOptions {
    /// Set the attributes corresponding to the connection options to an allocated connection
    /// handle. Usually you would rather provide the options then creating the connection with e.g.
    /// [`crate::Environment::connect_with_connection_string`] rather than calling this method
    /// yourself.
    pub fn apply(&self, handle: &handles::Connection) -> Result<(), Error> {
        if let Some(timeout) = self.login_timeout_sec {
            handle.set_login_timeout_sec(timeout).into_result(handle)?;
        }
        if let Some(packet_size) = self.packet_size {
            handle.set_packet_size(packet_size).into_result(handle)?;
        }
        Ok(())
    }
}

/// Connection attributes specific to a driver. Applications which desire to set driver specific
/// attributes can implement this trait and pass its implementation to
/// [`crate::Environment::connect_with_connection_string_and_driver_specific_options`].
///
/// `'c` is the lifetime of the connection. Memory referenced by the attributes must outlive it.
pub trait DriverSpecificConnectionOptions<'c> {
    /// Attributes to set before connecting.
    fn attributes(&self) -> impl Iterator<Item = SetConnectionAttributeArgs<'c>>;
}

/// Arguments for a single call to `SQLSetConnectAttr`: the attribute, its value and the string
/// length.
///
/// Users of `odbc-api` usually would not want to create these themselves and rather use safe
/// abstractions, e.g. [`MicrosoftConnectionOptions`]. A reason to create one in your application
/// code however, could be that you want to set an attribute which is not part of the ODBC
/// standard, but only supported by your specific driver, which you happen to know your
/// application will use.
///
/// `'a` is the lifetime of any memory referenced by the value. Setting the attribute requires it
/// to outlive the connection. Attributes without referenced memory are `'static`.
///
/// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/develop-app/connection-attributes>
#[derive(Debug, Clone, Copy)]
pub struct SetConnectionAttributeArgs<'a> {
    attribute: ConnectionAttribute,
    value: Pointer,
    string_length: i32,
    /// Covariant, so a `SetConnectionAttributeArgs<'static>` can be used wherever a shorter
    /// lifetime is required.
    _referenced_memory: PhantomData<&'a ()>,
}

impl<'a> SetConnectionAttributeArgs<'a> {
    /// # Safety
    ///
    /// `value` and `string_length` must match the expectations of the ODBC driver for
    /// `attribute`. Depending on `attribute`, `value` is either interpreted as an integer, or
    /// points to memory. In the latter case, the memory must stay valid for `'a`.
    ///
    /// See: <https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function>
    pub unsafe fn new(attribute: ConnectionAttribute, value: Pointer, string_length: i32) -> Self {
        Self {
            attribute,
            value,
            string_length,
            _referenced_memory: PhantomData,
        }
    }

    /// The connection attribute to set.
    pub fn attribute(&self) -> ConnectionAttribute {
        self.attribute
    }

    /// Value of the attribute. Depending on [`Self::attribute`] either an integer or a pointer to
    /// memory which is valid for `'a`.
    pub fn value(&self) -> Pointer {
        self.value
    }

    /// String length argument of `SQLSetConnectAttr`. For pointers and integers it describes the
    /// kind of value rather than a length, e.g. `IS_POINTER` or `IS_UINTEGER`.
    pub fn string_length(&self) -> i32 {
        self.string_length
    }
}

/// Microsoft Entra ID access token. Only understood by the Microsoft ODBC Driver for SQL Server.
/// The token must outlive the connection, since the driver keeps the pointer.
impl<'a> From<&'a SsAccessToken> for SetConnectionAttributeArgs<'a> {
    fn from(access_token: &'a SsAccessToken) -> Self {
        // Safety: The value is a thin pointer to the start of the struct, as expected by the
        // driver. The length metadata of the fat pointer is dropped, the driver reads the length
        // from `data_size`. `IS_POINTER` tells the driver manager to pass the pointer through,
        // without copying or converting. The token is borrowed for `'a`.
        unsafe {
            SetConnectionAttributeArgs::new(
                ConnectionAttribute::SS_ACCESS_TOKEN,
                access_token as *const SsAccessToken as Pointer,
                IS_POINTER,
            )
        }
    }
}

/// Driver specific options for connections opened with the Microsoft ODBC Driver for SQL Server.
/// Pass them to [`crate::Environment::connect_with_connection_string_and_driver_specific_options`].
///
/// These options contain attributes only understood by the Microsoft ODBC Driver for SQL Server.
/// Applying them to a connection opened with any other driver may cause undefined behavior.
#[derive(Default, Clone, Copy)]
pub struct MicrosoftConnectionOptions<'a> {
    /// Microsoft Entra ID access token used to authenticate. The connection string must not
    /// contain `UID`, `PWD`, `Authentication` or `Trusted_Connection` if it is set. The token must
    /// outlive the connection, since the driver keeps a pointer to it.
    ///
    /// See:
    /// <https://learn.microsoft.com/sql/connect/odbc/using-azure-active-directory#authenticating-with-an-access-token>
    pub access_token: Option<&'a SsAccessToken>,
}

impl<'a> DriverSpecificConnectionOptions<'a> for MicrosoftConnectionOptions<'a> {
    fn attributes(&self) -> impl Iterator<Item = SetConnectionAttributeArgs<'a>> {
        self.access_token
            .map(SetConnectionAttributeArgs::from)
            .into_iter()
    }
}
