/// Specifies how the driver and driver manager complete the incoming connection string. See
/// [`crate::Environment::driver_connect`].
#[derive(Clone, Copy, Debug)]
pub enum DriverCompleteOption {
    /// Do not show a prompt to the user. This implies that the connection string, must already
    /// provide all information needed to Connect to the data source, otherwise the operation fails.
    /// This is the only supported variant on non windows platforms
    NoPrompt,
    /// Always show a prompt to the user.
    #[cfg(all(target_os = "windows", feature = "prompt"))]
    Prompt,
    /// Only show a prompt to the user if the information in the connection string is not sufficient
    /// to connect to the data source.
    #[cfg(all(target_os = "windows", feature = "prompt"))]
    Complete,
    /// Like complete, but the user may not change any information already provided within the
    /// connection string.
    #[cfg(all(target_os = "windows", feature = "prompt"))]
    CompleteRequired,
}

impl DriverCompleteOption {
    pub fn as_sys(&self) -> odbc_sys::DriverConnectOption {
        match self {
            DriverCompleteOption::NoPrompt => odbc_sys::DriverConnectOption::NoPrompt,
            #[cfg(all(target_os = "windows", feature = "prompt"))]
            DriverCompleteOption::Prompt => odbc_sys::DriverConnectOption::Prompt,
            #[cfg(all(target_os = "windows", feature = "prompt"))]
            DriverCompleteOption::Complete => odbc_sys::DriverConnectOption::Complete,
            #[cfg(all(target_os = "windows", feature = "prompt"))]
            DriverCompleteOption::CompleteRequired => {
                odbc_sys::DriverConnectOption::CompleteRequired
            }
        }
    }
}
