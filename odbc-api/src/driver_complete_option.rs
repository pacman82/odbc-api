// Currently only windows driver manager supports prompt.
#[cfg(target_os = "windows")]
use odbc_sys::HWnd;
#[cfg(target_os = "windows")]
use raw_window_handle::{HasRawWindowHandle, RawWindowHandle};

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
        use std::ptr::null_mut;

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
