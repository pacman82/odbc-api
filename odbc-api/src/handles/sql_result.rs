use odbc_sys::SqlReturn;

/// Result of an ODBC function call. Variants hold the same meaning as the constants associated with
/// [`SqlReturn`]. This type may hold results, but it is still the responsibility of the user to
/// fetch and handle the diagnostics in case of an Error.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum SqlResult<T> {
    /// The function has been executed successfully.
    Success(T),
    /// The function has been executed successfully. There have been warnings.
    SuccessWithInfo(T),
    /// No more data is available
    NoData,
    /// The function was started asynchronously and is still executing.
    StillExecuting,
    /// The function returned an error state. Check diagnostics.
    Error {
        /// Name of the ODBC Api call which caused the error. This might help interpreting
        /// associatedif the error ODBC diagnostics if the error is bubbeld all the way up to the
        /// end users output, but the context is lost.
        function: &'static str,
    },
}

impl SqlResult<()> {
    /// Append a return value a successful to Result
    pub fn on_success<F, T>(self, f: F) -> SqlResult<T>
    where
        F: FnOnce() -> T,
    {
        self.map(|()| f())
    }
}

impl<T> SqlResult<T> {
    /// `True` if variant is [`SqlResult::Error`].
    pub fn is_err(&self) -> bool {
        matches!(self, SqlResult::Error { .. })
    }

    /// Applies `f` to any value wrapped in `Success` or `SuccessWithInfo`.
    pub fn map<U, F>(self, f: F) -> SqlResult<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            SqlResult::Success(v) => SqlResult::Success(f(v)),
            SqlResult::SuccessWithInfo(v) => SqlResult::SuccessWithInfo(f(v)),
            SqlResult::Error { function } => SqlResult::Error { function },
            SqlResult::StillExecuting => SqlResult::StillExecuting,
            SqlResult::NoData => SqlResult::NoData,
        }
    }

    pub fn unwrap(self) -> T {
        match self {
            SqlResult::Success(v) | SqlResult::SuccessWithInfo(v) => v,
            SqlResult::Error { .. } => panic!("Unwraping SqlResult::Error"),
            SqlResult::NoData => panic!("Unwraping SqlResult::NoData"),
            SqlResult::StillExecuting => panic!("Unwraping SqlResult::StillExecuting"),
        }
    }
}

pub trait ExtSqlReturn {
    fn into_sql_result(self, function_name: &'static str) -> SqlResult<()>;

    /// Use this instead of [`Self::into_sql_result`] if you expect [`SqlReturn::NO_DATA`] to be a
    /// valid value. [`SqlReturn::NO_DATA`] is mapped to `Ok(false)`, all other success values are
    /// `Ok(true)`.
    fn into_sql_result_bool(self, function_name: &'static str) -> SqlResult<bool>;
}

impl ExtSqlReturn for SqlReturn {
    fn into_sql_result(self, function: &'static str) -> SqlResult<()> {
        match self {
            SqlReturn::SUCCESS => SqlResult::Success(()),
            SqlReturn::SUCCESS_WITH_INFO => SqlResult::SuccessWithInfo(()),
            SqlReturn::ERROR => SqlResult::Error { function },
            SqlReturn::STILL_EXECUTING => SqlResult::StillExecuting,
            r => panic!(
                "Unexpected return value '{:?}' for ODBC function '{}'",
                r, function
            ),
        }
    }

    fn into_sql_result_bool(self, function: &'static str) -> SqlResult<bool> {
        match self {
            SqlReturn::NO_DATA => SqlResult::Success(false),
            other => other.into_sql_result(function).on_success(|| true),
        }
    }
}
