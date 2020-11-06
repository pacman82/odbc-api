use crate::Parameters;

/// An instance can be consumed and to create a parameter which can be bound to a statement during
/// execution.
///
/// Due to specific layout requirements and the necessity to provide pointers to length and indicator
/// values, as opposed to taking the actual values it is often necessary starting from idiomatic
/// Rust types, to convert, enrich and marshal them into values which can be bound to ODBC. This
/// also provides a safe extension point for all kinds of parameters, as only the implementation of
/// `Parameters` is unsafe.
pub trait IntoParameters {
    type Parameters: Parameters;

    /// Convert into parameters for statement execution.
    fn into_parameters(self) -> Self::Parameters;
}

impl<T: Parameters> IntoParameters for T {
    type Parameters = T;

    fn into_parameters(self) -> Self::Parameters {
        self
    }
}
