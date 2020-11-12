use crate::parameter::{Parameter, VarCharParam};

/// An instance can be consumed and to create a parameter which can be bound to a statement during
/// execution.
///
/// Due to specific layout requirements and the necessity to provide pointers to length and
/// indicator values, as opposed to taking the actual values it is often necessary starting from
/// idiomatic Rust types, to convert, enrich and marshal them into values which can be bound to
/// ODBC. This also provides a safe extension point for all kinds of parameters, as only the
/// implementation of `Parameters` is unsafe.
pub trait IntoParameter {
    type Parameter: Parameter;

    fn into_parameter(self) -> Self::Parameter;
}

impl<T> IntoParameter for T
where
    T: Parameter,
{
    type Parameter = Self;

    fn into_parameter(self) -> Self::Parameter {
        self
    }
}

impl<'a> IntoParameter for &'a str {
    type Parameter = VarCharParam<'a>;

    fn into_parameter(self) -> Self::Parameter {
        VarCharParam::new(self.as_bytes())
    }
}
