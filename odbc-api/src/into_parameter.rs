use crate::{fixed_sized::Pod, parameter::VarCharSlice, InputParameter, Nullable};

/// An instance can be consumed and to create a parameter which can be bound to a statement during
/// execution.
///
/// Due to specific layout requirements and the necessity to provide pointers to length and
/// indicator values, as opposed to taking the actual values it is often necessary starting from
/// idiomatic Rust types, to convert, enrich and marshal them into values which can be bound to
/// ODBC. This also provides a safe extension point for all kinds of parameters, as only the
/// implementation of `Parameters` is unsafe.
pub trait IntoParameter {
    type Parameter: InputParameter;

    fn into_parameter(self) -> Self::Parameter;
}

impl<T> IntoParameter for T
where
    T: InputParameter,
{
    type Parameter = Self;

    fn into_parameter(self) -> Self::Parameter {
        self
    }
}

impl<'a> IntoParameter for &'a str {
    type Parameter = VarCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        VarCharSlice::new(self.as_bytes())
    }
}

impl<'a> IntoParameter for Option<&'a str> {
    type Parameter = VarCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            None => VarCharSlice::NULL,
        }
    }
}

impl<T> IntoParameter for Option<T>
where
    T: Pod + InputParameter,
{
    type Parameter = Nullable<T>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(value) => Nullable::new(value),
            None => Nullable::null(),
        }
    }
}
