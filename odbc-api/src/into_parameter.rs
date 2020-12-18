use crate::{fixed_sized::FixedSizedCType, parameter::VarChar, Nullable, Parameter};

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
    type Parameter = VarChar<'a>;

    fn into_parameter(self) -> Self::Parameter {
        VarChar::new(self.as_bytes())
    }
}

impl<'a> IntoParameter for Option<&'a str> {
    type Parameter = VarChar<'a>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            None => VarChar::null(),
        }
    }
}

impl<T> IntoParameter for Option<T>
where
    T: FixedSizedCType + Parameter,
{
    type Parameter = Nullable<T>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(value) => Nullable::new(value),
            None => Nullable::null(),
        }
    }
}
