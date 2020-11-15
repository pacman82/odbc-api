use crate::handles::{Input, VarChar};

/// An instance can be consumed and to create a parameter which can be bound to a statement during
/// execution.
///
/// Due to specific layout requirements and the necessity to provide pointers to length and
/// indicator values, as opposed to taking the actual values it is often necessary starting from
/// idiomatic Rust types, to convert, enrich and marshal them into values which can be bound to
/// ODBC. This also provides a safe extension point for all kinds of parameters, as only the
/// implementation of `Parameters` is unsafe.
pub trait IntoInput {
    type Input: Input;

    fn into_input(self) -> Self::Input;
}

impl<T> IntoInput for T
where
    T: Input,
{
    type Input = Self;

    fn into_input(self) -> Self::Input {
        self
    }
}

impl<'a> IntoInput for &'a str {
    type Input = VarChar<'a>;

    fn into_input(self) -> Self::Input {
        VarChar::new(self.as_bytes())
    }
}
