use widestring::{U16Str, U16String};

use crate::{
    buffers::Indicator,
    fixed_sized::Pod,
    parameter::{InputParameter, VarBinaryBox, VarBinarySlice, VarWCharBox, VarWCharSlice},
    Nullable,
};

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
use crate::parameter::{VarCharBox, VarCharSlice};

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

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
impl<'a> IntoParameter for &'a str {
    type Parameter = VarCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        VarCharSlice::new(self.as_bytes())
    }
}

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
impl IntoParameter for &'_ str {
    type Parameter = VarWCharBox;

    fn into_parameter(self) -> Self::Parameter {
        VarWCharBox::from_str_slice(self)
    }
}

impl<'a> IntoParameter for Option<&'a str> {
    type Parameter = <&'a str as IntoParameter>::Parameter;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            #[cfg(not(any(
                feature = "wide",
                all(not(feature = "narrow"), target_os = "windows")
            )))]
            None => VarCharSlice::NULL,
            #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
            None => VarWCharBox::null(),
        }
    }
}

#[cfg(not(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows"))))]
impl IntoParameter for String {
    type Parameter = VarCharBox;

    fn into_parameter(self) -> Self::Parameter {
        VarCharBox::from_string(self)
    }
}

#[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
impl IntoParameter for String {
    type Parameter = VarWCharBox;

    fn into_parameter(self) -> Self::Parameter {
        VarWCharBox::from_str_slice(&self)
    }
}

impl IntoParameter for Option<String> {
    type Parameter = <String as IntoParameter>::Parameter;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            #[cfg(not(any(
                feature = "wide",
                all(not(feature = "narrow"), target_os = "windows")
            )))]
            None => VarCharBox::null(),
            #[cfg(any(feature = "wide", all(not(feature = "narrow"), target_os = "windows")))]
            None => VarWCharBox::null(),
        }
    }
}

impl<'a> IntoParameter for &'a [u8] {
    type Parameter = VarBinarySlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        VarBinarySlice::new(self)
    }
}

impl<'a> IntoParameter for Option<&'a [u8]> {
    type Parameter = VarBinarySlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            None => VarBinarySlice::NULL,
        }
    }
}

impl IntoParameter for Vec<u8> {
    type Parameter = VarBinaryBox;

    fn into_parameter(self) -> Self::Parameter {
        VarBinaryBox::from_vec(self)
    }
}

impl IntoParameter for Option<Vec<u8>> {
    type Parameter = VarBinaryBox;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            None => VarBinaryBox::null(),
        }
    }
}

impl<'a> IntoParameter for &'a U16Str {
    type Parameter = VarWCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        let slice = self.as_slice();
        let length_in_bytes = slice.len() * 2;
        VarWCharSlice::from_buffer(slice, Indicator::Length(length_in_bytes))
    }
}

impl<'a> IntoParameter for Option<&'a U16Str> {
    type Parameter = VarWCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            None => VarWCharSlice::NULL,
        }
    }
}

impl IntoParameter for U16String {
    type Parameter = VarWCharBox;

    fn into_parameter(self) -> Self::Parameter {
        VarWCharBox::from_u16_string(self)
    }
}

impl IntoParameter for Option<U16String> {
    type Parameter = VarWCharBox;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => str.into_parameter(),
            None => VarWCharBox::null(),
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
