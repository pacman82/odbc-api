use crate::{
    parameter::{VarCharBox, VarCharSlice},
    IntoParameter,
};

/// Newtype wrapper intend to be used around `String`s or `str` slices to bind them always as narrow
/// text independent of wether the `narrow` feature is set or not.
pub struct Narrow<T>(pub T);

impl<'a> IntoParameter for Narrow<&'a str> {
    type Parameter = VarCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        VarCharSlice::new(self.0.as_bytes())
    }
}

impl<'a> IntoParameter for Narrow<Option<&'a str>> {
    type Parameter = VarCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        match self.0 {
            Some(str) => Narrow(str).into_parameter(),
            None => VarCharSlice::NULL,
        }
    }
}

impl<'a> IntoParameter for Option<Narrow<&'a str>> {
    type Parameter = VarCharSlice<'a>;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => Narrow(str.0).into_parameter(),
            None => VarCharSlice::NULL,
        }
    }
}

impl IntoParameter for Narrow<String> {
    type Parameter = VarCharBox;

    fn into_parameter(self) -> Self::Parameter {
        VarCharBox::from_string(self.0)
    }
}

impl IntoParameter for Narrow<Option<String>> {
    type Parameter = VarCharBox;

    fn into_parameter(self) -> Self::Parameter {
        match self.0 {
            Some(str) => Narrow(str).into_parameter(),
            None => VarCharBox::null(),
        }
    }
}

impl IntoParameter for Option<Narrow<String>> {
    type Parameter = VarCharBox;

    fn into_parameter(self) -> Self::Parameter {
        match self {
            Some(str) => Narrow(str.0).into_parameter(),
            None => VarCharBox::null(),
        }
    }
}
