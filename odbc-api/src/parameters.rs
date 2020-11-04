use crate::{buffers::FixedSizedCType, handles::Statement, DataType, Error};
use odbc_sys::{CDataType, Len, ParamType, Pointer};
use std::{convert::TryInto, ptr::null_mut};

mod tuple;

/// An instance can be consumed and to create a parameter which can be bound to a statement during
/// execution.
///
/// Due to spefic layout requirements and the necessity to provide pointers to length and indicator
/// values, as opposed to taking the actual values it is often necessary starting from idiomatic
/// Rust types, to convert, enrich and marshal them into values which can be bound to ODBC. This
/// also provides a safe extension point for all kinds of parameters, as only the implementation of
/// `Parameters` is unsafe.
pub trait IntoParameters {
    type Parameters: Parameters;

    /// Convert into parameters for statement execution.
    fn into_parameters(self) -> Self::Parameters;
}

impl<T: Parameters> IntoParameters for T where{
    type Parameters = T;

    fn into_parameters(self) -> Self::Parameters {
        self
    }
}

/// SQL Parameters used to execute a query.
///
/// ODBC allows to place question marks (`?`) in the statement text as placeholders. For each such
/// placeholder a parameter needs to be bound to the statement before executing it.
pub unsafe trait Parameters {
    /// # Safety
    ///
    /// Implementers should take care that the values bound by this method to the statement live at
    /// least for the Duration of `self`. The most straight forward way of achieving this is of
    /// course, to bind members.
    unsafe fn bind_input_parameters(&self, stmt: &mut Statement) -> Result<(), Error>;
}

unsafe impl<A: SingleParameter> Parameters for A {
    unsafe fn bind_input_parameters(&self, stmt: &mut Statement) -> Result<(), Error> {
        self.bind_single_input_parameter(stmt, 1)
    }
}

/// Can be bound to a single `Placeholder` in an SQL statement.
///
/// Users usually won't utilize this trait directly, but implementations of `Parameters` are likely
/// to bind one or more instances of SingleParameter to a statement.
pub unsafe trait SingleParameter {
    /// # Parameters
    ///
    /// * `stmt`: Statement handle the parameter should be bound to.
    /// * `parameter_number`: 1 based index of the parameter. The index refers to the position of
    ///   the `?` placeholder in the SQL statement text.
    ///
    /// # Safety
    ///
    /// Implementers should take care that the values bound by this method to the statement live at
    /// least for the Duration of `self`. The most straight forward way of achieving this is of
    /// course, to bind members.
    unsafe fn bind_single_input_parameter(
        &self,
        stmt: &mut Statement,
        parameter_number: u16,
    ) -> Result<(), Error>;
}

unsafe impl<T> Parameters for &[T]
where
    T: SingleParameter,
{
    unsafe fn bind_input_parameters(&self, stmt: &mut Statement) -> Result<(), Error> {
        for (index, parameter) in self.iter().enumerate() {
            parameter.bind_single_input_parameter(stmt, index as u16 + 1)?;
        }
        Ok(())
    }
}

pub struct WithDataType<T> {
    pub value: T,
    pub data_type: DataType,
}

unsafe impl<T> SingleParameter for WithDataType<T>
where
    T: FixedSizedCType,
{
    unsafe fn bind_single_input_parameter(
        &self,
        stmt: &mut Statement<'_>,
        parameter_number: u16,
    ) -> Result<(), Error> {
        stmt.bind_parameter(
            parameter_number,
            ParamType::Input,
            T::C_DATA_TYPE,
            DataType::Integer,
            &self.value as *const T as *mut T as Pointer,
            0,
            null_mut(),
        )
        .unwrap();
        Ok(())
    }
}

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a seperate value.
pub struct VarCharParam<'a> {
    value: &'a [u8],
    /// Will be set to value.len() by constructor.
    length: Len,
}

impl<'a> VarCharParam<'a> {
    pub fn new(value: &'a [u8]) -> Self {
        VarCharParam {
            value,
            length: value.len().try_into().unwrap(),
        }
    }
}

unsafe impl SingleParameter for VarCharParam<'_> {
    unsafe fn bind_single_input_parameter(
        &self,
        stmt: &mut Statement<'_>,
        parameter_number: u16,
    ) -> Result<(), Error> {
        stmt.bind_parameter(
            parameter_number,
            ParamType::Input,
            CDataType::Char,
            DataType::Varchar {
                length: self.value.len().try_into().unwrap(),
            },
            self.value.as_ptr() as Pointer,
            self.length, // Since we only bind a single input parameter this value should be ignored.
            &self.length as *const Len as *mut Len,
        )
    }
}
