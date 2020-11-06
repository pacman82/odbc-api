use crate::{buffers::FixedSizedCType, handles::Statement, DataType, Error};
use odbc_sys::{CDataType, Len, ParamType, Pointer};
use std::{convert::TryInto, ptr::null_mut};
use self::single_parameter::SingleParameter;
mod into_parameters;
mod single_parameter;
mod tuple;

pub use into_parameters::IntoParameters;

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

unsafe impl<T: SingleParameter> Parameters for T {
    unsafe fn bind_input_parameters(&self, stmt: &mut Statement) -> Result<(), Error> {
        self.bind_single_input_parameter(stmt, 1)
    }
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

/// Annotates an instance of an inner type with an SQL Data type in order to indicate how it should
/// be bound as a parameter to an SQL Statement.
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
    }
}

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
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
