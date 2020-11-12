//! Implementation and types required to bind arguments to ODBC parameters.

use crate::{
    buffers::{Bit, FixedSizedCType},
    handles::Statement,
    DataType, Error,
};
use odbc_sys::{CDataType, Date, Len, Pointer};
use std::{convert::TryInto, ffi::c_void, mem::size_of, ptr::null_mut};

/// Can be bound to a single `Placeholder` in an SQL statement.
///
/// Users usually won't utilize this trait directly, but implementations of `Parameters` are likely
/// to bind one or more instances of SingleParameter to a statement.
pub unsafe trait Parameter {
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
    unsafe fn bind_parameter_to(
        &self,
        stmt: &mut Statement,
        parameter_number: u16,
    ) -> Result<(), Error>;
}

// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
// which are a natural match for the C-Type in question. These can be used to spare the user to
// specify that an i32 is supposed to be bound as an SQL Integer.

macro_rules! impl_single_parameter_for_fixed_sized {
    ($t:ident, $data_type:expr) => {
        unsafe impl Parameter for $t {
            unsafe fn bind_parameter_to(
                &self,
                stmt: &mut Statement,
                parameter_number: u16,
            ) -> Result<(), Error> {
                stmt.bind_input_parameter(
                    parameter_number,
                    $t::C_DATA_TYPE,
                    $data_type,
                    self as *const $t as *const c_void,
                    size_of::<$t>() as Len,
                    null_mut(),
                )
            }
        }
    };
}

impl_single_parameter_for_fixed_sized!(f64, DataType::Double);
impl_single_parameter_for_fixed_sized!(f32, DataType::Real);
impl_single_parameter_for_fixed_sized!(Date, DataType::Date);
impl_single_parameter_for_fixed_sized!(i16, DataType::SmallInt);
impl_single_parameter_for_fixed_sized!(i32, DataType::Integer);
impl_single_parameter_for_fixed_sized!(i8, DataType::Tinyint);
impl_single_parameter_for_fixed_sized!(Bit, DataType::Bit);
impl_single_parameter_for_fixed_sized!(i64, DataType::Bigint);

// Support for fixed size types, which are not unsigned. Time, Date and timestamp types could be
// supported, implementation DataType would need to takeinstance into account.

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
pub struct VarCharParam<'a> {
    bytes: &'a [u8],
    /// Will be set to value.len() by constructor.
    length: Len,
}

impl<'a> VarCharParam<'a> {
    pub fn new(value: &'a [u8]) -> Self {
        VarCharParam {
            bytes: value,
            length: value.len().try_into().unwrap(),
        }
    }
}

unsafe impl Parameter for VarCharParam<'_> {
    unsafe fn bind_parameter_to(
        &self,
        stmt: &mut Statement<'_>,
        parameter_number: u16,
    ) -> Result<(), Error> {
        stmt.bind_input_parameter(
            parameter_number,
            CDataType::Char,
            DataType::Varchar {
                length: self.bytes.len().try_into().unwrap(),
            },
            self.bytes.as_ptr() as Pointer,
            self.length, /* Since we only bind a single input parameter this value should be
                          * ignored. */
            &self.length as *const Len as *mut Len,
        )
    }
}

/// Annotates an instance of an inner type with an SQL Data type in order to indicate how it should
/// be bound as a parameter to an SQL Statement.
pub struct WithDataType<T> {
    pub value: T,
    pub data_type: DataType,
}

unsafe impl<T> Parameter for WithDataType<T>
where
    T: FixedSizedCType,
{
    unsafe fn bind_parameter_to(
        &self,
        stmt: &mut Statement<'_>,
        parameter_number: u16,
    ) -> Result<(), Error> {
        stmt.bind_input_parameter(
            parameter_number,
            T::C_DATA_TYPE,
            DataType::Integer,
            &self.value as *const T as *const c_void,
            0,
            null_mut(),
        )
    }
}
