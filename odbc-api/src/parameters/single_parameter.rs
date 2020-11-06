use std::{ptr::null_mut, mem::size_of};
use odbc_sys::{Date, Len, ParamType, Pointer};
use crate::{DataType, Error, buffers::{Bit, FixedSizedCType}, handles::Statement};

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

// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
// which are a natural match for the C-Type in question. These can be used to spare the user to
// specify that an i32 is supposed to be bound as an SQL Integer.

macro_rules! impl_single_parameter_for_fixed_sized {
    ($t:ident, $data_type:expr) => {
        unsafe impl SingleParameter for $t {
            unsafe fn bind_single_input_parameter(
                &self,
                stmt: &mut Statement,
                parameter_number: u16,
            ) -> Result<(), Error> {
                stmt.bind_parameter(
                    parameter_number,
                    ParamType::Input,
                    $t::C_DATA_TYPE,
                    $data_type,
                    self as *const $t as *mut $t as Pointer,
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
