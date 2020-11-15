//! Implementation and types required to bind arguments to ODBC parameters.

use crate::{
    buffers::{Bit, FixedSizedCType},
    DataType,
};
use odbc_sys::{CDataType, Date, Len};
use std::{convert::TryInto, ffi::c_void, ptr::null};

/// A type which has a layout suitable to be bound to ODBC.
pub unsafe trait CData {
    /// The CDataType corresponding with this types layout.
    fn cdata_type(&self) -> CDataType;

    /// Indicates the length of variable sized types. May be zero for fixed sized types.
    fn indicator_ptr(&self) -> *const Len;

    /// Pointer to a value corresponding to the one described by `cdata_type`.
    fn value_ptr(&self) -> *const c_void;
}

/// Can be bound to a single placeholder in an SQL statement.
///
/// Users usually won't utilize this trait directly.
pub unsafe trait Input: CData {
    fn data_type(&self) -> DataType;
}

/// A type which can be used to receive Data from ODBC.
///
/// Users usually won't utilize this trait directly.
pub unsafe trait Output: CData {
    /// Maximum length of the type. This is required for ODBC to not write into invalid memory then
    /// writing output. It is also required to index values in bound buffers, if more than one
    /// parameter is bound.
    fn buffer_length(&self) -> Len;
}

// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
// which are a natural match for the C-Type in question. These can be used to spare the user to
// specify that an i32 is supposed to be bound as an SQL Integer.

macro_rules! impl_input_fixed_sized {
    ($t:ident, $data_type:expr) => {
        unsafe impl CData for $t {
            fn cdata_type(&self) -> CDataType {
                $t::C_DATA_TYPE
            }

            fn indicator_ptr(&self) -> *const Len {
                // Fixed sized types do not require a length indicator.
                null()
            }

            fn value_ptr(&self) -> *const c_void {
                self as *const $t as *const c_void
            }
        }

        unsafe impl Input for $t {
            fn data_type(&self) -> DataType {
                $data_type
            }
        }
    };
}

impl_input_fixed_sized!(f64, DataType::Double);
impl_input_fixed_sized!(f32, DataType::Real);
impl_input_fixed_sized!(Date, DataType::Date);
impl_input_fixed_sized!(i16, DataType::SmallInt);
impl_input_fixed_sized!(i32, DataType::Integer);
impl_input_fixed_sized!(i8, DataType::Tinyint);
impl_input_fixed_sized!(Bit, DataType::Bit);
impl_input_fixed_sized!(i64, DataType::Bigint);

// Support for fixed size types, which are not unsigned. Time, Date and timestamp types could be
// supported, implementation DataType would need to take an instance into account.

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
pub struct VarChar<'a> {
    bytes: &'a [u8],
    /// Will be set to value.len() by constructor.
    length: Len,
}

impl<'a> VarChar<'a> {
    pub fn new(value: &'a [u8]) -> Self {
        VarChar {
            bytes: value,
            length: value.len().try_into().unwrap(),
        }
    }
}

unsafe impl CData for VarChar<'_> {
    fn cdata_type(&self) -> CDataType {
        CDataType::Char
    }

    fn indicator_ptr(&self) -> *const Len {
        &self.length
    }

    fn value_ptr(&self) -> *const c_void {
        self.bytes.as_ptr() as *const c_void
    }
}

unsafe impl Input for VarChar<'_> {
    fn data_type(&self) -> DataType {
        DataType::Varchar {
            length: self.bytes.len().try_into().unwrap(),
        }
    }
}

/// Annotates an instance of an inner type with an SQL Data type in order to indicate how it should
/// be bound as a parameter to an SQL Statement.
pub struct WithDataType<T> {
    pub value: T,
    pub data_type: DataType,
}

unsafe impl<T> CData for WithDataType<T>
where
    T: CData,
{
    fn cdata_type(&self) -> CDataType {
        self.value.cdata_type()
    }

    fn indicator_ptr(&self) -> *const Len {
        self.value.indicator_ptr()
    }

    fn value_ptr(&self) -> *const c_void {
        self.value.value_ptr()
    }
}

unsafe impl<T> Input for WithDataType<T>
where
    T: CData,
{
    fn data_type(&self) -> DataType {
        self.data_type
    }
}
