use crate::{
    buffers::{FetchRowMember, Indicator},
    handles::{CData, CDataMut, DataType, HasDataType},
    parameter::{CElement, OutputParameter},
};
use odbc_sys::{CDataType, Date, Numeric, Time, Timestamp};
use std::{
    ffi::c_void,
    ptr::{null, null_mut},
};

/// New type wrapping u8 and binding as SQL_BIT.
///
/// If rust would guarantee the representation of `bool` to be an `u8`, `bool` would be the obvious
/// choice instead. Alas it is not and someday on some platform `bool` might be something else than
/// a `u8` so let's use this new type instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Ord, PartialOrd)]
pub struct Bit(pub u8);

impl Bit {
    /// Maps `true` to `1` and `false` to `0`.
    ///
    /// ```
    /// use odbc_api::Bit;
    ///
    /// assert_eq!(Bit(0), Bit::from_bool(false));
    /// assert_eq!(Bit(1), Bit::from_bool(true));
    /// ```
    pub fn from_bool(boolean: bool) -> Self {
        if boolean {
            Bit(1)
        } else {
            Bit(0)
        }
    }

    /// Maps `1` to `true`, `0` to `false`. Panics if `Bit` should be invalid (not `0` or `1`).
    pub fn as_bool(self) -> bool {
        match self.0 {
            0 => false,
            1 => true,
            _ => panic!("Invalid boolean representation in Bit."),
        }
    }
}

/// A plain old data type. With an associated C Type. Must be completely stack allocated without any
/// external references. In addition to that the buffer size must be known to ODBC in advance.
///
/// # Safety
///
/// A type implementing this trait, must be a fixed sized type. The information in the `C_DATA_TYPE`
/// constant must be enough to determine both the size and the buffer length of an Instance.
pub unsafe trait Pod: Default + Copy + CElement + CDataMut + 'static {
    /// ODBC C Data type used to bind instances to a statement.
    const C_DATA_TYPE: CDataType;
}

macro_rules! impl_pod {
    ($t:ident, $c_data_type:expr) => {
        unsafe impl CData for $t {
            fn cdata_type(&self) -> CDataType {
                $c_data_type
            }

            fn indicator_ptr(&self) -> *const isize {
                // Fixed sized types do not require a length indicator.
                null()
            }

            fn value_ptr(&self) -> *const c_void {
                self as *const $t as *const c_void
            }

            fn buffer_length(&self) -> isize {
                0
            }
        }

        unsafe impl CDataMut for $t {
            /// Indicates the length of variable sized types. May be zero for fixed sized types.
            fn mut_indicator_ptr(&mut self) -> *mut isize {
                null_mut()
            }

            /// Pointer to a value corresponding to the one described by `cdata_type`.
            fn mut_value_ptr(&mut self) -> *mut c_void {
                self as *mut $t as *mut c_void
            }
        }

        unsafe impl CElement for $t {
            /// Fixed sized types are always complete
            fn assert_completness(&self) {}
        }

        unsafe impl Pod for $t {
            const C_DATA_TYPE: CDataType = $c_data_type;
        }

        unsafe impl FetchRowMember for $t {
            fn indicator(&self) -> Option<Indicator> {
                None
            }
        }
    };
}

impl_pod!(f64, CDataType::Double);
impl_pod!(f32, CDataType::Float);
impl_pod!(Date, CDataType::TypeDate);
impl_pod!(Timestamp, CDataType::TypeTimestamp);
impl_pod!(Time, CDataType::TypeTime);
impl_pod!(Numeric, CDataType::Numeric);
impl_pod!(i16, CDataType::SShort);
impl_pod!(u16, CDataType::UShort);
impl_pod!(i32, CDataType::SLong);
impl_pod!(u32, CDataType::ULong);
impl_pod!(i8, CDataType::STinyInt);
impl_pod!(u8, CDataType::UTinyInt);
impl_pod!(Bit, CDataType::Bit);
impl_pod!(i64, CDataType::SBigInt);
impl_pod!(u64, CDataType::UBigInt);

// While the C-Type is independent of the Data (SQL) Type in the source, there are often DataTypes
// which are a natural match for the C-Type in question. These can be used to spare the user to
// specify that an i32 is supposed to be bound as an SQL Integer.

macro_rules! impl_input_fixed_sized {
    ($t:ident, $data_type:expr) => {
        impl HasDataType for $t {
            fn data_type(&self) -> DataType {
                $data_type
            }
        }

        unsafe impl OutputParameter for $t {}
    };
}

impl_input_fixed_sized!(f64, DataType::Double);
impl_input_fixed_sized!(f32, DataType::Real);
impl_input_fixed_sized!(Date, DataType::Date);
impl_input_fixed_sized!(i16, DataType::SmallInt);
impl_input_fixed_sized!(i32, DataType::Integer);
impl_input_fixed_sized!(i8, DataType::TinyInt);
impl_input_fixed_sized!(Bit, DataType::Bit);
impl_input_fixed_sized!(i64, DataType::BigInt);

// Support for fixed size types, which are not unsigned. Time, Date and timestamp types could be
// supported, implementation DataType would need to take an instance into account.

#[cfg(test)]
mod tests {

    use super::Bit;

    /// `as_bool` should panic if bit is neither 0 or 1.
    #[test]
    #[should_panic(expected = "Invalid boolean representation in Bit.")]
    fn invalid_bit() {
        let bit = Bit(2);
        bit.as_bool();
    }
}
