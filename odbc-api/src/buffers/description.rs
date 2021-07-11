use std::mem::size_of;

use odbc_sys::{Date, Time, Timestamp};

use crate::{Bit, DataType};

/// Used to describe a column of a [`crate::buffers::ColumnarRowSet`].
///
/// While related to to the [`crate::DataType`] of the column this is bound to, the Buffer type is
/// different as it does not describe the type of the data source but the format the data is going
/// to be represented in memory. While the data source is often considered to choose the buffer type
/// the kind of processing which is supposed to be applied to the data may be even more important
/// if choosing the a buffer for the cursor type. I.e. if you intend to print a date to standard out
/// it may be more reasonable to bind it as `Text` rather than `Date`.
#[derive(Clone, Copy, Debug)]
pub struct BufferDescription {
    /// This indicates whether or not the buffer will be able to represent NULL values. This will
    /// cause an indicator buffer to be bound if the selected buffer kind does not already require
    /// one anyway.
    pub nullable: bool,
    /// The type of CData the buffer will be holding.
    pub kind: BufferKind,
}

impl BufferDescription {
    /// Returns the element size of such a buffer if bound as a columnar row. Can be used to
    /// estimate memory for columnar bindings.
    pub fn bytes_per_row(&self) -> usize {
        let indicator = size_of::<isize>();
        let opt_indicator = if self.nullable { indicator } else { 0 };
        match self.kind {
            BufferKind::Binary { length } => length + indicator,
            BufferKind::Text { max_str_len } => max_str_len + 1 + indicator,
            BufferKind::WText { max_str_len } => (max_str_len + 1) * 2 + indicator,
            BufferKind::F64 => size_of::<f64>() + opt_indicator,
            BufferKind::F32 => size_of::<f32>() + opt_indicator,
            BufferKind::Date => size_of::<Date>() + opt_indicator,
            BufferKind::Time => size_of::<Time>() + opt_indicator,
            BufferKind::Timestamp => size_of::<Timestamp>() + opt_indicator,
            BufferKind::I8 => size_of::<i8>() + opt_indicator,
            BufferKind::I16 => size_of::<i16>() + opt_indicator,
            BufferKind::I32 => size_of::<i32>() + opt_indicator,
            BufferKind::I64 => size_of::<i64>() + opt_indicator,
            BufferKind::U8 => size_of::<u8>() + opt_indicator,
            BufferKind::Bit => size_of::<Bit>() + opt_indicator,
        }
    }
}

/// This class is used together with [`crate::buffers::BufferDescription`] to specify the layout of
/// buffers bound to ODBC cursors and statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferKind {
    /// Variable sized binary buffer, holding up to `length` bytes per value.
    Binary {
        /// Maximum number of bytes per value.
        length: usize,
    },
    /// Text buffer holding strings with binary length of up to `max_str_len`.
    Text {
        /// Maximum string length. Terminating zero is excluded, i.e. memory for it will be
        /// implicitly allocated if required.
        max_str_len: usize,
    },
    /// UTF-16 encoded text buffer holding strings with length of up to `max_str_len`. Length is in
    /// terms of 2-Byte characters.
    WText {
        /// Maximum string length. Terminating zero is excluded, i.e. memory for it will be
        /// implicitly allocated if required.
        max_str_len: usize,
    },
    /// 64 bit floating point
    F64,
    /// 32 bit floating point
    F32,
    /// Describes a buffer holding [`crate::sys::Date`] values.
    Date,
    /// Describes a buffer holding [`crate::sys::Time`] values.
    Time,
    /// Describes a buffer holding [`crate::sys::Timestamp`] values.
    Timestamp,
    /// Signed 8 Bit integer
    I8,
    /// Signed 16 Bit integer
    I16,
    /// Signed 32 Bit integer
    I32,
    /// Signed 64 Bit integer
    I64,
    /// Unsigned 8 Bit integer
    U8,
    /// Can either be zero or one
    Bit,
}

impl BufferKind {
    /// Describe a buffer which fits best the SQL Data Type.
    ///
    /// ```
    /// use odbc_api::{DataType, buffers::BufferKind};
    ///
    /// assert_eq!(BufferKind::from_data_type(DataType::Unknown), None);
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 2, scale: 0 }),
    ///     Some(BufferKind::I8)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 9, scale: 0 }),
    ///     Some(BufferKind::I32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 18, scale: 0 }),
    ///     Some(BufferKind::I64)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Numeric { precision: 20, scale: 5 }),
    ///     Some(BufferKind::Text { max_str_len: 20 + 2 })
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Varchar { length: 42 }),
    ///     Some(BufferKind::Text { max_str_len: 42 })
    /// );
    /// // We do not care about the encoding in the datasource. WVarchar is mapped to `Text`, too
    /// // (instead of `WText`).
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::WVarchar { length: 42 }),
    ///     Some(BufferKind::Text { max_str_len: 42 })
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::BigInt),
    ///     Some(BufferKind::I64)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Integer),
    ///     Some(BufferKind::I32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::SmallInt),
    ///     Some(BufferKind::I16)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::TinyInt),
    ///     Some(BufferKind::I8)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Float),
    ///     Some(BufferKind::F32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Double),
    ///     Some(BufferKind::F64)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Date),
    ///     Some(BufferKind::Date)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Time { precision: 0 }),
    ///     Some(BufferKind::Time)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Bit),
    ///     Some(BufferKind::Bit)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Time { precision: 3 }),
    ///     Some(BufferKind::Text { max_str_len: 12 })
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Timestamp { precision: 3 }),
    ///     Some(BufferKind::Timestamp)
    /// );
    /// ```
    pub fn from_data_type(data_type: DataType) -> Option<Self> {
        let buffer_kind = match data_type {
            DataType::Unknown
            | DataType::Other { data_type: _, column_size: _, decimal_digits: _ } => return None,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 3 => BufferKind::I8,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 10 => BufferKind::I32,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 19 => BufferKind::I64,
            DataType::Integer => BufferKind::I32,
            DataType::SmallInt => BufferKind::I16,
            DataType::Float | DataType::Real => BufferKind::F32,
            DataType::Double => BufferKind::F64,
            DataType::Date => BufferKind::Date,
            DataType::Time { precision: 0 } => BufferKind::Time,
            DataType::Timestamp { precision: _ } => BufferKind::Timestamp,
            DataType::BigInt => BufferKind::I64,
            DataType::TinyInt => BufferKind::I8,
            DataType::Bit => BufferKind::Bit,
            DataType::Varbinary { length }
            | DataType::Binary { length  } => BufferKind::Binary { length },
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            // Currently no special buffers for fixed lengths text implemented.
            | DataType::WChar {length }
            | DataType::Char { length }
            | DataType::LongVarchar { length } => BufferKind::Text { max_str_len : length },
            // Specialized buffers for Numeric and decimal are not yet supported.
            | DataType::Numeric { precision: _, scale: _ }
            | DataType::Decimal { precision: _, scale: _ }
            | DataType::Time { precision: _ } => BufferKind::Text { max_str_len: data_type.display_size().unwrap() },
        };
        Some(buffer_kind)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    #[cfg(target_pointer_width = "64")] // Indicator size is platform dependent.
    fn bytes_per_row() {
        let bpr = |kind, nullable| BufferDescription { nullable, kind }.bytes_per_row();

        assert_eq!(5 + 8, bpr(BufferKind::Binary { length: 5 }, false));
        assert_eq!(5 + 1 + 8, bpr(BufferKind::Text { max_str_len: 5 }, false));
        assert_eq!(10 + 2 + 8, bpr(BufferKind::WText { max_str_len: 5 }, false));
        assert_eq!(6, bpr(BufferKind::Date, false));
        assert_eq!(6, bpr(BufferKind::Time, false));
        assert_eq!(16, bpr(BufferKind::Timestamp, false));
        assert_eq!(1, bpr(BufferKind::Bit, false));
        assert_eq!(1 + 8, bpr(BufferKind::Bit, true));
        assert_eq!(4, bpr(BufferKind::F32, false));
        assert_eq!(8, bpr(BufferKind::F64, false));
        assert_eq!(1, bpr(BufferKind::I8, false));
        assert_eq!(2, bpr(BufferKind::I16, false));
        assert_eq!(4, bpr(BufferKind::I32, false));
        assert_eq!(8, bpr(BufferKind::I64, false));
        assert_eq!(1, bpr(BufferKind::U8, false));
    }
}
