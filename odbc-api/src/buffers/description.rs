use std::mem::size_of;

use odbc_sys::{Date, Time, Timestamp};

use crate::{Bit, DataType};

/// Describes a column of a [`crate::buffers::ColumnarBuffer`].
///
/// While related to to the [`crate::DataType`] of the column this is bound to, the Buffer type is
/// different as it does not describe the type of the data source but the format the data is going
/// to be represented in memory. While the data source is often considered to choose the buffer type
/// the kind of processing which is supposed to be applied to the data may be even more important
/// if choosing the a buffer for the cursor type. E.g. if you intend to print a date to standard out
/// it may be more reasonable to bind it as `Text` rather than `Date`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferDesc {
    /// Variable sized binary buffer, holding up to `length` bytes per value.
    Binary {
        /// Maximum number of bytes per value.
        length: usize,
    },
    /// Text buffer holding strings with binary length of up to `max_str_len`.
    ///
    /// Consider an upper bound choosing this based on the information in a [`DataType::Varchar`]
    /// column. E.g. PostgreSQL may return a field size of several GiB for individual values if a
    /// column is specified as `TEXT`, or Microsoft SQL Server may return `0` for a column of type
    /// `VARCHAR(max)`. In such situations, if values are truly that large, bulk fetching data is
    /// not recommended, but streaming individual fields one by one. Usually though, the actual
    /// cells of the table in the database contain much shorter values. The best thing todo is to
    /// adapt the database schema to better reflect the actual size of the values. Lacking control
    /// over the database schema, you can always choose a smaller buffer size than initializing the
    /// buffer in disagreement with the database schema.
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
    F64 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// 32 bit floating point
    F32 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Describes a buffer holding [`crate::sys::Date`] values.
    Date {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Describes a buffer holding [`crate::sys::Time`] values.
    Time {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Describes a buffer holding [`crate::sys::Timestamp`] values.
    Timestamp {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Signed 8 Bit integer
    I8 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Signed 16 Bit integer
    I16 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Signed 32 Bit integer
    I32 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Signed 64 Bit integer
    I64 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Unsigned 8 Bit integer
    U8 {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
    /// Can either be zero or one
    Bit {
        /// This indicates whether or not the buffer will be able to represent NULL values. This will
        /// cause an indicator buffer to be bound.
        nullable: bool,
    },
}

impl BufferDesc {
    pub fn from_data_type(data_type: DataType, nullable: bool) -> Option<Self> {
        let buffer_desc = match data_type {
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 3 => BufferDesc::I8 { nullable },
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 10 => BufferDesc::I32 { nullable },
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 19 => BufferDesc::I64 { nullable },
            DataType::Integer => BufferDesc::I32 { nullable },
            DataType::SmallInt => BufferDesc::I16 { nullable },
            DataType::Float { precision: 0..=24 } | DataType::Real => BufferDesc::F32 { nullable },
            DataType::Float { precision: 25..=53 } |DataType::Double => BufferDesc::F64 { nullable },
            DataType::Date => BufferDesc::Date { nullable },
            DataType::Time { precision: 0 } => BufferDesc::Time { nullable },
            DataType::Timestamp { precision: _ } => BufferDesc::Timestamp { nullable },
            DataType::BigInt => BufferDesc::I64 { nullable },
            DataType::TinyInt => BufferDesc::I8 { nullable },
            DataType::Bit => BufferDesc::Bit { nullable },
            DataType::Varbinary { length }
            | DataType::Binary { length  }
            | DataType::LongVarbinary { length } => length.map(|l| BufferDesc::Binary { length: l.get() })?,
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            // Currently no special buffers for fixed lengths text implemented.
            | DataType::WChar {length }
            | DataType::Char { length }
            | DataType::LongVarchar { length } => {
                length.map(|length| BufferDesc::Text { max_str_len : length.get() } )?
            },
            // Specialized buffers for Numeric and decimal are not yet supported.
            | DataType::Numeric { precision: _, scale: _ }
            | DataType::Decimal { precision: _, scale: _ }
            | DataType::Time { precision: _ } => BufferDesc::Text { max_str_len: data_type.display_size().unwrap().get() },
            DataType::Unknown
            | DataType::Float { precision: _ }
            | DataType::Other { data_type: _, column_size: _, decimal_digits: _ } => return None,
        };
        Some(buffer_desc)
    }

    /// Element size of buffer if bound as a columnar row. Can be used to estimate memory for
    /// columnar bindings.
    pub fn bytes_per_row(&self) -> usize {
        let size_indicator = |nullable: bool| if nullable { size_of::<isize>() } else { 0 };
        match *self {
            BufferDesc::Binary { length } => length + size_indicator(true),
            BufferDesc::Text { max_str_len } => max_str_len + 1 + size_indicator(true),
            BufferDesc::WText { max_str_len } => (max_str_len + 1) * 2 + size_indicator(true),
            BufferDesc::F64 { nullable } => size_of::<f64>() + size_indicator(nullable),
            BufferDesc::F32 { nullable } => size_of::<f32>() + size_indicator(nullable),
            BufferDesc::Date { nullable } => size_of::<Date>() + size_indicator(nullable),
            BufferDesc::Time { nullable } => size_of::<Time>() + size_indicator(nullable),
            BufferDesc::Timestamp { nullable } => size_of::<Timestamp>() + size_indicator(nullable),
            BufferDesc::I8 { nullable } => size_of::<i8>() + size_indicator(nullable),
            BufferDesc::I16 { nullable } => size_of::<i16>() + size_indicator(nullable),
            BufferDesc::I32 { nullable } => size_of::<i32>() + size_indicator(nullable),
            BufferDesc::I64 { nullable } => size_of::<i64>() + size_indicator(nullable),
            BufferDesc::U8 { nullable } => size_of::<u8>() + size_indicator(nullable),
            BufferDesc::Bit { nullable } => size_of::<Bit>() + size_indicator(nullable),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    #[cfg(target_pointer_width = "64")] // Indicator size is platform dependent.
    fn bytes_per_row() {
        assert_eq!(5 + 8, BufferDesc::Binary { length: 5 }.bytes_per_row());
        assert_eq!(
            5 + 1 + 8,
            BufferDesc::Text { max_str_len: 5 }.bytes_per_row()
        );
        assert_eq!(
            10 + 2 + 8,
            BufferDesc::WText { max_str_len: 5 }.bytes_per_row()
        );
        assert_eq!(6, BufferDesc::Date { nullable: false }.bytes_per_row());
        assert_eq!(6, BufferDesc::Time { nullable: false }.bytes_per_row());
        assert_eq!(
            16,
            BufferDesc::Timestamp { nullable: false }.bytes_per_row()
        );
        assert_eq!(1, BufferDesc::Bit { nullable: false }.bytes_per_row());
        assert_eq!(1 + 8, BufferDesc::Bit { nullable: true }.bytes_per_row());
        assert_eq!(4, BufferDesc::F32 { nullable: false }.bytes_per_row());
        assert_eq!(8, BufferDesc::F64 { nullable: false }.bytes_per_row());
        assert_eq!(1, BufferDesc::I8 { nullable: false }.bytes_per_row());
        assert_eq!(2, BufferDesc::I16 { nullable: false }.bytes_per_row());
        assert_eq!(4, BufferDesc::I32 { nullable: false }.bytes_per_row());
        assert_eq!(8, BufferDesc::I64 { nullable: false }.bytes_per_row());
        assert_eq!(1, BufferDesc::U8 { nullable: false }.bytes_per_row());
    }
}
