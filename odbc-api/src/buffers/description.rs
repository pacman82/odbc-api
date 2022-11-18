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
            | DataType::LongVarbinary { length } => BufferDesc::Binary { length },
            DataType::Varchar { length }
            | DataType::WVarchar { length }
            // Currently no special buffers for fixed lengths text implemented.
            | DataType::WChar {length }
            | DataType::Char { length }
            | DataType::LongVarchar { length } => BufferDesc::Text { max_str_len : length },
            // Specialized buffers for Numeric and decimal are not yet supported.
            | DataType::Numeric { precision: _, scale: _ }
            | DataType::Decimal { precision: _, scale: _ }
            | DataType::Time { precision: _ } => BufferDesc::Text { max_str_len: data_type.display_size().unwrap() },
            DataType::Unknown
            | DataType::Float { precision: _ }
            | DataType::Other { data_type: _, column_size: _, decimal_digits: _ } => return None,
        };
        Some(buffer_desc)
    }
}

/// Describes a column of a [`crate::buffers::ColumnarBuffer`].
///
/// While related to to the [`crate::DataType`] of the column this is bound to, the Buffer type is
/// different as it does not describe the type of the data source but the format the data is going
/// to be represented in memory. While the data source is often considered to choose the buffer type
/// the kind of processing which is supposed to be applied to the data may be even more important
/// if choosing the a buffer for the cursor type. E.g. if you intend to print a date to standard out
/// it may be more reasonable to bind it as `Text` rather than `Date`.
#[derive(Clone, Copy, Debug)]
pub struct BufferDescription {
    /// This indicates whether or not the buffer will be able to represent NULL values. This will
    /// cause an indicator buffer to be bound.
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

impl From<BufferDescription> for BufferDesc {
    fn from(source: BufferDescription) -> Self {
        let nullable = source.nullable;
        match source.kind {
            BufferKind::Binary { length } => BufferDesc::Binary { length },
            BufferKind::Text { max_str_len } => BufferDesc::Text { max_str_len },
            BufferKind::WText { max_str_len } => BufferDesc::WText { max_str_len },
            BufferKind::F64 => BufferDesc::F64 { nullable },
            BufferKind::F32 => BufferDesc::F32 { nullable },
            BufferKind::Date => BufferDesc::Date { nullable },
            BufferKind::Time => BufferDesc::Time { nullable },
            BufferKind::Timestamp => BufferDesc::Timestamp { nullable },
            BufferKind::I8 => BufferDesc::I8 { nullable },
            BufferKind::I16 => BufferDesc::I16 { nullable },
            BufferKind::I32 => BufferDesc::I32 { nullable },
            BufferKind::I64 => BufferDesc::I64 { nullable },
            BufferKind::U8 => BufferDesc::U8 { nullable },
            BufferKind::Bit => BufferDesc::Bit { nullable },
        }
    }
}

/// This class is used together with [`BufferDescription`] to specify the layout of buffers bound to
/// ODBC cursors and statements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferKind {
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
    ///     BufferKind::from_data_type(DataType::Float { precision : 24 }),
    ///     Some(BufferKind::F32)
    /// );
    /// assert_eq!(
    ///     BufferKind::from_data_type(DataType::Float { precision : 53 }),
    ///     Some(BufferKind::F64)
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
    #[deprecated = "Use BufferDesc::from_data_type instead"]
    pub fn from_data_type(data_type: DataType) -> Option<Self> {
        let buffer_kind = match data_type {
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 3 => BufferKind::I8,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 10 => BufferKind::I32,
            DataType::Numeric { precision, scale }
            | DataType::Decimal { precision, scale } if scale == 0 && precision < 19 => BufferKind::I64,
            DataType::Integer => BufferKind::I32,
            DataType::SmallInt => BufferKind::I16,
            DataType::Float { precision: 0..=24 } | DataType::Real => BufferKind::F32,
            DataType::Float { precision: 25..=53 } |DataType::Double => BufferKind::F64,
            DataType::Date => BufferKind::Date,
            DataType::Time { precision: 0 } => BufferKind::Time,
            DataType::Timestamp { precision: _ } => BufferKind::Timestamp,
            DataType::BigInt => BufferKind::I64,
            DataType::TinyInt => BufferKind::I8,
            DataType::Bit => BufferKind::Bit,
            DataType::Varbinary { length }
            | DataType::Binary { length  }
            | DataType::LongVarbinary { length } => BufferKind::Binary { length },
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
            DataType::Unknown
            | DataType::Float { precision: _ }
            | DataType::Other { data_type: _, column_size: _, decimal_digits: _ } => return None,
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
