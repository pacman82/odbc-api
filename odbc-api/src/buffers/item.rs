use odbc_sys::{Date, Time, Timestamp};

use super::{AnySlice, AnySliceMut, BufferDesc, NullableSlice, NullableSliceMut};
use crate::{BindParamDesc, Bit};

/// Can either be extracted as a slice or a [`NullableSlice`] from an [`AnySlice`]. This allows
/// the user to avoid matching on all possible variants of an [`AnySlice`] in case the
/// buffered type is known at compile time.
///
/// Usually used in generic code. E.g.:
///
/// ```
/// use odbc_api::{Connection, buffers::Item};
///
/// fn insert_tuple2_vec<A: Item, B: Item>(
///     conn: &Connection<'_>,
///     insert_sql: &str,
///     source: &[(A, B)],
/// ) {
///     let mut prepared = conn.prepare(insert_sql).unwrap();
///     // Number of rows submitted in one round trip
///     let capacity = source.len();
///     // We do not need a nullable buffer since elements of source are not optional
///     let descriptions = [A::bind_param_desc(false), B::bind_param_desc(false)];
///     let mut inserter = prepared.column_inserter(capacity, descriptions).unwrap();
///     // We send everything in one go.
///     inserter.set_num_rows(source.len());
///     // Now let's copy the row based tuple into the columnar structure
///     for (index, (a, b)) in source.iter().enumerate() {
///         inserter.column_mut(0).as_slice::<A>().unwrap()[index] = *a;
///         inserter.column_mut(1).as_slice::<B>().unwrap()[index] = *b;
///     }
///     inserter.execute().unwrap();
/// }
/// ```
pub trait Item: Sized + Copy {
    /// Useful to instantiate a [`crate::ColumnarBulkInserter`] in generic code.
    fn bind_param_desc(nullable: bool) -> BindParamDesc;

    /// Can be used to instantiate a [`super::ColumnarBuffer`]. This is useful to allocate the
    /// correct buffers in generic code.
    ///
    /// # Example:
    ///
    /// Specification:
    ///
    /// ```
    /// use odbc_api::buffers::{Item, BufferDesc};
    ///
    /// assert_eq!(BufferDesc::I64{ nullable: true }, i64::buffer_desc(true));
    /// assert_eq!(BufferDesc::I64{ nullable: false }, i64::buffer_desc(false));
    /// ```
    #[deprecated(note = "Use `bind_param_desc` instead")]
    fn buffer_desc(nullable: bool) -> BufferDesc;

    /// Extract the array type from an [`AnySlice`].
    fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]>;
    /// Extract the typed nullable buffer from an [`AnySlice`].
    fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<'_, Self>>;

    /// Extract the array type from an [`AnySliceMut`].
    fn as_slice_mut(variant: AnySliceMut<'_>) -> Option<&'_ mut [Self]>;

    /// Extract the typed nullable buffer from an [`AnySliceMut`].
    fn as_nullable_slice_mut(variant: AnySliceMut<'_>) -> Option<NullableSliceMut<'_, Self>>;
}

macro_rules! impl_item {
    ($t:ident, $bind:ident, $plain:ident, $null:ident) => {
        impl Item for $t {
            fn bind_param_desc(nullable: bool) -> BindParamDesc {
                BindParamDesc::$bind(nullable)
            }

            fn buffer_desc(nullable: bool) -> BufferDesc {
                BufferDesc::$plain { nullable }
            }

            fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]> {
                match variant {
                    AnySlice::$plain(vals) => Some(vals),
                    _ => None,
                }
            }

            fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<'_, Self>> {
                match variant {
                    AnySlice::$null(vals) => Some(vals),
                    _ => None,
                }
            }

            fn as_slice_mut(variant: AnySliceMut<'_>) -> Option<&'_ mut [Self]> {
                match variant {
                    AnySliceMut::$plain(vals) => Some(vals),
                    _ => None,
                }
            }

            fn as_nullable_slice_mut(
                variant: AnySliceMut<'_>,
            ) -> Option<NullableSliceMut<'_, Self>> {
                match variant {
                    AnySliceMut::$null(vals) => Some(vals),
                    _ => None,
                }
            }
        }
    };
}

impl_item!(f64, f64, F64, NullableF64);
impl_item!(f32, f32, F32, NullableF32);
impl_item!(u8, u8, U8, NullableU8);
impl_item!(i8, i8, I8, NullableI8);
impl_item!(i16, i16, I16, NullableI16);
impl_item!(i32, i32, I32, NullableI32);
impl_item!(i64, i64, I64, NullableI64);
impl_item!(Date, date, Date, NullableDate);
impl_item!(Bit, bit, Bit, NullableBit);

/// Since buffer shapes are the same for all time/timestamp types independent of the precision and
/// we do not know the precise SQL type. In order to still be able to bind time/timestamp buffers as
/// input without requiring the user to separately specify the precision, we declare 100 nanosecond
/// precision. This was the highest precision still supported by MSSQL in the tests.
const DEFAULT_TIME_PRECISION: i16 = 7;

impl Item for Time {
    fn bind_param_desc(nullable: bool) -> BindParamDesc {
        BindParamDesc::time(nullable, DEFAULT_TIME_PRECISION)
    }

    fn buffer_desc(nullable: bool) -> BufferDesc {
        BufferDesc::Time { nullable }
    }

    fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]> {
        if let AnySlice::Time(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }

    fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<'_, Self>> {
        if let AnySlice::NullableTime(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }

    fn as_slice_mut(variant: AnySliceMut<'_>) -> Option<&'_ mut [Self]> {
        if let AnySliceMut::Time(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }

    fn as_nullable_slice_mut(variant: AnySliceMut<'_>) -> Option<NullableSliceMut<'_, Self>> {
        if let AnySliceMut::NullableTime(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }
}

impl Item for Timestamp {
    fn bind_param_desc(nullable: bool) -> BindParamDesc {
        BindParamDesc::timestamp(nullable, DEFAULT_TIME_PRECISION)
    }

    fn buffer_desc(nullable: bool) -> BufferDesc {
        BufferDesc::Timestamp { nullable }
    }

    fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]> {
        if let AnySlice::Timestamp(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }

    fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<'_, Self>> {
        if let AnySlice::NullableTimestamp(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }

    fn as_slice_mut(variant: AnySliceMut<'_>) -> Option<&'_ mut [Self]> {
        if let AnySliceMut::Timestamp(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }

    fn as_nullable_slice_mut(variant: AnySliceMut<'_>) -> Option<NullableSliceMut<'_, Self>> {
        if let AnySliceMut::NullableTimestamp(vals) = variant {
            Some(vals)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BindParamDesc,
        sys::{Date, Time, Timestamp},
    };

    use super::{Bit, Item as _};

    #[test]
    fn item_to_bind_param_desc() {
        assert_eq!(BindParamDesc::i32(false), i32::bind_param_desc(false));
        assert_eq!(BindParamDesc::i32(true), i32::bind_param_desc(true));
        assert_eq!(BindParamDesc::f64(false), f64::bind_param_desc(false));
        assert_eq!(BindParamDesc::f32(false), f32::bind_param_desc(false));
        assert_eq!(BindParamDesc::u8(false), u8::bind_param_desc(false));
        assert_eq!(BindParamDesc::i8(false), i8::bind_param_desc(false));
        assert_eq!(BindParamDesc::i16(false), i16::bind_param_desc(false));
        assert_eq!(BindParamDesc::i64(false), i64::bind_param_desc(false));
        assert_eq!(BindParamDesc::date(false), Date::bind_param_desc(false));
        assert_eq!(BindParamDesc::bit(false), Bit::bind_param_desc(false));
        assert_eq!(BindParamDesc::time(false, 7), Time::bind_param_desc(false));
        assert_eq!(
            BindParamDesc::timestamp(false, 7),
            Timestamp::bind_param_desc(false)
        );
    }
}
