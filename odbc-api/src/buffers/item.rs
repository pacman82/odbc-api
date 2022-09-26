use odbc_sys::{Date, Time, Timestamp};

use super::{AnySlice, AnySliceMut, BufferKind, NullableSlice, NullableSliceMut};
use crate::Bit;

/// Can either be extracted as a slice or a [`NullableSlice`] from an [`AnyColumnView`]. This allows
/// the user to avoid matching on all possibile variants of an [`AnyColumnView`] in case the
/// buffered type is known at compile time.
pub trait Item: Sized + Copy {
    /// E.g. [`BufferKind::I64`] for `i64`. The kind can be used in a buffer description to
    /// instantiate a [`super::ColumnarBuffer`].
    const BUFFER_KIND: BufferKind;

    /// Extract the array type from an [`AnyColumnView`].
    fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]>;
    /// Extract the typed nullable buffer from an [`AnyColumnView`].
    fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<Self>>;

    /// Extract the array type from an [`AnyColumnSliceMut`].
    fn as_slice_mut(variant: AnySliceMut<'_>) -> Option<&'_ mut [Self]>;

    /// Extract the typed nullable buffer from an [`AnyColumnSliceMut`].
    fn as_nullable_slice_mut(variant: AnySliceMut<'_>) -> Option<NullableSliceMut<'_, Self>>;
}

macro_rules! impl_item {
    ($t:ident, $plain:ident, $null:ident) => {
        impl Item for $t {
            const BUFFER_KIND: BufferKind = BufferKind::$plain;

            fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]> {
                match variant {
                    AnySlice::$plain(vals) => Some(vals),
                    _ => None,
                }
            }

            fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<Self>> {
                match variant {
                    AnySlice::$null(vals) => Some(vals),
                    _ => None,
                }
            }

            fn as_slice_mut<'a>(variant: AnySliceMut<'a>) -> Option<&'a mut [Self]> {
                match variant {
                    AnySliceMut::$plain(vals) => Some(vals),
                    _ => None,
                }
            }

            fn as_nullable_slice_mut<'a>(
                variant: AnySliceMut<'a>,
            ) -> Option<NullableSliceMut<'a, Self>> {
                match variant {
                    AnySliceMut::$null(vals) => Some(vals),
                    _ => None,
                }
            }
        }
    };
}

impl_item!(f64, F64, NullableF64);
impl_item!(f32, F32, NullableF32);
impl_item!(u8, U8, NullableU8);
impl_item!(i8, I8, NullableI8);
impl_item!(i16, I16, NullableI16);
impl_item!(i32, I32, NullableI32);
impl_item!(i64, I64, NullableI64);
impl_item!(Date, Date, NullableDate);
impl_item!(Bit, Bit, NullableBit);
impl_item!(Time, Time, NullableTime);
impl_item!(Timestamp, Timestamp, NullableTimestamp);
