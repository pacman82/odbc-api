use odbc_sys::{Date, Time, Timestamp};

use super::{AnySlice, AnySliceMut, BufferDesc, NullableSlice, NullableSliceMut};
use crate::Bit;

#[allow(deprecated)]
use super::BufferKind;

/// Can either be extracted as a slice or a [`NullableSlice`] from an [`AnySlice`]. This allows
/// the user to avoid matching on all possibile variants of an [`AnySlice`] in case the
/// buffered type is known at compile time.
pub trait Item: Sized + Copy {
    /// E.g. [`BufferKind::I64`] for `i64`. The kind can be used in a buffer description to
    /// instantiate a [`super::ColumnarBuffer`].
    #[deprecated = "Use associated method buffer_desc instead."]
    #[allow(deprecated)]
    const BUFFER_KIND: BufferKind;

    /// Can be used to instantiate a [`super::ColumnarBuffer`].
    fn buffer_desc(nullable: bool) -> BufferDesc;

    /// Extract the array type from an [`AnySlice`].
    fn as_slice(variant: AnySlice<'_>) -> Option<&[Self]>;
    /// Extract the typed nullable buffer from an [`AnySlice`].
    fn as_nullable_slice(variant: AnySlice<'_>) -> Option<NullableSlice<Self>>;

    /// Extract the array type from an [`AnySliceMut`].
    fn as_slice_mut(variant: AnySliceMut<'_>) -> Option<&'_ mut [Self]>;

    /// Extract the typed nullable buffer from an [`AnySliceMut`].
    fn as_nullable_slice_mut(variant: AnySliceMut<'_>) -> Option<NullableSliceMut<'_, Self>>;
}

macro_rules! impl_item {
    ($t:ident, $plain:ident, $null:ident) => {
        impl Item for $t {
            #[allow(deprecated)]
            const BUFFER_KIND: BufferKind = BufferKind::$plain;

            fn buffer_desc(nullable: bool) -> BufferDesc {
                BufferDesc::$plain { nullable }
            }

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
