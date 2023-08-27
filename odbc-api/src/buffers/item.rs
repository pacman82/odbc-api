use odbc_sys::{Date, Time, Timestamp};

use super::{AnySlice, AnySliceMut, BufferDesc, NullableSlice, NullableSliceMut};
use crate::Bit;

/// Can either be extracted as a slice or a [`NullableSlice`] from an [`AnySlice`]. This allows
/// the user to avoid matching on all possibile variants of an [`AnySlice`] in case the
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
///     let descriptions = [A::buffer_desc(false), B::buffer_desc(false)];
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
