use std::{
    borrow::{Borrow, BorrowMut},
    ffi::c_void,
    marker::PhantomData,
    mem::{size_of, size_of_val},
    num::NonZeroUsize,
    str::Utf8Error,
};

use odbc_sys::{CDataType, NULL_DATA};
use widestring::{U16Str, U16String};

use crate::{
    buffers::{FetchRowMember, Indicator},
    handles::{CData, CDataMut, HasDataType},
    DataType, OutputParameter,
};

use super::CElement;

/// A tag used to differentiate between different types of variadic buffers.
///
/// # Safety
///
/// * [`Self::TERMINATING_ZEROES`] is used to calculate buffer offsets. The number of terminating
///   zeroes is expressed in `BufferElement`s.
/// * [`Self::C_DATA_TYPE`] is used to bind parameters. Providing wrong values like e.g. a fixed length
///   types, would cause even a correctly implemented odbc driver to access invalid memory.
pub unsafe trait VarKind {
    /// Either `u8` for binary and narrow text or `u16` for wide text. Wide text could also be
    /// represented as `u8`, after all everything is bytes. This makes it difficult though to create
    /// owned VarCell types from `u16` buffers.
    type Element: Copy + Eq;
    /// Zero for buffer element.
    const ZERO: Self::Element;
    /// Number of terminating zeroes required for this kind of variadic buffer.
    const TERMINATING_ZEROES: usize;
    const C_DATA_TYPE: CDataType;
    /// Relational type used to bind the parameter. `buffer_length` is specified in elements rather
    /// than bytes, if the two differ.
    fn relational_type(buffer_length: usize) -> DataType;
}

/// Intended to be used as a generic argument for [`VarCell`] to declare that this buffer is used to
/// hold narrow (as opposed to wide UTF-16) text.
#[derive(Clone, Copy)]
pub struct Text;

unsafe impl VarKind for Text {
    type Element = u8;
    const ZERO: u8 = 0;
    const TERMINATING_ZEROES: usize = 1;
    const C_DATA_TYPE: CDataType = CDataType::Char;

    fn relational_type(length: usize) -> DataType {
        // Since we might use as an input buffer, we report the full buffer length in the type and
        // do not deduct 1 for the terminating zero.
        DataType::Varchar {
            length: NonZeroUsize::new(length),
        }
    }
}

/// Intended to be used as a generic argument for [`VarCell`] to declare that this buffer is used to
/// hold wide UTF-16 (as opposed to narrow ASCII or UTF-8) text. Use this to annotate `[u16]`
/// buffers.
#[derive(Clone, Copy)]
pub struct WideText;

unsafe impl VarKind for WideText {
    type Element = u16;
    const ZERO: u16 = 0;
    const TERMINATING_ZEROES: usize = 1;
    const C_DATA_TYPE: CDataType = CDataType::WChar;

    fn relational_type(length: usize) -> DataType {
        // Since we might use as an input buffer, we report the full buffer length in the type and
        // do not deduct 1 for the terminating zero.
        DataType::WVarchar {
            length: NonZeroUsize::new(length),
        }
    }
}

/// Intended to be used as a generic argument for [`VarCell`] to declare that this buffer is used to
/// hold raw binary input.
#[derive(Clone, Copy)]
pub struct Binary;

unsafe impl VarKind for Binary {
    type Element = u8;
    const ZERO: u8 = 0;
    const TERMINATING_ZEROES: usize = 0;
    const C_DATA_TYPE: CDataType = CDataType::Binary;

    fn relational_type(length: usize) -> DataType {
        DataType::Varbinary {
            length: NonZeroUsize::new(length),
        }
    }
}

/// Binds a byte array as Variadic sized character data. It can not be used for columnar bulk
/// fetches, but if the buffer type is stack allocated it can be utilized in row wise bulk fetches.
///
/// Meaningful instantiations of this type are:
///
/// * [`self::VarCharSlice`] - immutable borrowed narrow character strings
/// * [`self::VarCharSliceMut`] - mutable borrowed input / output narrow character strings
/// * [`self::VarCharArray`] - stack allocated owned input / output narrow character strings
/// * [`self::VarCharBox`] - heap allocated owned input /output narrow character strings
/// * [`self::VarWCharSlice`] - immutable borrowed wide character string
/// * [`self::VarWCharSliceMut`] - mutable borrowed input / output wide character string
/// * [`self::VarWCharArray`] - stack allocated owned input / output wide character string
/// * [`self::VarWCharBox`] - heap allocated owned input /output wide character string
/// * [`self::VarBinarySlice`] - immutable borrowed parameter.
/// * [`self::VarBinarySliceMut`] - mutable borrowed input / output parameter
/// * [`self::VarBinaryArray`] - stack allocated owned input / output parameter
/// * [`self::VarBinaryBox`] - heap allocated owned input /output parameter
#[derive(Debug, Clone, Copy)]
pub struct VarCell<B, K> {
    /// Contains the value. Characters must be valid up to the index indicated by `indicator`. If
    /// `indicator` is longer than buffer, the last element in buffer must be a terminating zero,
    /// which is not regarded as being part of the payload itself.
    buffer: B,
    /// Indicates the length of the value stored in `buffer`. Should indicator exceed the buffer
    /// length the value stored in buffer is truncated, and holds actually `buffer.len() - 1` valid
    /// characters. The last element of the buffer being the terminating zero. If indicator is
    /// exactly the buffer length, the value should be considered valid up to the last element,
    /// unless the value is `\0`. In that case we assume `\0` to be a terminating zero left over
    /// from truncation, rather than the last character of the string.
    indicator: isize,
    /// Variadic Kind, declaring wether the buffer holds text or binary data.
    kind: PhantomData<K>,
}

pub type VarBinary<B> = VarCell<B, Binary>;
pub type VarChar<B> = VarCell<B, Text>;
pub type VarWChar<B> = VarCell<B, WideText>;

/// Parameter type for owned, variable sized narrow character data.
///
/// We use `Box<[u8]>` rather than `Vec<u8>` as a buffer type since the indicator pointer already
/// has the role of telling us how many bytes in the buffer are part of the payload.
pub type VarCharBox = VarChar<Box<[u8]>>;

/// Parameter type for owned, variable sized wide character data.
///
/// We use `Box<[u16]>` rather than `Vec<u16>` as a buffer type since the indicator pointer already
/// has the role of telling us how many characters in the buffer are part of the payload.
pub type VarWCharBox = VarWChar<Box<[u16]>>;

/// Parameter type for owned, variable sized binary data.
///
/// We use `Box<[u8]>` rather than `Vec<u8>` as a buffer type since the indicator pointer already
/// has the role of telling us how many bytes in the buffer are part of the payload.
pub type VarBinaryBox = VarBinary<Box<[u8]>>;

impl<K> VarCell<Box<[K::Element]>, K>
where
    K: VarKind,
{
    /// Constructs a 'missing' value.
    pub fn null() -> Self {
        // We do not want to use the empty buffer (`&[]`) here. It would be bound as `VARCHAR(0)`
        // which caused errors with Microsoft Access and older versions of the Microsoft SQL Server
        // ODBC driver.
        Self::from_buffer(Box::new([K::ZERO]), Indicator::Null)
    }

    /// Create a VarChar box from a `Vec`.
    pub fn from_vec(val: Vec<K::Element>) -> Self {
        let indicator = Indicator::Length(val.len() * size_of::<K::Element>());
        let buffer = val.into_boxed_slice();
        Self::from_buffer(buffer, indicator)
    }
}

impl<K> VarCell<Box<[u8]>, K>
where
    K: VarKind<Element = u8>,
{
    /// Create an owned parameter containing the character data from the passed string.
    pub fn from_string(val: String) -> Self {
        Self::from_vec(val.into_bytes())
    }
}

impl<K> VarCell<Box<[u16]>, K>
where
    K: VarKind<Element = u16>,
{
    /// Create an owned parameter containing the character data from the passed string.
    pub fn from_u16_string(val: U16String) -> Self {
        Self::from_vec(val.into_vec())
    }

    /// Create an owned parameter containing the character data from the passed string. Converts it
    /// to UTF-16 and allocates it.
    pub fn from_str_slice(val: &str) -> Self {
        let utf16 = U16String::from_str(val);
        Self::from_u16_string(utf16)
    }
}

impl<B, K> VarCell<B, K>
where
    K: VarKind,
    B: Borrow<[K::Element]>,
{
    /// Creates a new instance from an existing buffer. For text should the indicator be `NoTotal`
    /// or indicate a length longer than buffer, the last element in the buffer must be nul (`\0`).
    pub fn from_buffer(buffer: B, indicator: Indicator) -> Self {
        let buf = buffer.borrow();
        if indicator.is_truncated(size_of_val(buf)) {
            // Value is truncated. Let's check that all required terminating zeroes are at the end
            // of the buffer.
            if !ends_in_zeroes(buf, K::TERMINATING_ZEROES, K::ZERO) {
                panic!("Truncated value must be terminated with zero.")
            }
        }

        Self {
            buffer,
            indicator: indicator.to_isize(),
            kind: PhantomData,
        }
    }

    /// Call this method to ensure that the entire field content did fit into the buffer. If you
    /// retrieve a field using [`crate::CursorRow::get_data`], you can repeat the call until this
    /// method is false to read all the data.
    ///
    /// ```
    /// use odbc_api::{CursorRow, parameter::VarCharArray, Error, handles::Statement};
    ///
    /// fn process_large_text(
    ///     col_index: u16,
    ///     row: &mut CursorRow<'_>
    /// ) -> Result<(), Error>{
    ///     let mut buf = VarCharArray::<512>::NULL;
    ///     row.get_data(col_index, &mut buf)?;
    ///     while !buf.is_complete() {
    ///         // Process bytes in stream without allocation. We can assume repeated calls to
    ///         // get_data do not return `None` since it would have done so on the first call.
    ///         process_text_slice(buf.as_bytes().unwrap());
    ///     }
    ///     Ok(())
    /// }
    ///
    /// fn process_text_slice(text: &[u8]) { /*...*/}
    ///
    /// ```
    ///
    /// ```
    /// use odbc_api::{CursorRow, parameter::VarBinaryArray, Error, handles::Statement};
    ///
    /// fn process_large_binary(
    ///     col_index: u16,
    ///     row: &mut CursorRow<'_>
    /// ) -> Result<(), Error>{
    ///     let mut buf = VarBinaryArray::<512>::NULL;
    ///     row.get_data(col_index, &mut buf)?;
    ///     while !buf.is_complete() {
    ///         // Process bytes in stream without allocation. We can assume repeated calls to
    ///         // get_data do not return `None` since it would have done so on the first call.
    ///         process_slice(buf.as_bytes().unwrap());
    ///     }
    ///     Ok(())
    /// }
    ///
    /// fn process_slice(text: &[u8]) { /*...*/}
    ///
    /// ```
    pub fn is_complete(&self) -> bool {
        let slice = self.buffer.borrow();
        let max_value_length = if ends_in_zeroes(slice, K::TERMINATING_ZEROES, K::ZERO) {
            slice.len() - K::TERMINATING_ZEROES
        } else {
            slice.len()
        };
        !self
            .indicator()
            .is_truncated(max_value_length * size_of::<K::Element>())
    }

    /// Read access to the underlying ODBC indicator. After data has been fetched the indicator
    /// value is set to the length the buffer should have had to hold the entire value. It may also
    /// be [`Indicator::Null`] to indicate `NULL` or [`Indicator::NoTotal`] which tells us the data
    /// source does not know how big the buffer must be to hold the complete value.
    /// [`Indicator::NoTotal`] implies that the content of the current buffer is valid up to its
    /// maximum capacity.
    pub fn indicator(&self) -> Indicator {
        Indicator::from_isize(self.indicator)
    }

    /// Call this method to reset the indicator to a value which matches the length returned by the
    /// [`Self::as_bytes`] method. This is useful if you want to insert values into the database
    /// despite the fact, that they might have been truncated. Otherwise the behaviour of databases
    /// in this situation is driver specific. Some drivers insert up to the terminating zero, others
    /// detect the truncation and throw an error.
    pub fn hide_truncation(&mut self) {
        if !self.is_complete() {
            let binary_length = size_of_val(self.buffer.borrow());
            self.indicator = (binary_length - K::TERMINATING_ZEROES).try_into().unwrap();
        }
    }

    /// Length of the (potentially truncated) value within the cell in bytes. Excluding
    /// terminating zero.
    pub fn len_in_bytes(&self) -> Option<usize> {
        // The maximum length is one larger for untruncated values without terminating zero. E.g.
        // if instantiated from string literal.
        let max_trunc_len_in_bytes =
            (self.buffer.borrow().len() - K::TERMINATING_ZEROES) * size_of::<K::Element>();
        match self.indicator() {
            Indicator::Null => None,
            Indicator::NoTotal => Some(max_trunc_len_in_bytes),
            Indicator::Length(len) => {
                if self.is_complete() {
                    Some(len)
                } else {
                    Some(max_trunc_len_in_bytes)
                }
            }
        }
    }

    /// The payload in bytes the buffer can hold including terminating zeroes
    pub fn capacity_in_bytes(&self) -> usize {
        size_of_val(self.buffer.borrow())
    }

    /// Method backing the implementation of the CElement trait
    fn impl_assert_completness(&self) {
        // There is one edge case in that this is different from `is_complete``, and this is with
        // regards to values of which the payload ends with a terminating zero. All we care about
        // is that the buffer we bind as input is valid. Not necessarily if the value in it is
        // complete.
        let slice = self.buffer.borrow();
        // Terminating zero intenionally not accounted for. Since `VarCell` may hold values without
        // it, if constructed from string literals.
        let max_len_bytes = size_of_val(slice);
        if self.indicator().is_truncated(max_len_bytes) {
            panic!("Truncated values must not be used be bound as input parameters.")
        }
    }
}

impl<B, K> VarCell<B, K>
where
    B: Borrow<[K::Element]>,
    K: VarKind,
{
    /// Valid payload of the buffer (excluding terminating zeroes) returned as slice or `None` in
    /// case the indicator is `NULL_DATA`.
    pub fn as_slice(&self) -> Option<&[K::Element]> {
        let slice = self.buffer.borrow();
        self.len_in_bytes()
            .map(|len| &slice[..(len / size_of::<K::Element>())])
    }
}

impl<B, K> VarCell<B, K>
where
    B: Borrow<[u8]>,
    K: VarKind<Element = u8>,
{
    /// Valid payload of the buffer (excluding terminating zeroes) returned as slice or `None` in
    /// case the indicator is `NULL_DATA`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.as_slice()
    }
}

impl<B> VarCell<B, Text>
where
    B: Borrow<[u8]>,
{
    pub fn as_str(&self) -> Result<Option<&str>, Utf8Error> {
        if let Some(bytes) = self.as_bytes() {
            let text = std::str::from_utf8(bytes)?;
            Ok(Some(text))
        } else {
            Ok(None)
        }
    }
}

impl<B> VarCell<B, WideText>
where
    B: Borrow<[u16]>,
{
    pub fn as_utf16(&self) -> Option<&U16Str> {
        if let Some(chars) = self.as_slice() {
            let text = U16Str::from_slice(chars);
            Some(text)
        } else {
            None
        }
    }
}

unsafe impl<B, K> CData for VarCell<B, K>
where
    B: Borrow<[K::Element]>,
    K: VarKind,
{
    fn cdata_type(&self) -> CDataType {
        K::C_DATA_TYPE
    }

    fn indicator_ptr(&self) -> *const isize {
        &self.indicator as *const isize
    }

    fn value_ptr(&self) -> *const c_void {
        self.buffer.borrow().as_ptr() as *const c_void
    }

    fn buffer_length(&self) -> isize {
        // This is the maximum buffer length, but it is NOT the length of an instance of Self due to
        // the missing size of the indicator value. As such the buffer length can not be used to
        // correctly index a columnar buffer of Self.
        size_of_val(self.buffer.borrow()).try_into().unwrap()
    }
}

impl<B, K> HasDataType for VarCell<B, K>
where
    B: Borrow<[K::Element]>,
    K: VarKind,
{
    fn data_type(&self) -> DataType {
        K::relational_type(self.buffer.borrow().len())
    }
}

unsafe impl<B, K> CDataMut for VarCell<B, K>
where
    B: BorrowMut<[K::Element]>,
    K: VarKind,
{
    fn mut_indicator_ptr(&mut self) -> *mut isize {
        &mut self.indicator as *mut isize
    }

    fn mut_value_ptr(&mut self) -> *mut c_void {
        self.buffer.borrow_mut().as_mut_ptr() as *mut c_void
    }
}

/// Binds a byte array as a VarChar input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// strings which are not zero terminated we need to store the length in a separate value.
///
/// This type is created if `into_parameter` of the `IntoParameter` trait is called on a `&str`.
///
/// # Example
///
/// ```no_run
/// use odbc_api::{Environment, ConnectionOptions, IntoParameter};
///
/// let env = Environment::new()?;
///
/// let mut conn = env.connect(
///     "YourDatabase", "SA", "My@Test@Password1",
///     ConnectionOptions::default()
/// )?;
/// if let Some(cursor) = conn.execute(
///     "SELECT year FROM Birthdays WHERE name=?;",
///     &"Bernd".into_parameter())?
/// {
///     // Use cursor to process query results.
/// };
/// # Ok::<(), odbc_api::Error>(())
/// ```
pub type VarCharSlice<'a> = VarChar<&'a [u8]>;

pub type VarWCharSlice<'a> = VarWChar<&'a [u16]>;

/// Binds a byte array as a variadic binary input parameter.
///
/// While a byte array can provide us with a pointer to the start of the array and the length of the
/// array itself, it can not provide us with a pointer to the length of the buffer. So to bind
/// byte slices (`&[u8]`) we need to store the length in a separate value.
///
/// This type is created if `into_parameter` of the `IntoParameter` trait is called on a `&[u8]`.
pub type VarBinarySlice<'a> = VarBinary<&'a [u8]>;

impl<K> VarCell<&'_ [u8], K> {
    /// Indicates missing data
    pub const NULL: Self = Self {
        // We do not want to use the empty buffer (`&[]`) here. It would be bound as `VARCHAR(0)`
        // which caused errors with Microsoft Access and older versions of the Microsoft SQL Server
        // ODBC driver.
        buffer: &[0],
        indicator: NULL_DATA,
        kind: PhantomData,
    };
}

impl<K> VarCell<&'_ [u16], K> {
    /// Indicates missing data
    pub const NULL: Self = Self {
        // We do not want to use the empty buffer (`&[]`) here. It would be bound as `VARCHAR(0)`
        // which caused errors with Microsoft Access and older versions of the Microsoft SQL Server
        // ODBC driver.
        buffer: &[0],
        indicator: NULL_DATA,
        kind: PhantomData,
    };
}

impl<'a, K> VarCell<&'a [K::Element], K>
where
    K: VarKind,
{
    /// Constructs a new VarChar containing the text in the specified buffer.
    ///
    /// Caveat: This constructor is going to create a truncated value in case the input slice ends
    /// with `nul`. Should you want to insert an actual string those payload ends with `nul` into
    /// the database you need a buffer one byte longer than the string. You can instantiate such a
    /// value using [`Self::from_buffer`].
    pub fn new(value: &'a [K::Element]) -> Self {
        Self::from_buffer(value, Indicator::Length(size_of_val(value)))
    }
}

/// Wraps a slice so it can be used as an output parameter for narrow character data.
pub type VarCharSliceMut<'a> = VarChar<&'a mut [u8]>;

/// Wraps a slice so it can be used as an output parameter for wide character data.
pub type VarWCharSliceMut<'a> = VarWChar<&'a mut [u8]>;

/// Wraps a slice so it can be used as an output parameter for binary data.
pub type VarBinarySliceMut<'a> = VarBinary<&'a mut [u8]>;

/// A stack allocated VARCHAR type.
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// a row-by-row output, but not be used in columnar parameter arrays or output buffers.
///
/// You can also use [`VarCharArray`] as an output type for statement execution using
/// [`crate::parameter::Out`] or [`crate::parameter::InOut`].
///
/// # Example
///
/// ```no_run
/// # use odbc_api::{Connection, Error, parameter::{VarCharArray, Out}};
/// # fn output_example(connection: Connection<'_>) -> Result<(), Error> {
/// let mut out_msg: VarCharArray<255> = VarCharArray::NULL;
/// connection.execute("CALL PROCEDURE_NAME(?)", (Out(&mut out_msg),))?;
/// # Ok(())
/// # }
/// ```
pub type VarCharArray<const LENGTH: usize> = VarChar<[u8; LENGTH]>;

/// A stack allocated NVARCHAR type.
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// a row-by-row output, but not be used in columnar parameter arrays or output buffers.
pub type VarWCharArray<const LENGTH: usize> = VarWChar<[u16; LENGTH]>;

/// A stack allocated VARBINARY type.
///
/// Due to its memory layout this type can be bound either as a single parameter, or as a column of
/// a row-by-row output, but not be used in columnar parameter arrays or output buffers.
pub type VarBinaryArray<const LENGTH: usize> = VarBinary<[u8; LENGTH]>;

impl<const LENGTH: usize, K, E> Default for VarCell<[E; LENGTH], K>
where
    E: Default + Copy,
{
    fn default() -> Self {
        Self {
            buffer: [E::default(); LENGTH],
            indicator: Indicator::Null.to_isize(),
            kind: Default::default(),
        }
    }
}

impl<const LENGTH: usize, K: VarKind> VarCell<[K::Element; LENGTH], K> {
    /// Indicates a missing value.
    pub const NULL: Self = Self {
        buffer: [K::ZERO; LENGTH],
        indicator: NULL_DATA,
        kind: PhantomData,
    };

    /// Construct from a slice. If value is longer than `LENGTH` it will be truncated. In that case
    /// the last byte will be set to `0`.
    pub fn new(elements: &[K::Element]) -> Self {
        let indicator = (size_of_val(elements)).try_into().unwrap();
        let mut buffer = [K::ZERO; LENGTH];
        if elements.len() > LENGTH {
            buffer.copy_from_slice(&elements[..LENGTH]);
            *buffer.last_mut().unwrap() = K::ZERO;
        } else {
            buffer[..elements.len()].copy_from_slice(elements);
        };
        Self {
            buffer,
            indicator,
            kind: PhantomData,
        }
    }
}

/// Figures out, wether or not the buffer ends with a fixed number of zeroes.
fn ends_in_zeroes<T>(buffer: &[T], number_of_zeroes: usize, zero: T) -> bool
where
    T: Copy + Eq,
{
    buffer.len() >= number_of_zeroes
        && buffer
            .iter()
            .rev()
            .copied()
            .take(number_of_zeroes)
            .all(|byte| byte == zero)
}

// We can't go all out and implement these traits for anything implementing Borrow and BorrowMut,
// because erroneous but still safe implementation of these traits could cause invalid memory access
// down the road. E.g. think about returning a different slice with a different length for borrow
// and borrow_mut.
unsafe impl<K: VarKind> CElement for VarCell<&'_ [K::Element], K> {
    fn assert_completness(&self) {
        self.impl_assert_completness()
    }
}

unsafe impl<const LENGTH: usize, K: VarKind> CElement for VarCell<[K::Element; LENGTH], K> {
    fn assert_completness(&self) {
        self.impl_assert_completness()
    }
}
unsafe impl<const LENGTH: usize, K: VarKind> OutputParameter for VarCell<[K::Element; LENGTH], K> {}

unsafe impl<K: VarKind> CElement for VarCell<&'_ mut [K::Element], K> {
    fn assert_completness(&self) {
        self.impl_assert_completness()
    }
}
unsafe impl<K: VarKind> OutputParameter for VarCell<&'_ mut [K::Element], K> {}

unsafe impl<K: VarKind> CElement for VarCell<Box<[K::Element]>, K> {
    fn assert_completness(&self) {
        self.impl_assert_completness()
    }
}
unsafe impl<K: VarKind> OutputParameter for VarCell<Box<[K::Element]>, K> {}

unsafe impl<const LENGTH: usize> FetchRowMember for VarCharArray<LENGTH> {
    fn indicator(&self) -> Option<Indicator> {
        Some(self.indicator())
    }
}

unsafe impl<const LENGTH: usize> FetchRowMember for VarWCharArray<LENGTH> {
    fn indicator(&self) -> Option<Indicator> {
        Some(self.indicator())
    }
}

unsafe impl<const LENGTH: usize> FetchRowMember for VarBinaryArray<LENGTH> {
    fn indicator(&self) -> Option<Indicator> {
        Some(self.indicator())
    }
}

#[cfg(test)]
mod tests {

    use super::{Indicator, VarCharSlice};

    #[test]
    fn must_accept_fitting_values_and_correctly_truncated_ones() {
        // Fine: not truncated
        VarCharSlice::from_buffer(b"12345", Indicator::Length(5));
        // Fine: truncated, but ends in zero
        VarCharSlice::from_buffer(b"1234\0", Indicator::Length(10));
    }

    #[test]
    #[should_panic]
    fn must_ensure_truncated_values_are_terminated() {
        // Not fine, value is too long, but not terminated by zero
        VarCharSlice::from_buffer(b"12345", Indicator::Length(10));
    }
}
