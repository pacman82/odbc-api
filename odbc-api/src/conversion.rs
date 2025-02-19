use std::ops::{Add, MulAssign, Sub};

use atoi::{FromRadix10, FromRadix10Signed};

/// Convert the text representation of a decimal into an integer representation. The integer
/// representation is not truncating the fraction, but is instead the value of the decimal times 10
/// to the power of scale. E.g. 123.45 of a Decimal with scale 3 is thought of as 123.450 and
/// represented as 123450. This method will regard any non digit character as a radix character with
/// the exception of a `+` or `-` at the beginning of the string.
///
/// This method is robust against representation which do not have trailing zeroes as well as
/// arbitrary radix character. If you do not write a generic application and now the specific way
/// your database formats decimals you may come up with faster methods to parse decimals.
pub fn decimal_text_to_i128(text: &[u8], scale: usize) -> i128 {
    decimal_text_to_integer(text, scale)
}

impl ToDecimal for i128 {
    const ZERO: Self = 0;
    const TEN: Self = 10;
}

/// Convert the text representation of a decimal into an integer representation. The integer
/// representation is not truncating the fraction, but is instead the value of the decimal times 10
/// to the power of scale. E.g. 123.45 of a Decimal with scale 3 is thought of as 123.450 and
/// represented as 123450. This method will regard any non digit character as a radix character with
/// the exception of a `+` or `-` at the beginning of the string.
///
/// This method is robust against representation which do not have trailing zeroes as well as
/// arbitrary radix character. If you do not write a generic application and now the specific way
/// your database formats decimals you may come up with faster methods to parse decimals.
pub fn decimal_text_to_i64(text: &[u8], scale: usize) -> i64 {
    decimal_text_to_integer(text, scale)
}

impl ToDecimal for i64 {
    const ZERO: Self = 0;
    const TEN: Self = 10;
}

/// Convert the text representation of a decimal into an integer representation. The integer
/// representation is not truncating the fraction, but is instead the value of the decimal times 10
/// to the power of scale. E.g. 123.45 of a Decimal with scale 3 is thought of as 123.450 and
/// represented as 123450. This method will regard any non digit character as a radix character with
/// the exception of a `+` or `-` at the beginning of the string.
///
/// This method is robust against representation which do not have trailing zeroes as well as
/// arbitrary radix character. If you do not write a generic application and now the specific way
/// your database formats decimals you may come up with faster methods to parse decimals.
pub fn decimal_text_to_i32(text: &[u8], scale: usize) -> i32 {
    decimal_text_to_integer(text, scale)
}

impl ToDecimal for i32 {
    const ZERO: Self = 0;
    const TEN: Self = 10;
}

fn decimal_text_to_integer<I>(text: &[u8], scale: usize) -> I
where
    I: ToDecimal,
{
    // High is now the number before the decimal point
    let (mut high, num_digits_high) = I::from_radix_10_signed(text);
    let (low, num_digits_low) = if num_digits_high == text.len() {
        (I::ZERO, 0)
    } else {
        I::from_radix_10(&text[(num_digits_high + 1)..])
    };
    // Left shift high so it is compatible with low
    for _ in 0..num_digits_low {
        high *= I::TEN;
    }
    // We want to increase the absolute of high by low without changing highs sign
    let mut n = if high < I::ZERO || (high == I::ZERO && text[0] == b'-') {
        high - low
    } else {
        high + low
    };
    // We would be done now, if every database would include trailing zeroes, but they might choose
    // to omit those. Therfore we see if we need to leftshift n further in order to meet scale.
    for _ in 0..(scale - num_digits_low) {
        n *= I::TEN;
    }
    n
}

trait ToDecimal:
    FromRadix10 + FromRadix10Signed + Add<Output = Self> + Sub<Output = Self> + MulAssign + Ord
{
    const ZERO: Self;
    const TEN: Self;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An user of an Oracle database got invalid values from decimal after setting
    /// `NLS_NUMERIC_CHARACTERS` to ",." instead of ".".
    ///
    /// See issue:
    /// <https://github.com/pacman82/arrow-odbc-py/discussions/74#discussioncomment-8083928>
    #[test]
    fn decimal_is_represented_with_comma_as_radix() {
        let actual = decimal_text_to_i128(b"10,00000", 5);
        assert_eq!(1_000_000, actual);
    }

    /// Since scale is 5 in this test case we would expect five digits after the radix, yet Oracle
    /// seems to not emit trailing zeroes. Also see issue:
    /// <https://github.com/pacman82/arrow-odbc-py/discussions/74#discussioncomment-8083928>
    #[test]
    fn decimal_with_less_zeroes() {
        let actual = decimal_text_to_i128(b"10.0", 5);
        assert_eq!(1_000_000, actual);
    }

    #[test]
    fn negative_decimal() {
        let actual = decimal_text_to_i128(b"-10.00000", 5);
        assert_eq!(-1_000_000, actual);
    }

    #[test]
    fn negative_decimal_small() {
        let actual = decimal_text_to_i128(b"-0.1", 5);
        assert_eq!(-10000, actual);
    }

    // i64

    /// Since scale is 5 in this test case we would expect five digits after the radix, yet Oracle
    /// seems to not emit trailing zeroes. Also see issue:
    /// <https://github.com/pacman82/arrow-odbc-py/discussions/74#discussioncomment-8083928>
    #[test]
    fn decimal_with_less_zeroes_i64() {
        let actual = decimal_text_to_i64(b"10.0", 5);
        assert_eq!(1_000_000, actual);
    }

    #[test]
    fn negative_decimal_i64() {
        let actual = decimal_text_to_i64(b"-10.00000", 5);
        assert_eq!(-1_000_000, actual);
    }

    #[test]
    fn negative_decimal_small_i64() {
        let actual = decimal_text_to_i64(b"-0.1", 5);
        assert_eq!(-10000, actual);
    }

    // i32

    /// Since scale is 5 in this test case we would expect five digits after the radix, yet Oracle
    /// seems to not emit trailing zeroes. Also see issue:
    /// <https://github.com/pacman82/arrow-odbc-py/discussions/74#discussioncomment-8083928>
    #[test]
    fn decimal_with_less_zeroes_i32() {
        let actual = decimal_text_to_i32(b"10.0", 5);
        assert_eq!(1_000_000, actual);
    }

    #[test]
    fn negative_decimal_i32() {
        let actual = decimal_text_to_i32(b"-10.00000", 5);
        assert_eq!(-1_000_000, actual);
    }

    #[test]
    fn negative_decimal_small_i32() {
        let actual = decimal_text_to_i32(b"-0.1", 5);
        assert_eq!(-10000, actual);
    }
}
