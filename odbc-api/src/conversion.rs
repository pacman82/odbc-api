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
    // High is now the number before the decimal point
    let (mut high, num_digits_high) = i128::from_radix_10_signed(text);
    let (low, num_digits_low) = if num_digits_high == text.len() {
        (0, 0)
    } else {
        i128::from_radix_10(&text[(num_digits_high + 1)..])
    };
    // Left shift high so it is compatible with low
    for _ in 0..num_digits_low {
        high *= 10;
    }
    // We want to increase the absolute of high by low without changing highs sign
    let mut n = if high < 0 || (high == 0 && text[0] == b'-') {
        high - low
    } else {
        high + low
    };
    // We would be done now, if every database would include trailing zeroes, but they might choose
    // to omit those. Therfore we see if we need to leftshift n further in order to meet scale.
    for _ in 0..(scale - num_digits_low) {
        n *= 10;
    }
    n
}

#[cfg(test)]
mod tests {
    use super::decimal_text_to_i128;

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
}
