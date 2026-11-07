//! Exact parsing of exchange decimal strings into the output's fixed point.

/// The output's fixed point: 1_000_000 is one USDC, or one share/contract.
pub const ONE: i64 = 1_000_000;

const DECIMALS: usize = 6;

/// Parses a decimal string such as `"0.0800"` or `"-54.00"` into fixed point, without going
/// through floats. More than six decimals is an error rather than a silent rounding.
pub fn parse(text: &str) -> anyhow::Result<i64> {
    let invalid = || anyhow::anyhow!("not a fixed-point decimal: {text:?}");
    let (sign, digits) = match text.strip_prefix('-') {
        Some(unsigned) => (-1, unsigned),
        None => (1, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let all_digits = |part: &str| part.bytes().all(|byte| byte.is_ascii_digit());
    if whole.is_empty() || fraction.len() > DECIMALS || !all_digits(whole) || !all_digits(fraction)
    {
        return Err(invalid());
    }
    let whole: i64 = whole.parse().map_err(|_| invalid())?;
    let fraction: i64 = match fraction {
        "" => 0,
        digits => {
            let scale = 10_i64.pow((DECIMALS - digits.len()) as u32);
            digits.parse::<i64>().map_err(|_| invalid())? * scale
        }
    };
    whole
        .checked_mul(ONE)
        .and_then(|value| value.checked_add(fraction))
        .map(|value| sign * value)
        .ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exchange_prices_and_quantities_exactly() {
        assert_eq!(parse("0.0800").unwrap(), 80_000);
        assert_eq!(parse("0.96").unwrap(), 960_000);
        assert_eq!(parse("300.00").unwrap(), 300_000_000);
        assert_eq!(parse("219.217767").unwrap(), 219_217_767);
        assert_eq!(parse("-54.00").unwrap(), -54_000_000);
        assert_eq!(parse("2116131.59").unwrap(), 2_116_131_590_000);
    }

    #[test]
    fn parses_integers_and_a_bare_trailing_point() {
        assert_eq!(parse("0").unwrap(), 0);
        assert_eq!(parse("5").unwrap(), 5 * ONE);
        assert_eq!(parse("5.").unwrap(), 5 * ONE);
    }

    #[test]
    fn rejects_anything_that_is_not_an_exact_decimal() {
        for text in [
            "",
            "-",
            ".5",
            "1.2345678",
            "1e3",
            "+1",
            " 1",
            "1.2.3",
            "0x10",
            "9999999999999999999",
        ] {
            assert!(parse(text).is_err(), "{text:?} should be rejected");
        }
    }

    #[test]
    fn rejects_values_that_overflow_fixed_point() {
        assert!(parse("9300000000000").is_err());
    }
}
