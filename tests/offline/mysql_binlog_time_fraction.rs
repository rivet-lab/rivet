//! The MySQL binlog TIME2 decoder reads a negative TIME(1)/TIME(2) with a fraction as the server stored it.

use mysql_common::Value;
use mysql_common::binlog::misc::{my_time_packed_from_binary, time_from_packed};

fn decode(bytes: &[u8], dec: u32) -> Value {
    time_from_packed(my_time_packed_from_binary(bytes, dec).expect("a TIME2 payload decodes"))
}

#[test]
fn a_negative_time_with_a_one_or_two_digit_fraction_is_read_as_stored() {
    for dec in [1, 2] {
        assert_eq!(
            decode(&[0x7f, 0xff, 0xff, 0xce], dec),
            Value::Time(true, 0, 0, 0, 0, 500_000),
            "TIME({dec}) -00:00:00.50"
        );
        assert_eq!(
            decode(&[0x7f, 0xff, 0xfe, 0xe7], dec),
            Value::Time(true, 0, 0, 0, 1, 250_000),
            "TIME({dec}) -00:00:01.25"
        );
    }
}

#[test]
fn a_negative_time_at_the_range_edge_with_a_fraction_is_read_as_stored() {
    assert_eq!(
        decode(&[0x7f, 0xff, 0xff, 0x9d], 2),
        Value::Time(true, 0, 0, 0, 0, 990_000),
        "TIME(2) -00:00:00.99"
    );
}

#[test]
fn time_values_beside_a_negative_fraction_decode_as_before() {
    assert_eq!(
        decode(&[0x80, 0x00, 0x00, 0x32], 2),
        Value::Time(false, 0, 0, 0, 0, 500_000)
    );
    assert_eq!(
        decode(&[0x7f, 0xff, 0xff, 0x00], 2),
        Value::Time(true, 0, 0, 0, 1, 0)
    );
    assert_eq!(
        decode(&[0x7f, 0xff, 0xff, 0xec, 0x78], 3),
        Value::Time(true, 0, 0, 0, 0, 500_000)
    );
    assert_eq!(
        decode(&[0x7f, 0xff, 0xff, 0xec, 0x78], 4),
        Value::Time(true, 0, 0, 0, 0, 500_000)
    );
}
