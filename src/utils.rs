pub(crate) fn set_bit(value: &u64, index: u8) -> u64 {
    assert!(index < u64::BITS as u8, "index too large");
    return value | (1 << index);
} 

pub(crate) fn clear_bit(value: &u64, index: u8) -> u64 {
    assert!(index < u64::BITS as u8, "index too large");
    return value & (!(1 << index));
} 

pub(crate) fn get_bit(value: &u64, index: u8) -> bool {
    assert!(index < u64::BITS as u8, "index too large");
    return ((value >> index) & 1) != 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn bit_tests() {
        // set bit tests
        assert_eq!(set_bit(&0b100001, 0), 0b100001);
        assert_eq!(set_bit(&0b100000, 0), 0b100001);
        assert_eq!(set_bit(&0b100001, 1), 0b100011);
        assert_eq!(set_bit(&0b100011, 2), 0b100111);
        assert_eq!(set_bit(&0b100001, 2), 0b100101);

        // clear bit tests
        assert_eq!(clear_bit(&0b100000, 0), 0b100000);
        assert_eq!(clear_bit(&0b100001, 0), 0b100000);
        assert_eq!(clear_bit(&0b100111, 1), 0b100101);

        // get bit tests
        assert_eq!(get_bit(&0b100000, 0), false);
        assert_eq!(get_bit(&0b100001, 0), true);
        assert_eq!(get_bit(&0b100111, 1), true);
    }
}