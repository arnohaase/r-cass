

pub (crate) fn ser_u32(buf: &mut Vec<u8>, value: u32) {
    buf.push((value >> 24) as u8);
    buf.push((value >> 16) as u8);
    buf.push((value >>  8) as u8);
    buf.push((value      ) as u8);
}
pub (crate) fn deser_u32(buf: &[u8], offs: usize) -> u32 {
    (buf[offs]   as u32) << 24 +
    (buf[offs+1] as u32) << 16 +
    (buf[offs+2] as u32) <<  8 +
    (buf[offs+3] as u32)
}
