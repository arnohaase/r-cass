use std::io::ErrorKind;
use uuid::{Uuid, Variant, Version};


/// a partition key's hash used to assign the key to node(s), among other things
pub type Token = u128;

/// timestamps are nanos since EPOCH
pub type DbTimestamp = u64;


/// expiry timestamps are seconds since EPOCH (u32 means overflow end of 21st century - enough for now)
pub type DbExpiryTimestamp = u32;

pub (crate) fn other_error<T>(text: &str) -> std::io::Result<T> {
    Err(std::io::Error::new(ErrorKind::Other, text))
}
