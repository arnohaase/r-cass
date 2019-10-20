use std::io::{Write, Seek, SeekFrom};
use std::mem::size_of;
use crate::util::*;
use uuid::Uuid;
use std::intrinsics::transmute;
use std::convert::TryInto;


pub trait CassSerializer<T> {
    fn ser<W>(out: &mut CassWrite<W>, o: &T) -> std::io::Result<()> where W: Write+Seek;
}
pub trait CassDeserializer<T> {
    fn deser(r: &mut CassRead) -> T;
}


pub struct CassWrite<W> where W: Write+Seek {
    out: W
}

impl<W> CassWrite<W> where W: Write+Seek {
    pub fn new(out: W) -> CassWrite<W> {
        CassWrite { out }
    }

    pub fn position(&mut self) -> std::io::Result<u64> {
        self.out.seek(SeekFrom::Current(0))
    }

    #[inline]
    pub fn write_u8(&mut self, value: u8) -> std::io::Result<()> {
        self.write_raw(&[value])
    }
    #[inline]
    pub fn write_u16(&mut self, value: u16) -> std::io::Result<()> {
        let value_be = u16::to_be(value);
        let ptr = &value_be as *const u16 as *const u8;
        self.write_raw(unsafe { std::slice::from_raw_parts(ptr, size_of::<u16>()) })
    }
    #[inline]
    pub fn write_u32(&mut self, value: u32) -> std::io::Result<()> {
        let value_be = u32::to_be(value);
        let ptr = &value_be as *const u32 as *const u8;
        self.write_raw(unsafe { std::slice::from_raw_parts(ptr, size_of::<u32>()) })
    }
    #[inline]
    pub fn write_u64(&mut self, value: u64) -> std::io::Result<()> {
        let value_be = u64::to_be(value);
        let ptr = &value_be as *const u64 as *const u8;
        self.write_raw(unsafe { std::slice::from_raw_parts(ptr, size_of::<u64>()) })
    }

    #[inline]
    pub fn write_uuid(&mut self, value: &Uuid) -> std::io::Result<()> {
        self.write_raw(value.as_bytes())
    }

    #[inline]
    pub fn write_db_timestamp(&mut self, value: DbTimestamp) -> std::io::Result<()> {
        self.write_u64(value)
    }
    #[inline]
    pub fn write_db_expiry_timestamp(&mut self, value: DbExpiryTimestamp) -> std::io::Result<()> {
        self.write_u32(value)
    }

    #[inline]
    pub fn write_bool(&mut self, value: bool) -> std::io::Result<()> {
        if value {
            self.write_u8(1u8)
        }
        else {
            self.write_u8(0u8)
        }
    }

    pub fn write_utf8(&mut self, value: &str) -> std::io::Result<()>  {
        let len = value.len();
        if len > std::u32::MAX as usize {
            return other_error("string too long");
        }
        self.write_u32(len as u32)?;
        self.out.write_all(value.as_bytes())?;
        Ok(())
    }

    pub fn write_raw(&mut self, value: &[u8]) -> std::io::Result<()> {
        self.out.write_all(value)
    }

    pub fn into_inner(mut self) -> W {
        self.out
    }
}

pub struct CassRead <'a> {
    buf: &'a[u8],
    pub pos: usize,
}

impl<'a> CassRead<'a> {
    pub fn wrap(buf: &[u8]) -> CassRead {
        CassRead {
            buf,
            pos: 0,
        }
    }

    #[inline]
    pub fn assert_remaining(&self, size: usize) {
        assert!(self.buf.len() >= self.pos + size);
    }


    #[inline]
    pub fn read_slice(&mut self, size: usize) -> &'a[u8] {
        let result = &self.buf[self.pos..self.pos+size];
        self.pos += size;
        result
    }

    #[inline]
    pub fn peek_u8(&self) -> u8 {
        self.buf[self.pos]
    }
    #[inline]
    pub fn read_u8(&mut self) -> u8 {
        let result = self.peek_u8();
        self.pos += 1;
        result
    }

    #[inline]
    pub fn peek_u32(&self) -> u32 {
        self.peek_u32_offs(0)
    }
    #[inline]
    pub fn peek_u32_offs(&self, offs: usize) -> u32 {
        let (int_bytes, _) = self.buf[self.pos+offs..].split_at(std::mem::size_of::<u32>());
        u32::from_be_bytes(int_bytes.try_into().unwrap())
    }
    #[inline]
    pub fn read_u32(&mut self) -> u32 {
        let result = self.peek_u32();
        self.pos += size_of::<u32>();
        result
    }

    #[inline]
    pub fn peek_u64(&self) -> u64 {
        let (int_bytes, _) = self.buf[self.pos..].split_at(std::mem::size_of::<u64>());
        u64::from_be_bytes(int_bytes.try_into().unwrap())
    }
    #[inline]
    pub fn read_u64(&mut self) -> u64 {
        let result = self.peek_u64();
        self.pos += size_of::<u64>();
        result
    }

    #[inline]
    pub fn read_uuid(&mut self) -> Uuid {
        let slice = self.read_slice(16);
        let bytes: &[u8;16] = slice.try_into().unwrap();
        Uuid::from_bytes(*bytes)
    }

    #[inline]
    pub fn read_db_timestamp(&mut self) -> DbTimestamp {
        self.read_u64()
    }
    #[inline]
    pub fn read_db_expiry_timestamp(&mut self) -> DbExpiryTimestamp {
        self.read_u32()
    }


    pub fn read_utf8(&mut self) -> &str {
        let len = self.read_u32() as usize;
        //TODO unchecked or checked?
        unsafe { std::str::from_utf8_unchecked(&self.buf[0..len]) }
    }
}
