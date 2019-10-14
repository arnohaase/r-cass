use std::io::Write;
use std::mem::size_of;
use crate::util::other_error;


macro_rules! writer {
    ($tpe: ty, $fn_name: ident, $shift0: literal $(, $shift: literal)*) => {
    #[inline]
    pub fn $fn_name(&mut self, value: $tpe) -> std::io::Result<()> {
        let mut buf = [(value >> $shift0) as u8];
        self.out.write_all (&buf)?;
        $(
            buf[0] = (value >> $shift) as u8;
            self.out.write_all (&buf)?;
        )*
        Ok(())
    }
    }
}

pub struct CassWrite<W> where W: Write {
    out: W
}

impl<W> CassWrite<W> where W: Write {
    writer!(u8, write_u8, 0);
    writer!(u32, write_u32, 24, 16, 8, 0);

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

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

pub struct CassRead <'a> {
    buf: &'a[u8],
    pos: usize,
}

impl CassRead<'_> {
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
        (self.buf[self.pos]   as u32) << 24 +
        (self.buf[self.pos+1] as u32) << 16 +
        (self.buf[self.pos+2] as u32) <<  8 +
        (self.buf[self.pos+3] as u32)
    }
    #[inline]
    pub fn read_u32(&mut self) -> u32 {
        let result = self.peek_u32();
        self.pos += size_of::<u32>();
        result
    }

    pub fn read_utf8(&mut self) -> &str {
        let len = self.read_u32() as usize;
        //TODO unchecked or checked?
        unsafe { std::str::from_utf8_unchecked(&self.buf[0..len]) }
    }
}
