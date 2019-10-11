use crate::util;

use std::sync::Arc;
use std::io::{Write, Read, ErrorKind};

/// timestamps are nanos since EPOCH
type DbTimestamp = u64;

/// expiry timestamps are seconds since EPOCH (u32 means overflow end of 21st century - enough for now)
type DbExpiryTimestamp = u32;


/// a (typically sparse) in-memory representation of a row's data, i.e. primary keys (partition and
///  cluster) and corresponding column data.
pub struct TableRow<'a> {
    meta_data: Arc<TableMetaData>,
    kind: RowKind,
    data: Vec<TableCell<'a>>,
}

#[derive(Debug)]
enum KindOfMissing {
    NoSuchColumn,
    Tombstone,
}

fn other_error<T>(text: &str) -> std::io::Result<T> {
    Err(std::io::Error::new(ErrorKind::Other, text))
}

impl TableRow<'_> {
    pub fn ser<W>(&self, out: &mut W) -> std::io::Result<()> where W: Write {
        self.kind.ser(out)?;



        Ok(()) //TODO
    }

    pub fn deser<R>(meta_data: Arc<TableMetaData>, r: &mut R, offs: usize) -> std::io::Result<TableRow> where R: Read {
        let kind = RowKind::deser(r)?;


        other_error("todo") //TODO
    }

    fn add_col(&self, col_idx: usize, vec: &mut Vec<u8>) -> Result<(), KindOfMissing> {
        let col_meta: &ColumnMetaData = &self.meta_data.columns[col_idx];

        //TODO make ColumnMetaData implement Ord
        let cell_idx = match self.data.binary_search_by(|cell| cell.meta_data.name.cmp(&col_meta.name)) {
            Ok(idx) => idx,
            Err(_) => return Err(KindOfMissing::NoSuchColumn)
        };

        match &self.data[cell_idx].data {
            TableCellData::Data(d) => {
                vec.extend_from_slice(d);
                Ok(())
            },
            TableCellData::Tombstone => Err(KindOfMissing::Tombstone)
        }
    }

    pub fn partition_key(&self) -> Vec<u8> {
        let mut v = Vec::new();

        match &self.meta_data.partition_keys {
            PartitionKeys::Single(col_idx) => {
                self.add_col(*col_idx, &mut v).unwrap();
            },
            PartitionKeys::Multi(idxs) => {
                for col_idx in idxs {
                    self.add_col(*col_idx, &mut v).unwrap();
                }
            }
        }
        v
    }

    pub fn primary_key(&self) -> Vec<u8> {
        let mut v = self.partition_key();

        for col_idx in &self.meta_data.cluster_keys {
            self.add_col(*col_idx, &mut v).unwrap(); //TODO do we require all primary key columns to be present all the time?
        }

        v
    }
}

pub enum RowKind {
    PartitionTombstone,
    RowTombstone,
    Data,
}
impl RowKind {
    fn ser<W>(&self, out: &mut W) -> std::io::Result<()> where W: Write {
        let id: u8 = match self {
            RowKind::PartitionTombstone => 1,
            RowKind::RowTombstone => 2,
            RowKind::Data => 3,
        };
        Ok(out.write_all(&[id])?)
    }
    fn deser<R>(r: &mut R) -> std::io::Result<RowKind> where R: Read {
        let mut buf = [0u8];
        r.read_exact(&mut buf)?;
        match buf[0] {
            1 => Ok(RowKind::PartitionTombstone),
            2 => Ok(RowKind::RowTombstone),
            3 => Ok(RowKind::Data),
            n => other_error(format!("invalid encoding for RowKind: {}", n).as_str()),
        }
    }
}

pub struct TableCell<'a> {
    meta_data: Arc<ColumnMetaData>,
    timestamp: DbTimestamp,
    expiry: DbExpiryTimestamp,
    data: TableCellData<'a>,
}

pub enum TableCellData<'a> {
    Tombstone,
    Data(&'a[u8])
}

pub struct ColumnMetaData {
    name: String,
    col_type: ColumnType
}

pub enum ColumnType {
    Text,      // UTF-8, with u32 as maximum length
    Uuid,
    Int,
    Long,
    Timestamp, // millis since epoch stored as i64
    Boolean
}

impl ColumnType {
    /// number of bytes that this columns value takes in a buffer at a given offset
    fn num_bytes<R>(&self, r: &mut R, offs: usize) -> usize where R: Read {
        use ColumnType::*;
        match self {
            Text => 99, //TODO util::deser_u32(buf, offs) as usize, // text is encoded with a leading 32 bit number for the actual string's length in bytes
            Uuid => 16,
            Int => 4,
            Long => 8,
            Timestamp => 8,
            Boolean => 1,

        }
    }

    fn ser_text<W>(buf: &mut W, value: &str) -> Result<(), &'static str> where W: Write {
        let len = value.len();
        if len > std::u32::MAX as usize {
            return Err("string too long");
        }

        //TODO util::ser_u32(buf, len as u32);
        //TODO        buf.extend_from_slice(value.as_bytes());
        Ok(())
    }
    fn deser_text(buf: &[u8], offs: usize) -> Result<&str, &'static str> {
        let len_bytes = util::deser_u32(buf, offs) as usize;
        match std::str::from_utf8(& buf[offs+4..offs+4+len_bytes]) {
            Ok(s) => Ok(s),
            Err(_) => Err("invalid UTF-8"),
        }
    }

    //TODO other data types

    fn ser_boolean(buf: &mut Vec<u8>, value: bool) {
        if value {
            buf.push(1u8);
        }
        else {
            buf.push(0u8);
        }
    }
    fn deser_boolean(buf: &[u8], offs: usize) -> Result<bool, &'static str> {
        match buf[offs] {
            0 => Ok(false),
            1 => Ok(true),
            n => Err("invalid encoding for a bool"),
        }
    }
}

pub type ClusterKeys = Vec<usize>;

pub struct TableMetaData {
    name: String,
    columns: Vec<ColumnMetaData>, // sorted by name
    partition_keys: PartitionKeys,
    cluster_keys: ClusterKeys
}

pub enum PartitionKeys {
    Single(usize),
    Multi(Vec<usize>)
}
