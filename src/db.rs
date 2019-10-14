use crate::io;

use std::sync::Arc;
use std::io::{Write, Read, ErrorKind};

use uuid::*;
use crate::util::*;


/// a (sparse) in-memory representation of a row's data, i.e. primary keys (partition and
///  cluster) and corresponding column data.
pub struct TableRow<'a> {
    meta_data: Arc<TableMetaData>,

    pub partition_key: TableCell<'a>,
    pub details: RowDetails<'a>,
}

//#[derive(Debug)]
//enum KindOfMissing {
//    NoSuchColumn,
//    Tombstone,
//}

pub struct RegularRowData<'a> {
    pub pk_expiry: DbExpiryTimestamp,

    /// must be complete and in *key definition order*
    pub cluster_key: Vec<TableCell<'a>>,
    pub regular_cols: Vec<TableCell<'a>>,
}

pub struct KeyBound<'a> {
    pub cluster_key_prefix: Vec<TableCell<'a>>,
    pub is_inclusive: bool,
}

pub struct RowTombstoneData<'a> {
    pub lower_bound: Option<KeyBound<'a>>,
    pub upper_bound: Option<KeyBound<'a>>,
}

pub enum RowDetails<'a> {
    Regular (RegularRowData<'a>),
    RowTombstone (RowTombstoneData<'a>),
}

impl TableRow<'_> {

//    fn add_col(&self, col_idx: usize, vec: &mut Vec<u8>) -> Result<(), KindOfMissing> {
//        let col_meta: &ColumnMetaData = &self.meta_data.columns[col_idx];
//
//        //TODO make ColumnMetaData implement Ord
//        let cell_idx = match self.data.binary_search_by(|cell| cell.meta_data.name.cmp(&col_meta.name)) {
//            Ok(idx) => idx,
//            Err(_) => return Err(KindOfMissing::NoSuchColumn)
//        };
//
//        match &self.data[cell_idx].data {
//            TableCellData::Data(d) => {
//                vec.extend_from_slice(d);
//                Ok(())
//            },
//            TableCellData::Tombstone => Err(KindOfMissing::Tombstone)
//        }
//    }
//
//    pub fn partition_key(&self) -> Vec<u8> {
//        let mut v = Vec::new();
//
//        match &self.meta_data.partition_keys {
//            PartitionKeys::Single(col_idx) => {
//                self.add_col(*col_idx, &mut v).unwrap();
//            },
//            PartitionKeys::Multi(idxs) => {
//                for col_idx in idxs {
//                    self.add_col(*col_idx, &mut v).unwrap();
//                }
//            }
//        }
//        v
//    }
//
//    pub fn primary_key(&self) -> Vec<u8> {
//        let mut v = self.partition_key();
//
//        for col_idx in &self.meta_data.cluster_keys {
//            self.add_col(*col_idx, &mut v).unwrap(); //TODO do we require all primary key columns to be present all the time?
//        }
//
//        v
//    }
}

pub struct TableCell<'a> {
    pub meta_data: Arc<ColumnMetaData>,
    pub timestamp: DbTimestamp,
    pub expiry: DbExpiryTimestamp,
    pub data: TableCellData<'a>,
}

pub enum TableCellData<'a> {
    Tombstone,
    Regular(&'a[u8])
}

pub struct ColumnMetaData {
    pub name: String,
    pub id: Uuid,
    pub key_type: ColumnKeyType,
    pub col_type: ColumnType,
}

pub enum ColumnKeyType {
    PartitionKey,
    ClusterKey,
    Regular,
}

pub enum ColumnType {
    Text,      // UTF-8, with u32 as maximum length
    Uuid,
    Int,
    Long,
    Timestamp, // millis since epoch stored as i64
    Boolean,
    Tuple(Vec<ColumnType>),
}

pub type ClusterKeys = Vec<usize>;

pub struct TableMetaData {
    pub name: String,
    pub id: Uuid,
    columns: Vec<ColumnMetaData>, // sorted by name
    partition_key: usize,
    cluster_keys: ClusterKeys
}
