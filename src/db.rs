use crate::io;

use std::sync::Arc;
use std::io::{Write, Read, ErrorKind};

use uuid::*;
use crate::util::*;
use std::collections::HashMap;


/// a (sparse) in-memory representation of a row's data, i.e. primary keys (partition and
///  cluster) and corresponding column data.
pub struct TableRow<'a> {
    meta_data: Arc<TableMetaData>,

    pub partition_key: TableCellData<'a>,
    pub details: RowDetails<'a>,
}

impl TableRow<'_> {
    pub fn new<'a> (meta_data: Arc<TableMetaData>, partition_key: TableCellData<'a>, details: RowDetails<'a>) -> TableRow<'a> {

        TableRow {
            meta_data,
            partition_key,
            details
        }
    }
}

//#[derive(Debug)]
//enum KindOfMissing {
//    NoSuchColumn,
//    Tombstone,
//}

pub struct RegularRowData<'a> {
    pub pk_expiry: DbExpiryTimestamp,

    /// must be complete and in *key definition order*
    pub cluster_key: Vec<TableCellData<'a>>,
    pub regular_cols: Vec<TableCell<'a>>,
}

pub struct KeyBound<'a> {
    pub cluster_key_prefix: Vec<TableCellData<'a>>,
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
    pub columns: Vec<Arc<ColumnMetaData>>, // sorted by name
    pub idx_partition_key: usize,
    pub idx_cluster_keys: ClusterKeys,
    columns_by_id: HashMap<Uuid, Arc<ColumnMetaData>>,
}
impl TableMetaData {
    pub fn new(name: String, id: Uuid, columns: Vec<Arc<ColumnMetaData>>, idx_partition_key: usize, idx_cluster_keys: ClusterKeys) -> TableMetaData {
        let mut columns_by_id = HashMap::new();
        for col in columns.iter() {
            columns_by_id.insert(col.id, col.clone());
        }

        TableMetaData {
            name,
            id,
            columns,
            idx_partition_key,
            idx_cluster_keys,
            columns_by_id
        }
    }

    pub fn partition_key(&self) -> Arc<ColumnMetaData> {
        self.columns.get(self.idx_partition_key).unwrap().clone()
    }

    pub fn cluster_key(&self, idx: usize) -> Arc<ColumnMetaData> {
        self.columns.get(idx+1).unwrap().clone()
    }

    pub fn column_by_id(&self, col_id: &Uuid) -> Arc<ColumnMetaData> {
        self.columns_by_id.get(col_id).unwrap().clone() //TODO error reporting
    }
}
