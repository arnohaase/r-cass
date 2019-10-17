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

    pub partition_key: &'a[u8],
    pub token: Token,
    pub details: RowDetails<'a>,
}

impl TableRow<'_> {
    pub fn new<'a> (meta_data: Arc<TableMetaData>, partition_key: &'a[u8], details: RowDetails<'a>) -> TableRow<'a> {
        TableRow {
            meta_data,
            partition_key,
            token: fasthash::murmur3::hash128(partition_key),
            details
        }
    }

    /// for rows read from the database via index so we know the token - now need to re-calculate it
    pub fn new_with_known_token<'a> (meta_data: Arc<TableMetaData>, partition_key: &'a[u8], token: Token, details: RowDetails<'a>) -> TableRow<'a> {
        TableRow {
            meta_data,
            partition_key,
            token,
            details
        }
    }
}

pub struct RegularRowData<'a> {
    pub pk_expiry: DbExpiryTimestamp,

    /// must be complete and in *key definition order*
    pub cluster_key: Vec<&'a [u8]>,
    pub regular_cols: Vec<TableCell<'a>>,
}

pub struct KeyBound<'a> {
    pub cluster_key_prefix: Vec<&'a [u8]>,
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

#[derive(Debug)]
pub struct ColumnMetaData {
    pub name: String,
    pub id: Uuid,
    pub col_type: ColumnType,
}


#[derive(Debug)]
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
