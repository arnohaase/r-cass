use std::path::{Path, PathBuf};

use crate::db::{TableMetaData, TableRow, RowDetails, TableCell, TableCellData, ColumnMetaData, ColumnType, RowTombstoneData, RegularRowData, KeyBound};
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter};
use uuid::*;
use crate::io::{CassWrite, CassRead};
use std::sync::Arc;
use crate::util::DbTimestamp;
use std::mem::size_of;

mod row_data;
mod index;

const ID_ROW_TOMBSTONE: u8 = 0;
const ID_ROW_REGULAR: u8 = 1;

const ID_KEY_BOUND_NONE: u8 = 0;
const ID_KEY_BOUND_INCLUSIVE: u8 = 1;
const ID_KEY_BOUND_EXCLUSIVE: u8 = 2;

const ID_CELL_DATA_TOMBSTONE: u8 = 0;
const ID_CELL_DATA_REGULAR: u8 = 1;


#[derive(Clone)]
struct SstableMetaData {
    pub table_metadata: Arc<TableMetaData>,
    sstable_uuid: Uuid,
    folder: Box<PathBuf>,
}
impl SstableMetaData {
    pub fn data_filename(&self) -> PathBuf {
        self.filename("data")
    }
    pub fn index_filename(&self) -> PathBuf {
        self.filename("index")
    }

    fn filename(&self, extension: &str) -> PathBuf {
        self.folder.join(format!("{}_{}_{}.{}",
                                 self.sstable_uuid.to_hyphenated(),
                                 self.table_metadata.name,
                                 self.table_metadata.id.to_hyphenated(),
                                 extension
        ))
    }
}
