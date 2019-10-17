use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use uuid::*;

use crate::db::{ColumnMetaData, ColumnType, KeyBound, RegularRowData, RowDetails, RowTombstoneData, TableCell, TableCellData, TableMetaData, TableRow};
use crate::io::{CassRead, CassWrite};
use crate::sstable::{SstableMetaData, ID_ROW_REGULAR, ID_ROW_TOMBSTONE, ID_KEY_BOUND_NONE, ID_KEY_BOUND_INCLUSIVE, ID_KEY_BOUND_EXCLUSIVE, ID_CELL_DATA_TOMBSTONE, ID_CELL_DATA_REGULAR};
use crate::util::DbTimestamp;

struct RowDataReader<'a> {
    meta_data: SstableMetaData,
    buf: CassRead<'a>,
}

impl <'a> RowDataReader<'a> {
    fn new(meta_data: SstableMetaData, buf: CassRead) -> RowDataReader {
        RowDataReader { meta_data, buf }
    }

    fn read_row(&mut self) -> TableRow<'a> {
        let partition_key_def = self.meta_data.table_metadata.partition_key();
        let partition_key = self.read_table_cell_data_raw(partition_key_def);

        let table_metadata = self.meta_data.table_metadata.clone();

        match self.buf.read_u8() {
            ID_ROW_TOMBSTONE => {
                let row_details = RowDetails::RowTombstone(RowTombstoneData {
                    lower_bound: self.read_key_bound(),
                    upper_bound: self.read_key_bound(),
                });

                TableRow::new(table_metadata, partition_key, row_details)
            },
            ID_ROW_REGULAR => {
                let pk_expiry = self.buf.read_db_expiry_timestamp();
                let mut cluster_key = Vec::new();
                for idx in table_metadata.idx_cluster_keys.iter() {
                    let key_col_meta = &table_metadata.columns.get(*idx).unwrap().clone();
                    let key_cell = self.read_table_cell_data_raw(key_col_meta.clone());
                    cluster_key.push(key_cell);
                }

                let mut regular_cols = Vec::new();

                let num_regular_cols = self.buf.read_u32();
                for _ in 0..num_regular_cols {
                    regular_cols.push(self.read_table_cell());
                }

                let row_details = RowDetails::Regular(RegularRowData {
                    pk_expiry,
                    cluster_key,
                    regular_cols,
                });

                TableRow::new(table_metadata, partition_key, row_details)
            },
            n => panic!("invalid row kind ID: {}", n),
        }
    }

    fn read_key_bound(&mut self) -> Option<KeyBound<'a>> {
        let is_inclusive = match self.buf.read_u8() {
            ID_KEY_BOUND_NONE => return None,
            ID_KEY_BOUND_INCLUSIVE => true,
            ID_KEY_BOUND_EXCLUSIVE => false,
            n => panic!("invalid key bound id: {}", n),
        };

        let mut cluster_key_prefix = Vec::new();
        let num_cluster_key_cols = self.buf.read_u8();
        for idx in 0..num_cluster_key_cols {
            cluster_key_prefix.push(self.read_table_cell_data_raw(self.meta_data.table_metadata.cluster_key(idx as usize)))
        }

        Some(KeyBound {
            cluster_key_prefix,
            is_inclusive
        })
    }

    fn read_table_cell(&mut self) -> TableCell<'a> {
        let col_id = self.buf.read_uuid();
        let timestamp = self.buf.read_db_timestamp();
        let expiry = self.buf.read_db_expiry_timestamp();

        let column_metadata = self.meta_data.table_metadata.column_by_id(&col_id).clone();
        let data = self.read_table_cell_data(column_metadata.clone());

        TableCell {
            meta_data: column_metadata,
            timestamp,
            expiry,
            data
        }
    }

    fn read_table_cell_data(&mut self, column_meta_data: Arc<ColumnMetaData>) -> TableCellData<'a> {
        match self.buf.read_u8() {
            ID_CELL_DATA_TOMBSTONE => TableCellData::Tombstone,
            ID_CELL_DATA_REGULAR => TableCellData::Regular(self.read_table_cell_data_raw(column_meta_data)),
            n => panic!("invalid cell data id: {}", n)
        }
    }

    fn read_table_cell_data_raw(&mut self, column_meta_data: Arc<ColumnMetaData>) -> &'a [u8] {
        let len = self.size(&column_meta_data.col_type, 0);
        self.buf.read_slice(len)
    }

    fn size(&mut self, col_type: &ColumnType, offs: usize) -> usize {
        match col_type {
            ColumnType::Text => {
                let len = self.buf.peek_u32_offs(offs);
                size_of::<u32>() + len as usize
            },
            ColumnType::Uuid => 16,
            ColumnType::Boolean => 1,
            ColumnType::Int => 4,
            ColumnType::Long => 8,
            ColumnType::Timestamp => size_of::<DbTimestamp>(),
            ColumnType::Tuple(parts) => {
                let mut result = 0;
                for part in parts {
                    result += self.size(part, offs + result);
                }
                result
            }
        }
    }
}

//TODO do not store partition key (or cluster key for regular rows) - they are available by access through index

struct RowDataFileCreator {
    meta_data: SstableMetaData,
    out: CassWrite<BufWriter<File>>,
}

impl RowDataFileCreator {
    pub fn new(meta_data: SstableMetaData) -> std::io::Result<RowDataFileCreator> {
        let data_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(meta_data.data_filename())?;

        let data_out = CassWrite::new(BufWriter::new(data_file));

        Ok(RowDataFileCreator {
            meta_data,
            out: data_out,
        })
    }

    /// no shadowing inside a single sstable, i.e. callers must e.g. split range tombstones
    ///  if a row is added inside the range
    pub fn append_row(&mut self, row: &TableRow) -> std::io::Result<()> {
        //TODO write index (incl. oldest / youngest timestamp)
        //TODO write bloom filter

        self.write_raw_cell_data(&row.partition_key)?;

        match &row.details {
            RowDetails::RowTombstone(data) => self.write_tombstone_row(data),
            RowDetails::Regular(data) => self.write_regular_row(data),
        }
    }

    fn write_regular_row(&mut self, data: &RegularRowData) -> std::io::Result<()> {
        self.out.write_u8(ID_ROW_REGULAR)?;
        self.out.write_db_expiry_timestamp(data.pk_expiry)?;
        for cell in &data.cluster_key {
            self.write_raw_cell_data(cell)?;
        }

        self.out.write_u32(data.regular_cols.len() as u32);
        for cell in &data.regular_cols {
            self.write_cell(cell)?;
        }

        Ok(())
    }

    fn write_tombstone_row(&mut self, data: &RowTombstoneData) -> std::io::Result<()> {
        self.out.write_u8(ID_ROW_TOMBSTONE)?;

        for b in [&data.lower_bound, &data.upper_bound].iter() {
            match b {
                None => {
                    self.out.write_u8(ID_KEY_BOUND_NONE)?;
                },
                Some(key_bound) => {
                    if key_bound.is_inclusive {
                        self.out.write_u8(ID_KEY_BOUND_INCLUSIVE)?;
                    }
                    else {
                        self.out.write_u8(ID_KEY_BOUND_EXCLUSIVE)?;
                    }

                    self.out.write_u8(key_bound.cluster_key_prefix.len() as u8); //TODO enforce max 255 columns in cluster key

                    for cell in &key_bound.cluster_key_prefix {
                        self.write_raw_cell_data(&cell);
                    }
                }
            }
        }
        Ok(())
    }

    fn write_cell(&mut self, cell: &TableCell) -> std::io::Result<()> {
        self.out.write_uuid(&cell.meta_data.id)?;
        self.out.write_db_timestamp(cell.timestamp)?;
        self.out.write_db_expiry_timestamp(cell.expiry)?;

        self.write_cell_data(&cell.data)
    }

    fn write_raw_cell_data(&mut self, cell_data: &[u8]) -> std::io::Result<()> {
        self.out.write_raw(cell_data)
    }
    fn write_cell_data(&mut self, cell_data: &TableCellData) -> std::io::Result<()> {
        match cell_data {
            TableCellData::Tombstone => {
                self.out.write_u8(ID_CELL_DATA_TOMBSTONE)
            },
            TableCellData::Regular(data) => {
                self.out.write_u8(ID_CELL_DATA_REGULAR)?;
                self.out.write_raw(data)
            }
        }
    }


    //TODO move this to orchestrator?
    fn finalize(mut self) -> std::io::Result<()>{
        let mut data_file = self.out.into_inner();
        data_file.flush()?;

        Ok(()) //TODO return type?

//        Ok(Sstable {
//            meta_data: self.meta_data,
//        })
    }
}


#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Cursor;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use memmap::MmapOptions;
    use uuid::Uuid;

    use crate::db::{ColumnMetaData, ColumnType, RegularRowData, RowDetails, TableCell, TableCellData, TableMetaData, TableRow};
    use crate::io::{CassRead, CassWrite};
    use crate::sstable::{SstableMetaData};
    use crate::sstable::row_data::{RowDataFileCreator, RowDataReader};

    fn sstable_metadata() -> SstableMetaData {
        let col_partition_key = ColumnMetaData {
            name: "id".to_string(),
            id: Uuid::new_v4(),
            col_type: ColumnType::Long,
        };
        let col_name = ColumnMetaData {
            name: "name".to_string(),
            id: Uuid::new_v4(),
            col_type: ColumnType::Text,
        };

        let columns = vec!(
            Arc::new(col_partition_key),
            Arc::new(col_name),
        );

        let table_metadata =
            TableMetaData::new("person".to_string(), Uuid::new_v4(), columns, 0, Vec::new());

        SstableMetaData {
            table_metadata: Arc::new(table_metadata),
            sstable_uuid: Uuid::new_v4(),
            folder: Box::new(std::env::temp_dir())
        }
    }

    fn ser_utf8(s: &str) -> Vec<u8> {
        let mut w = CassWrite::new(Cursor::new(Vec::new()));
        w.write_utf8(s);
        w.into_inner().into_inner()
    }
    fn ser_u64(n: u64) -> Vec<u8> {
        let mut w = CassWrite::new(Cursor::new(Vec::new()));
        w.write_u64(n);
        w.into_inner().into_inner()
    }


    #[test]
    pub fn test_write_read() {
        let meta_data = sstable_metadata();
        let table_metadata = meta_data.table_metadata.clone();

        let mut creator = RowDataFileCreator::new(meta_data.clone()).unwrap();

        let id_buf = ser_u64(99);
        let name_buf = ser_utf8("Arno");

        let id_cell = &id_buf;
        let name_cell = TableCell {
            meta_data: table_metadata.columns.get(0).unwrap().clone(),
            timestamp: 8888,
            expiry: 7777,
            data: TableCellData::Regular(&name_buf),
        };

        let row = TableRow::new(
            table_metadata.clone(),
            id_cell,
            RowDetails::Regular(RegularRowData {
                pk_expiry: 9999u32,
                cluster_key: Vec::new(),
                regular_cols: vec!(name_cell),
            })
        );

        creator.append_row(&row);
        creator.finalize();
        println!("data file: {:?}", meta_data.data_filename());

        let f = File::open(meta_data.data_filename()).unwrap();
        let m = unsafe { MmapOptions::new().map(&f).unwrap() };

        let mut reader = RowDataReader::new(meta_data, CassRead::wrap(&m));
        let read_row = reader.read_row();

        assert_eq!(*read_row.partition_key, *id_buf);

        match read_row.details {
            RowDetails::Regular(row_data) => {
                assert_eq!(9999, row_data.pk_expiry);
                assert!(row_data.cluster_key.is_empty());
                assert_eq!(1, row_data.regular_cols.len());

                let col = row_data.regular_cols.get(0).unwrap();
                assert_eq!(8888, col.timestamp);
                assert_eq!(7777, col.expiry);
                match col.data {
                    TableCellData::Regular(buf) => assert_eq!(*buf, *name_buf),
                    _ => assert!(false)
                }
            },
            _ => assert!(false)
        }
    }
}
