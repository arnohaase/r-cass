use std::path::{Path, PathBuf};

use crate::db::{TableMetaData, TableRow, RowDetails, TableCell, TableCellData};
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter};
use uuid::Uuid;
use crate::io::CassWrite;


const ID_ROW_TOMBSTONE: u8 = 0;
const ID_ROW_REGULAR: u8 = 1;

const ID_KEY_BOUND_NONE: u8 = 0;
const ID_KEY_BOUND_INCLUSIVE: u8 = 1;
const ID_KEY_BOUND_EXCLUSIVE: u8 = 2;

const ID_CELL_DATA_TOMBSTONE: u8 = 0;
const ID_CELL_DATA_REGULAR: u8 = 1;


struct SstableMetaData {
    table_metadata: TableMetaData,
    sstable_uuid: Uuid,
    folder: Box<Path>,
}

impl SstableMetaData {
    pub fn data_filename(&self) -> PathBuf {
        self.folder.join(format!("{}_{}_{}.data",
                                 self.sstable_uuid.to_hyphenated(),
                                 self.table_metadata.name,
                                 self.table_metadata.id.to_hyphenated()))
    }
}


struct SstableCreator {
    meta_data: SstableMetaData,
    data_out: CassWrite<BufWriter<File>>,
}

impl SstableCreator{
    pub fn new(meta_data: SstableMetaData) -> std::io::Result<SstableCreator> {
        let data_file = OpenOptions::new()
            .create_new(true)
            .open(meta_data.data_filename())?;

        let data_out = CassWrite::new(BufWriter::new(data_file));

        Ok(SstableCreator {
            meta_data,
            data_out,
        })
    }

    /// no shadowing inside a single sstable
    pub fn add_row(&mut self, row: &TableRow) -> std::io::Result<()> {
        //TODO write index (incl. oldest / youngest timestamp)
        //TODO write bloom filter


        self.write_cell_data(&row.partition_key.data, true)?;

        match &row.details {
            RowDetails::RowTombstone(data) => {
                self.data_out.write_u8(ID_ROW_TOMBSTONE)?;

                for b in [&data.lower_bound, &data.upper_bound].iter() {
                    match b {
                        None => {
                            self.data_out.write_u8(ID_KEY_BOUND_NONE)?;
                        },
                        Some(key_bound) => {
                            if key_bound.is_inclusive {
                                self.data_out.write_u8(ID_KEY_BOUND_INCLUSIVE)?;
                            }
                            else {
                                self.data_out.write_u8(ID_KEY_BOUND_EXCLUSIVE)?;
                            }

                            self.data_out.write_u8(key_bound.cluster_key_prefix.len() as u8); //TODO enforce max 255 columns in cluster key

                            for cell in &key_bound.cluster_key_prefix {
                                self.write_cell_data(&cell.data, true);
                            }
                        }
                    }
                }
                Ok(())
            },
            RowDetails::Regular(data) => {
                self.data_out.write_u8(ID_ROW_REGULAR)?;
                self.data_out.write_db_expiry_timestamp(data.pk_expiry)?;
                for cell in &data.cluster_key {
                    self.write_cell_data(&cell.data, true)?;
                }

                for cell in &data.regular_cols {
                    self.write_cell(cell)?;
                }

                Ok(())
            }
        }
    }

    fn write_cell(&mut self, cell: &TableCell) -> std::io::Result<()> {
        self.data_out.write_uuid(&cell.meta_data.id)?;
        self.data_out.write_db_timestamp(cell.timestamp)?;
        self.data_out.write_db_expiry_timestamp(cell.expiry)?;

        self.write_cell_data(&cell.data, false)
    }

    fn write_cell_data(&mut self, cell_data: &TableCellData, expect_data: bool) -> std::io::Result<()> {
        match cell_data {
            TableCellData::Tombstone => {
                assert!(!expect_data);
                self.data_out.write_u8(ID_CELL_DATA_TOMBSTONE)
            },
            TableCellData::Regular(data) => {
                self.data_out.write_u8(ID_CELL_DATA_REGULAR)?;
                self.data_out.write_raw(data)
            }
        }
    }

    fn finalize(mut self) -> std::io::Result<()>{
        self.data_out.flush()?;

        Ok(())//TODO return type?
    }
}

