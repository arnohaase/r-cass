use std::path::{Path, PathBuf};

use crate::db::{TableMetaData, TableRow, RowDetails};
use std::fs::{File, OpenOptions};
use std::io::Write;
use uuid::Uuid;

fn asdf() {
    println!("yo");

    let x = Box::new(Path::new("yo").to_owned());

    File::open("asdf");
}


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
    data_file: File,
}

impl SstableCreator {
    fn new(meta_data: SstableMetaData) -> std::io::Result<SstableCreator> {
        let data_file = OpenOptions::new()
            .create_new(true)
            .open(meta_data.data_filename())?;

        Ok(SstableCreator {
            meta_data,
            data_file
        })
    }

//    /// no shadowing inside a single sstable
//    fn add_row(&mut self, row: &TableRow) -> std::io::Result<()> {
//        match row.kind {
//            RowDetails::PartitionTombstone => {
//                self.data_file.write_all(&[1])?;
//            }
//            RowDetails::RowTombstone =>
//            RowDetails::Data =>
//        }
//
//        row.kind.ser(&mut self.data_file)?;
//
//
//
//        Ok(())
//    }

    fn finalize(self) -> std::io::Result<()>{
        self.data_file.sync_data()?;

        Ok(())//TODO return type?
    }
}

