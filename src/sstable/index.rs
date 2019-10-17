
use crate::sstable::SstableMetaData;
use crate::io::CassWrite;
use std::io::BufWriter;
use std::fs::File;
use fasthash::FarmHasherExt;
use crate::util::{Token, DbTimestamp};
use crate::db::TableRow;


//TODO arity?
//TODO cluster key based second-level index format?

/// Creates a B-Tree index for a given SSTable.
/// Lookup is through the partition key's token rather than the actual key. This allows lookup
///  by token or token range, and it can handle variable length keys (i.e. strings).
/// This can lead to collisions, i.e. different partition keys can have the same token. So ... TODO
/// The number of tokens inserted is known in advance -> TODO
struct IndexFileCreator {
    meta_data: SstableMetaData,
    index_out: CassWrite<BufWriter<File>>,
    min_token: Option<Token>,
    max_token: Option<Token>,
    min_timestamp: Option<DbTimestamp>,
    max_timestamp: Option<DbTimestamp>,
    stack: Vec<u64>, // TODO distinguish between "small" (u32) and "large" (u64) indices -> index meta data
}

impl IndexFileCreator {
    pub fn new(meta_data: SstableMetaData) -> std::io::Result<IndexFileCreator> {
        let index_file = File::open(meta_data.index_filename())?;
        Ok(IndexFileCreator {
            meta_data,
            min_token: None,
            max_token: None,
            min_timestamp: None,
            max_timestamp: None,
            index_out: CassWrite::new (BufWriter::new (index_file)),
            stack: Vec::new(),
        })
    }

    pub fn add_row(&mut self, row: &TableRow, offs_data: u64) -> std::io::Result<()> {



        Ok(())
    }
}


// locality: 16 bytes per token, 4 or 8 bytes per pointer ->  20 or 24 bytes per Entry. Block size 4k --> 200ary tree
// for SSDs: page size can be up to 16k

//TODO meta data: min and max token, min and max timestamp