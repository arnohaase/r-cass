use std::fs::File;
use std::io::{Result, BufReader, Read, ErrorKind, Seek, SeekFrom};
use std::time::{SystemTime, Instant};
use memmap::MmapOptions;

mod db;
mod io;
mod util;

mod sstable;

const PATH_HUGE: &str = "/home/arno/tmp/huge-file";
const PATH_MEDIUM: &str = "/home/arno/tmp/medium-file";
const PATH_SMALL: &str = "/home/arno/tmp/small-file";

const OFFSET: usize = 100_000_000;

fn main() {
    seek();
    read();
}

fn seek() {
    let path = PATH_HUGE;
    println!("\nseeking {}", path);

    let buf = [0u8; 65536];
    do_timed("loop", || {
        let mut result = 0u8;
        for idx in (0..65536).step_by(1000) {
            result = result.wrapping_add(buf[idx]);
        }
        Ok(result)
    });

    do_timed("file     1", || seek_file(path, 1));
    do_timed("mapped   1", || seek_mapped(path, 1));
    do_timed("file     1", || seek_file(path, 1));
    do_timed("mapped   1", || seek_mapped(path, 1));
    do_timed("file     1", || seek_file(path, 1));
    do_timed("mapped   1", || seek_mapped(path, 1));

    do_timed("file     3", || seek_file(path, 3));
    do_timed("mapped   3", || seek_mapped(path, 3));
    do_timed("file     3", || seek_file(path, 3));
    do_timed("mapped   3", || seek_mapped(path, 3));

    do_timed("file    10", || seek_file(path, 10));
    do_timed("mapped  10", || seek_mapped(path, 20));
    do_timed("file    10", || seek_file(path, 10));
    do_timed("mapped  10", || seek_mapped(path, 20));

    do_timed("file    20", || seek_file(path, 20));
    do_timed("mapped  20", || seek_mapped(path, 20));
    do_timed("file    20", || seek_file(path, 20));
    do_timed("mapped  20", || seek_mapped(path, 20));

    do_timed("file   100", || seek_file(path, 100));
    do_timed("mapped 100", || seek_mapped(path, 100));
    do_timed("file   100", || seek_file(path, 100));
    do_timed("mapped 100", || seek_mapped(path, 100));

    do_timed("file   1000", || seek_file(path, 1000));
    do_timed("mapped 1000", || seek_mapped(path, 1000));
    do_timed("file   1000", || seek_file(path, 1000));
    do_timed("mapped 1000", || seek_mapped(path, 1000));
}

fn do_timed<F>(text: &str, f: F) where F: FnOnce() -> Result<u8> {
    let start = Instant::now();
    let result = f();
    let end = Instant::now();
    println!("{}: {:?} in {:?}", text, result, end.duration_since(start));
}

fn read() {
    println!("\nreading {}", PATH_SMALL);
    do_timed("tuned", || read_tuned(PATH_SMALL));
    do_timed("mapped", || read_mapped(PATH_SMALL));
    do_timed("tuned", || read_tuned(PATH_SMALL));
    do_timed("mapped", || read_mapped(PATH_SMALL));
    do_timed("tuned", || read_tuned(PATH_SMALL));
    do_timed("mapped", || read_mapped(PATH_SMALL));
    do_timed("tuned", || read_tuned(PATH_SMALL));
    do_timed("mapped", || read_mapped(PATH_SMALL));

    println!("\nreading {}", PATH_MEDIUM);
    do_timed("tuned", || read_tuned(PATH_MEDIUM));
    do_timed("mapped", || read_mapped(PATH_MEDIUM));
    do_timed("tuned", || read_tuned(PATH_MEDIUM));
    do_timed("mapped", || read_mapped(PATH_MEDIUM));
    do_timed("tuned", || read_tuned(PATH_MEDIUM));
    do_timed("mapped", || read_mapped(PATH_MEDIUM));
    do_timed("tuned", || read_tuned(PATH_MEDIUM));
    do_timed("mapped", || read_mapped(PATH_MEDIUM));

    println!("\nreading {}", PATH_HUGE);
    do_timed("tuned", || read_tuned(PATH_HUGE));
    do_timed("mapped", || read_tuned(PATH_HUGE));
    do_timed("tuned", || read_tuned(PATH_HUGE));
    do_timed("mapped", || read_tuned(PATH_HUGE));
    do_timed("tuned", || read_tuned(PATH_HUGE));
    do_timed("mapped", || read_tuned(PATH_HUGE));
    do_timed("tuned", || read_tuned(PATH_HUGE));
    do_timed("mapped", || read_tuned(PATH_HUGE));
}

fn seek_file(path: &str, num_iter: usize) -> Result<u8> {
    let mut result = 0u8;

    let mut buf = [0u8; 65536];

    let mut f = File::open(path)?;

    for n in 0..num_iter {
        f.seek(SeekFrom::Start(OFFSET as u64));
        f.read_exact(&mut buf)?;
        for idx in (0..buf.len()).step_by(1000) {
            result = result.wrapping_add(buf[idx]);
        }
    }

    Ok(result)
}

fn seek_mapped(path: &str, num_iter: usize) -> Result<u8> {
    let f = File::open(path)?;
    let m = unsafe { MmapOptions::new().map(&f)? };

    let mut result = 0u8;

    for n in 0..num_iter {
        for idx in (OFFSET..OFFSET+65536).step_by(1000) {
            result = result.wrapping_add(m[idx]);
        }
    }

    Ok(result)
}


fn read_tuned(path: &str) -> Result<u8> {
    for _ in 0..2 {
        _read_tuned(path)?;
    }
    Ok(0)
}

fn _read_tuned(path: &str) -> Result<u8> {
    let mut f = File::open(path)?;

    let mut buf = [0u8;65536];
    let mut result = 0u8;

    loop {
        match f.read(&mut buf) {
            Ok(0) => return Ok(result),
            Ok(n) => {
                result = buf[0];
//                for idx in 0..n {
//                    if buf[idx] == 3 || buf[idx] > 250 {
//                        result = result.wrapping_add(buf[idx]);
//                    }
//                }
            },
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {},
            Err(e) => return Err(e)
        }
    }
}

fn read_buffered(path: &str) -> Result<u8> {
    let f = File::open(path)?;
    let mut r = BufReader::new(f);

    let mut buf = [0u8];
    let mut result = 0u8;

    loop {
        match r.read(&mut buf) {
            Ok(0) => return Ok(result),
            Ok(_) =>
                if buf[0] == 3 || buf[0] > 250 {
                    result = result.wrapping_add(buf[0])
                },
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {},
            Err(e) => return Err(e)
        }
    }
}

fn read_mapped(path: &str) -> Result<u8> {
    let f = File::open(path)?;
    let m = unsafe { MmapOptions::new().map(&f)? };

    let mut result = 0u8;

    for _ in 0..2 {
    for idx in (0..m.len()).step_by(1024) {
        result = result.wrapping_add(m[idx]);
//        if m[idx] == 3 || m[idx] > 250 {
//            result = result.wrapping_add(m[idx]);
//        }
    }
    }

    Ok(result)
}

