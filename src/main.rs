use std::fs::File;
use std::io::{Result, BufReader, Read, ErrorKind, Seek, SeekFrom};
use std::time::SystemTime;
use memmap::MmapOptions;

mod db;
mod util;

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

    do_timed("file", || seek_file(path));
    do_timed("file", || seek_file(path));
    do_timed("mapped", || seek_mapped(path));
    do_timed("mapped", || seek_mapped(path));

    do_timed("file", || seek_file(path));
    do_timed("file", || seek_file(path));
    do_timed("mapped", || seek_mapped(path));
    do_timed("mapped", || seek_mapped(path));
}

fn do_timed<F>(text: &str, f: F) where F: FnOnce() -> Result<u8> {
    let start = SystemTime::now();
    let result = f();
    let end = SystemTime::now();
    println!("{}: {:?} in {:?}", text, result, end.duration_since(start).unwrap());
}

fn read() {
    println!("\nreading {}", PATH_SMALL);
    do_timed("tuned", || read_tuned(PATH_SMALL));
//    do_timed("buffered", || read_buffered(PATH_SMALL));
    do_timed("mapped", || read_mapped(PATH_SMALL));
    do_timed("tuned", || read_tuned(PATH_SMALL));
//    do_timed("buffered", || read_buffered(PATH_SMALL));
    do_timed("mapped", || read_mapped(PATH_SMALL));

    println!("\nreading {}", PATH_MEDIUM);
    do_timed("tuned", || read_tuned(PATH_MEDIUM));
//    do_timed("buffered", || read_buffered(PATH_MEDIUM));
    do_timed("mapped", || read_mapped(PATH_MEDIUM));
    do_timed("tuned", || read_tuned(PATH_MEDIUM));
//    do_timed("buffered", || read_buffered(PATH_MEDIUM));
    do_timed("mapped", || read_mapped(PATH_MEDIUM));

    println!("\nreading {}", PATH_HUGE);
    do_timed("tuned", || read_tuned(PATH_HUGE));
    do_timed("mapped", || read_tuned(PATH_HUGE));
}

fn seek_file(path: &str) -> Result<u8> {
    let mut result = 0u8;

    let mut buf = [0u8; 65536];

    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(OFFSET as u64));
    f.read_exact(&mut buf)?;
    for idx in 0..buf.len() {
        if buf[idx] == 3 || buf[idx] > 250 {
            result = result.wrapping_add(buf[idx]);
        }
    }

    Ok(result)
}

fn seek_mapped(path: &str) -> Result<u8> {
    let f = File::open(path)?;
    let m = unsafe { MmapOptions::new().map(&f)? };

    let mut result = 0u8;

    for idx in OFFSET..OFFSET+65536 {
        if m[idx] == 3 || m[idx] > 250 {
            result = result.wrapping_add(m[idx]);
        }
    }

    Ok(result)
}


fn read_tuned(path: &str) -> Result<u8> {
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

    for idx in (0..m.len()).step_by(1024) {
        result = result.wrapping_add(m[idx]);
//        if m[idx] == 3 || m[idx] > 250 {
//            result = result.wrapping_add(m[idx]);
//        }
    }

    Ok(result)
}

