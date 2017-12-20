extern crate tsz;
extern crate chrono;
extern crate byteorder;

use std::vec::Vec;
use tsz::{DataPoint, Encode, /* Decode, */ StdEncoder /* StdDecoder */};
use tsz::stream::BufferedWriter;
//use tsz::decode::Error;
use std::fs::File;
use std::io::{BufReader, BufRead, Write};
use chrono::naive::NaiveDateTime;
//use byteorder::{WriteBytesExt, LittleEndian};

fn main() {
    let mut writer = ChunkedWriter::new();

    let file = File::open("/home/andy/mains.dat").unwrap();

    let mut t = 1363547563u64;

    for line in BufReader::new(file).lines().take(100000000) {
        let l = line.unwrap();
        let substrings: Vec<&str> = l.split(" ").collect();
        //let t = substrings[0].parse::<f64>().unwrap().floor() as u64;
        let v = substrings[1].parse::<f64>().unwrap();
        writer.write(t, v);
        t=t+1;
    }
}

fn is_same_chunk(a: u64, b: u64) -> bool {
    let a = NaiveDateTime::from_timestamp(a as _, 0);
    let b = NaiveDateTime::from_timestamp(b as _, 0);

    a.date() == b.date()
}

struct ChunkedWriter {
    encoder: Option<StdEncoder<BufferedWriter>>,
    //vec: Vec<u8>,
    start: Option<u64>,
    end: Option<u64>,
}

impl ChunkedWriter {
    fn new() -> Self {
        ChunkedWriter {
            encoder: None,
            //vec: vec![],
            start: None,
            end: None,
        }
    }

    fn write(&mut self, t: u64, v: f64) {
        if self.start.is_none() || !is_same_chunk(self.start.unwrap(), t) {
            self.flush();

            self.start = Some(t);
            //self.vec = vec![];
            let w = BufferedWriter::new();
            self.end = Some(t);
            self.encoder = Some(StdEncoder::new(t, w));
        }

        self.end = Some(t);
        let dp = DataPoint::new(t, v);
        //self.vec.write_u64::<LittleEndian>(t);
        //self.vec.write_f64::<LittleEndian>(v);
        self.encoder.as_mut().map(|ref mut x| x.encode(dp));
    }

    fn flush(&mut self) {

        if self.encoder.is_some() {
            let encoder = self.encoder.take().unwrap();
            let bytes = encoder.close();

            File::create(format!(
                "/home/andy/mains/{}-{}.dat.gorilla",
                self.start.unwrap(),
                self.end.unwrap_or(self.start.unwrap())
            )).unwrap()
                .write_all(bytes.as_ref()).ok();

/*             File::create(format!(
                "/home/andy/mains/{}-{}.dat",
                self.start.unwrap(),
                self.end.unwrap_or(self.start.unwrap())
            )).unwrap()
                .write_all(self.vec.as_ref()).ok(); */
        }
    }
}

impl Drop for ChunkedWriter {
    fn drop(&mut self) {
        self.flush();
    }
}