use std::io::{BufReader, BufRead, Write};
use std::fs::File;
use std::iter::FusedIterator;

use crate::cache::LSMCacheManager;

pub(crate) struct LSMBlock {
    pub(crate) block_file_name: String,
    lower_bound: String,
    upper_bound: String
}

pub(crate) struct LSMBlockIter {
    block_file_handle: BufReader<File>
}

impl LSMBlockIter {
    fn new(block_file_name: &str) -> Self {
        LSMBlockIter {
            block_file_handle: BufReader::new(File::with_options().read(true).open(block_file_name).unwrap())
        }
    }
}

impl Iterator for LSMBlockIter {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = String::new();
        if let Ok(bytes_read) = self.block_file_handle.read_line(&mut buf) {
            if bytes_read == 0 {
                None
            } else {
                let parts: Vec<_> = buf.split(":").collect();
                assert_eq!(parts.len(), 2);
                Some((parts[0].to_string(), parts[1].to_string()))
            }
        } else {
            panic!()
        }
    }
}

impl FusedIterator for LSMBlockIter { }

impl LSMBlock {
    pub fn new(block_file_name: String, lower_bound: String, upper_bound: String) -> Self {
        LSMBlock {
            block_file_name, lower_bound, upper_bound
        }
    }

    pub fn create(block_file_name: String, data: Vec<(String, String)>) -> Self {
        let lower_bound = data.first().unwrap().0.clone();
        let upper_bound = data.last().unwrap().0.clone();

        let mut file = File::with_options().write(true).open(&block_file_name).unwrap();
        for data_line in data {
            file.write(format!("{}:{}\n", data_line.0, data_line.1).as_bytes()).unwrap();
        }

        LSMBlock::new(block_file_name, lower_bound, upper_bound)
    }

    pub fn get<'a>(&self, key: &str, cache_manager: &'a mut LSMCacheManager) -> Option<&'a str> {
        if key >= self.lower_bound.as_str() && key <= self.upper_bound.as_str() {
            cache_manager.get_cache(&self.block_file_name).lookup(key)
        } else {
            None
        }
    }

    pub fn iter(&self) -> LSMBlockIter {
        LSMBlockIter::new(self.block_file_name.as_str())
    }

    pub fn intersect(b1: &LSMBlock, b2: &LSMBlock) -> bool {
        (b1.lower_bound < b2.upper_bound && b2.lower_bound < b1.upper_bound)
            || (b2.lower_bound < b1.upper_bound && b1.lower_bound < b2.upper_bound)
    }
}
