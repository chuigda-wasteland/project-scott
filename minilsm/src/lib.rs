#![feature(with_options)]
#![feature(drain_filter)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, BufReader, BufRead, Write};

use lru::LruCache;
use std::iter::FusedIterator;

pub struct LSMBlockCache {
    block_file_name: String,
    data: BTreeMap<String, String>
}

impl LSMBlockCache {
    pub fn new(block_file_name: String) -> Self {
        let file = File::with_options().read(true).open(&block_file_name).unwrap();
        let mut file = BufReader::new(file);
        let mut s = String::new();
        file.read_to_string(&mut s).unwrap();

        let mut ret = LSMBlockCache {
            block_file_name,
            data: BTreeMap::new()
        };

        for line in file.lines() {
            let line = line.unwrap();
            let parts: Vec<_> = line.split(":").collect();
            assert_eq!(parts.len(), 2);
            ret.data.insert(parts[0].to_string(), parts[1].to_string());
        }

        ret
    }

    pub fn lookup(&self, key: &str) -> Option<&str> {
        self.data.get(key).map(|s| s.as_str())
    }
}

pub struct LSMCacheManager {
    lru: LruCache<String, LSMBlockCache>,
    max_cache_count: usize
}

impl LSMCacheManager {
    fn new(max_cache_count: usize) -> Self {
        LSMCacheManager {
            lru: LruCache::new(max_cache_count),
            max_cache_count
        }
    }

    fn get_cache(&mut self, file_name: &String) -> &LSMBlockCache {
        if self.lru.contains(file_name) {
            self.lru.get(file_name).unwrap()
        } else {
            let new_cache = LSMBlockCache::new(file_name.to_string());
            self.lru.put(file_name.to_string(), new_cache);
            self.lru.get(file_name).unwrap()
        }
    }

    fn max_cache_count(&self) -> usize {
        self.max_cache_count
    }
}

pub struct LSMBlock {
    block_file_name: String,
    lower_bound: String,
    upper_bound: String
}

pub struct LSMBlockIter {
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

struct LSMLevel {
    level: u32,
    blocks: Vec<LSMBlock>
}

struct ManifestUpdate {
    added_files: Vec<String>,
    removed_files: Vec<String>
}

impl ManifestUpdate {
    fn new(added_files: Vec<String>, removed_files: Vec<String>) -> Self {
        ManifestUpdate { added_files, removed_files }
    }
}

impl LSMLevel {
    fn new(level: u32) -> Self {
        LSMLevel { level, blocks: Vec::new() }
    }

    fn get<'a>(&self, key: &str, cache_manager: &'a mut LSMCacheManager) -> Option<&'a str> {
        /// This piece of code should have been:
        /// ```no_run
        /// use minilsm::LSMCacheManager;
        /// fn t(cache_manager: &mut LSMCacheManager) -> Option<&str> {
        ///     for block in blocks {
        ///         if let Some(value) = block.get(key, cache_manager) {
        ///             return Some(value);
        ///         }
        ///     }
        ///     None
        /// }
        /// ```
        /// But borrow checker does not support this. We temporarily use unsafe instead.
        unsafe {
            let cache_manager= cache_manager as *mut LSMCacheManager;
            for block in &self.blocks {
                if let Some(value) = block.get(key, cache_manager.as_mut().unwrap()) {
                    return Some(value);
                }
            }
        }
        None
    }

    fn merge_blocks(mut self, mut blocks: Vec<LSMBlock>) -> (LSMLevel, ManifestUpdate, bool) {
        let self_to_merge =
            self.blocks.drain_filter(|self_block| {
                blocks.iter().any(|block| LSMBlock::intersect(block, self_block))
            }).collect::<Vec<_>>();
        let self_stand_still = self.blocks;

        let incoming_to_merge =
            blocks.drain_filter(|block| {
                self_to_merge.iter().any(|self_block| LSMBlock::intersect(block, self_block))
            }).collect::<Vec<_>>();
        let incoming_stand_still = blocks;

        let removed_files =
            self_to_merge
                .iter()
                .chain(incoming_to_merge.iter())
                .map(|block| block.block_file_name.clone())
                .collect::<Vec<_>>();

        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
