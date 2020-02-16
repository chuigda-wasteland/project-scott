use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufRead};

use lru::LruCache;
use crate::block::{LSMBlock, LSMBlockMeta};

pub struct LSMBlockCache {
    block_file_meta: LSMBlockMeta,
    data: BTreeMap<String, String>
}

impl LSMBlockCache {
    pub fn new(block_file_meta: LSMBlockMeta) -> Self {
        let block_file_name = block_file_meta.block_file_name();
        let file = File::with_options().read(true).open(block_file_name).unwrap();
        let mut file = BufReader::new(file);

        let mut ret = LSMBlockCache {
            block_file_meta,
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
    lru: LruCache<LSMBlockMeta, LSMBlockCache>,
    max_cache_count: usize
}

impl LSMCacheManager {
    pub fn new(max_cache_count: usize) -> Self {
        LSMCacheManager {
            lru: LruCache::new(max_cache_count),
            max_cache_count
        }
    }

    pub fn get_cache(&mut self, block_meta: LSMBlockMeta) -> &LSMBlockCache {
        if self.lru.contains(&block_meta) {
            self.lru.get(&block_meta).unwrap()
        } else {
            let new_cache = LSMBlockCache::new(block_meta);
            self.lru.put(block_meta, new_cache);
            self.lru.get(&block_meta).unwrap()
        }
    }

    pub fn max_cache_count(&self) -> usize {
        self.max_cache_count
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_cached_read() {

    }
}