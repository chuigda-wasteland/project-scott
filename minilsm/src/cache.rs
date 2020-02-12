use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufRead, Read};

use lru::LruCache;

pub struct LSMBlockCache {
    block_file_name: String,
    data: BTreeMap<String, String>
}

impl LSMBlockCache {
    pub fn new(block_file_name: String) -> Self {
        let file = File::with_options().read(true).open(&block_file_name).unwrap();
        let mut file = BufReader::new(file);

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
    pub fn new(max_cache_count: usize) -> Self {
        LSMCacheManager {
            lru: LruCache::new(max_cache_count),
            max_cache_count
        }
    }

    pub fn get_cache(&mut self, file_name: &String) -> &LSMBlockCache {
        if self.lru.contains(file_name) {
            self.lru.get(file_name).unwrap()
        } else {
            let new_cache = LSMBlockCache::new(file_name.to_string());
            self.lru.put(file_name.to_string(), new_cache);
            self.lru.get(file_name).unwrap()
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