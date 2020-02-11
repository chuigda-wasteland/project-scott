use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;

use lru::LruCache;
use std::cell::RefCell;

pub struct LSMBlockCache {
    block_file_name: String,
    data: BTreeMap<String, String>
}

impl LSMBlockCache {
    pub fn new(block_file_name: String) -> Self {
        let mut file = File::open(&block_file_name).unwrap();
        let mut s = String::new();
        file.read_to_string(&mut s).unwrap();

        let mut ret = LSMBlockCache {
            block_file_name,
            data: BTreeMap::new()
        };



        for (key, value) in s.split("\n")
                             .map( |line| { let p: Vec<_> = line.split(":").collect(); (p[0], p[1]) }) {
            ret.data.insert(key.to_string(), value.to_string());
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

    fn add_cache(&mut self, file_name: String, cache: LSMBlockCache) {
        let _ = self.lru.put(file_name, cache);
    }

    fn get_cache(&self, file_name: &String) -> Option<&LSMBlockCache> {
        type LRU = LruCache<String, LSMBlockCache>;
        unsafe {
            ((&self.lru) as *const LRU as *mut LRU).as_mut().unwrap().get(file_name)
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

impl LSMBlock {
    pub fn new(block_file_name: String, lower_bound: String, upper_bound: String) -> Self {
        LSMBlock {
            block_file_name, lower_bound, upper_bound
        }
    }

    pub fn get<'a>(&self, key: &str, cache_manager: &'a mut LSMCacheManager) -> Option<&'a str> {
        if key >= self.lower_bound.as_str() && key <= self.upper_bound.as_str() {
            unsafe {
                let cache_manager = cache_manager as *mut LSMCacheManager;
                if let Some(cache) = cache_manager.as_mut().unwrap().get_cache(&self.block_file_name) {
                    return cache.lookup(key);
                }

                let new_cache = LSMBlockCache::new(self.block_file_name.clone());
                cache_manager.as_mut().unwrap().add_cache(self.block_file_name.clone(), new_cache);
                cache_manager.as_ref().unwrap().get_cache(&self.block_file_name).unwrap().lookup(key)
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
