use std::io::{BufReader, BufRead, Write};
use std::fs::File;
use std::iter::FusedIterator;

use crate::cache::LSMCacheManager;
use crate::KVPair;

pub struct LSMBlock {
    block_file_name: String,
    lower_bound: String,
    upper_bound: String
}

pub struct LSMBlockIter {
    block_file_handle: BufReader<File>
}

impl LSMBlockIter {
    pub fn new(block_file_name: &str) -> Self {
        LSMBlockIter {
            block_file_handle: BufReader::new(File::with_options().read(true).open(block_file_name).unwrap())
        }
    }
}

impl Iterator for LSMBlockIter {
    type Item = KVPair;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf = String::new();
        let bytes_read = self.block_file_handle.read_line(&mut buf).unwrap();
        if bytes_read == 0 {
            None
        } else {
            let parts: Vec<_> = buf.trim().split(":").collect();
            assert_eq!(parts.len(), 2);
            Some(KVPair(parts[0].to_string(), parts[1].to_string()))
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

    pub fn create(block_file_name: String, data: Vec<KVPair>) -> Self {
        let lower_bound = data.first().unwrap().0.clone();
        let upper_bound = data.last().unwrap().0.clone();

        let mut file =
            File::with_options()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&block_file_name).unwrap();
        for data_line in data {
            file.write(format!("{}:{}\n", data_line.0, data_line.1).as_bytes()).unwrap();
        }

        LSMBlock::new(block_file_name, lower_bound, upper_bound)
    }

    pub fn block_file_name(&self) -> &str {
        self.block_file_name.as_str()
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

    pub fn interleave(b1: &LSMBlock, b2: &LSMBlock) -> bool {
        (b1.lower_bound <= b2.upper_bound && b2.lower_bound <= b1.upper_bound)
         || (b2.lower_bound <= b1.upper_bound && b1.lower_bound <= b2.upper_bound)
    }
}

#[cfg(test)]
mod test {
    use crate::block::LSMBlock;
    use crate::cache::LSMCacheManager;
    use crate::KVPair;
    use std::fs::File;
    use std::io::{BufReader, BufRead};

    #[test]
    fn test_build_sst() {
        let mut data = vec![
            ("Yifan ZHU", "CHUK(SZ)"),
            ("ice1000", "PSU"),
            ("ICEY", "QDU"),
            ("Lyzh", "Railway Middle School"),
            ("Accelerator", "長期コンピューター学校"),
            ("MisakawaMikoto", "常盤台中学校"),
            ("KamijouTouma", "PERMISSION_DENIED"),
        ];
        data.sort_by(|&(k1, _), &(k2, _)| k1.cmp(k2));
        let data =
            data.iter()
                .map(|&(key, value)| KVPair(key.to_string(), value.to_string()))
                .collect::<Vec<_>>();

        let block = LSMBlock::create("test_build_sst.msst".to_string(), data);
        let mut cache_manager = LSMCacheManager::new(10);

        assert_eq!(block.get("ice1000", &mut cache_manager).unwrap(), "PSU");
    }

    #[test]
    fn test_block_iter() {
        let mut data = vec![
            ("Yifan ZHU", "CHUK(SZ)"),
            ("ice1000", "PSU"),
            ("ICEY", "QDU"),
            ("Lyzh", "Railway Middle School"),
            ("Accelerator", "長期コンピューター学校"),
            ("MisakawaMikoto", "常盤台中学校"),
            ("KamijouTouma", "PERMISSION_DENIED"),
        ];
        let mut data =
            data.iter()
                .map(|&(key, value)| KVPair(key.to_string(), value.to_string()))
                .collect::<Vec<_>>();
        data.sort();
        let block = LSMBlock::create("test_block_iter.msst".to_string(), data.clone());
        let block_iter = block.iter();

        let iter_read_data = block_iter.collect::<Vec<_>>();
        assert_eq!(iter_read_data, data);
    }
}
