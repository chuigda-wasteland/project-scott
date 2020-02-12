#![feature(with_options)]
#![feature(drain_filter)]

pub mod cache;
pub mod block;

use cache::*;
use block::*;

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::File;
use std::io::{Read, BufReader, BufRead, Write};

use lru::LruCache;
use std::iter::FusedIterator;
use std::cmp::Ordering;

struct FileIdManager {
    current: u32
}

impl FileIdManager {
    fn new(start_with: u32) -> Self {
        FileIdManager { current: start_with }
    }

    fn allocate(&mut self) -> u32 {
        let ret = self.current;
        self.current += 1;
        ret
    }

    fn current(&mut self) -> u32 {
        self.current
    }
}

impl Default for FileIdManager {
    fn default() -> Self {
        Self::new(0)
    }
}

struct LSMLevel {
    level: u32,
    blocks: Vec<LSMBlock>,
    file_id_manager: FileIdManager
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

impl Default for ManifestUpdate {
    fn default() -> Self {
        ManifestUpdate::new(Vec::new(), Vec::new())
    }
}

impl LSMLevel {
    fn new(level: u32, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks: Vec::new(), file_id_manager }
    }

    fn with_blocks(level: u32, blocks: Vec<LSMBlock>, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks, file_id_manager }
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

    fn merge_blocks_intern(mut iters: Vec<LSMBlockIter>) -> Vec<LSMBlock> {
        #[derive(Eq, PartialEq)]
        struct HeapTriplet(String, String, usize);

        impl PartialOrd for HeapTriplet {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some({
                    let HeapTriplet(key1, _, block_idx1) = self;
                    let HeapTriplet(key2, _, block_idx2) = other;
                    match key1.cmp(key2) {
                        Ordering::Equal => block_idx1.cmp(block_idx2),
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Less => Ordering::Less
                    }
                }.reverse())
            }
        }

        impl Ord for HeapTriplet {
            fn cmp(&self, other: &Self) -> Ordering {
                self.partial_cmp(other).unwrap()
            }
        }

        let mut buffer: Vec<(String, String)> = Vec::new();
        let mut blocks_built: Vec<LSMBlock> = Vec::new();
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(HeapTriplet(key, value, i))
            }
        }

        let mut last_block_idx: Option<usize> = None;
        while let Some(HeapTriplet(key, value, block_idx)) = heap.pop() {
            if let Some((last_key, _)) = buffer.last() {
                if *last_key == key {
                    let last_block_idx = last_block_idx.unwrap();
                    if block_idx > last_block_idx {
                        let _ = buffer.pop();
                    } else {
                        // Do nothing
                    }
                } else {
                    // Do nothing
                }
            }
            buffer.push((key, value));

            if buffer.len() >= 256 {
                /// TODO implement block building
                buffer.clear();
                unimplemented!()
            }

            last_block_idx.replace(block_idx);
            if let Some((key, value)) = iters[block_idx].next() {
                heap.push(HeapTriplet(key, value, block_idx));
            }
        }

        blocks_built
    }

    fn merge_blocks(mut self, mut blocks: Vec<LSMBlock>) -> (LSMLevel, ManifestUpdate, bool) {
        if self.level == 1 {
            self.blocks.append(&mut blocks);
            let b = self.blocks.len() > 10;
            return (self, ManifestUpdate::default(), b)
        }

        let mut self_to_merge =
            self.blocks
                .drain_filter(|self_block| {
                    blocks.iter().any(|block| LSMBlock::intersect(block, self_block))
                })
                .collect::<Vec<_>>();
        let self_stand_still = self.blocks;

        let mut incoming_to_merge =
            blocks
                .drain_filter(|block| {
                    self_to_merge.iter().any(|self_block| LSMBlock::intersect(block, self_block))
                })
                .collect::<Vec<_>>();
        let mut incoming_stand_still = blocks;

        let removed_files =
            self_to_merge
                .iter()
                .chain(incoming_to_merge.iter())
                .map(|block| block.block_file_name.clone())
                .collect::<Vec<_>>();

        self_to_merge.append(&mut incoming_to_merge);
        drop(incoming_to_merge);

        let merging_iters =
            self_to_merge
                .iter()
                .map(|block| block.iter())
                .collect::<Vec<_>>();

        let mut new_blocks = LSMLevel::merge_blocks_intern(merging_iters);
        let added_files =
            new_blocks
                .iter()
                .map(|block| block.block_file_name.clone())
                .collect();

        let mut all_blocks = self_stand_still;
        all_blocks.append(&mut incoming_stand_still);
        all_blocks.append(&mut new_blocks);
        let b = all_blocks.len() >= 10usize.pow(self.level);
        (LSMLevel::with_blocks(self.level, all_blocks, self.file_id_manager),
         ManifestUpdate::new(added_files, removed_files),
         b)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("{}", env!("PWD"));
        assert_eq!(2 + 2, 4);
    }
}
