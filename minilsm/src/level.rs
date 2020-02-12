use crate::block::{LSMBlock, LSMBlockIter};
use crate::cache::LSMCacheManager;
use crate::metadata::ManifestUpdate;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use crate::KVPair;

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

impl LSMLevel {
    fn new(level: u32, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks: Vec::new(), file_id_manager }
    }

    fn with_blocks(level: u32, blocks: Vec<LSMBlock>, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks, file_id_manager }
    }

    fn get<'a>(&self, key: &str, cache_manager: &'a mut LSMCacheManager) -> Option<&'a str> {
        unsafe {
            let cache_manager= cache_manager as *mut LSMCacheManager;
            for block in self.blocks.iter().rev() {
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

        impl Ord for HeapTriplet {
            fn cmp(&self, other: &Self) -> Ordering {
                {
                    let HeapTriplet(key1, _, block_idx1) = self;
                    let HeapTriplet(key2, _, block_idx2) = other;
                    match key1.cmp(key2) {
                        Ordering::Equal => block_idx1.cmp(block_idx2),
                        Ordering::Greater => Ordering::Greater,
                        Ordering::Less => Ordering::Less
                    }
                }.reverse()
            }
        }

        impl PartialOrd for HeapTriplet {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut buffer: Vec<(String, String)> = Vec::new();
        let mut blocks_built: Vec<LSMBlock> = Vec::new();
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(KVPair(key, value)) = iter.next() {
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
            if let Some(KVPair(key, value)) = iters[block_idx].next() {
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
        } else {}

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
                .map(|block| block.block_file_name().to_string())
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
                .map(|block| block.block_file_name().to_string())
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
mod test {
    use crate::block::LSMBlock;
    use crate::level::{LSMLevel, FileIdManager};
    use crate::cache::LSMCacheManager;
    use crate::KVPair;

    #[test]
    fn test_level1_lookup() {
        let mut data_pieces = vec![
            vec![
                ("ice1000", "100"),
                ("xyy", "99"),
                ("chu1gda", "60"),
                ("lyzh", "60/182.5")
            ],
            vec![
                ("ice1000", "101"),
                ("fish", "55"),
                ("xtl", "45"),
                ("duangsuse", "0")
            ],
            vec![
                ("chu1gda", "75"),
                ("duangsuse", "1"),
                ("xtl", "55"),
                ("huge", "95")
            ],
            vec![
                ("fzy", "100"),
                ("zyf", "100"),
                ("lyzh", "75"),
                ("ice1000", "1000")
            ]
        ];
        let blocks =
            data_pieces.into_iter().enumerate().map(|(i, vec)| {
                let mut data =
                    vec.into_iter()
                       .map(|(k, v)| KVPair(k.to_string(), v.to_string()))
                       .collect::<Vec<_>>();
                data.sort();
                let block_file_name = format!("test_level1_lookup_{}.msst", i);
                LSMBlock::create(block_file_name, data)
            }).collect::<Vec<_>>();
        let file_id_manager = FileIdManager::default();
        let mut cache_manager = LSMCacheManager::new(4);
        let level = LSMLevel::with_blocks(1, blocks, file_id_manager);

        assert_eq!(level.get("lyzh", &mut cache_manager).unwrap(), "75");
        assert_eq!(level.get("chu1gda", &mut cache_manager).unwrap(), "75");
        assert_eq!(level.get("xtl", &mut cache_manager).unwrap(), "55");
        assert_eq!(level.get("huge", &mut cache_manager).unwrap(), "95");
        assert_eq!(level.get("xyy", &mut cache_manager).unwrap(), "99");
        assert_eq!(level.get("zyf", &mut cache_manager).unwrap(), "100");
        assert_eq!(level.get("fzy", &mut cache_manager).unwrap(), "100");
        assert!(level.get("zyy", &mut cache_manager).is_none());
    }

    #[test]
    fn test_lvn_lookup() {
        // TODO it takes some time to build up testing data pieces
    }

    #[test]
    fn test_merge() {
        // TODO it takes some time to build up testing data pieces
    }
}
