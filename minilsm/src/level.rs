use crate::block::{LSMBlock, LSMBlockIter};
use crate::cache::LSMCacheManager;
use crate::metadata::ManifestUpdate;
use crate::{KVPair, split2, LSMConfig};

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufRead, Write};

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

    fn current(&self) -> u32 {
        self.current
    }
}

impl Default for FileIdManager {
    fn default() -> Self {
        Self::new(0)
    }
}

struct LSMLevel<'a> {
    level: u32,
    blocks: Vec<LSMBlock>,
    config: &'a LSMConfig,
    file_id_manager: FileIdManager
}

impl<'a> LSMLevel<'a> {
    fn new(level: u32, config: &'a LSMConfig, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks: Vec::new(), config, file_id_manager }
    }

    fn from_meta_file(config: &'a LSMConfig, level: u32) -> Self {
        let level_meta_file = LSMLevel::meta_file_name_int(level);
        let f = File::with_options().read(true).open(&level_meta_file).unwrap();
        let mut f = BufReader::new(f);

        let mut file_id_line = String::new();
        f.read_line(&mut file_id_line).unwrap();

        let cur_file_id = file_id_line.trim().parse::<u32>().unwrap();
        let file_id_manager = FileIdManager::new(cur_file_id);

        let mut blocks = Vec::new();
        while f.read_line(&mut file_id_line).unwrap() != 0 {
            let mut parts: Vec<String> = file_id_line.trim().split(":").map(|s| s.to_string()).collect();

            let upper_bound = parts.pop().unwrap();
            let lower_bound = parts.pop().unwrap();
            let block_file_id = parts.pop().unwrap().parse::<u32>().unwrap();
            let origin_level = parts.pop().unwrap().parse::<u32>().unwrap();

            assert_eq!(parts.len(), 3);

            blocks.push(LSMBlock::new(origin_level, block_file_id, lower_bound, upper_bound));
        }

        LSMLevel::with_blocks(level, blocks, config, file_id_manager)
    }

    fn update_meta_file(&self) {
        let level_meta_file = self.meta_file_name();
        let mut f = File::with_options().write(true).create(true).truncate(true).open(level_meta_file).unwrap();
        write!(f, "{}\n", self.file_id_manager.current()).unwrap();
        for block in self.blocks.iter() {
            write!(f, "{}:{}:{}\n", block.block_file_name(), block.lower_bound(), block.upper_bound()).unwrap();
        }
    }

    fn meta_file_name(&self) -> String {
        LSMLevel::meta_file_name_int(self.level)
    }

    fn meta_file_name_int(level: u32) -> String {
        format!("lv{}_meta.mfst", level)
    }

    fn with_blocks(level: u32, blocks: Vec<LSMBlock>, config: &'a LSMConfig, file_id_manager: FileIdManager) -> Self {
        LSMLevel { level, blocks, config, file_id_manager }
    }

    fn get<'b>(&self, key: &str, cache_manager: &'b mut LSMCacheManager) -> Option<&'b str> {
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

    fn merge_blocks_intern(mut iters: Vec<LSMBlockIter>, level: u32,
                           config: &LSMConfig, file_id_manager: &mut FileIdManager) -> Vec<LSMBlock> {
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

        let mut buffer: Vec<KVPair> = Vec::new();
        let mut blocks_built: Vec<LSMBlock> = Vec::new();
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.iter_mut().enumerate() {
            if let Some(KVPair(key, value)) = iter.next() {
                heap.push(HeapTriplet(key, value, i))
            }
        }

        let mut last_block_idx: Option<usize> = None;
        while let Some(HeapTriplet(key, value, block_idx)) = heap.pop() {
            if let Some(KVPair(last_key, _)) = buffer.last() {
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
            buffer.push(KVPair(key, value));

            last_block_idx.replace(block_idx);
            if let Some(KVPair(key, value)) = iters[block_idx].next() {
                heap.push(HeapTriplet(key, value, block_idx));
            }
        }

        while buffer.len() >= config.block_size {
            blocks_built.push(LSMBlock::create(
                level, file_id_manager.allocate(),
                buffer.drain(0..config.block_size).collect()
            ));
        }
        if !buffer.is_empty() {
            blocks_built.push(LSMBlock::create(
                level, file_id_manager.allocate(),
                buffer
            ));
        }

        blocks_built
    }

    fn merge_blocks(&mut self, mut blocks: Vec<LSMBlock>) -> (ManifestUpdate, bool) {
        if self.level == 1 {
            self.blocks.append(&mut blocks);
            let b = self.blocks.len() > self.config.level1_size;
            return (ManifestUpdate::default(), b)
        }

        let mut self_blocks = Vec::new();
        self_blocks.append(&mut self.blocks);

        let (mut self_to_merge, mut self_stand_still) =
            split2(self_blocks, |self_block| {
                blocks.iter().any(|block| LSMBlock::interleave(block, self_block))
            });

        let (mut incoming_to_merge, mut incoming_stand_still) =
            split2(blocks, |block| {
                self_to_merge.iter().any(|self_block| LSMBlock::interleave(block, self_block))
            });

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

        let mut new_blocks =
            LSMLevel::merge_blocks_intern(merging_iters, self.level,
                                          self.config, &mut self.file_id_manager);
        let added_files =
            new_blocks
                .iter()
                .map(|block| block.block_file_name().to_string())
                .collect();

        let mut all_blocks = self_stand_still;
        all_blocks.append(&mut incoming_stand_still);
        all_blocks.append(&mut new_blocks);

        self.blocks.append(&mut all_blocks);
        let b = self.blocks.len() >= 10usize.pow(self.level);
        (ManifestUpdate::new(added_files, removed_files),
         b)
    }
}

#[cfg(test)]
mod test {
    use crate::block::LSMBlock;
    use crate::level::{LSMLevel, FileIdManager};
    use crate::cache::LSMCacheManager;
    use crate::{KVPair, LSMConfig};

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
                LSMBlock::create(9, i as u32, data)
            }).collect::<Vec<_>>();
        let lsm_config = LSMConfig::testing();
        let file_id_manager = FileIdManager::default();
        let mut cache_manager = LSMCacheManager::new(4);
        let level = LSMLevel::with_blocks(1, blocks, &lsm_config, file_id_manager);

        assert_eq!(level.get("lyzh", &mut cache_manager).unwrap(), "75");
        assert_eq!(level.get("chu1gda", &mut cache_manager).unwrap(), "75");
        assert_eq!(level.get("xtl", &mut cache_manager).unwrap(), "55");
        assert_eq!(level.get("huge", &mut cache_manager).unwrap(), "95");
        assert_eq!(level.get("xyy", &mut cache_manager).unwrap(), "99");
        assert_eq!(level.get("zyf", &mut cache_manager).unwrap(), "100");
        assert_eq!(level.get("fzy", &mut cache_manager).unwrap(), "100");
        assert!(level.get("zyy", &mut cache_manager).is_none());
    }

    use crate::kv_pair;
    use crate::test_util::gen_kv;
    use std::collections::BTreeMap;

    #[test]
    fn test_lvn_lookup() {
        let lsm_config = LSMConfig::testing();
        let file_id_manager = FileIdManager::default();
        let mut cache_manager = LSMCacheManager::new(4);


        let kvs = vec![
            gen_kv("aaa", lsm_config.block_size),
            gen_kv("aba", lsm_config.block_size),
            gen_kv("aca", lsm_config.block_size),
            gen_kv("ada", lsm_config.block_size)
        ];
        let blocks =
            kvs.clone()
                .into_iter()
                .enumerate()
                .map(|(i, data)| {
                    LSMBlock::create(10, i as u32, data)
                })
                .collect();
        let level2 = LSMLevel::with_blocks(2, blocks, &lsm_config, file_id_manager);

        for kv_piece in &kvs {
            for KVPair(key, value) in kv_piece {
                assert_eq!(level2.get(key, &mut cache_manager).unwrap(), value);
            }
        }
    }

    #[test]
    fn test_merge() {
        // Input blocks
        // LV1       [AAE ~ AAL]                   [ACA-ACH]
        // LV2 [AAA ~ AAJ] [AAK ~ AAT] [ABA ~ ABH]           [ADA ~ ADH]
        //
        // expected output blocks, blocks marked '*' are newly created
        // LV2 *[AAA ~ AAH] *[AAI ~ AAP] *[AAQ ~ AAT] [ABA ~ ABH] [ACA ~ ACH] [ADA ~ ADH]

        let lsm_config = LSMConfig::testing();

        // We start from a large file id so that it does not over write our existing blocks
        let lv2_file_id_manager = FileIdManager::new(99);
        let mut cache_manager = LSMCacheManager::new(4);

        let lv1_data = vec![
            gen_kv("aae", 8),
            gen_kv("aca", 8)
        ];
        let lv2_data = vec![
            // the first two blocks must be built manually
            vec![
                kv_pair!("aaa", "unique1"),
                kv_pair!("aab", "unique2"),
                kv_pair!("aac", "unique3"),
                kv_pair!("aad", "unique4"),
                kv_pair!("aae", "special1"),
                kv_pair!("aag", "special2"),
                kv_pair!("aah", "special3"),
                kv_pair!("aaj", "special4"),
            ],
            vec![
                kv_pair!("aal", "很多时候"),
                kv_pair!("aam", "重复地向别人请教一些高度相似的问题"),
                kv_pair!("aan", "尤其是在这个问题很trivial的情况下"),
                kv_pair!("aao", "是一种很不礼貌的行为"),
                kv_pair!("aaq", "最近在网上看到一个人"),
                kv_pair!("aar", "他每天都会重复一件事"),
                kv_pair!("aas", "首先问一个用直觉就能感觉出来答案的问题"),
                kv_pair!("aat", "然后在刚才的问题中的名词随便选一个"),
            ],
            gen_kv("aba", 8),
            gen_kv("ada", 8)
        ];

        let mut expectations = BTreeMap::new();
        lv2_data.iter().chain(lv1_data.iter()).for_each(|kvs| {
            kvs.iter().for_each(|KVPair(k, v)| {
                let _ = expectations.insert(k, v);
            })
        });

        let lv1_blocks =
            lv1_data
                .clone().into_iter()
                .enumerate()
                .map(|(i, kvs)| {
                    LSMBlock::create(1, i as u32, kvs)
                })
                .collect::<Vec<_>>();

        let lv2_blocks =
            lv2_data
                .clone().into_iter()
                .enumerate()
                .map(|(i, kvs)| {
                    LSMBlock::create(2, i as u32, kvs)
                })
                .collect::<Vec<_>>();

        let mut level2 = LSMLevel::with_blocks(2, lv2_blocks, &lsm_config, lv2_file_id_manager);
        let _ = level2.merge_blocks(lv1_blocks);

        for (&k, &v) in expectations.iter() {
            assert_eq!(level2.get(k, &mut cache_manager).unwrap(), v)
        }

        for (i, b1) in level2.blocks.iter().enumerate() {
            for (j, b2) in level2.blocks.iter().enumerate() {
                if i != j {
                    assert!(!LSMBlock::interleave(b1, b2))
                }
            }
        }
    }
}
