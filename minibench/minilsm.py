#!/usr/bin/python3

from typing import Dict
from collections import OrderedDict

class LSMBlockCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache_blocks: OrderedDict[str, Dict[str, str]] = OrderedDict()

    def allocate_cache(self, block_file_name: str) -> Dict[str, str]:
        if len(self.cahce_blocks) >= self.cache_size_max:
            self.cache_blocks.popitem()
        ret = dict()
        self.cache_blocks[block_file_name] = ret
        return ret

    def get_cache(self, block_file_name: str) -> Dict[str, str]:
        ret = self.cache_blocks.get(block_file_name)
        if ret is not None:
            self.cache_blocks.move_to_end(block_file_name)
        return ret

    def remove_cache(self, block_file_name: str):
        if block_file_name in self.cache_blocks:
            del self.cache_blocks[block_file_name]

class LSMBlock:
    def __init__(self, block_file_name: str, low: str, high: str, block_cache: LSMBlockCache):
        self.block_file_name = block_file_name
        self.low = low
        self.high = high
        self.block_cache = block_cache

    def get_low(self) -> str:
        return self.low

    def get_high(self) -> str:
        return self.high
        
    def get(self, key: str) -> str:
        if key < self.low or key > self.get_high:
            return None
        cache = self.block_cache.get_cache(self.block_file_name)
        if cache is not None:
            return cache.get(key)
        else:
            new_cache = self.block_cache.allocate_cache(self.block_file_name)
            with open(self.block_file_name, 'r') as f:
                for line in f.readlines():
                    kvpair = line.split(':')
                    new_cache[kvpair[0]] = kvpair[1]
            return new_cache.get(key)

    def interleaves_with(self, another: LSMBlock) -> bool:
        if self.get_low() <= another.get_high() and self.get_high() >= another.get_low():
            return True
        elif another.get_low() <= self.get_high() and another.get_high() >= self.get_low():
            return True
        else:
            return False

def insert_unique(l: list, a: any):
    for item in l:
        if item == a:
            return
    l.push(a)

class LSMLevel:
    def __init__(self, level: int, manifest_file_name: str):
        self.manifest_file_name = manifest_file_name
        self.level = level
        self.blocks = []

    def get(self, key: str) -> str:
        ret = None
        for block in self.blocks:
            ret = block.get(key)
            if ret is not None and self.level is not 1:
                break
        return ret
    
    def merge_blocks(self, blocks: [LSMBlock]) -> bool:
        if self.level is 1:
            self.blocks += blocks
        else:
            blocks_to_merge = []
            for self_block in self.blocks:
                for incoming_block in blocks:
                    if self_block.interleaves_with(incoming_block):
                        insert_unique(blocks_to_merge, incoming_block)
            pass

class LSMTree:
    def __init__(self, folder_name: str, memtable_size_thres: int):
        self.folder_name = folder_name
        self.mem_table: Dict[str, str] = dict()
        self.levels: [LSMLevel] = []

    def put(self, key: str, value: str) -> bool:
        self.mem_table[key] = value
        maybe_compaction()

    def delete(self, key: str) -> bool:
        self.mem_table[key] = None
        maybe_compaction()

    def maybe_compaction(self):
        if len(self.mem_table) >= self.memtable_size_thres:
            pass

    def get(self, key: str) -> str:
        ret = self.mem_table.get(key)
        if ret is not None:
            return ret
        for level in self.levels:
            ret = level.get(key)
            if ret is not None:
                if ret == '':
                    return None
                else:
                    return ret
        return None

    def scan(self, key1: str, key2: str) -> str:
        pass
