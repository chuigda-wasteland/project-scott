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
        pass

class LSMTree:
    def __init__(self, folder_name: str):
        self.folder_name = folder_name
        self.mut_table = []

    def put(self, key: str, value: str) -> bool:
        pass

    def delete(self, key: str) -> bool:
        pass

    def get(self, key: str) -> str:
        pass

    def scan(self, key1: str, key2: str) -> str:
        pass
