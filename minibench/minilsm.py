#!/usr/bin/python3

class LSMBlock:
    def __init__(block_file_name: str):
        self.block_file_name = block_file_name

    def get(key: str) -> str:
        pass

    def put(key: str, value: str) -> bool:
        pass

    def delete(key: str) -> bool:
        pass

    

class LSMLevel:
    def __init__(manifest_file_name: str):
        self.manifest_file_name = manifest_file_name
        self.blocks = []

    def get(key: str) -> str:
        
        
    def merge_blocks(blocks: [Block]) -> bool:
        pass

class LSMTree:
    def __init__(folder_name: str):
        self.folder_name = folder_name
        self.mut_table = []

    def put(key: str, value: str) -> bool:
        pass

    def delete(key: str) -> bool:
        pass

    def get(key: str) -> str:
        pass

    def scan(key1: str, key2: str) -> str:
        pass
