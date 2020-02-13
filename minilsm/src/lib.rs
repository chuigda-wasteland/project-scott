#![feature(with_options)]
#![feature(drain_filter)]

mod cache;
mod block;
mod metadata;
mod level;
mod test_util;

use cache::*;
use block::*;
use metadata::*;
use level::*;

use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct KVPair(String, String);

impl PartialEq for KVPair {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for KVPair {}

impl Ord for KVPair {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for KVPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn split2<T, F>(mut v: Vec<T>, f: F) -> (Vec<T>, Vec<T>)
    where F: Fn(&mut T) -> bool {
    let v1 = v.drain_filter(f).collect::<Vec<_>>();
    (v1, v)
}

#[derive(Debug)]
pub struct LSMConfig {
    pub level1_size: usize,
    pub level2_size: usize,
    pub size_scale: usize,
    pub block_size: usize,
    pub merge_step_size: usize
}

impl LSMConfig {
    fn new(level1_size: usize,
           level2_size: usize,
           size_scale: usize,
           block_size: usize,
           merge_step_size: usize) -> Self {
        LSMConfig { level1_size, level2_size, size_scale, block_size, merge_step_size }
    }

    fn testing() -> Self {
        LSMConfig::new(2, 4, 2, 8, 1)
    }

    fn level_size_max(&self, level: usize) -> usize {
        if level == 1 {
            self.level1_size
        } else {
            self.level2_size * self.size_scale.pow((level - 1) as u32)
        }
    }
}

impl Default for LSMConfig {
    fn default() -> Self {
        LSMConfig::new(4, 10, 10, 1024, 4)
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
