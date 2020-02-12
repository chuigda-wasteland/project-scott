#![feature(with_options)]
#![feature(drain_filter)]

mod cache;
mod block;
mod metadata;
mod level;

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

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("{}", env!("PWD"));
        assert_eq!(2 + 2, 4);
    }
}
