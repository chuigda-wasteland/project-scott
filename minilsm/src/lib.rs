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

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fs::File;
use std::io::{Read, BufReader, BufRead, Write};

use lru::LruCache;
use std::iter::FusedIterator;
use std::cmp::Ordering;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        println!("{}", env!("PWD"));
        assert_eq!(2 + 2, 4);
    }
}
