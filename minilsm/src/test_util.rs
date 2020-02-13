use lazy_static::lazy_static;
use std::ops::Deref;
use rand::Rng;

lazy_static! {
    static ref A2Z_VECTOR: Vec<String> = {
        let mut ret = Vec::new();
        for c1 in b'a'..=b'z' {
            for c2 in b'a'..=b'z' {
                for c3 in b'a'..=b'z' {
                    ret.push(format!("{}{}{}", char::from(c1), char::from(c2), char::from(c3)));
                }
            }
        }
        ret
    };

    static ref CANDIDATE_VALUES: Vec<String> = {
        vec![
            "不传谣", "不信谣", "可防", "可控", "可治",
            "逮捕", "八人", "依法处理", "能", "明白",
            "鄂A0260W", "名誉会长", "帐篷", "莆田系", "心力交瘁",
            "武汉商学院", "武汉加油", "湖北加油", "中国加油", "历史会记住你们"
        ].into_iter().map(|s| s.to_string()).collect()
    };
}

fn gen_keys_range(start: &str, count: usize) -> Vec<String> {
    let search_vec: &Vec<String> = A2Z_VECTOR.deref();
    let start_idx = search_vec.binary_search(&start.to_string()).unwrap();
    let end_idx = start_idx + count;

    assert!(end_idx < search_vec.len());
    (&search_vec[start_idx..end_idx]).to_owned()
}

fn gen_value(count: usize) -> Vec<String> {
    let mut ret = Vec::new();
    let search_vec: &Vec<String> = CANDIDATE_VALUES.deref();
    for i in 0..count {
        ret.push(search_vec[rand::thread_rng().gen_range(0, search_vec.len())].to_owned());
    }
    ret
}


