use std::collections::HashMap;

pub fn bulk_string(s: &str) -> String {
    format!("${}\r\n{}\r\n", s.len(), s)
}

pub fn simple_string(s: &str) -> String {
    format!("+{}\r\n", s)
}

pub fn okay_string() -> String {
    simple_string("OK")
}

pub fn empty_string() -> String {
    "$-1\r\n".to_string()
}

#[allow(dead_code)]
pub fn error_string(s: &str) -> String {
    format!("-{}\r\n", s)
}

pub fn bulk_string_from_hashmap(map: &HashMap<&str, &str>) -> String {
    let s = map.iter().fold(String::new(), |acc, (k, v)| {
        format!("{}{}:{}\r\n", acc, k, v)
    });
    bulk_string(&s)
}
