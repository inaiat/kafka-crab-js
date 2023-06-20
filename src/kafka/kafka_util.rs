use std::collections::HashMap;

use rdkafka::message::{BorrowedHeaders, Headers};

pub fn convert_kakfa_headers_to_hashmap(headers: Option<&BorrowedHeaders>) -> HashMap<&str, &[u8]> {
  match headers {
    Some(value) => value
      .iter()
      .filter(|it| it.value.is_some())
      .map(|it| (it.key, it.value.unwrap()))
      .collect::<HashMap<&str, &[u8]>>(),
    _ => HashMap::new(),
  }
}

pub trait ExtractValueOnKafkaHashMap<T> {
  fn get_value(&self, key: &str) -> Option<T>;
}

impl ExtractValueOnKafkaHashMap<usize> for HashMap<&str, &[u8]> {
  fn get_value(&self, key: &str) -> Option<usize> {
    match self.get(key) {
      Some(it) => {
        let parsed = match String::from_utf8(it.to_vec()) {
          Ok(it) => Some(it.parse::<usize>()),
          Err(_) => None,
        };
        match parsed {
          Some(value) => value.ok(),
          _ => None,
        }
      }
      None => None,
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::HashMap;

  use rdkafka::message::{Header, OwnedHeaders};

  use crate::kafka::kafka_util::{convert_kakfa_headers_to_hashmap, ExtractValueOnKafkaHashMap};

  fn create_header<'a>(key: &'a str, value: Option<&'a str>) -> Header<'a, &'a str> {
    Header { key, value }
  }

  #[test]
  fn filter_out_nones() {
    let _x = Header {
      key: "header_key",
      value: Some("header_value"),
    };
    let headers = OwnedHeaders::new()
      .insert(create_header("key1", Some("1")))
      .insert(create_header("key2", None))
      .insert(create_header("key4", None))
      .insert(create_header("key5", Some("5")));

    let hash_map = convert_kakfa_headers_to_hashmap(Some(headers.as_borrowed()));

    assert_eq!(hash_map.len(), 2);
  }

  #[test]
  fn extract_usize() {
    let hash_map = HashMap::from([("key1", "32".as_bytes())]);
    assert_eq!(hash_map.get_value("key1"), Some(32));
  }

  #[test]
  fn extract_empty_value() {
    let hash_map = HashMap::from([("key1", "32".as_bytes())]);
    assert_eq!(hash_map.get_value("key2"), None);
  }
}
