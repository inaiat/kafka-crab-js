use std::collections::HashMap;

use rdkafka::message::Headers;

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

  

  use crate::kafka::kafka_util::ExtractValueOnKafkaHashMap;

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
