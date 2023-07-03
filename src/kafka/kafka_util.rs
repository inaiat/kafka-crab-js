use std::collections::HashMap;

use napi::bindgen_prelude::Buffer;
use rdkafka::{
  consumer::{ConsumerContext, Rebalance},
  error::KafkaResult,
  message::{BorrowedHeaders, Header, Headers, OwnedHeaders},
  ClientContext, Offset, TopicPartitionList,
};
use tracing::info;

use super::model::{OffsetModel, PartitionPosition};

pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
  fn pre_rebalance(&self, rebalance: &Rebalance) {
    info!("Pre rebalance {:?}", rebalance);
  }

  fn post_rebalance(&self, rebalance: &Rebalance) {
    info!("Post rebalance {:?}", rebalance);
  }

  fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
    info!("Committing offsets: {:?}. Offset: {:?}", result, _offsets);
  }
}

pub fn convert_to_rdkafka_offset(offset_model: Option<OffsetModel>) -> Option<Offset> {
  offset_model.map(|model| match model.position {
    Some(PartitionPosition::Beginning) => Offset::Beginning,
    Some(PartitionPosition::End) => Offset::End,
    Some(PartitionPosition::Stored) => Offset::Stored,
    None => match model.offset {
      Some(value) => Offset::Offset(value),
      None => Offset::Stored, // Default to stored
    },
  })
}

pub fn hashmap_to_kafka_headers(map: &HashMap<String, Buffer>) -> OwnedHeaders {
  map.iter().fold(OwnedHeaders::new(), |acc, (key, value)| {
    let value: &[u8] = value.as_ref();
    acc.insert(Header {
      key,
      value: Some(value),
    })
  })
}

pub fn kakfa_headers_to_hashmap(headers: Option<&BorrowedHeaders>) -> HashMap<&str, &[u8]> {
  match headers {
    Some(value) => value
      .iter()
      .filter(|it| it.value.is_some())
      .map(|it| (it.key, it.value.unwrap()))
      .collect::<HashMap<&str, &[u8]>>(),
    _ => HashMap::new(),
  }
}

pub fn kakfa_headers_to_hashmap_buffer(
  headers: Option<&BorrowedHeaders>,
) -> HashMap<String, Buffer> {
  match headers {
    Some(value) => value
      .iter()
      .filter(|it| it.value.is_some())
      .map(|it| (it.key.to_owned(), it.value.unwrap().into()))
      .collect::<HashMap<String, Buffer>>(),
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

  use napi::bindgen_prelude::Buffer;
  use rdkafka::message::Headers;

  use crate::kafka::kafka_util::{
    hashmap_to_kafka_headers, kakfa_headers_to_hashmap, ExtractValueOnKafkaHashMap,
  };

  #[test]
  fn headers_test() {
    let hash_map: HashMap<String, Buffer> = HashMap::from([
      ("key_a".to_owned(), "A".as_bytes().into()),
      ("key_b".to_owned(), "B".as_bytes().into()),
    ]);

    let rd_headers = hashmap_to_kafka_headers(&hash_map);

    dbg!(rd_headers.get(0));
    dbg!(rd_headers.get(1));

    let result = kakfa_headers_to_hashmap(Some(rd_headers.as_borrowed()));
    assert_eq!(result.len(), 2);
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
