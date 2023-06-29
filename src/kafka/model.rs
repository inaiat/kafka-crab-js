use napi::bindgen_prelude::*;

use std::{collections::HashMap, fmt};

use napi::bindgen_prelude::Buffer;

#[napi(object)]
#[derive(Clone)]
pub struct MessageModel {
  pub topic: String,
  pub key: Buffer,
  pub value: Buffer,
  pub headers: Option<HashMap<String, Buffer>>,
}

#[napi(object)]
#[derive(Clone)]
pub struct ProduceRecord {
  pub topic: String,
  pub messages: Vec<MessageModel>,
}

#[derive(Debug)]
#[napi(string_enum)]
pub enum AutoOffsetReset {
  Smallest,
  Earliest,
  Beginning,
  Largest,
  Latest,
  End,
  Error,
}

impl fmt::Display for AutoOffsetReset {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{}", format!("{:?}", self).to_lowercase())
  }
}

#[napi(string_enum)]
#[derive(Debug, PartialEq)]
pub enum KafkaCommitMode {
  AutoCommit,
  Sync,
  Async,
}

#[napi(object)]
pub struct OwnedDelivery {
  pub partition: i32,
  pub offset: i64,
}

#[napi(object)]
#[derive(Debug)]
pub enum PartitionPosition {
  Beginning,
  End,
  Stored,
}
#[napi(object)]
#[derive(Clone, Debug)]
pub struct OffsetModel {
  pub offset: Option<i64>,
  pub position: Option<PartitionPosition>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct RetryStrategy {
  pub retries: i32,
  pub retry_topic: Option<String>,
  pub dql_topic: Option<String>,
  pub pause_consumer_duration: Option<i64>,
}

#[napi(string_enum)]
#[derive(Debug)]
pub enum ConsumerResult {
  Ok,
  Retry,
}
