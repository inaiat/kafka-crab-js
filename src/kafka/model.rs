use napi::bindgen_prelude::*;

use std::{collections::HashMap, fmt};

use napi::bindgen_prelude::Buffer;

#[napi(object)]
#[derive(Clone)]
pub struct MessageModel {
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
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub topic: String,
  pub group_id: String,
  pub retry_strategy: Option<RetryStrategy>,
  pub offset: Option<OffsetModel>,
  pub create_topic: Option<bool>,
  pub commit_mode: Option<KafkaCommitMode>,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
}
#[napi(object)]
#[derive(Clone, Debug)]
pub struct ProducerConfiguration {
  pub topic: String,
  pub configuration: Option<HashMap<String, String>>,
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
  pub next_topic_on_fail: String,
  pub pause_consumer_duration: Option<i64>,
}

#[napi(string_enum)]
#[derive(Debug)]
pub enum ConsumerResult {
  Ok,
  Retry,
}
