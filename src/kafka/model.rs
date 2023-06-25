use napi::bindgen_prelude::*;

use std::{collections::HashMap, fmt};

use napi::bindgen_prelude::Buffer;
use rdkafka::Offset;

#[derive(Clone, Debug)]
pub struct ConsumerModel {
  pub group_id: String,
  pub topic: String,
  pub retries: i32,
  pub next_topic_on_fail: String,
  pub pause_consumer_duration: Option<u64>,
  pub offset: Option<Offset>,
}

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

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub topic: String,
  pub retry_strategy: Option<RetryStrategy>,
  pub offset: Option<OffsetModel>,
  pub create_topic: Option<bool>,
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
