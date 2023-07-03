use napi::bindgen_prelude::*;

use std::{collections::HashMap, fmt};

use napi::bindgen_prelude::Buffer;

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
pub enum CommitMode {
  AutoCommit,
  Sync,
  Async,
}

#[napi(object)]
pub struct Payload {
  pub value: Buffer,
  pub key: Buffer,
  pub headers: Option<HashMap<String, Buffer>>,
  pub topic: String,
  pub partition: i32,
  pub offset: i64,
}

impl Payload {
  pub fn new(
    value: Buffer,
    key: Buffer,
    headers: Option<HashMap<String, Buffer>>,
    topic: String,
    partition: i32,
    offset: i64,
  ) -> Self {
    Self {
      value,
      key,
      headers,
      topic,
      partition,
      offset,
    }
  }
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
#[derive(Clone)]
pub struct KafkaCrabError {
  pub code: i32,
  pub message: String,
}

#[napi(object)]
#[derive(Clone)]
pub struct RecordMetadata {
  pub topic: String,
  pub partition: i32,
  pub offset: i64,
  pub error: Option<KafkaCrabError>,
}

#[napi(object)]
#[derive(Clone)]
pub struct MessageModel {
  pub value: Buffer,
  pub key: Option<Buffer>,
  pub headers: Option<HashMap<String, Buffer>>,
}
