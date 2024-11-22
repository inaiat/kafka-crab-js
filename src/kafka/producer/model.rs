use std::collections::HashMap;

use napi::bindgen_prelude::Buffer;

#[derive(Clone)]
#[napi(object)]
pub struct Message {
  pub payload: Buffer,
  pub key: Option<Buffer>,
  pub headers: Option<HashMap<String, Buffer>>,
  pub topic: String,
  pub partition: i32,
  pub offset: i64,
}

impl Message {
  pub fn new(
    payload: Buffer,
    key: Option<Buffer>,
    headers: Option<HashMap<String, Buffer>>,
    topic: String,
    partition: i32,
    offset: i64,
  ) -> Self {
    Self {
      payload,
      key,
      headers,
      topic,
      partition,
      offset,
    }
  }
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
pub struct MessageProducer {
  pub payload: Buffer,
  pub key: Option<Buffer>,
  pub headers: Option<HashMap<String, Buffer>>,
}

#[napi(object)]
#[derive(Clone)]
pub struct ProducerRecord {
  pub topic: String,
  pub messages: Vec<MessageProducer>,
}

#[napi(object)]
#[derive(Clone)]
pub struct KafkaCrabError {
  pub code: i32,
  pub message: String,
}

#[napi(object)]
#[derive(Clone, Debug)]
/*
 * Configuration for the producer
 * default values are set
 * auto_flush: true
 * queue_timeout: 5000
 * thrown_on_error: false
 */
pub struct ProducerConfiguration {
  pub queue_timeout: Option<i64>,
  pub thrown_on_error: Option<bool>,
  pub auto_flush: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
}
