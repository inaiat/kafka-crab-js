use std::collections::HashMap;

use rdkafka::{consumer::{ConsumerContext, Rebalance, StreamConsumer}, error::KafkaResult, ClientContext, TopicPartitionList};
use tracing::info;

use crate::kafka::model::OffsetModel;

pub type LoggingConsumer = StreamConsumer<CustomContext>;

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

#[napi(object)]
#[derive(Clone, Debug)]
pub struct RetryStrategy {
  pub retries: i32,
  pub retry_topic: Option<String>,
  pub dql_topic: Option<String>,
  pub pause_consumer_duration: Option<i64>,
  pub offset: Option<OffsetModel>,
  pub configuration: Option<HashMap<String, String>>,
}

#[napi(string_enum)]
#[derive(Debug, PartialEq)]
pub enum CommitMode {
  AutoCommit,
  Sync,
  Async,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub topic: String,
  pub group_id: String,
  #[deprecated(note = "this will deprecated in the future")]
  pub retry_strategy: Option<RetryStrategy>,
  pub offset: Option<OffsetModel>,
  pub create_topic: Option<bool>,
  pub commit_mode: Option<CommitMode>,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
}

