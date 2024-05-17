use std::{collections::HashMap, time::Duration};

use rdkafka::{
  consumer::{ConsumerContext, Rebalance, StreamConsumer},
  error::KafkaResult,
  ClientContext, TopicPartitionList,
};
use tracing::info;

pub const DEFAULT_FECTH_METADATA_TIMEOUT: Duration = Duration::from_millis(2000);

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

#[napi]
#[derive(Debug, PartialEq)]
pub enum CommitMode {
  Sync = 0,
  Async = 1,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub group_id: String,
  pub create_topic: Option<bool>,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
  pub fecth_metadata_timeout: Option<i64>,
}

#[napi(string_enum)]
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
pub struct PartitionOffset {
  pub partition: i32,
  pub offset: OffsetModel,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct TopicPartitionConfig {
  pub topic: String,
  pub all_offsets: Option<OffsetModel>,
  pub partition_offset: Option<Vec<PartitionOffset>>,
}
