use async_trait::async_trait;
use napi::bindgen_prelude::*;

use rdkafka::{
  client::ClientContext,
  consumer::{CommitMode as RdKfafkaCommitMode, ConsumerContext, Rebalance},
  error::KafkaResult,
  producer::FutureProducer,
  topic_partition_list::TopicPartitionList,
  ClientConfig,
};

use std::{collections::HashMap, time::Duration};
use tracing::info;

use crate::kafka::model::OffsetModel;

pub const RETRY_COUNTER_NAME: &str = "kafka-crab-js-retry-counter";
pub const DEFAULT_QUEUE_TIMEOUT: u64 = 5000;
pub const DEFAULT_PAUSE_DURATION: i64 = 1000;
pub const DEFAULT_RETRY_TOPIC_SUFFIX: &str = "-retry";
pub const DEFAULT_DLQ_TOPIC_SUFFIX: &str = "-dlq";

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

#[napi(string_enum)]
#[derive(Debug, PartialEq)]
pub enum CommitMode {
  AutoCommit,
  Sync,
  Async,
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
#[derive(Debug)]
pub enum ConsumerResult {
  Ok,
  Retry,
}

#[derive(Clone)]
#[napi]
pub struct ProducerHelper {
  future_producer: FutureProducer,
  queue_timeout: Duration,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub topic: String,
  pub group_id: String,
  pub retry_strategy: Option<RetryStrategy>,
  pub offset: Option<OffsetModel>,
  pub create_topic: Option<bool>,
  pub commit_mode: Option<CommitMode>,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
}

#[derive(Clone)]
pub struct KafkaConsumer {
  pub client_config: ClientConfig,
  pub consumer_configuration: ConsumerConfiguration,
  pub producer: ProducerHelper,
  pub commit_mode: Option<RdKfafkaCommitMode>,
  pub topic: String,
}

fn setup_future_producer(
  client_config: &ClientConfig,
  queue_timeout_in_millis: Option<u64>,
) -> anyhow::Result<ProducerHelper> {
  let queue_timeout =
    Duration::from_millis(queue_timeout_in_millis.unwrap_or(DEFAULT_QUEUE_TIMEOUT));

  let future_producer = client_config.create::<FutureProducer>()?;

  Ok(ProducerHelper {
    future_producer,
    queue_timeout,
  })
}

#[async_trait]
pub trait KafkaConsumerContext<T> {
  async fn create_stream(&self, topic: &str, offset: Option<OffsetModel>) -> anyhow::Result<T>;
}
