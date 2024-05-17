use napi::{threadsafe_function::ThreadsafeFunction, Result};

use rdkafka::{consumer::CommitMode as RdKfafkaCommitMode, producer::FutureProducer, ClientConfig};

use std::{collections::HashMap, time::Duration};

use crate::kafka::{
  kafka_client::KafkaClient,
  kafka_util::AnyhowToNapiError,
  model::{OffsetModel, PartitionPosition},
  producer::model::Payload,
};

use super::{
  consumer_helper::create_stream_consumer_and_setup_everything,
  consumer_thread::ConsumerThread,
  model::{CommitMode, ConsumerConfiguration, RetryStrategy, DEFAULT_FECTH_METADATA_TIMEOUT},
};

pub const RETRY_COUNTER_NAME: &str = "kafka-crab-js-retry-counter";
pub const DEFAULT_QUEUE_TIMEOUT: u64 = 5000;
pub const DEFAULT_PAUSE_DURATION: i64 = 1000;
pub const DEFAULT_RETRY_TOPIC_SUFFIX: &str = "-retry";
pub const DEFAULT_DLQ_TOPIC_SUFFIX: &str = "-dlq";

#[napi(string_enum)]
#[derive(Debug)]
pub enum ConsumerResult {
  Ok,
  Retry,
}

#[derive(Clone)]
pub struct ProducerHelper {
  pub future_producer: FutureProducer,
  pub queue_timeout: Duration,
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

#[napi(object)]
#[derive(Clone, Debug)]
pub struct KafkaConsumerConfiguration {
  pub topic: String,
  pub group_id: String,
  pub retry_strategy: Option<RetryStrategy>,
  pub offset: Option<OffsetModel>,
  pub create_topic: Option<bool>,
  pub commit_mode: Option<CommitMode>,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
  pub fecth_metadata_timeout: Option<i64>,
}

#[derive(Clone)]
#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  consumer_configuration: KafkaConsumerConfiguration,
  producer: ProducerHelper,
  commit_mode: Option<RdKfafkaCommitMode>,
  topic: String,
  fetch_metadata_timeout: Duration,
}

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: KafkaClient,
    consumer_configuration: KafkaConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let commit_mode = match consumer_configuration.commit_mode {
      None => None, //Default AutoCommit
      Some(CommitMode::Sync) => Some(RdKfafkaCommitMode::Sync),
      Some(CommitMode::Async) => Some(RdKfafkaCommitMode::Async),
    };

    let KafkaConsumerConfiguration {
      topic,
      fecth_metadata_timeout,
      ..
    } = consumer_configuration.clone();

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_configuration,
      producer: setup_future_producer(client_config, None).map_err(|e| e.convert_to_napi())?,
      commit_mode,
      topic,
      fetch_metadata_timeout: Duration::from_millis(
        fecth_metadata_timeout.unwrap_or(DEFAULT_FECTH_METADATA_TIMEOUT) as u64,
      ),
    })
  }

  #[napi(
    ts_args_type = "callback: (error: Error | undefined, result: Payload) => Promise<ConsumerResult | undefined>"
  )]
  pub async fn start_consumer(&self, func: ThreadsafeFunction<Payload>) -> Result<()> {
    let KafkaConsumerConfiguration { retry_strategy, .. } = self.consumer_configuration.clone();

    if let Some(strategy) = retry_strategy.clone() {
      self.start_retry_consumer(strategy, &func).await?;
    };

    self.start_main_consumer(retry_strategy, &func).await?;

    Ok(())
  }

  async fn start_main_consumer(
    &self,
    retry_strategy: Option<RetryStrategy>,
    func: &ThreadsafeFunction<Payload>,
  ) -> Result<()> {
    let next_topic_on_fail = match retry_strategy {
      Some(strategy) => {
        let next_topic = if (strategy.retries) > 0 {
          strategy
            .retry_topic
            .unwrap_or(format!("{}{}", &self.topic, DEFAULT_RETRY_TOPIC_SUFFIX))
        } else {
          strategy
            .dql_topic
            .unwrap_or(format!("{}{}", &self.topic, DEFAULT_DLQ_TOPIC_SUFFIX))
        };
        Some(next_topic)
      }
      None => None,
    };

    let consumer_thread = ConsumerThread {
      stream_consumer: create_stream_consumer_and_setup_everything(
        &self.client_config,
        &convert_to_consumer_configuration(&self.consumer_configuration),
        &self.topic,
        &self.consumer_configuration.offset.clone(),
        self.consumer_configuration.configuration.clone(),
        self.fetch_metadata_timeout,
      )
      .await
      .map_err(|e| e.convert_to_napi())?,
      func: func.clone(),
      commit_mode: self.commit_mode,
      retries: 0,
      next_topic_on_fail,
      producer: self.producer.clone(),
      pause_consumer_duration: None,
    };

    tokio::spawn(async move {
      consumer_thread.start().await;
    });

    Ok(())
  }

  async fn start_retry_consumer(
    &self,
    strategy: RetryStrategy,
    func: &ThreadsafeFunction<Payload>,
  ) -> Result<()> {
    let retry_topic = strategy
      .clone()
      .retry_topic
      .unwrap_or(format!("{}{}", self.topic, DEFAULT_RETRY_TOPIC_SUFFIX));

    let next_topic_on_fail = Some(
      strategy
        .clone()
        .dql_topic
        .unwrap_or(format!("{}{}", self.topic, DEFAULT_DLQ_TOPIC_SUFFIX)),
    );
    let pause_consumer_duration = Some(Duration::from_millis(
      strategy
        .pause_consumer_duration
        .unwrap_or(DEFAULT_PAUSE_DURATION) as u64,
    ));
    let offset_model = strategy.offset.clone().unwrap_or(OffsetModel {
      position: Some(PartitionPosition::Stored),
      offset: None,
    });

    let consumer_thread = ConsumerThread {
      stream_consumer: create_stream_consumer_and_setup_everything(
        &self.client_config,
        &convert_to_consumer_configuration(&self.consumer_configuration),
        &retry_topic,
        &Some(offset_model),
        strategy.configuration.clone(),
        self.fetch_metadata_timeout,
      )
      .await
      .map_err(|e| e.convert_to_napi())?,
      func: func.clone(),
      commit_mode: self.commit_mode,
      retries: strategy.retries,
      next_topic_on_fail,
      producer: self.producer.clone(),
      pause_consumer_duration,
    };

    tokio::spawn(async move {
      consumer_thread.start().await;
    });

    Ok(())
  }
}

fn convert_to_consumer_configuration(config: &KafkaConsumerConfiguration) -> ConsumerConfiguration {
  ConsumerConfiguration {
    group_id: config.group_id.clone(),
    create_topic: config.create_topic,
    enable_auto_commit: config.enable_auto_commit,
    configuration: config.configuration.clone(),
    fecth_metadata_timeout: Some(DEFAULT_FECTH_METADATA_TIMEOUT),
  }
}
