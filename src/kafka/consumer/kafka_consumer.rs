use napi::{threadsafe_function::ThreadsafeFunction, Result};

use rdkafka::{
  consumer::CommitMode as RdKfafkaCommitMode,
  producer::FutureProducer,
  ClientConfig,
};

use std::time::Duration;

use crate::kafka::{
  kafka_client::KafkaClient, kafka_util::AnyhowToNapiError, model::{OffsetModel, PartitionPosition, Payload}
};

use super::{
  consumer_helper::create_stream_consumer, consumer_model::{CommitMode, ConsumerConfiguration, RetryStrategy}, consumer_thread::ConsumerThread
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

#[derive(Clone)]
#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  consumer_configuration: ConsumerConfiguration,
  producer: ProducerHelper,
  commit_mode: Option<RdKfafkaCommitMode>,
  topic: String,
}

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: KafkaClient,
    consumer_configuration: ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let commit_mode = match consumer_configuration.commit_mode {
      None | Some(CommitMode::AutoCommit) => None, //Default AutoCommit
      Some(CommitMode::Sync) => Some(RdKfafkaCommitMode::Sync),
      Some(CommitMode::Async) => Some(RdKfafkaCommitMode::Async),
    };

    let topic = consumer_configuration.topic.clone();

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_configuration,
      producer: setup_future_producer(client_config, None).map_err(|e| e.convert_to_napi())?,
      commit_mode,
      topic,
    })
  }

  #[napi(
    ts_args_type = "callback: (error: Error | undefined, result: Payload) => Promise<ConsumerResult | undefined>"
  )]
  pub async fn start_consumer(&self, func: ThreadsafeFunction<Payload>) -> Result<()> {
    let ConsumerConfiguration { retry_strategy, .. } = self.consumer_configuration.clone();

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
      stream_consumer:
        create_stream_consumer(
          &self.client_config,
          &self.consumer_configuration,
          &self.topic,
          &self.consumer_configuration.offset.clone(),
          self.consumer_configuration.configuration.clone(),
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
      stream_consumer: 
        create_stream_consumer(
          &self.client_config,
          &self.consumer_configuration,
          &retry_topic,
          &Some(offset_model),
          strategy.configuration.clone(),
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
