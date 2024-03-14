use napi::{threadsafe_function::ThreadsafeFunction, Result};

use rdkafka::{
  config::RDKafkaLogLevel,
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  producer::FutureProducer,
  topic_partition_list::TopicPartitionList,
  ClientConfig,
};

use std::{collections::HashMap, time::Duration, vec};
use tracing::{debug, error, info, warn};

use crate::kafka::{
  kafka_admin::KafkaAdmin,
  kafka_client::KafkaClient,
  kafka_util::{convert_to_rdkafka_offset, AnyhowToNapiError},
  model::{OffsetModel, PartitionPosition, Payload},
};

use super::{
  consumer_context::{CustomContext, LoggingConsumer},
  consumer_thread::ConsumerThread,
};

pub const RETRY_COUNTER_NAME: &str = "kafka-crab-js-retry-counter";
pub const DEFAULT_QUEUE_TIMEOUT: u64 = 5000;
pub const DEFAULT_PAUSE_DURATION: i64 = 1000;
pub const DEFAULT_RETRY_TOPIC_SUFFIX: &str = "-retry";
pub const DEFAULT_DLQ_TOPIC_SUFFIX: &str = "-dlq";

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

  // #[napi(ts_args_type = "callback: (error: Error) => Promise<ConsumerResult | undefined>")]
  // pub async fn create_consumer(&self) -> Result<()> {
  //   Ok(())
  // }

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
      stream_consumer: self
        .create_stream_consumer(
          &self.topic,
          self.consumer_configuration.offset.clone(),
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
      stream_consumer: self
        .create_stream_consumer(
          &retry_topic,
          Some(offset_model),
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

  async fn create_stream_consumer(
    &self,
    topic: &str,
    offset: Option<OffsetModel>,
    configuration: Option<HashMap<String, String>>,
  ) -> anyhow::Result<StreamConsumer<CustomContext>> {
    let context = CustomContext;

    let ConsumerConfiguration {
      create_topic,
      group_id,
      enable_auto_commit,
      ..
    } = self.consumer_configuration.clone();

    let mut consumer_config: ClientConfig = self.client_config.clone();

    if let Some(config) = configuration {
      consumer_config.extend(config);
    }

    let consumer: LoggingConsumer = consumer_config
      .clone()
      .set("group.id", group_id.clone())
      .set(
        "enable.auto.commit",
        enable_auto_commit.unwrap_or(true).to_string(),
      )
      .set_log_level(RDKafkaLogLevel::Debug)
      .create_with_context(context)?;

    if create_topic.unwrap_or(true) {
      info!("Creating topic: {:?}", topic);
      let admin = KafkaAdmin::new(&self.client_config);
      let result = admin.create_topic(topic).await;
      if let Err(e) = result {
        warn!("Fail to create topic {:?}", e);
        return Err(anyhow::Error::msg(format!("Fail to create topic: {:?}", e)));
      }
      info!("Topic created: {:?}", topic)
    }

    if let Some(offset) = convert_to_rdkafka_offset(offset) {
      debug!("Setting offset to: {:?}", offset);
      let metadata = consumer.fetch_metadata(Some(topic), Duration::from_millis(1500))?;

      metadata.topics().iter().for_each(|meta_topic| {
        let mut tpl = TopicPartitionList::new();
        meta_topic.partitions().iter().for_each(|meta_partition| {
          tpl.add_partition(topic, meta_partition.id());
        });
        //TODO: Handle error
        match tpl.set_all_offsets(offset) {
          Ok(_) => {
            debug!("Offset set to: {:?}", offset);
          }
          Err(e) => {
            error!("Fail to set offset: {:?}", e)
          }
        };
        //TODO: Handle error
        match consumer.assign(&tpl) {
          Ok(_) => {
            debug!("Assigning topic: {:?}", topic);
          }
          Err(e) => {
            error!("Fail to assign topic: {:?}", e);
          }
        }
      });
    }

    consumer
      .subscribe(vec![&*topic.to_string()].as_slice())
      .map_err(|e| {
        anyhow::Error::msg(format!(
          "Can't subscribe to specified topics. Error: {:?}",
          e
        ))
      })?;

    info!(
      "Consumer created. Group id: {:?}, Topic: {:?}",
      group_id, topic
    );
    Ok(consumer)
  }
}
