use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction, Result};

use rdkafka::{
  client::ClientContext,
  config::RDKafkaLogLevel,
  consumer::{
    stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer, ConsumerContext,
    Rebalance,
  },
  error::KafkaResult,
  message::{BorrowedMessage, Header, OwnedHeaders},
  producer::{FutureProducer, FutureRecord},
  topic_partition_list::TopicPartitionList,
  ClientConfig, Message,
};

use std::{collections::HashMap, time::Duration, vec};
use tracing::{debug, error, info, warn};

use crate::kafka::kafka_util::{
  convert_to_rdkafka_offset, kakfa_headers_to_hashmap, ExtractValueOnKafkaHashMap,
};

use super::{
  kafka_admin::KafkaAdmin,
  kafka_client::KafkaClient,
  kafka_util::{kakfa_headers_to_hashmap_buffer, AnyhowToNapiError},
  model::{OffsetModel, Payload},
};

const RETRY_COUNTER_NAME: &str = "kafka-crab-js-retry-counter";
const DEFAULT_QUEUE_TIMEOUT: u64 = 5000;
const DEFAULT_PAUSE_DURATION: i64 = 1000;
const DEFAULT_RETRY_TOPIC_SUFFIX: &str = "-retry";
const DEFAULT_DLQ_TOPIC_SUFFIX: &str = "-dlq";

type LoggingConsumer = StreamConsumer<CustomContext>;

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

  #[napi(
    ts_args_type = "callback: (err: Error | null, result: Payload) => Promise<ConsumerResult | undefined>"
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
        if (strategy.retries) > 0 {
          strategy
            .retry_topic
            .unwrap_or(format!("{}{}", &self.topic, DEFAULT_RETRY_TOPIC_SUFFIX))
        } else {
          strategy
            .dql_topic
            .unwrap_or(format!("{}{}", &self.topic, DEFAULT_DLQ_TOPIC_SUFFIX))
        }
      }
      None => format!("{}{}", &self.topic, DEFAULT_DLQ_TOPIC_SUFFIX),
    };
    let consumer_thread = ConsumerThread {
      stream_consumer: self
        .create_stream_consumer(&self.topic)
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

    let next_topic_on_fail = strategy
      .clone()
      .dql_topic
      .unwrap_or(format!("{}{}", self.topic, DEFAULT_DLQ_TOPIC_SUFFIX));
    let pause_consumer_duration = Some(Duration::from_millis(
      strategy
        .pause_consumer_duration
        .unwrap_or(DEFAULT_PAUSE_DURATION) as u64,
    ));
    let consumer_thread = ConsumerThread {
      stream_consumer: self
        .create_stream_consumer(&retry_topic)
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
  ) -> anyhow::Result<StreamConsumer<CustomContext>> {
    let context = CustomContext;

    let ConsumerConfiguration {
      offset,
      create_topic,
      group_id,
      configuration,
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
      .create_with_context(context)
      .expect("Consumer creation failed");

    if create_topic.unwrap_or(true) {
      let admin = KafkaAdmin::new(&self.client_config);
      admin.create_topic(topic).await?;
    }

    if let Some(offset) = convert_to_rdkafka_offset(offset) {
      debug!("Setting offset to: {:?}", offset);
      let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_millis(1500))
        .expect("Fail to retrive metadata from consumer");

      metadata.topics().iter().for_each(|meta_topic| {
        let mut tpl = TopicPartitionList::new();
        meta_topic.partitions().iter().for_each(|meta_partition| {
          tpl.add_partition(topic, meta_partition.id());
        });
        tpl.set_all_offsets(offset).expect("Fail to set offset");
        consumer
          .assign(&tpl)
          .expect("Assign topic partition list failed");
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

pub struct ConsumerThread {
  stream_consumer: StreamConsumer<CustomContext>,
  func: ThreadsafeFunction<Payload>,
  commit_mode: Option<RdKfafkaCommitMode>,
  retries: i32,
  next_topic_on_fail: String,
  producer: ProducerHelper,
  pause_consumer_duration: Option<Duration>,
}

impl ConsumerThread {
  pub async fn start(&self) {
    loop {
      if let Some(duration) = &self.pause_consumer_duration {
        tracing::debug!("Pause consumer for {}", duration.as_millis());
        tokio::time::sleep(*duration).await;
      };
      match self.stream_consumer.recv().await {
        Err(e) => error!("Error while receiving from stream consumer: {:?}", e),
        Ok(message) => {
          let message_result = self.process_message(&message).await;
          match self.handle_message_result(&message, message_result).await {
            Ok(_) => {
              debug!(
                "Message consumed successfully. Partition: {}, Offset: {}",
                message.partition(),
                message.offset()
              );
            }
            Err(err) => {
              error!("Message processing failed. Error: {:?}", err);
            }
          }
        }
      };
    }
  }

  async fn process_message(
    &self,
    message: &BorrowedMessage<'_>,
  ) -> anyhow::Result<Option<ConsumerResult>> {
    match message.payload_view::<[u8]>() {
      None => Ok(None),
      Some(Ok(payload)) => {
        let payload_js = create_payload(message, payload);
        match self
          .func
          .call_async::<Promise<Option<ConsumerResult>>>(Ok(payload_js))
          .await
        {
          Ok(js_result) => match js_result.await {
            Ok(value) => match value {
              None | Some(ConsumerResult::Ok) => {
                if let Some(commit_mode) = self.commit_mode {
                  match self.stream_consumer.commit_message(message, commit_mode) {
                    Ok(_) => Ok(Some(ConsumerResult::Ok)),
                    Err(e) => Err(anyhow::Error::new(e)),
                  }
                } else {
                  Ok(Some(ConsumerResult::Ok))
                }
              }
              Some(ConsumerResult::Retry) => Ok(Some(ConsumerResult::Retry)),
            },
            Err(_) => {
              warn!("Error while calling return function");
              Ok(Some(ConsumerResult::Ok))
            }
          },
          Err(err) => {
            warn!("Error while calling function: {:?}", err.to_string());
            Ok(Some(ConsumerResult::Retry))
          }
        }
      }
      Some(_) => Err(anyhow::Error::msg(
        "Error while deserializing message payload",
      )),
    }
  }

  async fn handle_message_result(
    &self,
    message: &BorrowedMessage<'_>,
    message_result: anyhow::Result<Option<ConsumerResult>>,
  ) -> anyhow::Result<()> {
    match message_result {
      Ok(consumer_result) => match consumer_result {
        Some(result) => match result {
          ConsumerResult::Ok => Ok(()),
          ConsumerResult::Retry => match self.send_to_retry_strategy(message).await {
            Ok(_) => {
              debug!("Message sent to retry strategy");
              Ok(())
            }
            Err(e) => Err(anyhow::Error::msg(format!(
              "Error while sending message to retry strategy: {:?}",
              e
            ))),
          },
        },
        None => Ok(()),
      },
      Err(err) => Err(err),
    }
  }

  async fn send_to_retry_strategy(&self, message: &BorrowedMessage<'_>) -> anyhow::Result<()> {
    let retry_counter: usize = {
      let headers_map = kakfa_headers_to_hashmap(message.headers());
      let mut counter = headers_map.get_value(RETRY_COUNTER_NAME).unwrap_or(1);
      if self.retries > 0 {
        counter += 1;
      }
      counter
    };

    let next_topic = if self.retries > 0 && retry_counter <= self.retries.try_into().unwrap() {
      debug!(
        "Send message to same topic. Retry counter: {}",
        retry_counter
      );
      message.topic()
    } else {
      debug!(
        "Send message to next topic: {}. Retry counter {}",
        self.next_topic_on_fail, retry_counter
      );
      &self.next_topic_on_fail
    };
    let new_headers = message
      .headers()
      .unwrap_or(OwnedHeaders::new().as_borrowed())
      .detach()
      .insert(Header {
        key: RETRY_COUNTER_NAME,
        value: Some(retry_counter.to_string().as_str()),
      });
    let key = message.key().unwrap_or(&[]);

    let record = FutureRecord::to(next_topic)
      .key(key)
      .headers(new_headers)
      .payload(message.payload().unwrap_or(&[]));

    let _result = self
      .producer
      .future_producer
      .send(record, self.producer.queue_timeout)
      .await
      .map_err(|e| anyhow::Error::new(e.0))?;
    info!("Message sent to topic: {:?}", next_topic);
    Ok(())
  }
}

fn create_payload(message: &BorrowedMessage<'_>, payload: &[u8]) -> Payload {
  let key: Option<Buffer> = message.key().map(|bytes| bytes.into());
  let headers = Some(kakfa_headers_to_hashmap_buffer(message.headers()));
  let payload_js = Payload::new(
    payload.into(),
    key,
    headers,
    message.topic().to_owned(),
    message.partition(),
    message.offset(),
  );
  payload_js
}
