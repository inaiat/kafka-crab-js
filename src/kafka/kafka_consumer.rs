use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction, Error, Result, Status};

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
  kafka_util::kakfa_headers_to_hashmap_buffer,
  model::{OffsetModel, Payload},
};

const RETRY_COUNTER_NAME: &str = "kafka-crab-js-retry-counter";
const DEFAULT_QUEUE_TIMEOUT: u64 = 5000;

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
pub struct ProducerHelper {
  future_producer: FutureProducer,
  queue_timeout: Duration,
}

#[derive(Clone)]
#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  consumer_configuration: ConsumerConfiguration,
  producer: ProducerHelper,
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

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: KafkaClient,
    consumer_configuration: ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();
    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_configuration,
      producer: setup_future_producer(client_config, None)
        .map_err(|e| Error::new(Status::GenericFailure, e.to_string()))?,
    })
  }

  #[napi(
    ts_args_type = "callback: (err: Error | null, result: Payload) => Promise<ConsumerResult | undefined>"
  )]
  pub async fn start_consumer(&self, func: ThreadsafeFunction<Payload>) -> Result<()> {
    let ConsumerConfiguration {
      commit_mode,
      retry_strategy,
      topic,
      ..
    } = self.consumer_configuration.clone();

    let commit_mode = match commit_mode {
      None | Some(CommitMode::AutoCommit) => None, //Default AutoCommit
      Some(CommitMode::Sync) => Some(RdKfafkaCommitMode::Sync),
      Some(CommitMode::Async) => Some(RdKfafkaCommitMode::Async),
    };

    let producer = self.producer.clone();

    if let Some(strategy) = retry_strategy.clone() {
      self
        .setup_retry_consumer(topic.clone(), strategy, &func, &producer, commit_mode)
        .await;
    };

    let stream_consumer: StreamConsumer<CustomContext> = self
      .setup_consumer(topic.clone())
      .await
      .expect("Consumer creation failed");
    debug!("Stream consumer configured for topic: {}", topic);
    let next_topic_on_fail = setup_next_topic(
      self.consumer_configuration.topic.clone(),
      retry_strategy.clone(),
    );

    tokio::spawn(async move {
      start_consumer_loop(
        stream_consumer,
        func,
        commit_mode,
        0,
        &next_topic_on_fail,
        producer,
      )
      .await
    });

    Ok(())
  }

  async fn setup_retry_consumer(
    &self,
    topic: String,
    strategy: RetryStrategy,
    func: &ThreadsafeFunction<Payload>,
    producer: &ProducerHelper,
    commit_mode: Option<RdKfafkaCommitMode>,
  ) {
    let retry_topic = strategy
      .retry_topic
      .unwrap_or(format!("{}-retry", topic.clone()));
    let stream_consumer_retry: StreamConsumer<CustomContext> = self
      .setup_consumer(retry_topic.clone())
      .await
      .expect("Consumer creation failed");
    debug!(
      "Stream consumer configured for retry topic: {}",
      retry_topic
    );
    let (retry_topic, retry_func, retry_producer) = (topic.clone(), func.clone(), producer.clone());
    tokio::spawn(async move {
      start_consumer_loop(
        stream_consumer_retry,
        retry_func,
        commit_mode,
        strategy.retries,
        &strategy.dql_topic.unwrap_or(format!("{}-dql", retry_topic)),
        retry_producer,
      )
      .await
    });
  }

  async fn setup_consumer(&self, topic: String) -> anyhow::Result<StreamConsumer<CustomContext>> {
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
      admin.create_topic(&topic).await?;
    }

    if let Some(offset) = convert_to_rdkafka_offset(offset) {
      let metadata = consumer
        .fetch_metadata(Some(&topic), Duration::from_millis(1500))
        .expect("Fail to retrive metadata from consumer");

      metadata.topics().iter().for_each(|meta_topic| {
        let mut tpl = TopicPartitionList::new();
        meta_topic.partitions().iter().for_each(|meta_partition| {
          tpl.add_partition(&topic, meta_partition.id());
        });
        tpl.set_all_offsets(offset).expect("Fail to set offset");
        consumer
          .assign(&tpl)
          .expect("Assign topic partition list failed");
      });
    }

    consumer
      .subscribe(vec![&*topic.to_string()].as_slice())
      .expect("Can't subscribe to specified topics");

    info!(
      "Consumer created. Group id: {:?}, Topic: {:?}",
      group_id, topic
    );
    Ok(consumer)
  }
}

async fn start_consumer_loop(
  stream_consumer: StreamConsumer<CustomContext>,
  func: ThreadsafeFunction<Payload>,
  commit_mode: Option<RdKfafkaCommitMode>,
  retries: i32,
  next_topic_on_fail: &str,
  producer: ProducerHelper,
) {
  loop {
    match stream_consumer.recv().await {
      Err(e) => error!("Error while receiving from stream consumer: {:?}", e),
      Ok(message) => {
        let message_result = process_message(&commit_mode, &stream_consumer, &func, &message).await;
        match handle_message_result(
          retries,
          next_topic_on_fail,
          &producer,
          &message,
          message_result,
        )
        .await
        {
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

async fn handle_message_result(
  retries: i32,
  next_topic_on_fail: &str,
  producer: &ProducerHelper,
  message: &BorrowedMessage<'_>,
  message_result: anyhow::Result<Option<ConsumerResult>>,
) -> anyhow::Result<()> {
  match message_result {
    Ok(consumer_result) => match consumer_result {
      Some(result) => match result {
        ConsumerResult::Ok => Ok(()),
        ConsumerResult::Retry => {
          match send_to_retry_strategy(producer, message, retries, next_topic_on_fail).await {
            Ok(_) => {
              debug!("Message sent to retry strategy");
              Ok(())
            }
            Err(e) => Err(anyhow::Error::msg(format!(
              "Error while sending message to retry strategy: {:?}",
              e
            ))),
          }
        }
      },
      None => Ok(()),
    },
    Err(err) => Err(err),
  }
}

async fn process_message(
  commit_mode: &Option<RdKfafkaCommitMode>,
  stream_consumer: &StreamConsumer<CustomContext>,
  func: &ThreadsafeFunction<Payload>,
  message: &BorrowedMessage<'_>,
) -> anyhow::Result<Option<ConsumerResult>> {
  match message.payload_view::<[u8]>() {
    None => Ok(None),
    Some(Ok(payload)) => {
      let payload_js = create_payload(message, payload);
      match func
        .call_async::<Promise<Option<ConsumerResult>>>(Ok(payload_js))
        .await
      {
        Ok(js_result) => match js_result.await {
          Ok(value) => match value {
            None | Some(ConsumerResult::Ok) => {
              if let Some(commit_mode) = commit_mode {
                match stream_consumer.commit_message(message, *commit_mode) {
                  Ok(_) => Ok(Some(ConsumerResult::Ok)),
                  Err(e) => Err(anyhow::Error::new(e)),
                }
              } else {
                Ok(Some(ConsumerResult::Ok))
              }
            }
            Some(ConsumerResult::Retry) => Ok(Some(ConsumerResult::Retry)),
          },
          Err(_) => Ok(Some(ConsumerResult::Ok)),
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

async fn send_to_retry_strategy(
  producer: &ProducerHelper,
  message: &BorrowedMessage<'_>,
  retries: i32,
  next_topic_on_fail: &str,
) -> anyhow::Result<()> {
  let retry_counter: usize = {
    let headers_map = kakfa_headers_to_hashmap(message.headers());
    let mut counter = headers_map.get_value(RETRY_COUNTER_NAME).unwrap_or(1);
    if retries > 0 {
      counter += 1;
    }
    counter
  };

  let next_topic = if retries > 0 && retry_counter <= retries.try_into().unwrap() {
    debug!(
      "Send message to same topic. Retry counter: {}",
      retry_counter
    );
    message.topic()
  } else {
    debug!(
      "Send message to next topic: {}. Retry counter {}",
      next_topic_on_fail, retry_counter
    );
    next_topic_on_fail
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

  let _result = producer
    .future_producer
    .send(record, producer.queue_timeout)
    .await
    .map_err(|e| anyhow::Error::new(e.0))?;
  info!("Message sent to topic: {:?}", next_topic);
  Ok(())
}

fn setup_next_topic(main_topic: String, retry_strategy: Option<RetryStrategy>) -> String {
  match retry_strategy {
    Some(strategy) => {
      if (strategy.retries) > 0 {
        strategy
          .retry_topic
          .unwrap_or(format!("{}-retry", main_topic))
      } else {
        strategy.dql_topic.unwrap_or(format!("{}-dlq", main_topic))
      }
    }
    None => format!("{}-dlq", main_topic),
  }
}
