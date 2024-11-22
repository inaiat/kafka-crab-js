use std::time::Duration;

use napi::{Either, Result};

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  topic_partition_list::TopicPartitionList as RdTopicPartitionList,
  ClientConfig, Message as RdMessage, Offset,
};

use tracing::{debug, error, info};

use crate::kafka::{
  consumer::consumer_helper::{
    assign_offset_or_use_metadata, convert_to_rdkafka_offset, try_create_topic, try_subscribe,
  },
  kafka_client_config::KafkaClientConfig,
  kafka_util::{create_message, AnyhowToNapiError},
  producer::model::Message,
};

use super::{
  consumer_helper::{create_stream_consumer, set_offset_of_all_partitions},
  model::{
    CommitMode, ConsumerConfiguration, CustomContext, OffsetModel, TopicPartitionConfig,
    DEFAULT_FECTH_METADATA_TIMEOUT,
  },
};

pub const DEFAULT_SEEK_TIMEOUT: i64 = 1500;

#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  stream_consumer: StreamConsumer<CustomContext>,
  fecth_metadata_timeout: Duration,
}

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: KafkaClientConfig,
    consumer_configuration: &ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let ConsumerConfiguration { configuration, .. } = consumer_configuration;
    let stream_consumer =
      create_stream_consumer(client_config, consumer_configuration, configuration.clone())
        .map_err(|e| e.convert_to_napi())?;

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      stream_consumer,
      fecth_metadata_timeout: Duration::from_millis(
        consumer_configuration
          .fecth_metadata_timeout
          .unwrap_or(DEFAULT_FECTH_METADATA_TIMEOUT.as_millis() as i64) as u64,
      ),
    })
  }

  #[napi]
  pub async fn subscribe(
    &self,
    topic_configs: Either<String, Vec<TopicPartitionConfig>>,
  ) -> Result<()> {
    let topics = match topic_configs {
      Either::A(config) => {
        debug!("Subscribing to topic: {:#?}", &config);
        vec![TopicPartitionConfig {
          topic: config,
          all_offsets: None,
          partition_offset: None,
        }]
      }
      Either::B(config) => {
        debug!("Subscribing to topic config: {:#?}", &config);
        config
      }
    };

    let topics_name = topics
      .iter()
      .map(|x| x.topic.clone())
      .collect::<Vec<String>>();

    debug!("Creating topics if not exists: {:?}", &topics_name);
    try_create_topic(
      &topics_name,
      &self.client_config,
      self.fecth_metadata_timeout,
    )
    .await
    .map_err(|e| e.convert_to_napi())?;

    try_subscribe(&self.stream_consumer, &topics_name).map_err(|e| e.convert_to_napi())?;

    topics.iter().for_each(|item| {
      if let Some(all_offsets) = item.all_offsets.clone() {
        info!(
          "Subscribing to topic: {}. Setting all partitions to offset: {:?}",
          &item.topic, &all_offsets
        );
        set_offset_of_all_partitions(
          &all_offsets,
          &self.stream_consumer,
          &item.topic,
          self.fecth_metadata_timeout,
        )
        .map_err(|e| e.convert_to_napi())
        .unwrap();
      } else if let Some(partition_offset) = item.partition_offset.clone() {
        info!(
          "Subscribing to topic: {} with partition offsets: {:?}",
          &item.topic, &partition_offset
        );
        assign_offset_or_use_metadata(
          &item.topic,
          Some(partition_offset),
          None,
          &self.stream_consumer,
          self.fecth_metadata_timeout,
        )
        .map_err(|e| e.convert_to_napi())
        .unwrap();
      };
    });

    Ok(())
  }

  #[napi]
  pub fn unsubscribe(&self) -> Result<()> {
    self.stream_consumer.unsubscribe();
    Ok(())
  }

  #[napi]
  pub fn seek(
    &self,
    topic: String,
    partition: i32,
    offset_model: OffsetModel,
    timeout: Option<i64>,
  ) -> Result<()> {
    let offset = convert_to_rdkafka_offset(&offset_model);
    info!(
      "Seeking to topic: {}, partition: {}, offset: {:?}",
      topic, partition, offset
    );
    self
      .stream_consumer
      .seek(
        &topic,
        partition,
        offset,
        Duration::from_millis(timeout.unwrap_or(DEFAULT_SEEK_TIMEOUT) as u64),
      )
      .map_err(|e| {
        error!("Error while seeking: {:?}", e);
        napi::Error::new(
          napi::Status::GenericFailure,
          format!("Error while seeking: {:?}", e),
        )
      })?;
    Ok(())
  }

  #[napi]
  pub async fn recv(&self) -> Result<Message> {
    self
      .stream_consumer
      .recv()
      .await
      .map_err(|e| {
        napi::Error::new(
          napi::Status::GenericFailure,
          format!("Error while receiving from stream consumer: {:?}", e),
        )
      })
      .map(|message| create_message(&message, message.payload().unwrap_or(&[])))
  }

  #[napi]
  pub fn commit(
    &self,
    topic: String,
    partition: i32,
    offset: i64,
    commit: CommitMode,
  ) -> Result<()> {
    let mut tpl = RdTopicPartitionList::new();
    tpl
      .add_partition_offset(&topic, partition, Offset::Offset(offset))
      .map_err(|e| {
        napi::Error::new(
          napi::Status::GenericFailure,
          format!("Error while adding partition offset: {:?}", e),
        )
      })?;
    let commit_mode = match commit {
      CommitMode::Sync => RdKfafkaCommitMode::Sync,
      CommitMode::Async => RdKfafkaCommitMode::Async,
    };
    self
      .stream_consumer
      .commit(&tpl, commit_mode)
      .map_err(|e| {
        napi::Error::new(
          napi::Status::GenericFailure,
          format!("Error while committing: {:?}", e),
        )
      })?;
    debug!("Commiting done. Tpl: {:?}", &tpl);
    Ok(())
  }
}
