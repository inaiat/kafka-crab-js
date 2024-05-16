use std::time::Duration;

use napi::Result;

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  topic_partition_list::TopicPartitionList,
  ClientConfig, Message, Offset,
};

use tracing::{debug, info};

use crate::kafka::{
  consumer::consumer_helper::try_create_topic,
  kafka_client::KafkaClient,
  kafka_util::{convert_to_rdkafka_offset, create_payload, AnyhowToNapiError},
  model::{OffsetModel, Payload},
};

use super::{
  consumer_helper::{create_stream_consumer, set_offset_of_all_partitions},
  consumer_model::{CommitMode, ConsumerConfiguration, CustomContext},
};

#[napi]
pub struct KafkaStreamConsumer {
  client_config: ClientConfig,
  topic: String,
  stream_consumer: StreamConsumer<CustomContext>,
}

#[napi]
impl KafkaStreamConsumer {
  pub fn new(
    kafka_client: KafkaClient,
    consumer_configuration: &ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let ConsumerConfiguration {
      topic,
      configuration,
      ..
    } = consumer_configuration;
    let stream_consumer =
      create_stream_consumer(client_config, consumer_configuration, configuration.clone())
        .map_err(|e| e.convert_to_napi())?;

    Ok(KafkaStreamConsumer {
      client_config: client_config.clone(),
      topic: topic.clone(),
      stream_consumer,
    })
  }

  #[napi]
  pub async fn subscribe(&self, topics: Vec<String>) -> Result<()> {
    let topic_slices: Vec<&str> = topics.iter().map(|x| x.as_str()).collect();
    let topics = topic_slices.as_slice();
    info!("Subscribing to topics: {:?}", topics);

    for topic in topics {
      try_create_topic(topic, &self.client_config)
        .await
        .map_err(|e| e.convert_to_napi())?;
    }

    self.stream_consumer.subscribe(topics).map_err(|e| {
      napi::Error::new(
        napi::Status::GenericFailure,
        format!("Error while subscribing to topic: {:?}", e),
      )
    })?;
    Ok(())
  }

  #[napi]
  pub fn seek(&self, topic: String, partition: i32, offset_model: OffsetModel) -> Result<()> {
    let offset = convert_to_rdkafka_offset(&offset_model);
    self
      .stream_consumer
      .seek(&topic, partition, offset, Duration::from_secs(5))
      .map_err(|e| {
        napi::Error::new(
          napi::Status::GenericFailure,
          format!("Error while seeking: {:?}", e),
        )
      })?;
    Ok(())
  }

  #[napi]
  pub fn seek_all_partitions(&self, topic: String, offset_model: OffsetModel) -> Result<()> {
    set_offset_of_all_partitions(&offset_model, &self.stream_consumer, &topic)
      .map_err(|e| e.convert_to_napi())?;
    Ok(())
  }

  #[napi]
  pub async fn recv(&self) -> Result<Payload> {
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
      .map(|message| create_payload(&message, message.payload().unwrap_or(&[])))
  }

  #[napi]
  pub fn commit(&self, partition: i32, offset: i64, commit: CommitMode) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl
      .add_partition_offset(&self.topic, partition, Offset::Offset(offset))
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
