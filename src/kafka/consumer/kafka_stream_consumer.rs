use napi::Result;

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  topic_partition_list::TopicPartitionList,
  ClientConfig, Message, Offset,
};


use tracing::debug;

use crate::kafka::{
  kafka_client::KafkaClient,
  kafka_util::{create_payload, AnyhowToNapiError},
  model::Payload,
};

use super::{consumer_helper::create_stream_consumer, consumer_model::{CommitMode, ConsumerConfiguration, CustomContext}};


#[napi]
pub struct KafkaStreamConsumer {
  commit_mode: Option<RdKfafkaCommitMode>,
  topic: String,
  stream_consumer: StreamConsumer<CustomContext>,
}

#[napi]
impl KafkaStreamConsumer {
  pub async fn new(
    kafka_client: KafkaClient,
    consumer_configuration: &ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let commit_mode = match consumer_configuration.commit_mode {
      None | Some(CommitMode::AutoCommit) => None, //Default AutoCommit
      Some(CommitMode::Sync) => Some(RdKfafkaCommitMode::Sync),
      Some(CommitMode::Async) => Some(RdKfafkaCommitMode::Async),
    };

    let ConsumerConfiguration { topic, offset, configuration, ..  } = consumer_configuration;
    let stream_consumer = create_stream_consumer(client_config, consumer_configuration, &topic, offset, configuration.clone()).await.map_err(|e| e.convert_to_napi())?;

    Ok(KafkaStreamConsumer {
      commit_mode,
      topic: topic.clone(),
      stream_consumer
    })
  }

  #[napi]
  pub async fn recv(&self) -> Result<Payload> { 
    self.stream_consumer.recv().await.map_err(|e| {
      napi::Error::new(
        napi::Status::GenericFailure,
        format!("Error while receiving from stream consumer: {:?}", e),
      )
    }).map(|message| {
      create_payload(&message, message.payload().unwrap_or(&[]))
    })
  }

  #[napi]
  pub fn commit(&self, partition: i32, offset: i64) -> Result<()> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&self.topic, partition, Offset::Offset(offset)).map_err(|e| {
      napi::Error::new(
        napi::Status::GenericFailure,
        format!("Error while adding partition offset: {:?}", e),
      )
    })?;
    self.stream_consumer.commit(&tpl, self.commit_mode.unwrap_or(RdKfafkaCommitMode::Async)).map_err(|e| {
      napi::Error::new(
        napi::Status::GenericFailure,
        format!("Error while committing: {:?}", e),
      )
    })?;
    debug!("Commiting done. Tpl: {:?}", &tpl);
    Ok(())
  }

  #[napi]
  pub fn commit_consumer_state(&self) -> Result<()> {
    self.stream_consumer.commit_consumer_state(self.commit_mode.unwrap_or(RdKfafkaCommitMode::Async)).map_err(|e| {
      napi::Error::new(
        napi::Status::GenericFailure,
        format!("Error while committing: {:?}", e),
      )
    })
  }
}
