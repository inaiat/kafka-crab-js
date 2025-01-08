use std::{time::Duration, vec};
use tokio::sync::watch::{self};

use napi::{
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  Either, Result,
};

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  topic_partition_list::TopicPartitionList as RdTopicPartitionList,
  ClientConfig, Message as RdMessage, Offset,
};

use tracing::{debug, info};

use crate::kafka::{
  consumer::consumer_helper::{
    assign_offset_or_use_metadata, convert_to_rdkafka_offset, try_create_topic, try_subscribe,
  },
  kafka_client_config::KafkaClientConfig,
  kafka_util::{create_message, AnyhowToNapiError, IntoNapiError},
  producer::model::Message,
};

use super::{
  consumer_helper::{
    convert_tpl_to_array_of_topic_partition, create_stream_consumer, set_offset_of_all_partitions,
  },
  context::{KafkaCrabContext, KafkaEvent},
  model::{
    CommitMode, ConsumerConfiguration, OffsetModel, ShutdownSignal, TopicPartition,
    TopicPartitionConfig, DEFAULT_FECTH_METADATA_TIMEOUT,
  },
};

use tokio::select;

pub const DEFAULT_SEEK_TIMEOUT: i64 = 1500;

#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  stream_consumer: StreamConsumer<KafkaCrabContext>,
  fecth_metadata_timeout: Duration,
  shutdown_signal: ShutdownSignal,
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
      shutdown_signal: watch::channel(()),
    })
  }

  #[napi(ts_args_type = "callback: (error: Error | undefined, event: KafkaEvent) => void")]
  pub fn subscribe_to_consumer_events(
    &self,
    callback: ThreadsafeFunction<KafkaEvent>,
  ) -> Result<()> {
    let mut rx = self.stream_consumer.context().tx_rx_signal.1.clone();
    let mut shutdown_signal = self.shutdown_signal.1.clone();

    tokio::spawn(async move {
      loop {
        select! {
            _ = rx.changed() => {
                if let Some(event) = rx.borrow().clone() {
                    callback.call(Ok(event), ThreadsafeFunctionCallMode::NonBlocking);
                }
            }
            _ = shutdown_signal.changed() => {
                debug!("Subscription to consumer events is stopped");
                break;
            }
        }
      }
    });
    Ok(())
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
        debug!(
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
        debug!(
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

  fn get_partitions(&self) -> Result<RdTopicPartitionList> {
    let partitions = self
      .stream_consumer
      .assignment()
      .map_err(|e| e.into_napi_error("getting partitions"))?;
    Ok(partitions)
  }

  #[napi]
  pub fn pause(&self) -> Result<()> {
    self
      .stream_consumer
      .pause(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("error while pausing"))?;
    Ok(())
  }

  #[napi]
  pub fn resume(&self) -> Result<()> {
    self
      .stream_consumer
      .resume(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("error while resuming"))?;
    Ok(())
  }

  #[napi]
  pub fn unsubscribe(&self) -> Result<()> {
    info!("Unsubscribing from topics");
    self.stream_consumer.unsubscribe();
    Ok(())
  }

  #[napi]
  pub async fn shutdown_consumer(&self) -> Result<()> {
    info!("Shutting down consumer - this will unsubscribe and stop the consumer from receiving messages");

    // First unsubscribe from topics
    self.stream_consumer.unsubscribe();

    // Then send shutdown signal
    let tx = self.shutdown_signal.0.clone();
    tx.send(())
      .map_err(|e| e.into_napi_error("Error sending shutdown signal"))?;

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
    debug!(
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
      .map_err(|e| e.into_napi_error("Error while seeking"))?;
    Ok(())
  }

  #[napi]
  pub fn assignment(&self) -> Result<Vec<TopicPartition>> {
    let assignment = self
      .stream_consumer
      .assignment()
      .map_err(|e| e.into_napi_error("error while getting assignment"))?;
    Ok(convert_tpl_to_array_of_topic_partition(&assignment))
  }

  #[napi]
  pub async fn recv(&self) -> Result<Option<Message>> {
    let mut rx = self.shutdown_signal.1.clone();
    select! {
        message = self.stream_consumer.recv() => {
            message
                .map_err(|e| e.into_napi_error("Error while receiving from stream consumer"))
                .map(|message| Some(create_message(&message, message.payload().unwrap_or(&[]))))
        }
        _ = rx.changed() => {
            debug!("Shutdown signal received and this will stop the consumer from receiving messages");
            Ok(None)
        }
    }
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
      .map_err(|e| e.into_napi_error("Error while adding partition offset"))?;
    let commit_mode = match commit {
      CommitMode::Sync => RdKfafkaCommitMode::Sync,
      CommitMode::Async => RdKfafkaCommitMode::Async,
    };
    self
      .stream_consumer
      .commit(&tpl, commit_mode)
      .map_err(|e| e.into_napi_error("Error while committing"))?;
    debug!("Commiting done. Tpl: {:?}", &tpl);
    Ok(())
  }
}
