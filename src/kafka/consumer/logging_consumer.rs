use async_trait::async_trait;
use std::time::Duration;

use rdkafka::{
  config::RDKafkaLogLevel,
  consumer::{self, Consumer, StreamConsumer},
  ClientConfig, TopicPartitionList,
};
use tracing::{debug, info};

use crate::kafka::{
  consumer::consumer_model::ConsumerConfiguration,
  kafka_admin::KafkaAdmin,
  kafka_util::{convert_to_rdkafka_offset, CustomContext},
  model::OffsetModel,
};

use super::consumer_model::{KafkaConsumer, KafkaConsumerContext};

type LoggingConsumer = StreamConsumer<CustomContext>;

#[async_trait]
impl KafkaConsumerContext<LoggingConsumer> for KafkaConsumer {
  async fn create_stream(
    &self,
    topic: &str,
    offset: Option<OffsetModel>,
  ) -> anyhow::Result<LoggingConsumer> {
    let context = CustomContext;

    let consumer: LoggingConsumer = self.create_base_consumer();

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
