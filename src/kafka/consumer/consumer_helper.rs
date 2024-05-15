use std::{collections::HashMap, time::Duration};

use rdkafka::{config::RDKafkaLogLevel, consumer::{Consumer, StreamConsumer}, ClientConfig, TopicPartitionList};
use tracing::{debug, info, warn, error};

use crate::kafka::{consumer::consumer_model::LoggingConsumer, kafka_admin::KafkaAdmin, kafka_util::convert_to_rdkafka_offset, model::OffsetModel};

use super::{consumer_model::CustomContext, consumer_model::ConsumerConfiguration};

pub async fn create_stream_consumer(
    client_config: &ClientConfig,
    consumer_configuration: &ConsumerConfiguration,
    topic: &str,
    offset: &Option<OffsetModel>,
    configuration: Option<HashMap<String, String>>,
  ) -> anyhow::Result<StreamConsumer<CustomContext>> {
    let context = CustomContext;
  
    let ConsumerConfiguration {
      create_topic,
      group_id,
      enable_auto_commit,
      ..
    } = consumer_configuration.clone();
  
    let mut consumer_config: ClientConfig = client_config.clone();
  
    if let Some(config) = configuration {
      consumer_config.extend(config);
    }
  
    debug!("Creating consumer with configuration: {:?}", consumer_config);
  
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
      let admin = KafkaAdmin::new(client_config);
      let result = admin.create_topic(topic).await;
      if let Err(e) = result {
        warn!("Fail to create topic {:?}", e);
        return Err(anyhow::Error::msg(format!("Fail to create topic: {:?}", e)));
      }
      info!("Topic created: {:?}", topic)
    }
  
    if let Some(offset) = convert_to_rdkafka_offset(offset.clone()) {
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