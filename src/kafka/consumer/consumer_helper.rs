use std::{collections::HashMap, time::Duration};

use rdkafka::{
  config::RDKafkaLogLevel,
  consumer::{Consumer, StreamConsumer},
  ClientConfig, Offset, TopicPartitionList,
};
use tracing::{debug, error, info, warn};

use crate::kafka::{consumer::model::LoggingConsumer, kafka_admin::KafkaAdmin};

use super::model::{
  ConsumerConfiguration, CustomContext, OffsetModel, PartitionOffset, PartitionPosition,
};

pub fn convert_to_rdkafka_offset(offset_model: &OffsetModel) -> Offset {
  match offset_model.position {
    Some(PartitionPosition::Beginning) => Offset::Beginning,
    Some(PartitionPosition::End) => Offset::End,
    Some(PartitionPosition::Stored) => Offset::Stored,
    None => match offset_model.offset {
      Some(value) => Offset::Offset(value),
      None => Offset::Stored, // Default to stored
    },
  }
}

pub async fn create_stream_consumer_and_setup_everything(
  client_config: &ClientConfig,
  consumer_configuration: &ConsumerConfiguration,
  topic: &str,
  offset: &Option<OffsetModel>,
  configuration: Option<HashMap<String, String>>,
  timeout: Duration,
) -> anyhow::Result<StreamConsumer<CustomContext>> {
  let consumer = create_stream_consumer(client_config, consumer_configuration, configuration)?;

  if consumer_configuration.create_topic.unwrap_or(true) {
    try_create_topic(&vec![topic.to_owned()], client_config, timeout).await?;
  }

  if let Some(offset_model) = offset {
    set_offset_of_all_partitions(offset_model, &consumer, topic, timeout)?;
  }

  try_subscribe(&consumer, &vec![topic.to_owned()])?;

  Ok(consumer)
}

pub fn create_stream_consumer(
  client_config: &ClientConfig,
  consumer_configuration: &ConsumerConfiguration,
  configuration: Option<HashMap<String, String>>,
) -> anyhow::Result<StreamConsumer<CustomContext>> {
  let context = CustomContext;

  let ConsumerConfiguration {
    group_id,
    enable_auto_commit,
    ..
  } = consumer_configuration.clone();

  let mut consumer_config: ClientConfig = client_config.clone();

  if let Some(config) = configuration {
    consumer_config.extend(config);
  }

  debug!(
    "Creating consumer with configuration: {:?}",
    consumer_config
  );

  let consumer: LoggingConsumer = consumer_config
    .clone()
    .set("group.id", group_id.clone())
    .set(
      "enable.auto.commit",
      enable_auto_commit.unwrap_or(true).to_string(),
    )
    .set_log_level(RDKafkaLogLevel::Debug)
    .create_with_context(context)?;

  info!("Consumer created. Group id: {:?}", group_id);
  Ok(consumer)
}

pub fn try_subscribe(consumer: &LoggingConsumer, topics: &Vec<String>) -> anyhow::Result<()> {
  let topics_ref = topics.iter().map(|x| x.as_str()).collect::<Vec<&str>>();
  consumer.subscribe(topics_ref.as_slice()).map_err(|e| {
    anyhow::Error::msg(format!(
      "Can't subscribe to specified topic(s): {:?}. Error: {:?}",
      topics_ref, e
    ))
  })?;
  debug!("Subscribed to topic(s): {:?}", topics_ref);
  Ok(())
}

pub async fn try_create_topic(
  topics: &Vec<String>,
  client_config: &ClientConfig,
  fetch_metadata_timeout: Duration,
) -> anyhow::Result<()> {
  let admin = KafkaAdmin::new(client_config, Some(fetch_metadata_timeout));
  let result = admin.create_topic(topics).await;
  if let Err(e) = result {
    warn!("Fail to create topic {:?}", e);
    return Err(anyhow::Error::msg(format!("Fail to create topic: {:?}", e)));
  }
  info!("Topic(s) created: {:?}", topics);
  Ok(())
}

pub fn set_offset_of_all_partitions(
  offset_model: &OffsetModel,
  consumer: &StreamConsumer<CustomContext>,
  topic: &str,
  timeout: Duration,
) -> anyhow::Result<()> {
  let offset = convert_to_rdkafka_offset(offset_model);
  debug!("Setting offset to: {:?}", offset);
  let metadata = consumer.fetch_metadata(Some(topic), timeout)?;

  metadata.topics().iter().for_each(|meta_topic| {
    let mut tpl = TopicPartitionList::new();
    meta_topic.partitions().iter().for_each(|meta_partition| {
      info!("Adding partition: {:?}", meta_partition.id());
      tpl.add_partition(topic, meta_partition.id());
    });
    match tpl.set_all_offsets(offset) {
      Ok(_) => {
        debug!("Offset set to: {:?}", offset);
      }
      Err(e) => {
        error!("Fail to set offset: {:?}", e)
      }
    };
    match consumer.assign(&tpl) {
      Ok(_) => {
        debug!("Assigning topic: {:?}", topic);
      }
      Err(e) => {
        error!("Fail to assign topic: {:?}", e);
      }
    }
  });

  Ok(())
}

pub fn assign_offset_or_use_metadata(
  topic: &str,
  partition_offset: Option<Vec<PartitionOffset>>,
  offset_model: Option<&OffsetModel>,
  consumer: &StreamConsumer<CustomContext>,
  timeout: Duration,
) -> anyhow::Result<()> {
  let mut tpl = TopicPartitionList::new();

  if let Some(value) = partition_offset {
    for item in value {
      let offset = convert_to_rdkafka_offset(&item.offset);
      debug!(
        "Adding partition: {:?} with offset: {:?} for topic: {:?}",
        item.partition, offset, topic
      );
      tpl.add_partition_offset(topic, item.partition, offset)?;
    }
  } else if let Some(offset_model) = offset_model {
    let offset = convert_to_rdkafka_offset(offset_model);
    let metadata = consumer.fetch_metadata(Some(topic), timeout)?;
    for meta_topic in metadata.topics() {
      for meta_partition in meta_topic.partitions() {
        info!(
          "Adding partition: {:?} with offset: {:?} for topic: {:?}",
          meta_partition.id(),
          offset,
          topic
        );
        tpl.add_partition_offset(topic, meta_partition.id(), offset)?;
      }
    }
  } else {
    anyhow::bail!("At least one of partition_offset or offset_model should be provided");
  }
  consumer.assign(&tpl)?;
  Ok(())
}
