use napi::{
  bindgen_prelude::Buffer,
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  JsFunction, Result,
};
use rdkafka::{
  client::ClientContext,
  config::RDKafkaLogLevel,
  consumer::{stream_consumer::StreamConsumer, Consumer, ConsumerContext, Rebalance},
  error::KafkaResult,
  topic_partition_list::TopicPartitionList,
  Message, Offset,
};

use std::{time::Duration, vec};
use tracing::{info, warn};

use super::{
  kafka_admin::KafkaAdmin,
  kafka_client::KafkaClient,
  model::{AutoOffsetReset, ConsumerConfiguration, ConsumerModel},
};

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

#[derive(Clone, Debug)]
#[napi]
pub struct KafkaConsumer {
  kafka_client: KafkaClient,
  pub group_id: String,
  pub enable_auto_commit: Option<bool>,
  pub auto_offset_reset: Option<AutoOffsetReset>,
}

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: KafkaClient,
    group_id: String,
    enable_auto_commit: Option<bool>,
    auto_offset_reset: Option<AutoOffsetReset>,
  ) -> Self {
    KafkaConsumer {
      kafka_client,
      group_id,
      auto_offset_reset: None,
      enable_auto_commit: None,
    }
  }

  #[napi(
    ts_args_type = "consumerConfiguration: ConsumerConfiguration, callback: (err: Error | null, result: Buffer) => void"
  )]
  pub fn start_consumer(
    &self,
    consumer_configuration: ConsumerConfiguration,
    callback: JsFunction,
  ) -> Result<()> {
    let tsfn: ThreadsafeFunction<Buffer> =
      callback.create_threadsafe_function(0, |ctx| Ok(vec![ctx.value]))?;

    let ConsumerConfiguration {
      topic,
      retry_strategy,
      offset,
      create_topic,
    } = consumer_configuration;

    let stream_consumer = self.setup_consumer(&self.group_id, &topic, None);

    // &self.kafka_client

    // let admin = KafkaAdmin::new(self.kafka_client.get_client_config());

    napi::tokio::spawn(async move {
      // if create_topic.unwrap_or(true) {
      //   match admin.create_topic(&topic).await {
      //     Ok(_) => {}
      //     Err(_) => {}
      //   }
      // };
      loop {
        match stream_consumer.recv().await {
          Err(e) => warn!("Kafka error: {}", e),
          Ok(message) => {
            match message.payload_view::<[u8]>() {
              None => {}
              Some(Ok(payload)) => {
                tsfn.call(Ok(payload.into()), ThreadsafeFunctionCallMode::NonBlocking);
              }
              Some(Err(e)) => {
                warn!("Error while deserializing message payload: {:?}", e);
              }
            };
          }
        };
      }
    });

    Ok(())
  }

  fn setup_consumer(
    &self,
    group_id: &str,
    topic: &str,
    new_offset: Option<Offset>,
  ) -> StreamConsumer<CustomContext> {
    let context = CustomContext;

    let consumer: LoggingConsumer = self
      .kafka_client
      .get_client_config()
      .clone()
      .set("group.id", group_id)
      .set_log_level(RDKafkaLogLevel::Debug)
      .create_with_context(context)
      .expect("Consumer creation failed");

    if let Some(offset) = new_offset {
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
      .expect("Can't subscribe to specified topics");

    info!(
      "Consumer created. Group id: {:?}, Topic: {:?}",
      group_id, topic
    );
    consumer
  }

  pub async fn create_stream_consumer(
    &self,
    consumer_model: &ConsumerModel,
  ) -> anyhow::Result<StreamConsumer<CustomContext>> {
    let ConsumerModel {
      group_id,
      topic,

      offset,
      ..
    } = consumer_model;
    let client_config = self.kafka_client.get_client_config();
    let admin = KafkaAdmin::new(client_config);

    admin.create_topic(topic).await?;

    let consumer: StreamConsumer<CustomContext> = self.setup_consumer(group_id, topic, *offset);

    Ok(consumer)
  }
}
