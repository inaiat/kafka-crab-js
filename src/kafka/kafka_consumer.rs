use napi::{
  bindgen_prelude::{Buffer, Promise},
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  JsFunction, Result,
};
use rdkafka::{
  client::ClientContext,
  config::RDKafkaLogLevel,
  consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer, ConsumerContext, Rebalance},
  error::KafkaResult,
  topic_partition_list::TopicPartitionList,
  Message, Offset,
};
use tokio::sync::broadcast::error;

use std::{time::Duration, vec};
use tracing::{debug, error, info, warn};

use super::{
  kafka_admin::KafkaAdmin,
  kafka_client::KafkaClient,
  model::{AutoOffsetReset, ConsumerConfiguration, ConsumerModel, KafkaCommitMode},
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
  consumer_configuration: ConsumerConfiguration,
}

#[napi]
impl KafkaConsumer {
  pub fn new(kafka_client: KafkaClient, consumer_configuration: ConsumerConfiguration) -> Self {
    KafkaConsumer {
      kafka_client,
      consumer_configuration,
    }
  }

  #[napi(ts_args_type = "callback: (err: Error | null, result: Buffer) => void")]
  pub async fn start_consumer(&self, func: ThreadsafeFunction<Buffer>) -> Result<()> {
    let ConsumerConfiguration {
      topic,
      retry_strategy,
      offset,
      create_topic,
      commit_mode,
      group_id,
    } = self.consumer_configuration.clone();

    let admin = KafkaAdmin::new(self.kafka_client.get_client_config());
    match admin.create_topic(&topic).await {
      Ok(_) => info!("Topic created"),
      Err(_) => warn!("Topic already exists"),
    }

    let commit_mode = match commit_mode.unwrap_or(KafkaCommitMode::Async) {
      KafkaCommitMode::Sync => CommitMode::Sync,
      KafkaCommitMode::Async => CommitMode::Async,
    };

    let stream_consumer = self.setup_consumer(&group_id, &topic, None);

    tokio::spawn(async move {
      loop {
        match stream_consumer.recv().await {
          Err(e) => error!("Error while receiving from stream consumer: {:?}", e),
          Ok(message) => {
            match message.payload_view::<[u8]>() {
              None => {}
              Some(Ok(payload)) => match func.call_async::<Promise<()>>(Ok(payload.into())).await {
                Ok(js_result) => match js_result.await {
                  Ok(_) => match stream_consumer.commit_message(&message, commit_mode) {
                    Ok(_) => debug!("Message committed"),
                    Err(e) => error!("Error on commit: {:?}", e),
                  },
                  Err(e) => error!("Error on promise: {:?}", e),
                },
                Err(js_error) => error!("Error on js call: {:?}", js_error),
              },
              Some(Err(e)) => {
                error!("Error while deserializing message payload: {:?}", e);
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
