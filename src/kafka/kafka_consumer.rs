use napi::{
  bindgen_prelude::{Buffer, Promise},
  threadsafe_function::ThreadsafeFunction,
  Result,
};
use rdkafka::{
  client::ClientContext,
  config::RDKafkaLogLevel,
  consumer::{stream_consumer::StreamConsumer, CommitMode, Consumer, ConsumerContext, Rebalance},
  error::KafkaResult,
  message::BorrowedMessage,
  topic_partition_list::TopicPartitionList,
  Message, Offset,
};

use std::{time::Duration, vec};
use tracing::{debug, error, info};

use super::{
  kafka_admin::KafkaAdmin,
  kafka_client::KafkaClient,
  model::{ConsumerConfiguration, ConsumerResult, KafkaCommitMode, OffsetModel, PartitionPosition},
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

  #[napi(ts_args_type = "callback: (err: Error | null, result: Buffer) => Promise<ConsumerResult>")]
  pub async fn start_consumer(&self, func: ThreadsafeFunction<Buffer>) -> Result<()> {
    let ConsumerConfiguration { commit_mode, .. } = self.consumer_configuration.clone();

    let commit_mode = match commit_mode {
      None | Some(KafkaCommitMode::AutoCommit) => None, //Default AutoCommit
      Some(KafkaCommitMode::Sync) => Some(CommitMode::Sync),
      Some(KafkaCommitMode::Async) => Some(CommitMode::Async),
    };

    let stream_consumer: StreamConsumer<CustomContext> = self
      .setup_consumer()
      .await
      .expect("Consumer creation failed");

    tokio::spawn(async move {
      loop {
        match stream_consumer.recv().await {
          Err(e) => error!("Error while receiving from stream consumer: {:?}", e),
          Ok(message) => process_message(commit_mode, &stream_consumer, &func, &message).await,
        };
      }
    });

    Ok(())
  }

  async fn setup_consumer(&self) -> anyhow::Result<StreamConsumer<CustomContext>> {
    let context = CustomContext;

    let ConsumerConfiguration {
      topic,
      offset,
      create_topic,
      group_id,
      ..
    } = self.consumer_configuration.clone();

    let consumer: LoggingConsumer = self
      .kafka_client
      .get_client_config()
      .clone()
      .set("group.id", group_id.clone())
      .set("enable.auto.commit", "false")
      .set_log_level(RDKafkaLogLevel::Debug)
      .create_with_context(context)
      .expect("Consumer creation failed");

    if create_topic.unwrap_or(true) {
      let admin = KafkaAdmin::new(self.kafka_client.get_client_config());
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

fn convert_to_rdkafka_offset(offset_model: Option<OffsetModel>) -> Option<Offset> {
  match offset_model {
    Some(model) => Some(match model.position {
      Some(PartitionPosition::Beginning) => Offset::Beginning,
      Some(PartitionPosition::End) => Offset::End,
      Some(PartitionPosition::Stored) => Offset::Stored,
      None => match model.offset {
        Some(value) => Offset::Offset(value),
        None => Offset::Stored, // Default to stored
      },
    }),
    None => None,
  }
}

async fn process_message(
  commit_mode: Option<CommitMode>,
  stream_consumer: &StreamConsumer<CustomContext>,
  func: &ThreadsafeFunction<Buffer>,
  message: &BorrowedMessage<'_>,
) {
  match message.payload_view::<[u8]>() {
    None => {}
    Some(Ok(payload)) => match func
      .call_async::<Promise<ConsumerResult>>(Ok(payload.into()))
      .await
    {
      Ok(js_result) => match js_result.await {
        Ok(value) => {
          debug!("Message processed: {:?}", value);
          if let Some(commit_mode) = commit_mode {
            match stream_consumer.commit_message(&message, commit_mode) {
              Ok(_) => debug!("Message committed"),
              Err(e) => error!("Error on commit: {:?}", e),
            }
          }
        }
        Err(e) => error!("Error on promise: {:?}", e),
      },
      Err(js_error) => error!("Error on js call: {:?}", js_error),
    },
    Some(Err(e)) => {
      error!("Error while deserializing message payload: {:?}", e);
    }
  }
}
