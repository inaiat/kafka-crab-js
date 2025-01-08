use rdkafka::{
  consumer::{BaseConsumer, ConsumerContext, Rebalance, StreamConsumer},
  error::KafkaResult,
  ClientContext, TopicPartitionList,
};
use tokio::sync::watch;
use tracing::{debug, error};

use crate::kafka::consumer::consumer_helper::convert_tpl_to_array_of_topic_partition;

use super::model::TopicPartition;

pub type TxRxContext = (
  watch::Sender<Option<KafkaEvent>>,
  watch::Receiver<Option<KafkaEvent>>,
);

pub type LoggingConsumer = StreamConsumer<KafkaCrabContext>;

#[napi(object)]
#[derive(Clone, Debug)]
pub struct KafkaEventPayload {
  pub action: Option<String>,
  pub tpl: Vec<TopicPartition>,
  pub error: Option<String>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct KafkaEvent {
  pub name: String,
  pub payload: KafkaEventPayload,
}

pub struct KafkaCrabContext {
  pub tx_rx_signal: TxRxContext,
}

impl KafkaCrabContext {
  pub fn new() -> Self {
    let (tx, rx) = watch::channel(None);
    KafkaCrabContext {
      tx_rx_signal: (tx, rx),
    }
  }

  fn send_event(&self, event: KafkaEvent) {
    self.tx_rx_signal.0.send(Some(event)).unwrap_or_else(|err| {
      error!("Error while sending event: {:?}", err);
    });
  }
}

impl ClientContext for KafkaCrabContext {}

impl ConsumerContext for KafkaCrabContext {
  fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
    let event = KafkaEvent {
      name: "rebalance".to_string(),
      payload: convert_rebalance_to_kafka_payload(rebalance),
    };

    debug!(
      "Pre rebalance {:?}, consumer closed: {} ",
      rebalance,
      consumer.closed()
    );

    self.send_event(event);
  }

  fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
    let event = KafkaEvent {
      name: "post_rebalance".to_string(),
      payload: convert_rebalance_to_kafka_payload(rebalance),
    };

    debug!(
      "Post rebalance {:?}, consumer closed: {} ",
      rebalance,
      consumer.closed()
    );

    self.send_event(event);
  }

  fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
    let event = KafkaEvent {
      name: "commit_callback".to_string(),
      payload: KafkaEventPayload {
        action: None,
        tpl: convert_tpl_to_array_of_topic_partition(offsets),
        error: None,
      },
    };

    debug!("Committing offsets: {:?}. Offset: {:?}", result, offsets);

    self.send_event(event);
  }
}

fn convert_rebalance_to_kafka_payload(rebalance: &Rebalance) -> KafkaEventPayload {
  match rebalance {
    Rebalance::Assign(partitions) => KafkaEventPayload {
      action: Some("assign".to_string()),
      tpl: convert_tpl_to_array_of_topic_partition(partitions),
      error: None,
    },
    Rebalance::Revoke(partitions) => KafkaEventPayload {
      action: Some("revoke".to_string()),
      tpl: convert_tpl_to_array_of_topic_partition(partitions),
      error: None,
    },
    Rebalance::Error(err) => KafkaEventPayload {
      action: Some("error".to_string()),
      tpl: vec![],
      error: Some(err.to_string()),
    },
  }
}
