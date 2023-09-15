use rdkafka::consumer::StreamConsumer;
use tracing::info;

use rdkafka::{
  client::ClientContext,
  consumer::{ConsumerContext, Rebalance},
  error::KafkaResult,
  topic_partition_list::TopicPartitionList,
};

pub type LoggingConsumer = StreamConsumer<CustomContext>;

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
