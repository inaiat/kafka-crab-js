use std::time::Duration;

use napi::{Error, Result, Status};
use rdkafka::{
  message::ToBytes,
  producer::{FutureProducer, FutureRecord},
  ClientConfig,
};

use super::model::MessageModel;

#[derive(Clone, Debug)]
#[napi]
pub struct KafkaProducer {
  client_config: ClientConfig,
}

#[napi(object)]
pub struct OwnedDelivery {
  pub partition: i32,
  pub offset: i64,
}

#[napi]
impl KafkaProducer {
  pub fn new(client_config: ClientConfig) -> Self {
    KafkaProducer { client_config }
  }

  #[napi]
  pub async fn send(&self, topic: String, message: MessageModel) -> Result<OwnedDelivery> {
    let MessageModel {
      key,
      value,
      headers: _,
    } = message;
    
    let producer = &self
      .client_config
      .clone()
      .set("message.timeout.ms", "5000")
      .create::<FutureProducer>()
      .expect("Producer creation error");

    let record = FutureRecord::to(&topic)
      .key(key.to_bytes())
      .payload(value.to_bytes());

    match producer.send(record, Duration::from_secs(5000)).await {
      Ok((partition, offset)) => Ok(OwnedDelivery { partition, offset }),
      Err((kafka_error, _)) => Err(Error::new(Status::GenericFailure, kafka_error)),
    }
  }
}
