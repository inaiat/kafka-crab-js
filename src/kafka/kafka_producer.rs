use std::time::Duration;

use napi::{Error, Result, Status};
use rdkafka::{
  message::ToBytes,
  producer::{FutureProducer, FutureRecord},
  ClientConfig,
};
use tracing::{debug, info};

use super::model::{MessageModel, ProducerConfiguration};

#[derive(Clone)]
#[napi]
pub struct KafkaProducer {
  future_producer: FutureProducer,
  topic: String,
}

#[napi(object)]
pub struct OwnedDelivery {
  pub partition: i32,
  pub offset: i64,
}

#[napi]
impl KafkaProducer {
  pub fn new(client_config: ClientConfig, producer_configuration: ProducerConfiguration) -> Self {
    let mut future_producer_config = client_config;

    if let Some(config) = producer_configuration.configuration {
      future_producer_config.extend(config);
    }

    let future_producer = future_producer_config
      .create::<FutureProducer>()
      .expect("Producer creation error");

    KafkaProducer {
      future_producer,
      topic: producer_configuration.topic,
    }
  }

  #[napi]
  pub async fn send(&self, message: MessageModel) -> Result<OwnedDelivery> {
    let MessageModel {
      key,
      value,
      headers: _,
    } = message;

    let record = FutureRecord::to(&self.topic)
      .key(key.to_bytes())
      .payload(value.to_bytes());

    match self
      .future_producer
      .send(record, Duration::from_secs(5000))
      .await
    {
      Ok((partition, offset)) => Ok(OwnedDelivery { partition, offset }),
      Err((kafka_error, _)) => Err(Error::new(Status::GenericFailure, kafka_error)),
    }
  }

  pub async fn raw_send<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized>(
    &self,
    record: FutureRecord<'a, K, P>,
  ) -> anyhow::Result<(i32, i64)> {
    debug!("Trying to produce message.  Topic {}", record.topic);
    let producer: &FutureProducer = &self.future_producer;
    info!("Future producer was created. Topic {}", record.topic);

    producer
      .send(record, Duration::from_secs(5000))
      .await
      .map_err(|e| anyhow::Error::new(e.0))
  }
}
