use std::{collections::HashMap, time::Duration};

use napi::{Error, Result, Status};
use rdkafka::{
  message::{OwnedHeaders, ToBytes},
  producer::{FutureProducer, FutureRecord},
  ClientConfig,
};
use tracing::{debug, warn};

use super::{
  kafka_util::hashmap_to_kafka_headers,
  model::{MessageModel, OwnedDelivery},
};

const DEFAULT_QUEUE_TIMEOUT: i64 = 5000;

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ProducerConfiguration {
  pub queue_timeout: Option<i64>,
  pub configuration: Option<HashMap<String, String>>,
}

#[derive(Clone)]
#[napi]
pub struct KafkaProducer {
  future_producer: FutureProducer,
  queue_timeout: Duration,
}

#[napi]
impl KafkaProducer {
  pub fn new(
    client_config: ClientConfig,
    producer_configuration: ProducerConfiguration,
  ) -> Result<Self> {
    let mut future_producer_config = client_config;

    if let Some(config) = producer_configuration.configuration {
      future_producer_config.extend(config);
    }

    let future_producer = future_producer_config
      .create::<FutureProducer>()
      .map_err(|e| {
        warn!("Failed to create Kafka producer. Error: {:?}", e);
        Error::new(Status::GenericFailure, e)
      })?;

    let queue_timeout = Duration::from_millis(
      producer_configuration
        .queue_timeout
        .unwrap_or(DEFAULT_QUEUE_TIMEOUT)
        .try_into()
        .map_err(|e| Error::new(Status::GenericFailure, e))?,
    );

    Ok(KafkaProducer {
      future_producer,
      queue_timeout,
    })
  }

  #[napi]
  pub async fn send(&self, message: MessageModel) -> Result<OwnedDelivery> {
    let MessageModel {
      topic,
      key,
      value,
      headers,
    } = message;

    let kafka_headers = match headers {
      Some(v) => hashmap_to_kafka_headers(&v),
      None => OwnedHeaders::new(),
    };

    let record = FutureRecord::to(&topic)
      .key(key.to_bytes())
      .headers(kafka_headers)
      .payload(value.to_bytes());

    match self.raw_send(record).await {
      Ok((partition, offset)) => Ok(OwnedDelivery { partition, offset }),
      Err(e) => Err(Error::new(Status::GenericFailure, e)),
    }
  }

  pub async fn raw_send<'a, K: ToBytes + ?Sized, P: ToBytes + ?Sized>(
    &self,
    record: FutureRecord<'a, K, P>,
  ) -> anyhow::Result<(i32, i64)> {
    debug!("Trying to produce message. Topic {}", record.topic);
    let producer: &FutureProducer = &self.future_producer;

    producer
      .send(record, self.queue_timeout)
      .await
      .map_err(|(k, m)| {
        anyhow::Error::msg(format!(
          "Failed to send message to Kafka. Error: {:?}. Message: {:?}",
          k, m
        ))
      })
  }
}
