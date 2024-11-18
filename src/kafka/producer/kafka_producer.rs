use std::{
  sync::{Arc, Mutex},
  time::Duration,
};

use nanoid::nanoid;
use napi::{Error, Result, Status};
use rdkafka::message::ToBytes;
use rdkafka::{
  error::KafkaError,
  message::{OwnedHeaders, OwnedMessage},
  producer::{
    BaseRecord, DeliveryResult, NoCustomPartitioner, Partitioner, Producer, ProducerContext,
    ThreadedProducer,
  },
  ClientConfig, ClientContext, Message, Statistics,
};

use crate::kafka::kafka_util::hashmap_to_kafka_headers;

use super::model::{
  KafkaCrabError, MessageProducer, ProducerConfiguration, ProducerRecord, RecordMetadata,
};

const DEFAULT_QUEUE_TIMEOUT: i64 = 5000;

type ProducerDeliveryResult = (OwnedMessage, Option<KafkaError>, Arc<String>);

#[derive(Clone)]
struct CollectingContext<Part: Partitioner = NoCustomPartitioner> {
  stats: Arc<Mutex<Vec<Statistics>>>,
  results: Arc<Mutex<Vec<ProducerDeliveryResult>>>,
  partitioner: Option<Part>,
}

impl CollectingContext {
  fn new() -> CollectingContext {
    CollectingContext {
      stats: Arc::new(Mutex::new(Vec::new())),
      results: Arc::new(Mutex::new(Vec::new())),
      partitioner: None,
    }
  }
}

impl<Part: Partitioner + Send + Sync> ClientContext for CollectingContext<Part> {
  fn stats(&self, stats: Statistics) {
    let mut stats_vec = self.stats.lock().unwrap();
    (*stats_vec).push(stats);
  }
}

impl<Part: Partitioner + Send + Sync> ProducerContext<Part> for CollectingContext<Part> {
  type DeliveryOpaque = Arc<String>;

  fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
    let mut results = self.results.lock().unwrap();
    match *delivery_result {
      Ok(ref message) => (*results).push((message.detach(), None, delivery_opaque)),
      Err((ref err, ref message)) => {
        (*results).push((message.detach(), Some(err.clone()), delivery_opaque))
      }
    }
  }

  fn get_custom_partitioner(&self) -> Option<&Part> {
    match &self.partitioner {
      None => None,
      Some(p) => Some(p),
    }
  }
}

fn threaded_producer_with_context<Part, C>(
  context: C,
  client_config: ClientConfig,
) -> ThreadedProducer<C, Part>
where
  Part: Partitioner + Send + Sync + 'static,
  C: ProducerContext<Part>,
{
  client_config
    .create_with_context::<C, ThreadedProducer<_, _>>(context)
    .unwrap()
}

#[napi]
pub struct KafkaProducer {
  queue_timeout: Duration,
  client_config: ClientConfig,
}

#[napi]
impl KafkaProducer {
  pub fn new(
    client_config: ClientConfig,
    producer_configuration: ProducerConfiguration,
  ) -> Result<Self> {
    let mut producer_config = client_config;

    if let Some(config) = producer_configuration.configuration {
      producer_config.extend(config);
    }

    let queue_timeout = Duration::from_millis(
      producer_configuration
        .queue_timeout
        .unwrap_or(DEFAULT_QUEUE_TIMEOUT)
        .try_into()
        .map_err(|e| Error::new(Status::GenericFailure, e))?,
    );

    Ok(KafkaProducer {
      queue_timeout,
      client_config: producer_config,
    })
  }

  #[napi]
  pub async fn send(&self, producer_record: ProducerRecord) -> Result<Vec<RecordMetadata>> {
    let topic = producer_record.topic.as_str();
    let context = CollectingContext::new();
    let producer = threaded_producer_with_context(context.clone(), self.client_config.clone());

    let mut ids: Vec<String> = Vec::new();

    for message in producer_record.messages {
      let MessageProducer {
        payload, headers, ..
      } = message;
      let headers = match headers {
        Some(v) => hashmap_to_kafka_headers(&v),
        None => OwnedHeaders::new(),
      };

      let rd_key = if message.key.is_some() {
        message.key.as_ref().unwrap().to_bytes()
      } else {
        &[]
      };

      let record = BaseRecord::with_opaque_to(topic, Arc::new(nanoid!(10)))
        .payload(payload.to_bytes())
        .headers(headers)
        .key(rd_key);

      ids.push(record.delivery_opaque.to_string());

      producer
        .send(record)
        .map_err(|e| Error::new(Status::GenericFailure, e.0))?;
    }

    producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;

    let delivery_results = context.results.lock().unwrap();
    let mut result: Vec<RecordMetadata> = Vec::new();
    for (message, error, id) in &(*delivery_results) {
      if ids.contains(&id.to_string()) {
        let crab_error = error.as_ref().map(|err| KafkaCrabError {
          code: err
            .rdkafka_error_code()
            .unwrap_or(rdkafka::types::RDKafkaErrorCode::Unknown) as i32,
          message: err.to_string(),
        });
        result.push(RecordMetadata {
          topic: topic.to_string(),
          partition: message.partition(),
          offset: message.offset(),
          error: crab_error,
        });
      }
    }
    Ok(result)
  }
}
