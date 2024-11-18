use std::{
  collections::HashSet,
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
use tracing::debug;

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
  auto_flush: bool,
  context: CollectingContext,
  producer: ThreadedProducer<CollectingContext>,
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

    let auto_flush = producer_configuration.auto_flush.unwrap_or(true);

    let context = CollectingContext::new();
    let producer: ThreadedProducer<CollectingContext> =
      threaded_producer_with_context(context.clone(), producer_config);

    Ok(KafkaProducer {
      queue_timeout,
      auto_flush,
      context,
      producer,
    })
  }

  #[napi]
  pub fn in_flight_count(&self) -> Result<i32> {
    let count = self.producer.in_flight_count();
    Ok(count)
  }

  #[napi]
  pub async fn flush(&self) -> Result<()> {
    self
      .producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;
    Ok(())
  }

  #[napi]
  pub async fn send(&self, producer_record: ProducerRecord) -> Result<Vec<RecordMetadata>> {
    let topic = producer_record.topic.as_str();

    let ids: HashSet<String> = producer_record
      .messages
      .iter()
      .map(|_| nanoid!(14))
      .collect();

    for (message, record_id) in producer_record.messages.into_iter().zip(ids.iter()) {
      let MessageProducer {
        payload,
        headers,
        key,
      } = message;
      let headers = headers.map_or_else(OwnedHeaders::new, |v| hashmap_to_kafka_headers(&v));

      let rd_key: &[u8] = match &key {
        Some(k) => k.to_bytes(),
        None => &[],
      };

      let record = BaseRecord::with_opaque_to(topic, Arc::new(record_id.clone()))
        .payload(payload.to_bytes())
        .headers(headers)
        .key(rd_key);

      self
        .producer
        .send(record)
        .map_err(|e| Error::new(Status::GenericFailure, e.0))?;
    }

    if self.auto_flush {
      debug!("Auto flushing");
      self
        .producer
        .flush(self.queue_timeout)
        .map_err(|e| Error::new(Status::GenericFailure, e))?;
    }

    let delivery_results = self.context.results.lock().unwrap();

    let result: Vec<RecordMetadata> = delivery_results
      .iter()
      .filter_map(|(message, error, id)| {
        ids.contains(&id.to_string()).then(|| RecordMetadata {
          topic: topic.to_string(),
          partition: message.partition(),
          offset: message.offset(),
          error: error.as_ref().map(|err| KafkaCrabError {
            code: err
              .rdkafka_error_code()
              .unwrap_or(rdkafka::types::RDKafkaErrorCode::Unknown) as i32,
            message: err.to_string(),
          }),
        })
      })
      .collect();

    Ok(result)
  }
}
