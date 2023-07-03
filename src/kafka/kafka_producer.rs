use std::{
  collections::HashMap,
  sync::{Arc, Mutex},
  time::Duration,
};

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

use super::{
  kafka_util::hashmap_to_kafka_headers,
  model::{KafkaCrabError, MessageModel, RecordMetadata},
};

const DEFAULT_QUEUE_TIMEOUT: i64 = 5000;

type ProducerDeliveryResult = (OwnedMessage, Option<KafkaError>);

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
  type DeliveryOpaque = ();

  fn delivery(&self, delivery_result: &DeliveryResult, _: Self::DeliveryOpaque) {
    let mut results = self.results.lock().unwrap();
    match *delivery_result {
      Ok(ref message) => (*results).push((message.detach(), None)),
      Err((ref err, ref message)) => (*results).push((message.detach(), Some(err.clone()))),
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

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ProducerConfiguration {
  pub queue_timeout: Option<i64>,
  pub thrown_on_error: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
}

#[napi(object)]
#[derive(Clone)]
pub struct ProducerRecord {
  pub topic: String,
  pub messages: Vec<MessageModel>,
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

    for message in producer_record.messages {
      let MessageModel { value, headers, .. } = message;
      let headers = match headers {
        Some(v) => hashmap_to_kafka_headers(&v),
        None => OwnedHeaders::new(),
      };

      let rd_key = if message.key.is_some() {
        message.key.as_ref().unwrap().to_bytes()
      } else {
        &[]
      };
      producer
        .send(
          BaseRecord::to(topic)
            .payload(value.to_bytes())
            .headers(headers)
            .key(rd_key),
        )
        .map_err(|e| Error::new(Status::GenericFailure, e.0))?;
    }

    producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;

    let delivery_results = context.results.lock().unwrap();
    let mut result: Vec<RecordMetadata> = Vec::new();
    for (message, error) in &(*delivery_results) {
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
    Ok(result)
  }
}
