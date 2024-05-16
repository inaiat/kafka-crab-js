use std::{collections::HashMap, fmt, str::FromStr};

use napi::{bindgen_prelude::*, Result};
use rdkafka::{
  client::ClientContext,
  config::{ClientConfig, RDKafkaLogLevel},
  consumer::{ConsumerContext, Rebalance},
  error::KafkaResult,
  topic_partition_list::TopicPartitionList,
};

use tracing::{info, Level};

use super::{
  consumer::{
    consumer_model::ConsumerConfiguration,
    kafka_consumer::{KafkaConsumer, KafkaConsumerConfiguration},
    kafka_stream_consumer::KafkaStreamConsumer,
  },
  kafka_producer::{KafkaProducer, ProducerConfiguration},
};

struct CustomContext;

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

#[derive(Debug)]
#[napi(string_enum)]
pub enum SecurityProtocol {
  Plaintext,
  Ssl,
  SaslPlaintext,
  SaslSsl,
}

impl fmt::Display for SecurityProtocol {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      SecurityProtocol::Plaintext => write!(f, "plaintext"),
      SecurityProtocol::Ssl => write!(f, "ssl"),
      SecurityProtocol::SaslPlaintext => write!(f, "sasl_plaintext"),
      SecurityProtocol::SaslSsl => write!(f, "sasl_ssl"),
    }
  }
}

#[derive(Clone, Debug)]
#[napi(object)]
pub struct KafkaConfiguration {
  pub brokers: String,
  pub client_id: String,
  pub security_protocol: Option<SecurityProtocol>,
  pub configuration: Option<HashMap<String, String>>,
  pub log_level: Option<String>,
  pub broker_address_family: Option<String>,
}

#[derive(Clone, Debug)]
#[napi]
pub struct KafkaClient {
  rdkafka_client_config: ClientConfig,
  #[napi(readonly)]
  pub kafka_configuration: KafkaConfiguration,
}

#[napi]
impl KafkaClient {
  #[napi(constructor)]
  pub fn new(kafka_configuration: KafkaConfiguration) -> Self {
    let log_level = kafka_configuration.clone().log_level;
    match tracing_subscriber::fmt()
      .with_max_level(
        Level::from_str(log_level.unwrap_or("error".to_owned()).as_str()).unwrap_or(Level::ERROR),
      )
      .json()
      .with_ansi(false)
      .try_init()
    {
      Ok(_) => {}
      Err(e) => {
        eprintln!("Failed to initialize tracing: {:?}", e);
      }
    };
    KafkaClient::with_kafka_configuration(kafka_configuration)
  }

  pub fn get_client_config(&self) -> &ClientConfig {
    &self.rdkafka_client_config
  }

  pub fn with_kafka_configuration(kafka_configuration: KafkaConfiguration) -> Self {
    let KafkaConfiguration {
      brokers,
      security_protocol,
      client_id,
      configuration,
      broker_address_family,
      ..
    } = kafka_configuration.clone();

    let mut rdkafka_client_config = ClientConfig::new();

    rdkafka_client_config.set_log_level(RDKafkaLogLevel::Debug);
    rdkafka_client_config.set("bootstrap.servers", brokers);
    rdkafka_client_config.set("client.id", client_id);
    rdkafka_client_config.set(
      "broker.address.family",
      broker_address_family.unwrap_or("v4".to_string()),
    );
    if let Some(security_protocol) = security_protocol {
      rdkafka_client_config.set("security.protocol", security_protocol.to_string());
    }
    if let Some(config) = configuration {
      for (key, value) in config {
        rdkafka_client_config.set(key, value);
      }
    }

    KafkaClient {
      rdkafka_client_config,
      kafka_configuration,
    }
  }

  #[napi]
  pub fn create_producer(
    &self,
    producer_configuration: ProducerConfiguration,
  ) -> Result<KafkaProducer> {
    match KafkaProducer::new(self.rdkafka_client_config.clone(), producer_configuration) {
      Ok(producer) => Ok(producer),
      Err(e) => Err(Error::new(Status::GenericFailure, e.to_string())),
    }
  }

  #[napi]
  #[deprecated(note = "Use create_stream_consumer instead")]
  pub fn create_consumer(
    &self,
    consumer_configuration: KafkaConsumerConfiguration,
  ) -> Result<KafkaConsumer> {
    KafkaConsumer::new(self.clone(), consumer_configuration)
  }

  #[napi]
  pub fn create_stream_consumer(
    &self,
    consumer_configuration: ConsumerConfiguration,
  ) -> Result<KafkaStreamConsumer> {
    KafkaStreamConsumer::new(self.clone(), &consumer_configuration).map_err(|e| {
      Error::new(
        Status::GenericFailure,
        format!("Failed to create stream consumer: {:?}", e),
      )
    })
  }
}
