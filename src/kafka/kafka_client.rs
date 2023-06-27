use std::{collections::HashMap, fmt};

use napi::bindgen_prelude::*;
use rdkafka::{
  client::ClientContext,
  config::{ClientConfig, RDKafkaLogLevel},
  consumer::{ConsumerContext, Rebalance},
  error::KafkaResult,
  topic_partition_list::TopicPartitionList,
};
use tracing::info;

use super::{
  kafka_consumer::KafkaConsumer,
  kafka_producer::KafkaProducer,
  model::{ConsumerConfiguration, ProducerConfiguration},
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
#[napi]
pub struct KafkaConfiguration {
  pub brokers: String,
  pub client_id: String,
  pub security_protocol: Option<SecurityProtocol>,
  pub configuration: Option<HashMap<String, String>>,
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
  pub fn new(
    brokers: String,
    client_id: String,
    security_protocol: Option<SecurityProtocol>,
    configuration: Option<HashMap<String, String>>,
  ) -> Self {
    match tracing_subscriber::fmt::try_init() {
      Ok(_) => {}
      Err(e) => println!("Failed to initialize tracing: {:?}", e),
    }

    KafkaClient::with_kafka_configuration(KafkaConfiguration {
      brokers,
      client_id,
      security_protocol,
      configuration,
    })
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
    } = kafka_configuration.clone();

    let mut rdkafka_client_config = ClientConfig::new();

    rdkafka_client_config.set_log_level(RDKafkaLogLevel::Info);
    rdkafka_client_config.set("bootstrap.servers", brokers);
    rdkafka_client_config.set("client.id", client_id);
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
  pub fn create_producer(&self, producer_configuration: ProducerConfiguration) -> KafkaProducer {
    KafkaProducer::new(self.rdkafka_client_config.clone(), producer_configuration)
  }

  #[napi]
  pub fn create_consumer(&self, consumer_configuration: ConsumerConfiguration) -> KafkaConsumer {
    KafkaConsumer::new(self.clone(), consumer_configuration)
  }
}
