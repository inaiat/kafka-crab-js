use std::fmt;

use log::info;
use napi::bindgen_prelude::*;
use rdkafka::{
  client::ClientContext,
  config::{ClientConfig, RDKafkaLogLevel},
  consumer::{ConsumerContext, Rebalance},
  error::KafkaResult,
  topic_partition_list::TopicPartitionList,
};

use super::{kafka_consumer::KafkaConsumer, kafka_producer::KafkaProducer};

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
  pub allow_auto_create_topic: Option<bool>,
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
  pub fn new(brokers: String, client_id: String) -> Self {
    env_logger::init();
    KafkaClient::with_kafka_configuration(KafkaConfiguration {
      brokers,
      client_id,
      security_protocol: None,
      allow_auto_create_topic: None,
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
      allow_auto_create_topic,
    } = kafka_configuration.clone();

    let mut rdkafka_client_config = ClientConfig::new();

    rdkafka_client_config.set_log_level(RDKafkaLogLevel::Info);
    rdkafka_client_config.set("bootstrap.servers", brokers);
    rdkafka_client_config.set("client.id", client_id);
    if let Some(security_protocol) = security_protocol {
      rdkafka_client_config.set("security.protocol", security_protocol.to_string());
    }
    if let Some(allow_auto_create_topic) = allow_auto_create_topic {
      rdkafka_client_config.set(
        "allow.auto.create.topics",
        allow_auto_create_topic.to_string(),
      );
    }

    KafkaClient {
      rdkafka_client_config,
      kafka_configuration,
    }
  }

  #[napi]
  pub fn create_producer(&self) -> KafkaProducer {
    KafkaProducer::new(self.rdkafka_client_config.clone())
  }

  #[napi]
  pub fn create_consumer(&self, group_id: String) -> KafkaConsumer {
    KafkaConsumer::new(self.clone(), group_id, None, None)
  }
}
