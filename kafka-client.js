const { Readable } = require('stream')

const {
  ProducerConfiguration,
  ConsumerConfiguration,
  KafkaConfiguration,
  KafkaConsumer,
  CommitMode,
  PartitionPosition,
  SecurityProtocol,
  KafkaClientConfig,
  TopicPartitionConfig,
  KafkaProducer,
} = require('./js-binding')

const { KafkaStreamReadable } = require('./kafka-stream-readable')

/**
 * KafkaClient class
 */
class KafkaClient {
  /**
   * Creates a KafkaClient instance
   * @param {KafkaConfiguration} config - The Kafka configuration object
   * @throws {Error} If the configuration is invalid
   */
  constructor(config) {
    this.kafkaConfiguration = config
    this.kafkaClientConfig = new KafkaClientConfig(config)
  }

  /**
   * Creates a KafkaProducer instance
   * @param {ProducerConfiguration} [producerConfiguration] - Optional producer configuration
   * @returns {KafkaProducer} A KafkaProducer instance
   */
  createProducer(producerConfiguration) {
    if (producerConfiguration) {
      return this.kafkaClientConfig.createProducer(producerConfiguration)
    }
    return this.kafkaClientConfig.createProducer({})
  }

  /**
   * Creates a KafkaConsumer instance
   * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
   * @returns {KafkaConsumer} A KafkaConsumer instance
   * @throws {Error} If the configuration is invalid
   */
  createConsumer(consumerConfiguration) {
    return this.kafkaClientConfig.createConsumer(consumerConfiguration)
  }

  /**
   * Creates a KafkaStreamReadable instance
   * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
   * @returns {KafkaStreamReadable} A KafkaStreamReadable instance
   * @throws {Error} If the configuration is invalid
   */
  createStreamConsumer(consumerConfiguration) {
    return new KafkaStreamReadable(this.kafkaClientConfig.createConsumer(consumerConfiguration))
  }
}

module.exports = {
  KafkaClient,
  KafkaStreamReadable,
  ProducerConfiguration,
  ConsumerConfiguration,
  KafkaConfiguration,
  KafkaConsumer,
  CommitMode,
  PartitionPosition,
  SecurityProtocol,
  KafkaClientConfig,
  TopicPartitionConfig,
  KafkaProducer,
}
