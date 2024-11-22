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
   * @param { KafkaConfiguration } config
   */
  constructor(config) {
    this.kafkaConfiguration = config
    this.kafkaClientConfig = new KafkaClientConfig(config)
  }

  /**
   * Creates a KafkaProducer instance
   * @param { ProducerConfiguration | undefined } producerConfiguration
   * @returns {KafkaProducer}
   */
  createProducer(producerConfiguration) {
    if (producerConfiguration) {
      return this.kafkaClientConfig.createProducer(producerConfiguration)
    }
    return this.kafkaClientConfig.createProducer({})
  }

  /**
   * Creates a KafkaConsumer instance
   * @param {ConsumerConfiguration } consumerConfiguration
   * @returns {KafkaConsumer}
   */
  createConsumer(consumerConfiguration) {
    return this.kafkaClientConfig.createConsumer(consumerConfiguration)
  }

  /**
   * Creates a KafkaStreamReadable instance
   * @param { ConsumerConfiguration } consumerConfiguration
   * @returns {KafkaStreamReadable}
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
