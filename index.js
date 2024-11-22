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

/**
 * KafkaStreamReadable class
 * @extends Readable
 */
class KafkaStreamReadable extends Readable {
  /**
   * Creates a KafkaStreamReadable instance
   * @param { KafkaConsumer } kafkaConsumer
   */
  constructor(kafkaConsumer) {
    super({ objectMode: true })
    this.kafkaConsumer = kafkaConsumer
  }

  /**
   * Subscribes to topics
   * @param {string | Array<TopicPartitionConfig>} topics
   * @returns
   */
  async subscribe(topics) {
    return this.kafkaConsumer.subscribe(topics)
  }

  /**
   * Unsubscribe from topics
   */
  unsubscribe() {
    this.kafkaConsumer.unsubscribe()
  }

  /**
   * The internal method called by the Readable stream to fetch data
   */
  async _read() {
    const message = await this.kafkaConsumer.recv() // Call the napi-rs method
    if (message) {
      this.push(message) // Push message into the stream
    } else {
      this.push(null) // No more data, end of stream
    }
  }
}

/**
 * KafkaClient class
 */
class KafkaClient {
  /**
   * Creates a KafkaStreamReadable instance
   * @param { KafkaConfiguration } config
   */
  constructor(config) {
    this.kafkaConfiguration = config
    this.kafkaClientConfig = new KafkaClientConfig(config)
  }

  /**
   * Creates a KafkaProducer instance
   * @param { ProducerConfiguration } producerConfiguration
   * @returns {KafkaProducer}
   */
  createProducer(producerConfiguration) {
    return this.kafkaClientConfig.createProducer(producerConfiguration)
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
