const { Readable } = require('stream')

const {
  KafkaConsumer,
  TopicPartitionConfig,
} = require('./js-binding')

/**
 * KafkaStreamReadable class
 * @extends Readable
 */
class KafkaStreamReadable extends Readable {
  /**
   * Creates a KafkaStreamReadable instance
   * @param { KafkaConsumer } kafkaConsumer - The Kafka consumer instance
   */
  constructor(kafkaConsumer) {
    super({ objectMode: true })
    if (!kafkaConsumer) {
      throw new Error('A valid KafkaConsumer instance is required.')
    }
    this.kafkaConsumer = kafkaConsumer
  }

  /**
   * Subscribes to topics
   * @param {string | Array<TopicPartitionConfig>} topics  - Topics to subscribe to
   * @returns {Promise<void>} - A promise that resolves when the subscription is successful
   */
  async subscribe(topics) {
    if (!topics || (Array.isArray(topics) && topics.length === 0)) {
      throw new Error('Topics must be a non-empty string or array.')
    }
    await this.kafkaConsumer.subscribe(topics)
  }

  /**
   * Unsubscribe from topics
   * @returns {void}
   */
  unsubscribe() {
    this.kafkaConsumer.unsubscribe()
  }

  /**
   * Returns the raw Kafka consumer
   * @returns {KafkaConsumer} The Kafka consumer instance
   */
  rawConsumer() {
    return this.kafkaConsumer
  }

  /**
   * Internal method called by the Readable stream to fetch data
   * @private
   */
  async _read() {
    try {
      const message = await this.kafkaConsumer.recv() // Call the napi-rs method
      if (message) {
        this.push(message) // Push message into the stream
      } else {
        this.push(null) // No more data, end of stream
      }
    } catch (error) {
      this.destroy(error)
    }
  }
}

module.exports = {
  KafkaStreamReadable,
}
