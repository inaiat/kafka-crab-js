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

  pause() {
    this.kafkaConsumer.pause()
  }

  resume() {
    this.kafkaConsumer.resume()
  }

  /**
   * The internal method called by the Readable stream to fetch data
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
      this.emit('error', error)
    }
  }
}

module.exports = {
  KafkaStreamReadable,
}
