const { Readable } = require('stream')

const {
  KafkaConsumer,
  CommitMode,
  PartitionPosition,
  SecurityProtocol,
  KafkaClient,
  KafkaProducer,
} = require('./index.js')

class KafkaStreamReadable extends Readable {
  /**
   * Creates a KafkaStreamReadable instance
   * @param { KafkaConsumer } kafkaStreamConsumer
   */
  constructor(kafkaStreamConsumer) {
    super({ objectMode: true })
    this.kafkaStreamConsumer = kafkaStreamConsumer
  }

  /**
   * The internal method called by the Readable stream to fetch data
   */
  async _read() {
    const message = await this.kafkaStreamConsumer.recv() // Call the napi-rs method
    if (message) {
      this.push(message) // Push message into the stream
    } else {
      this.push(null) // No more data, end of stream
    }
  }
}

module.exports = {
  KafkaStreamReadable,
  KafkaConsumer,
  CommitMode,
  PartitionPosition,
  SecurityProtocol,
  KafkaClient,
  KafkaProducer,
}
