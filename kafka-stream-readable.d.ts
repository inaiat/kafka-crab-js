/**
 * KafkaStreamReadable class
 * @extends Readable
 */
export class KafkaStreamReadable extends Readable {
  /**
   * Creates a KafkaStreamReadable instance
   * @param { KafkaConsumer } kafkaConsumer
   */
  constructor(kafkaConsumer: KafkaConsumer)
  kafkaConsumer: KafkaConsumer
  /**
   * Subscribes to topics
   * @param {string | Array<TopicPartitionConfig>} topics
   * @returns
   */
  subscribe(topics: string | Array<TopicPartitionConfig>): Promise<void>
  /**
   * Unsubscribe from topics
   */
  unsubscribe(): void
  /**
   * The internal method called by the Readable stream to fetch data
   */
  _read(): Promise<void>
}
import { Readable } from 'stream'
import { KafkaConsumer } from './js-binding'
import { TopicPartitionConfig } from './js-binding'
