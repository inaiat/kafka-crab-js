/**
 * KafkaClient class
 */
export class KafkaClient {
  /**
   * Creates a KafkaStreamReadable instance
   * @param { KafkaConfiguration } config
   */
  constructor(config: KafkaConfiguration)
  kafkaConfiguration: KafkaConfiguration
  kafkaClientConfig: KafkaClientConfig
  /**
   * Creates a KafkaProducer instance
   * @param { ProducerConfiguration } producerConfiguration
   * @returns {KafkaProducer}
   */
  createProducer(producerConfiguration: ProducerConfiguration): KafkaProducer
  /**
   * Creates a KafkaConsumer instance
   * @param {ConsumerConfiguration } consumerConfiguration
   * @returns {KafkaConsumer}
   */
  createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer
  /**
   * Creates a KafkaStreamReadable instance
   * @param { ConsumerConfiguration } consumerConfiguration
   * @returns {KafkaStreamReadable}
   */
  createStreamConsumer(consumerConfiguration: ConsumerConfiguration): KafkaStreamReadable
}
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
import { ProducerConfiguration } from './js-binding'
import { ConsumerConfiguration } from './js-binding'
import { KafkaConfiguration } from './js-binding'
import { KafkaConsumer } from './js-binding'
import { CommitMode } from './js-binding'
import { PartitionPosition } from './js-binding'
import { SecurityProtocol } from './js-binding'
import { KafkaClientConfig } from './js-binding'
import { TopicPartitionConfig } from './js-binding'
import { KafkaProducer } from './js-binding'
export {
  CommitMode,
  ConsumerConfiguration,
  KafkaClientConfig,
  KafkaConfiguration,
  KafkaConsumer,
  KafkaProducer,
  PartitionPosition,
  ProducerConfiguration,
  SecurityProtocol,
  TopicPartitionConfig,
}
