/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export interface RetryStrategy {
  retries: number
  retryTopic?: string
  dqlTopic?: string
  pauseConsumerDuration?: number
  offset?: OffsetModel
  configuration?: Record<string, string>
}
export enum CommitMode {
  Sync = 0,
  Async = 1
}
export interface ConsumerConfiguration {
  groupId: string
  createTopic?: boolean
  enableAutoCommit?: boolean
  configuration?: Record<string, string>
  fecthMetadataTimeout?: number
}
export enum PartitionPosition {
  Beginning = 'Beginning',
  End = 'End',
  Stored = 'Stored'
}
export interface OffsetModel {
  offset?: number
  position?: PartitionPosition
}
export interface PartitionOffset {
  partition: number
  offset: OffsetModel
}
export interface TopicPartitionConfig {
  topic: string
  allOffsets?: OffsetModel
  partitionOffset?: Array<PartitionOffset>
}
export enum SecurityProtocol {
  Plaintext = 'Plaintext',
  Ssl = 'Ssl',
  SaslPlaintext = 'SaslPlaintext',
  SaslSsl = 'SaslSsl'
}
export interface KafkaConfiguration {
  brokers: string
  clientId: string
  securityProtocol?: SecurityProtocol
  configuration?: Record<string, string>
  logLevel?: string
  brokerAddressFamily?: string
}
export interface Message {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  topic: string
  partition: number
  offset: number
}
export interface RecordMetadata {
  topic: string
  partition: number
  offset: number
  error?: KafkaCrabError
}
export interface MessageProducer {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
}
export interface ProducerRecord {
  topic: string
  messages: Array<MessageProducer>
}
export interface KafkaCrabError {
  code: number
  message: string
}
export interface ProducerConfiguration {
  queueTimeout?: number
  thrownOnError?: boolean
  autoFlush?: boolean
  configuration?: Record<string, string>
}
export declare class KafkaConsumer {
  subscribe(topicConfigs: string | Array<TopicPartitionConfig>): Promise<void>
  pause(): void
  resume(): void
  unsubscribe(): void
  seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number | undefined | null): void
  recv(): Promise<Message>
  commit(topic: string, partition: number, offset: number, commit: CommitMode): void
}
export declare class KafkaClientConfig {
  readonly kafkaConfiguration: KafkaConfiguration
  constructor(kafkaConfiguration: KafkaConfiguration)
  createProducer(producerConfiguration: ProducerConfiguration): KafkaProducer
  createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer
}
export declare class KafkaProducer {
  inFlightCount(): number
  flush(): Promise<Array<RecordMetadata>>
  send(producerRecord: ProducerRecord): Promise<Array<RecordMetadata>>
}
