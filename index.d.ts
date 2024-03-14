/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export const enum CommitMode {
  AutoCommit = 'AutoCommit',
  Sync = 'Sync',
  Async = 'Async'
}
export interface RetryStrategy {
  retries: number
  retryTopic?: string
  dqlTopic?: string
  pauseConsumerDuration?: number
  offset?: OffsetModel
  configuration?: Record<string, string>
}
export const enum ConsumerResult {
  Ok = 'Ok',
  Retry = 'Retry'
}
export interface ConsumerConfiguration {
  topic: string
  groupId: string
  retryStrategy?: RetryStrategy
  offset?: OffsetModel
  createTopic?: boolean
  commitMode?: CommitMode
  enableAutoCommit?: boolean
  configuration?: Record<string, string>
}
export const enum SecurityProtocol {
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
}
export interface ProducerConfiguration {
  queueTimeout?: number
  thrownOnError?: boolean
  configuration?: Record<string, string>
}
export interface ProducerRecord {
  topic: string
  messages: Array<MessageModel>
}
export const enum AutoOffsetReset {
  Smallest = 'Smallest',
  Earliest = 'Earliest',
  Beginning = 'Beginning',
  Largest = 'Largest',
  Latest = 'Latest',
  End = 'End',
  Error = 'Error'
}
export interface Payload {
  value: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  topic: string
  partition: number
  offset: number
}
export const enum PartitionPosition {
  Beginning = 'Beginning',
  End = 'End',
  Stored = 'Stored'
}
export interface OffsetModel {
  offset?: number
  position?: PartitionPosition
}
export interface KafkaCrabError {
  code: number
  message: string
}
export interface RecordMetadata {
  topic: string
  partition: number
  offset: number
  error?: KafkaCrabError
}
export interface MessageModel {
  value: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
}
export class KafkaConsumer {
  startConsumer(callback: (error: Error | undefined, result: Payload) => Promise<ConsumerResult | undefined>): Promise<void>
}
export class KafkaClient {
  readonly kafkaConfiguration: KafkaConfiguration
  constructor(kafkaConfiguration: KafkaConfiguration)
  createProducer(producerConfiguration: ProducerConfiguration): KafkaProducer
  createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer
}
export class KafkaProducer {
  send(producerRecord: ProducerRecord): Promise<Array<RecordMetadata>>
}
