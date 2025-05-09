export declare class KafkaClientConfig {
  constructor(kafkaConfiguration: KafkaConfiguration)
  get configuration(): KafkaConfiguration
  createProducer(producerConfiguration: ProducerConfiguration): KafkaProducer
  createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer
}

export declare class KafkaConsumer {
  onEvents(callback: (error: Error | undefined, event: KafkaEvent) => void): void
  subscribe(topicConfigs: string | Array<TopicPartitionConfig>): Promise<void>
  pause(): void
  resume(): void
  unsubscribe(): void
  disconnect(): Promise<void>
  seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number | undefined | null): void
  assignment(): Array<TopicPartition>
  recv(): Promise<Message | null>
  commit(topic: string, partition: number, offset: number, commit: CommitMode): void
}

export declare class KafkaProducer {
  inFlightCount(): number
  flush(): Promise<Array<RecordMetadata>>
  send(producerRecord: ProducerRecord): Promise<Array<RecordMetadata>>
}

export type CommitMode =  'Sync'|
'Async';

export interface ConsumerConfiguration {
  groupId: string
  createTopic?: boolean
  enableAutoCommit?: boolean
  configuration?: Record<string, string>
  fetchMetadataTimeout?: number
}

export interface KafkaConfiguration {
  brokers: string
  clientId: string
  securityProtocol?: SecurityProtocol
  configuration?: Record<string, string>
  logLevel?: string
  brokerAddressFamily?: string
}

export interface KafkaCrabError {
  code: number
  message: string
}

export interface KafkaEvent {
  name: KafkaEventName
  payload: KafkaEventPayload
}

export type KafkaEventName =  'PreRebalance'|
'PostRebalance'|
'CommitCallback';

export interface KafkaEventPayload {
  action?: string
  tpl: Array<TopicPartition>
  error?: string
}

export interface Message {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  topic: string
  partition: number
  offset: number
}

export interface MessageProducer {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
}

export interface OffsetModel {
  offset?: number
  position?: PartitionPosition
}

export interface PartitionOffset {
  partition: number
  offset: OffsetModel
}

export type PartitionPosition =  'Beginning'|
'End'|
'Stored'|
'Invalid';

export interface ProducerConfiguration {
  queueTimeout?: number
  thrownOnError?: boolean
  autoFlush?: boolean
  configuration?: Record<string, string>
}

export interface ProducerRecord {
  topic: string
  messages: Array<MessageProducer>
}

export interface RecordMetadata {
  topic: string
  partition: number
  offset: number
  error?: KafkaCrabError
}

export interface RetryStrategy {
  retries: number
  retryTopic?: string
  dqlTopic?: string
  pauseConsumerDuration?: number
  offset?: OffsetModel
  configuration?: Record<string, string>
}

export type SecurityProtocol =  'Plaintext'|
'Ssl'|
'SaslPlaintext'|
'SaslSsl';

export interface TopicPartition {
  topic: string
  partitionOffset: Array<PartitionOffset>
}

export interface TopicPartitionConfig {
  topic: string
  allOffsets?: OffsetModel
  partitionOffset?: Array<PartitionOffset>
}
