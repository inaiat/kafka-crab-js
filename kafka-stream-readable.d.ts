import { Readable } from 'stream';
import { CommitMode } from '../js-binding';
import { KafkaConsumer, OffsetModel, TopicPartitionConfig } from './js-binding';
/**
 * KafkaStreamReadable class
 * @extends Readable
 */
export declare class KafkaStreamReadable extends Readable {
    private readonly kafkaConsumer;
    /**
     * Creates a KafkaStreamReadable instance
     */
    constructor(kafkaConsumer: KafkaConsumer);
    /**
     * Subscribes to topics
     */
    subscribe(topics: string | Array<TopicPartitionConfig>): Promise<void>;
    seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number | undefined): void;
    commit(topic: string, partition: number, offset: number, commit: CommitMode): void;
    /**
     * Unsubscribe from topics
     */
    unsubscribe(): void;
    /**
     * Returns the raw Kafka consumer
     * @returns {KafkaConsumer} The Kafka consumer instance
     */
    rawConsumer(): KafkaConsumer;
    /**
     * Internal method called by the Readable stream to fetch data
     * @private
     */
    _read(): Promise<void>;
}
