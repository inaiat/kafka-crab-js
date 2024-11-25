import { Readable } from 'stream';
import { KafkaConsumer, TopicPartitionConfig } from './js-binding';
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
