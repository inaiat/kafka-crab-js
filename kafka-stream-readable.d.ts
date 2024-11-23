/**
 * KafkaStreamReadable class
 * @extends Readable
 */
export class KafkaStreamReadable extends Readable {
    /**
     * Creates a KafkaStreamReadable instance
     * @param { KafkaConsumer } kafkaConsumer - The Kafka consumer instance
     */
    constructor(kafkaConsumer: KafkaConsumer);
    kafkaConsumer: KafkaConsumer;
    /**
     * Subscribes to topics
     * @param {string | Array<TopicPartitionConfig>} topics  - Topics to subscribe to
     * @returns {Promise<void>} - A promise that resolves when the subscription is successful
     */
    subscribe(topics: string | Array<TopicPartitionConfig>): Promise<void>;
    /**
     * Unsubscribe from topics
     * @returns {void}
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
    private _read;
}
import { Readable } from "stream";
import { KafkaConsumer } from "./js-binding";
import { TopicPartitionConfig } from "./js-binding";
