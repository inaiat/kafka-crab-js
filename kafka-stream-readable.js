"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaStreamReadable = void 0;
const stream_1 = require("stream");
/**
 * KafkaStreamReadable class
 * @extends Readable
 */
class KafkaStreamReadable extends stream_1.Readable {
    /**
     * Creates a KafkaStreamReadable instance
     */
    constructor(kafkaConsumer) {
        super({ objectMode: true });
        this.kafkaConsumer = kafkaConsumer;
        if (!kafkaConsumer) {
            throw new Error('A valid KafkaConsumer instance is required.');
        }
        this.kafkaConsumer = kafkaConsumer;
    }
    /**
     * Subscribes to topics
     */
    async subscribe(topics) {
        if (!topics || (Array.isArray(topics) && topics.length === 0)) {
            throw new Error('Topics must be a non-empty string or array.');
        }
        await this.kafkaConsumer.subscribe(topics);
    }
    seek(topic, partition, offsetModel, timeout) {
        this.kafkaConsumer.seek(topic, partition, offsetModel, timeout);
    }
    commit(topic, partition, offset, commit) {
        this.kafkaConsumer.commit(topic, partition, offset, commit);
    }
    /**
     * Unsubscribe from topics
     */
    unsubscribe() {
        this.kafkaConsumer.unsubscribe();
    }
    /**
     * Returns the raw Kafka consumer
     * @returns {KafkaConsumer} The Kafka consumer instance
     */
    rawConsumer() {
        return this.kafkaConsumer;
    }
    /**
     * Internal method called by the Readable stream to fetch data
     * @private
     */
    async _read() {
        try {
            const message = await this.kafkaConsumer.recv(); // Call the napi-rs method
            if (message) {
                this.push(message); // Push message into the stream
            }
            else {
                this.push(null); // No more data, end of stream
            }
        }
        catch (error) {
            if (error instanceof Error) {
                this.destroy(error);
            }
            else {
                this.destroy();
            }
        }
    }
}
exports.KafkaStreamReadable = KafkaStreamReadable;
