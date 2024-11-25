"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaClient = void 0;
const js_binding_js_1 = require("./js-binding.js");
const kafka_stream_readable_1 = require("./kafka-stream-readable");
/**
 * KafkaClient class
 */
class KafkaClient {
    /**
     * Creates a KafkaClient instance
     * @throws {Error} If the configuration is invalid
     */
    constructor(kafkaConfiguration) {
        this.kafkaConfiguration = kafkaConfiguration;
        this.kafkaClientConfig = new js_binding_js_1.KafkaClientConfig(kafkaConfiguration);
    }
    /**
     * Creates a KafkaProducer instance
     * @param {ProducerConfiguration} [producerConfiguration] - Optional producer configuration
     * @returns {KafkaProducer} A KafkaProducer instance
     */
    createProducer(producerConfiguration) {
        if (producerConfiguration) {
            return this.kafkaClientConfig.createProducer(producerConfiguration);
        }
        return this.kafkaClientConfig.createProducer({});
    }
    /**
     * Creates a KafkaConsumer instance
     * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
     * @returns {KafkaConsumer} A KafkaConsumer instance
     * @throws {Error} If the configuration is invalid
     */
    createConsumer(consumerConfiguration) {
        return this.kafkaClientConfig.createConsumer(consumerConfiguration);
    }
    /**
     * Creates a KafkaStreamReadable instance
     * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
     * @returns {KafkaStreamReadable} A KafkaStreamReadable instance
     * @throws {Error} If the configuration is invalid
     */
    createStreamConsumer(consumerConfiguration) {
        return new kafka_stream_readable_1.KafkaStreamReadable(this.kafkaClientConfig.createConsumer(consumerConfiguration));
    }
}
exports.KafkaClient = KafkaClient;
