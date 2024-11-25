import { ConsumerConfiguration, KafkaConfiguration, ProducerConfiguration } from './js-binding.js';
import { KafkaStreamReadable } from './kafka-stream-readable';
/**
 * KafkaClient class
 */
export declare class KafkaClient {
    private readonly kafkaConfiguration;
    private readonly kafkaClientConfig;
    /**
     * Creates a KafkaClient instance
     * @throws {Error} If the configuration is invalid
     */
    constructor(kafkaConfiguration: KafkaConfiguration);
    /**
     * Creates a KafkaProducer instance
     * @param {ProducerConfiguration} [producerConfiguration] - Optional producer configuration
     * @returns {KafkaProducer} A KafkaProducer instance
     */
    createProducer(producerConfiguration: ProducerConfiguration): import("./js-binding.js").KafkaProducer;
    /**
     * Creates a KafkaConsumer instance
     * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
     * @returns {KafkaConsumer} A KafkaConsumer instance
     * @throws {Error} If the configuration is invalid
     */
    createConsumer(consumerConfiguration: ConsumerConfiguration): import("./js-binding.js").KafkaConsumer;
    /**
     * Creates a KafkaStreamReadable instance
     * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
     * @returns {KafkaStreamReadable} A KafkaStreamReadable instance
     * @throws {Error} If the configuration is invalid
     */
    createStreamConsumer(consumerConfiguration: ConsumerConfiguration): KafkaStreamReadable;
}
