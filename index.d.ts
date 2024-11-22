/**
 * KafkaClient class
 */
export class KafkaClient {
    /**
     * Creates a KafkaClient instance
     * @param { KafkaConfiguration } config
     */
    constructor(config: KafkaConfiguration);
    kafkaConfiguration: KafkaConfiguration;
    kafkaClientConfig: KafkaClientConfig;
    /**
     * Creates a KafkaProducer instance
     * @param { ProducerConfiguration | undefined } producerConfiguration
     * @returns {KafkaProducer}
     */
    createProducer(producerConfiguration: ProducerConfiguration | undefined): KafkaProducer;
    /**
     * Creates a KafkaConsumer instance
     * @param {ConsumerConfiguration } consumerConfiguration
     * @returns {KafkaConsumer}
     */
    createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer;
    /**
     * Creates a KafkaStreamReadable instance
     * @param { ConsumerConfiguration } consumerConfiguration
     * @returns {KafkaStreamReadable}
     */
    createStreamConsumer(consumerConfiguration: ConsumerConfiguration): KafkaStreamReadable;
}
import { KafkaStreamReadable } from "./kafka-stream-readable";
import { ProducerConfiguration } from "./js-binding";
import { ConsumerConfiguration } from "./js-binding";
import { KafkaConfiguration } from "./js-binding";
import { KafkaConsumer } from "./js-binding";
import { CommitMode } from "./js-binding";
import { PartitionPosition } from "./js-binding";
import { SecurityProtocol } from "./js-binding";
import { KafkaClientConfig } from "./js-binding";
import { TopicPartitionConfig } from "./js-binding";
import { KafkaProducer } from "./js-binding";
export { KafkaStreamReadable, ProducerConfiguration, ConsumerConfiguration, KafkaConfiguration, KafkaConsumer, CommitMode, PartitionPosition, SecurityProtocol, KafkaClientConfig, TopicPartitionConfig, KafkaProducer };
