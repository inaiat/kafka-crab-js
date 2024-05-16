/* eslint-disable no-await-in-loop */

// import timersPromises from 'node:timers/promises';
import { Buffer } from 'node:buffer';
import { nanoid } from 'nanoid';
import {
  KafkaClient, CommitMode, PartitionPosition, ConsumerResult,
} from '../index.js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-js-group',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});
const topic = `topic-${nanoid()}`;

const consumer = kafkaClient.createConsumer({
  topic,
  groupId: 'my-js-group',
  commitMode: CommitMode.AutoCommit,
  offset: { position: PartitionPosition.Stored },
  configuration: { 'auto.offset.reset': 'earliest' },
  retryStrategy: {
    retries: 5,
    pauseConsumerDuration: 5000,
  },
});
const producer = kafkaClient.createProducer({ topic, configuration: { 'message.timeout.ms': '5000' } });

async function produce() {
  for (let i = 0; i < 10; i++) {
    try {
      const result = await producer.send(
        {
          topic,
          messages: [{ payload: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond Sample js","phone":"555"}`) }],
        },
      );
      console.log('Js message sent. Offset:', result);
    } catch (error) {
      console.error('Js Error on send', error);
    }
  }
}

async function startConsumer() {
  consumer.startConsumer(async (error, { value, partition, offset }) => {
    if (error) {
      console.error('Js Consumer error', error);
      return;
    }

    const message = JSON.parse(value.toString());

    console.log('Message received! Partition:', partition, 'Offset:', offset, 'Message =>', message);

    if (message._id === '5') {
      return ConsumerResult.Retry;
    }

    return ConsumerResult.Ok;
  });
}

console.time('consumer');

await startConsumer();

await produce();
