/* eslint-disable no-await-in-loop */

import timersPromises from 'node:timers/promises';
import {Buffer} from 'node:buffer';
import {KafkaClient, CommitMode, PartitionPosition, ConsumerResult} from '../index.js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-ids',
  groupId: 'my-js-group',
  enableAnsiLogger: true});
const topic = 'my-js-topic';

const consumer = kafkaClient.createConsumer({
  topic,
  groupId: 'my-js-group',
  // CommitMode: CommitMode.Async,
  offset: {position: PartitionPosition.Stored},
  configuration: {'enable.auto.commit': 'false', 'auto.offset.reset': 'earliest'},
  retryStrategy: {
    retries: 3,
  },
});
const producer = kafkaClient.createProducer({topic, configuration: {'message.timeout.ms': '5000'}});

async function produce() {
  for (let i = 0; i < 10; i++) {
    try {
      const result = await producer.send(
        {
          topic,
          messages: [{value: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond Sample js","phone":"555"}`)}],
        },
      );
      console.log('Js message sent. Offset:', result);
    } catch (error) {
      console.error('Js Error on send', error);
    }
  }
}

async function startConsumer() {
  consumer.startConsumer(async (error, {value, partition, offset}) => {
    if (error) {
      console.error('Js Consumer error', error);
      return;
    }

    const message = JSON.parse(value.toString());

    console.log('Message received! Partition:', partition, 'Offset:', offset, 'Message =>', message);

    // Const content = await process(value);

    if (message._id === '5') {
      return ConsumerResult.Retry;
    }

    // Counter++;
    // if (counter >= 0) {
    //   console.timeEnd('consumer');
    //   console.log('Js Counter:', counter, 'Content:', JSON.stringify(content));
    //   console.time('consumer');
    // }

    return ConsumerResult.Ok;
  });
}

await produce();

// Await timersPromises.setTimeout(500);

console.time('consumer');

await startConsumer();
// Await timersPromises.setTimeout(5000);

