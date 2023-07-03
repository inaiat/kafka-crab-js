/* eslint-disable no-await-in-loop */

import timersPromises from 'node:timers/promises';
import {Buffer} from 'node:buffer';
import {KafkaClient, CommitMode, PartitionPosition, ConsumerResult} from '../index.js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-id',
  enableAnsiLogger: true});
const topic = 'my-js-topic';

const counter = 0;

const consumer = kafkaClient.createConsumer({
  topic,
  groupId: 'my-js-group',
  CommitMode: CommitMode.Sync,
  offset: {position: PartitionPosition.Stored},
  configuration: {'enable.auto.commit': 'false'},
});

console.time('consumer');

consumer.startConsumer(async (error, {value, partition, offset}) => {
  if (error) {
    console.error('Js Consumer error', error);
    return;
  }

  const message = JSON.parse(value.toString());

  console.log('Message received! Partition:', partition, 'Offset:', offset, 'Message =>', message);

  // Const content = await process(value);

  // if (message._id !== '100') {
  //   throw new Error('Error on message 3');
  // }

  // Counter++;
  // if (counter >= 0) {
  //   console.timeEnd('consumer');
  //   console.log('Js Counter:', counter, 'Content:', JSON.stringify(content));
  //   console.time('consumer');
  // }

  return 'xxxok';
});

const producer = kafkaClient.createProducer({topic, configuration: {'message.timeout.ms': '5000'}});
console.log('Sending message');

await timersPromises.setTimeout(5000);

async function produce() {
  for (let i = 0; i < 2; i++) {
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

await produce();
