/* eslint-disable no-await-in-loop */
import timersPromises from 'node:timers/promises';
import {Buffer} from 'node:buffer';
import {KafkaClient, CommitMode, PartitionPosition} from '../index.js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-id',
  enableAnsiLogger: true});
const topic = 'my-js-topic';

let counter = 0;

async function process(message) {
  await timersPromises.setTimeout(1);
  return 'message received: ' + JSON.stringify(message);
}

const consumer = kafkaClient.createConsumer({
  topic,
  groupId: 'my-js-group',
  CommitMode: CommitMode.Async,
  offset: {position: PartitionPosition.Stored},
});

console.time('consumer');

consumer.startConsumer(async (error, {message}) => {
  if (error) {
    console.error('Js Consumer error', error);
    return;
  }

  const value = JSON.parse(message.toString());
  const content = await process(value);

  if (message._id === '3') {
    throw new Error('Error on message 3');
  }

  counter++;
  if (counter >= 0) {
    console.timeEnd('consumer');
    console.log('Js Counter:', counter, 'Content:', JSON.stringify(content));
    console.time('consumer');
  }
});

const producer = kafkaClient.createProducer({topic, configuration: {'message.timeout.ms': '5000'}});
console.log('Sending message');

await timersPromises.setTimeout(5000);

for (let i = 0; i < 20; i++) {
  try {
    const result = await producer.send(
      {
        topic,
        messages: [{key: Buffer.from('value1'), value: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond Sample js","phone":"555"}`)}],
      },
    );
    console.log('Js message sent. Offset:', result);
    await timersPromises.setTimeout(1500);
  } catch (error) {
    console.error('Js Error on send', error);
  }
}
