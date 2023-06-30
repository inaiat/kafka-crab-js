/* eslint-disable no-await-in-loop */
import timersPromises from 'node:timers/promises';
import {Buffer} from 'node:buffer';
import {ConsumerResult, KafkaClient, KafkaCommitMode, PartitionPosition} from '../index.js';

const kafkaClient = new KafkaClient({ 
  brokers: 'localhost:29092', 
  clientId: 'my-id', 
  enableAnsiLogger: false});
const topic = 'my-js-topic';

let counter = 0;

async function process(message) {
  await timersPromises.setTimeout(1);
  return 'message received: ' + JSON.stringify(message);
}

const consumer = kafkaClient.createConsumer({
  topic,
  groupId: 'my-js-group',
  commitMode: KafkaCommitMode.Sync,
  offset: {position: PartitionPosition.Stored},
  retryStrategy: {
    retries: 3,
  },
});

console.time('consumer');

consumer.startConsumer(async (error, value) => {
  if (error) {
    console.error('Js Consumer error', error);
    return;
  }

  const message = JSON.parse(value.toString());
  const content = await process(message);
  counter++;
  if (counter >= 0) {
    console.timeEnd('consumer');
    console.log('Js Counter:', counter, 'Content:', JSON.stringify(content));
    console.time('consumer');
  }

  if (message._id === '3') {
    console.error('Js Error on process message, let\'s retry', JSON.stringify(content));
    return ConsumerResult.Retry;
  }


  return ConsumerResult.Ok;
});

const producer = kafkaClient.createProducer({topic, configuration: {'message.timeout.ms': '5000'}});
console.log('Sending message');

for (let i = 0; i < 5; i++) {
  try {
    const result = await producer.send(
      {
        topic,
        key: Buffer.from('abc'),
        value: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond Sample js","phone":"555"}`),
        headers: {key: Buffer.from('value1')},
      },
    );
    console.log("Js message sent. Offset: ", result);
    await timersPromises.setTimeout(1200);
  } catch (error) {
    console.error('Js Error on send', error);
  }
}
