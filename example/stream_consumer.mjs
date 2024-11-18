import { CommitMode, KafkaClient, KafkaStreamConsumer, PartitionPosition } from "../index.js";
import { setTimeout as sleep } from 'node:timers/promises';
import { fakerPT_BR  } from '@faker-js/faker';
import { Readable } from "node:stream";

const kafkaClient = new KafkaClient({
    brokers: 'localhost:29092',
    clientId: 'my-js-group-11',
    logLevel: 'debug',
    brokerAddressFamily: 'v4',
  });

/**
 * Produces some messages to the topic
 * @param {string} topic 
 * @param {number} messages
 */
async function produce(topic, messages=1) {
  const producer = kafkaClient.createProducer({ topic, configuration: { 'message.timeout.ms': '5000' } });
  const index = Math.floor(Math.random() * 100_000)
  const records = []
  for (let i = index ?? 0; i < index+messages; i++) {
    records.push({ payload: Buffer.from(`{"_id":"${i}","name":"${fakerPT_BR.person.fullName()}","phone":"${fakerPT_BR.phone.number()}"}`) })
  }

  try {
    const result = await producer.send({ topic, messages: records });
    console.log('Number of messages:', result.length);
    console.log('Js message sent. Offset:', result);
  } catch (error) {
    console.error('Js Error on send', error);
  }

}


class KafkaStreamReadable extends Readable {
  /**
   * Creates a new KafkaStream
   * @param {KafkaStreamConsumer} kafkaStreamConsumer 
   */
  constructor(kafkaStreamConsumer) {
    super({ objectMode: true });
    this.kafkaStreamConsumer = kafkaStreamConsumer;
  }

  async _read() {
    let message = await this.kafkaStreamConsumer.recv();
    this.push(message)
  }
}

async function startConsumer() {
    const kafkaStreamConsumer = kafkaClient.createStreamConsumer({
      groupId: `crab-test`,
      enableAutoCommit: true,
    });
  
    // await streamerConsumer.subscribe(
    //   [{ 
    //     topic: 'foo', 
    //     partitionOffset: [
    //       { partition:0, offset: { position: PartitionPosition.Beginning }} 
    //     ]
    //   }, 
    //   { 
    //     topic: 'bar', 
    //     partitionOffset: [
    //       { partition:0, offset: { position: PartitionPosition.Stored }},
    //       { partition:1, offset: { position: PartitionPosition.Beginning }} 
    //     ]
    //   }]
    // );
    // await streamerConsumer.subscribe([ { topic: 'foo', allOffsets: { position: PartitionPosition.Stored } } ]);
    // await streamerConsumer.subscribe('foo');
    await kafkaStreamConsumer.subscribe([ { topic: 'foo' }, { topic: 'bar' } ]);
    
    const maxMessages = 20;
    let counter = 0;
    const kafkaStream = new KafkaStreamReadable(kafkaStreamConsumer);
    console.log('Starting consumer')
    kafkaStream.on('data', (message) => {
      counter++;
      console.log('>>> Message received:', { payload: message.payload.toString(), offset: message.offset, partition: message.partition, topic: message.topic })
      if (counter === maxMessages) {
        kafkaStream.destroy();
      }
      // streamerConsumer.commit(message.partition, message.offset+1, CommitMode.Sync)
    })

    kafkaStream.on('close', () => {
      kafkaStreamConsumer.unsubscribe();
      console.log('Stream ended')
    })
  }

  if (process.argv[2] === 'send') {
    const topic = process.argv[3]
    const messages = process.argv[4] ? Number(process.argv[4]) : 1
    console.log('Sending', messages, 'messages to', topic)
    const r = await Promise.all([produce(topic, messages), produce(topic, messages)])
    console.log(r)
    // await produce(topic, messages);
  } else { 
    await startConsumer();
  }