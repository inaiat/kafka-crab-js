import { CommitMode, KafkaClient, PartitionPosition } from "../index.js";
import { setTimeout as sleep } from 'node:timers/promises';
import { fakerPT_BR  } from '@faker-js/faker';

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
  for (let i = index ?? 0; i < index+messages; i++) {
    try {
      const result = await producer.send(
        {
          topic,
          messages: [{ value: Buffer.from(`{"_id":"${i}","name":"${fakerPT_BR.person.fullName()}","phone":"${fakerPT_BR.phone.number()}"}`) }],
        },
      );
      console.log('Js message sent. Offset:', result);
    } catch (error) {
      console.error('Js Error on send', error);
    }
  }
}

async function startConsumer() {
    const streamerConsumer = kafkaClient.createStreamConsumer({
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
    await streamerConsumer.subscribe([ { topic: 'foo' }, { topic: 'bar' } ]);
    // await streamerConsumer.subscribe('foo');
    
    
    console.log('Starting consumer')
    let counter = 0;
    while (counter < 100) {
      try {
        const message = await streamerConsumer.recv();
        let { payload, offset, partition, topic  } = message
        console.log('>>> Js message received:', { payload: payload.toString(), offset, partition, topic })
  
        // streamerConsumer.commit(partition, offset+1, CommitMode.Sync)
        console.log('Commiting partition, offset:', partition, offset)
      } catch (error) {
        console.error('Js Error on consume', error);
      }
      counter++;
      await sleep(50)
      if (counter >= 4) {
        console.log('Unsubscribing')
        streamerConsumer.unsubscribe();
      }
    }
  }

  if (process.argv[2] === 'send') {
    const topic = process.argv[3]
    const messages = process.argv[4] ? Number(process.argv[4]) : 1
    console.log('Sending', messages, 'messages to', topic)
    await produce(topic, messages);
  } else { 
    await startConsumer();
  }