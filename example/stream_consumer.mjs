import { fakerPT_BR } from '@faker-js/faker'
import { CommitMode, KafkaClient, KafkaStreamReadable, PartitionPosition } from '../index.js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-js-group-11',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
})

async function produce(topic, messages = 1) {
  const producer = kafkaClient.createProducer({ configuration: { 'message.timeout.ms': '5000' } })
  const index = Math.floor(Math.random() * 100_000)
  const records = []
  for (let i = index ?? 0; i < index + messages; i++) {
    const payload = {
      '_id': i,
      'name': fakerPT_BR.person.fullName(),
      'phone': fakerPT_BR.phone.number(),
    }
    records.push({
      payload: Buffer.from(JSON.stringify(payload)),
    })
    console.log('Payload to send', payload)
  }

  try {
    const result = await producer.send({ topic, messages: records })
    console.log('Number of messages:', result.length)
    console.log('Js message sent. Offset:', result)
  } catch (error) {
    console.error('Js Error on send', error)
  }
}

async function startConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: `crab-test`,
    enableAutoCommit: true,
  })

  await kafkaStream.subscribe([{ topic: 'foo' }, { topic: 'bar' }])

  let counter = 0
  console.log('Starting consumer')
  kafkaStream.on('data', (message) => {
    counter++
    if (counter === 5) {
      console.log('Pausing stream')
      kafkaStream.pause()
      setTimeout(() => {
        console.log('Resuming stream')
        kafkaStream.resume()
      }, 5_000)
    }
    console.log('>>> Message received:', {
      counter,
      payload: message.payload.toString(),
      offset: message.offset,
      partition: message.partition,
      topic: message.topic,
    })
    // streamerConsumer.commit(message.partition, message.offset+1, CommitMode.Sync)
  })

  kafkaStream.on('error', (error) => {
    console.error('Stream error', error)
  })

  kafkaStream.on('close', () => {
    console.log('Stream ended')
    kafkaStream.unsubscribe()
  })
}

if (process.argv[2] === 'send') {
  const topic = process.argv[3]
  const messages = process.argv[4] ? Number(process.argv[4]) : 1
  console.log('Sending', messages, 'messages to', topic)
  await produce(topic, messages)
} else {
  await startConsumer()
}
