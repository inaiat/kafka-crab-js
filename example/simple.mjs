import { nanoid } from 'nanoid'
import { Buffer } from 'node:buffer'
import { KafkaClient } from '../dist/index.js'
process.env.NAPI_RS_TOKIO_RUNTIME = '1'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-js-group',
  securityProtocol: 'Plaintext',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
})
const topic = `topic-${nanoid()}`

async function produce() {
  const producer = kafkaClient.createProducer({ topic, configuration: { 'message.timeout.ms': '5000' } })
  for (let i = 0; i < 10; i++) {
    try {
      const result = await producer.send(
        {
          topic,
          messages: [{
            key: Buffer.from(nanoid()),
            headers: { 'correlation-id': Buffer.from(nanoid()) },
            payload: Buffer.from(`{"_id":"${i}","name":"Elizeu Drummond","phone":"1234567890"}`),
          }],
        },
      )
      console.log('Js message sent. Offset:', result)
    } catch (error) {
      console.error('Js Error on send', error)
    }
  }
}

async function startConsumer() {
  const consumer = kafkaClient.createConsumer({
    topic,
    groupId: 'my-js-group2',
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  })
  await consumer.subscribe(topic)
  while (true) {
    const message = await consumer.recv()
    const { partition, offset, headers, payload } = message
    console.log(
      'Message received! Partition:',
      partition,
      'Offset:',
      offset,
      'headers:',
      Object.entries(headers).map(([k, v]) => ({ [k]: v.toString() })),
      'Message => ',
      payload.toString(),
    )
  }
}

await produce()
await startConsumer()
