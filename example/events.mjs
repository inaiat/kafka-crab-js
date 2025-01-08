import { KafkaClient } from '../index.js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'abc',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
})

const consumer = kafkaClient.createConsumer({
  topic,
  groupId: 'xyz',
})

// If you want to consume events, you need call shutdownConsumer() to stop the consumer and release resources
consumer.onEvents((err, event) => {
  console.log(
    'Event:',
    event.name,
    event
      .payload
      .tpl
      .map(it =>
        `Topic: ${it.topic}, 
                ${
          it.partitionOffset.map(po => `partition: ${po.partition}, offset: ${po.offset.offset}`)
            .join(',')
        }`
      ),
  )
})

consumer.subscribe('foo')

const printMessage = async () => {
  let shutdown = false
  while (!shutdown) {
    const msg = await consumer.recv()
    if (msg) {
      console.log('Message receive', msg.payload.toString())
    } else {
      console.log('The consumer has been shutdown')
      shutdown = true
    }
  }
}

process.on('SIGINT', () => {
  consumer.shutdownConsumer()
})

await printMessage()
