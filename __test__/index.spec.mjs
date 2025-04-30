import { deepEqual } from 'node:assert/strict'
import test from 'node:test'
import { KafkaClient } from '../index.js'

const brokers = 'localhost:29092'
const clientId = 'kafka-crab-client-id'

await test('send message', async () => {
  const client = new KafkaClient({ brokers, clientId })
  deepEqual(client.kafkaConfiguration.brokers, brokers)
})
