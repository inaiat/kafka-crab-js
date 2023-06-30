import test from 'ava'
import { KafkaClient } from '../index.js'

const brokers = "localhost:29092"
const clientId = "kafka-crab-client-id"

test('send message', async (t) => {
    const client = new KafkaClient({brokers, clientId})
    t.is(client.kafkaConfiguration.brokers, brokers)
})