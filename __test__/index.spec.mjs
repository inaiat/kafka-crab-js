import test from 'ava'
import { KafkaClient } from '../index.js'
import timersPromises from 'node:timers/promises'
import { Buffer } from 'node:buffer'
import { nanoid } from 'nanoid'

const broker = "localhost:29092"
const brokerId = "kafka-crab-client-id"

test('send message', async (t) => {
    const client = new KafkaClient(broker, brokerId)
    t.is(client.kafkaConfiguration.brokers, broker)
})