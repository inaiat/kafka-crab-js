import test from 'ava'
import { KafkaClient } from '../index.js'
import timersPromises from 'node:timers/promises'
import { Buffer } from 'node:buffer'
import { nanoid } from 'nanoid'

const broker = "localhost:29092"
const brokerId = "kafka-crab-client-id"


test('send message', async (t) => {
    const x = new KafkaClient(broker, brokerId)
    const result = await x.createProducer().send(
        nanoid(),
        {
            key: Buffer.from("abc"),
            value: Buffer.from("my message"),
            headers: {"key": Buffer.from("value1")}
        }
    )

    t.is(0, result.partition)

})

test('consumer', async (t) => {
    const groupId = nanoid()
    const topic = nanoid()
    const client = new KafkaClient(broker, brokerId)
    const consumer = client.createConsumer(groupId)

    async function process(message) {
        await timersPromises.setTimeout(10)
        return "message received: " + message
    }
    
    consumer.startConsumer({ 
        topic: topic,
    }, async (err, value)=> {
        const message = value.toString()
        const content = await process(message)
        console.log(content)
    })

})