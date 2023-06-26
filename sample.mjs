import { KafkaClient } from './index.js'
import timersPromises from 'node:timers/promises'
import { Buffer } from 'node:buffer';
import { nanoid } from 'nanoid'

const kafkaClient = new KafkaClient("localhost:29092", "my-id")

async function process(message) {
    await timersPromises.setTimeout(10)
    return "message received: " + message
}

const consumer = kafkaClient.createConsumer({topic: "my-js-topic", groupId: "my-js-group"})

consumer.startConsumer(async (err, value)=> {
    const message = value.toString()
    const content = await process(message)
    console.log(content)
})

await timersPromises.setTimeout(5000)


const result = await kafkaClient.createProducer().send(
    "my-js-topic",
    {
        key: Buffer.from("abc"),
        value: Buffer.from(`message ${nanoid()}`),
        headers: {"key": Buffer.from("value1")}
    }
)   
console.log(result)



