import { KafkaClient, tsfnReturnPromise } from './index.js'
import timersPromises from 'node:timers/promises'
import { Buffer } from 'node:buffer';



const x = new KafkaClient("localhost:29092", "my-id")
const result = await x.createProducer().send(
    "my-js-topic",
    {
        key: Buffer.from("abc"),
        value: Buffer.from("my message"),
        headers: {"key": Buffer.from("value1")}
    }
)

const consumer = x.createConsumer("my-group-id")

async function process(message) {
    await timersPromises.setTimeout(10)
    return "message received: " + message
}

consumer.startConsumer({ 
    topic: "my-js-topic",
}, async (err, value)=> {
    const message = value.toString()
    const content = await process(message)
    console.log(content)
})



