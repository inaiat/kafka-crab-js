import { KafkaClient, KafkaCommitMode, PartitionPosition } from '../index.js'
import timersPromises from 'node:timers/promises'
import { Buffer } from 'node:buffer';
import { nanoid } from 'nanoid'
import { Piscina } from 'piscina'

const kafkaClient = new KafkaClient("localhost:29092", "my-id")
const topic = "user-topic" //"my-js-topic"

let counter = 50;

async function process(message) {
    await timersPromises.setTimeout(1)
    return "message received: " + message
}

const consumer = kafkaClient.createConsumer({
    topic, 
    groupId: "my-js-group",
    commitMode: KafkaCommitMode.Sync,
    offset: { position: PartitionPosition.Stored },
})

console.time("consumer")

consumer.startConsumer(async (err, value)=> {
    if (err) {
        console.error("Consumer error", err)
        return
    }
    const message = value.toString()
    const content = await process(message)
    counter++
    if (counter >= 0) {
        console.timeEnd("consumer")
        console.log("Counter:", counter, " Content:", content)
        console.time("consumer")
    }
})

// const piscina = new Piscina({
//     // The URL must be a file:// URL
//     filename: new URL('./worker.mjs', import.meta.url).href
//   });

// for (let i = 0; i < 1; i++) {   
//    await piscina.run()
// }

await timersPromises.setTimeout(10_000)

const result = await kafkaClient.createProducer().send(
    topic,
    {
        key: Buffer.from("abc"),
        value: Buffer.from(`"_id":"${counter}","name":"Elizeu Drummond Sample js","phone":"555"}`),
        headers: {"key": Buffer.from("value1")}
    }
)   
console.log(result)



