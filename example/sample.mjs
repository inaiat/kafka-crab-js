import { ConsumerResult, KafkaClient, KafkaCommitMode, PartitionPosition } from '../index.js'
import timersPromises from 'node:timers/promises'
import { Buffer } from 'node:buffer';

const kafkaClient = new KafkaClient("localhost:29092", "my-id")
const topic = "my-js-topic"

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
    return ConsumerResult.Ok
})

await timersPromises.setTimeout(5_000)

const result = await kafkaClient.createProducer().send(
    topic,
    {
        key: Buffer.from("abc"),
        value: Buffer.from(`{"_id":"${counter}","name":"Elizeu Drummond Sample js","phone":"555"}`),
        headers: {"key": Buffer.from("value1")}
    }
)   
console.log(result)
