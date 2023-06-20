import { KafkaClient } from './index.js'
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

console.log(result)




