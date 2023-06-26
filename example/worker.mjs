import { Piscina } from 'piscina';
import { KafkaClient, PartitionPosition } from '../index.js'
import timersPromises from 'node:timers/promises'


async function process(message) {
    await timersPromises.setTimeout(1)
    return "message received: " + message
}

const kafkaClient = new KafkaClient("localhost:29092", "my-id")
const topic = "user-topic" //"my-js-topic"

const consumer = kafkaClient.createConsumer({
    topic, 
    groupId: "my-js-group",
    // commitMode: KafkaCommitMode.AutoCommit,
    offset: { position: PartitionPosition.Beginning },
})

const startAndConfigureClient = async () => {
    console.log("Starting consumer");
    return consumer.startConsumer(async (err, value)=> {
        const message = value.toString()
        const content = await process(message)
        console.log(content)
        // if (counter >= 300000) {
        //     console.timeEnd("consumer")
        //     console.log("Counter:", counter, " Content:", content)
        //     console.time("consumer")
        // }
    })
}

export default startAndConfigureClient;