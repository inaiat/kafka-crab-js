import { KafkaClient } from '../index.js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-js-group-11',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
})

/**
 * Creates and configures a new Kafka stream consumer
 */
async function createConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: `crab-test`,
    enableAutoCommit: true,
  })

  await kafkaStream.subscribe([
    { topic: 'foo' },
    { topic: 'bar' },
  ])

  return kafkaStream
}

/**
 * Starts a Kafka consumer with auto-restart capability
 */
async function startConsumer() {
  let retryCount = 0
  const MAX_RETRIES = 5
  const RETRY_DELAY = 5000 // 5 seconds

  async function setupConsumerWithRetry() {
    try {
      const kafkaStream = await createConsumer()
      retryCount = 0 // Reset retry count on successful connection
      let counter = 0

      console.log('Starting consumer')

      kafkaStream.on('data', (message) => {
        counter++
        console.log('>>> Message received:', {
          counter,
          payload: message.payload.toString(),
          offset: message.offset,
          partition: message.partition,
          topic: message.topic,
        })
      })

      kafkaStream.on('error', async (error) => {
        console.error('Stream error:', error)

        try {
          kafkaStream.unsubscribe()
        } catch (unsubError) {
          console.error('Error unsubscribing:', unsubError)
        }

        if (retryCount < MAX_RETRIES) {
          retryCount++
          console.log(
            `Attempting to restart consumer (attempt ${retryCount}/${MAX_RETRIES}) in ${RETRY_DELAY / 1000} seconds...`,
          )
          setTimeout(setupConsumerWithRetry, RETRY_DELAY)
        } else {
          console.error(`Maximum retry attempts (${MAX_RETRIES}) reached. Stopping consumer.`)
          process.exit(1)
        }
      })

      kafkaStream.on('close', () => {
        console.log('Stream ended')
        kafkaStream.unsubscribe()
      })
    } catch (error) {
      console.error('Error setting up consumer:', error)
      if (retryCount < MAX_RETRIES) {
        retryCount++
        console.log(
          `Attempting to restart consumer (attempt ${retryCount}/${MAX_RETRIES}) in ${RETRY_DELAY / 1000} seconds...`,
        )
        setTimeout(setupConsumerWithRetry, RETRY_DELAY)
      } else {
        console.error(`Maximum retry attempts (${MAX_RETRIES}) reached. Stopping consumer.`)
        process.exit(1)
      }
    }
  }

  await setupConsumerWithRetry()
}

await startConsumer()
