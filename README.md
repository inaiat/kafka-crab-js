<div align="center">

# 🦀 Kafka Crab JS 🦀

A lightweight and flexible Kafka client for JavaScript/TypeScript, built with Rust-inspired reliability.

[![npm version](https://img.shields.io/npm/v/kafka-crab-js.svg)](https://www.npmjs.com/package/kafka-crab-js)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
</div>

## Features

- 🦀 Simple and intuitive API
- 🚀 High-performance message processing
- 🔄 Automatic reconnection handling
- 🎯 Type-safe interfaces (TypeScript support)
- ⚡ Async/await support
- 🛠️ Configurable consumer and producer options
- 📊 Stream processing support
- 📦 Message batching capabilities
- 🔍 Comprehensive error handling

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Consumer Examples](#consumer-examples)
4. [Producer Examples](#producer-examples)
5. [Stream Processing](#stream-processing)
6. [Configuration](#configuration)
7. [Best Practices](#best-practices)
8. [Contributing](#contributing)
9. [License](#license)

## Installation

```bash
npm install kafka-crab-js
# or
yarn add kafka-crab-js
```

## Quick Start

### Basic Consumer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js';
async function run() {
  const kafkaClient = new KafkaClient({
    brokers: 'localhost:29092',
    clientId: 'foo-client',
    logLevel: 'debug',
    brokerAddressFamily: 'v4',
  });

  // Create consumer
  const consumer = kafkaClient.createConsumer({
    groupId: 'foo-group',
  });

  await consumer.subscribe([{ topic: 'foo' }]);

  const message = await consumer.recv();
  const { payload, partition, offset } = message;
  console.log({
    partition,
    offset,
    value: payload.toString()
  });

  consumer.unsubscribe();

}

await run();
```

### Basic Producer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

const producer = kafkaClient.createProducer({ configuration: { 'message.timeout.ms': '5000' } });

const message = {
  id: 1,
  name: "Sample Message",
  timestamp: new Date().toISOString()
};

const result = await producer.send({
  topic: 'my-topic',
  messages: [{
    payload: Buffer.from(JSON.stringify(message))
  }]
});

const errors = result.map(r => r.error).filter(Boolean);
if (errors.length > 0) {
  console.error('Error sending message:', errors);
} else {
  console.log('Message sent. Offset:', result);
}

```

## Stream Processing

### Stream Consumer Example

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

const kafkaStream = kafkaClient.createStreamConsumer({
  groupId: `my-groud-id`,
  enableAutoCommit: true,
});

await kafkaStream.subscribe([{ topic: 'foo' }, { topic: 'bar' }])

kafkaStream.on('data', (message) => {
  console.log('>>> Message received:', { payload: message.payload.toString(), offset: message.offset, partition: message.partition, topic: message.topic })
  if (message.offset > 10) {
    kafkaStream.destroy();
  }
})

kafkaStream.on('close', () => {
  kafkaStream.unsubscribe();
  console.log('Stream ended')
})
```

## Producer Examples

### Batch Message Production

```javascript
const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  brokerAddressFamily: 'v4',
});
const producer = kafkaClient.createProducer({});

const messages = Array.from({ length: 100 }, (_, i) => ({
  payload: Buffer.from(JSON.stringify({
    _id: i,
    name: `Batch Message ${i}`,
    timestamp: new Date().toISOString()
  }))
}));

try {
  const result = await producer.send({
    topic: 'my-topic',
    messages
  });
  console.log('Batch sent. Offset:', result);
  console.assert(result.length === 100);
} catch (error) {
  console.error('Batch error:', error);
}
```

### Producer with Keys and Headers

```javascript
async function produceWithMetadata() {
  const producer = await kafkaCrab.createProducer({ config });

  try {
    await producer.send({
      topic,
      messages: [{
        key: 'user-123',
        payload: Buffer.from(JSON.stringify({
          userId: 123,
          action: 'update'
        })),
        headers: {
          'correlation-id': 'txn-123',
          'source': 'user-service'
        }
      }]
    });
  } catch (error) {
    console.error('Error:', error);
  }
}
```

## Configuration

### Configuration properties

You can see the available options here: [librdkafka](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).

## Best Practices

### Error Handling
- Always wrap async operations in try-catch blocks
- Implement proper error logging and monitoring
- Handle both operational and programming errors separately

### Performance
- Use batch operations for high-throughput scenarios
- Configure appropriate batch sizes and compression
- Monitor and tune consumer group performance

### Resource Management
- Implement proper shutdown procedures
- Clean up resources (disconnect producers/consumers)
- Handle process signals (SIGTERM, SIGINT)

### Message Processing
- Validate message formats before processing
- Implement proper serialization/deserialization
- Handle message ordering when required

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

[![Built with Rust](https://img.shields.io/badge/Built%20with-Rust-orange)](https://www.rust-lang.org/)

</div>