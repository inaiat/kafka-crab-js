# Kafka Crab JS Documentation

A Node.js native binding for Apache Kafka using Rust, providing high performance and type safety.

## Installation

```bash
npm install kafka-crab-js
```

## Basic Usage

### Consumer Example

```typescript
import { KafkaConsumer, KafkaClientConfig } from 'kafka-crab-js';

const config: KafkaClientConfig = {
  brokers: ['localhost:9092'],
  groupId: 'my-group',
};

async function consumeMessages() {
  const consumer = new KafkaConsumer(config);
  
  // Subscribe to events before consuming
  consumer.on_events((error, event) => {
    if (error) {
      console.error('Event error:', error);
      return;
    }
    
    switch (event.name) {
      case 'PreRebalance':
        console.log('Starting rebalance');
        break;
      case 'PostRebalance':
        console.log('Finished rebalance');
        break;
      case 'CommitCallback':
        console.log('Offset committed');
        break;
    }
  });

  try {
    await consumer.subscribe('my-topic');
    
    while (true) {
      const message = await consumer.recv();
      if (message === null) {
        console.log('Consumer disconnected');
        break;
      }
      console.log('Received:', message);
    }
  } finally {
    await consumer.disconnect();
  }
}
```

### Consumer Example with Manual Commit

```typescript
import { KafkaConsumer, KafkaClientConfig } from 'kafka-crab-js';

const config: KafkaClientConfig = {
  brokers: ['localhost:9092'],
  groupId: 'my-group',
  // Disable auto-commit when you want to commit manually
  configuration: {
    'enable.auto.commit': 'false'
  }
};

async function consumeMessages() {
  const consumer = new KafkaConsumer(config);
  
  try {
    await consumer.subscribe('my-topic');
    
    while (true) {
      const message = await consumer.recv();
      if (message === null) {
        console.log('Consumer disconnected');
        break;
      }
      console.log('Received:', message);
      
      // Manual commit example
      await consumer.commit(
        message.topic,
        message.partition,
        message.offset,
        'Sync' // or 'Async'
      );
    }
  } finally {
    await consumer.disconnect();
  }
}
```

### Producer Example

```typescript
import { KafkaProducer, KafkaClientConfig } from 'kafka-crab-js';

const config: KafkaClientConfig = {
  brokers: ['localhost:9092']
};

async function produceMessages() {
  const producer = new KafkaProducer(config);
  
  await producer.send({
    topic: 'my-topic',
    message: 'Hello World'
  });
  
  await producer.flush();
}
```

## API Reference

> For detailed API documentation, see the [Complete API Reference](API-Reference.md)

### KafkaConsumer

#### Constructor
```typescript
new KafkaConsumer(config: KafkaClientConfig)
```

#### Methods

- **subscribe(topic: string | TopicPartitionConfig[]): Promise<void>**
  Subscribe to topics

- **recv(): Promise<Message | null>**
  Receive next message

- **on_events(callback: (error: Error | undefined, event: KafkaEvent) => void): void**
  Subscribe to consumer events

- **disconnect(): Promise<void>**
  Disconnect and cleanup consumer

- **pause(): Promise<void>**
  Pause consumption

- **resume(): Promise<void>**
  Resume consumption

- **seek(topic: string, partition: number, offset: OffsetModel): Promise<void>**
  Seek to specific offset

- **commit(topic: string, partition: number, offset: number, mode: CommitMode): Promise<void>**
  Commit offsets

### Events

```typescript
enum KafkaEventName {
  PreRebalance,
  PostRebalance,
  CommitCallback
}

interface TopicPartition {
  topic: string;
  partition: number;
  offset: number;
}

interface KafkaEventPayload {
  action?: string;
  tpl: Array<TopicPartition>;
  error?: string;
}

interface KafkaEvent {
  name: KafkaEventName;
  payload: KafkaEventPayload;
}
```

## Best Practices

1. **Resource Management**
   - Always call `disconnect()` when done
   - Use try/finally blocks
   - Handle cleanup properly

2. **Event Handling**
   - Subscribe to events before consuming
   - Handle all event types
   - Check for errors in callbacks

3. **Error Handling**
   - Use try/catch blocks
   - Implement proper error recovery
   - Check null returns from recv()

4. **Type Safety**
   - Use TypeScript for better type checking
   - Leverage provided type definitions
   - Use enum values for event names

5. **Commit Strategy**
   - By default, auto-commit is enabled (every 5 seconds)
   - Disable auto-commit when you need manual control
   - Use manual commits for:
     - At-least-once delivery guarantee
     - Custom commit intervals
     - Batch processing
   - Choose commit mode based on your needs:
     - 'Sync': Ensures commit is complete before continuing
     - 'Async': Better performance but no immediate confirmation

## Configuration Options

```typescript
interface KafkaClientConfig {
  brokers: string[];
  groupId?: string;
  clientId?: string;
  configuration?: {
    'enable.auto.commit'?: 'true' | 'false';  // Defaults to 'true'
    'auto.commit.interval.ms'?: string;        // Defaults to '5000'
    // For all available configuration options, see:
    // https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html
  }
}
```

> **Note**: All configuration attributes in the `configuration` object are passed directly to librdkafka. 
> For a complete list of available options, refer to the [librdkafka Configuration Properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).

## Common Issues and Solutions

1. **Connection Issues**
   - Verify broker addresses
   - Check network connectivity
   - Ensure proper security settings

2. **Performance Tuning**
   - Adjust batch sizes
   - Configure appropriate timeouts
   - Monitor memory usage

3. **Commit Issues**
   - Remember to disable auto-commit when using manual commits
   - Use Sync commit mode when you need confirmation
   - Consider commit frequency impact on performance
   - Watch for commit errors in event callbacks

## Contributing

Contributions are welcome! Please see our [Contributing Guide](../CONTRIBUTING.md) for more details.

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.
