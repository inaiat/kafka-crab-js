# Kafka Crab Js

🚧 Project in Alpha Stage: API Subject to Breaking Changes 🚧

Kafka-Crab-JS is a powerful Node.js library that allows developers to interact with Apache Kafka using Rust programming language, seamlessly integrated with Node.js applications. This documentation provides an overview of the Kafka-Crab-JS library, including installation instructions, usage guidelines, and examples.

The plugin can be found on [npm](https://www.npmjs.com/package/kafka-crab-js) and the source code is available on [GitHub](https://github.com/inaiat/kafka-crab-js).

## Table of Contents
1. [Installation](#installation)
2. [Usage](#usage)
   - [Client](#client)
   - [Producer](#producer)
   - [Consumer](#consumer)
3. [Examples](#examples)


## Installation
To use Kafka Crab JS, you need to have Node.js and npm (Node Package Manager) installed on your system. Once you have them set up, you can install the plugin by running the following command:

```shell
npm install kafka-crab-js
```
## Usage
### Client
Creating a Client Instance
Next, you can create a client instance by specifying the Kafka broker(s) and other optional configurations:

```javascript
import { KafkaClient }

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-id'});
```
### Producer
The producer component allows you to send messages to Kafka topics.

```javascript
const producer = kafkaClient.createProducer({topic: 'my-topic'});

const result = await producer.send({
    topic: 'my-js-topic',
    messages: [{value: Buffer.from(`{"name":"Elizeu Drummond","phone":"55219123456"}`)}],
});
console.log('Message sent with offset:', result);
```


### Consumer

**Creating a Consumer Instance**

Create a consumer instance by specifying the Kafka broker(s), group ID, and other optional configurations:

```javascript
const consumer = kafkaClient.createConsumer({
  topic: 'my-topic',
  groupId: 'my-group',
  retryStrategy: {
    retries: 3,
  },
});
```

You can consume messages using the run startConsumer:

```javascript
consumer.startConsumer(async (error, {value, partition, offset}) => {
  const message = JSON.parse(value.toString());
  console.log('Message received! Partition:', partition, 'Offset:', offset, 'Message =>', message.toString());

  return ConsumerResult.Ok;
});
```
## Examples

You can find examples of using Kafka Crab JS in the [GitHub repository](https://github.com/inaiat/kafka-crab-js).
