const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'backend-service',
  brokers: ['65.0.121.205:9092']
});

const producer = kafka.producer();

const start = async () => {
  await producer.connect();
  await producer.send({
    topic: 'test-topic',
    messages: [
      { value: 'Hello from Node.js on EC2!' },
    ],
  });
  await producer.disconnect();
};

start().catch(console.error);
