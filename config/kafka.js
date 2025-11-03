const { Kafka } = require('kafkajs');
const { awsIamAuthenticator } = require('@jm18457/kafkajs-msk-iam-authentication');

const brokers = (process.env.KAFKA_BROKER || '')
  .split(',')
  .map(b => b.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'aitota-backend',
  brokers,
  ssl: true,
  sasl: {
    mechanism: 'aws',
    authenticationProvider: awsIamAuthenticator,
  },
});

const producer = kafka.producer();
producer.connect().catch(() => {});
module.exports = producer;
