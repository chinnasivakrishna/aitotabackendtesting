const { Kafka, logLevel } = require('kafkajs');
const { awsIamAuthenticator } = require('@jm18457/kafkajs-msk-iam-authentication-mechanism');

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim()).filter(Boolean)
  : [];

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'demo-cluster-1',
  brokers,
  ssl: true,
  logLevel: logLevel.INFO,
  sasl: {
    mechanism: 'aws',
    authenticationProvider: awsIamAuthenticator({ region: process.env.KAFKA_REGION || process.env.AWS_REGION || 'ap-south-1' }),
  },
});

const producer = kafka.producer();
producer.connect().catch(() => {});
module.exports = producer;
