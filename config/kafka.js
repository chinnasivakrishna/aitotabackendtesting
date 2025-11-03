const { Kafka } = require('kafkajs');
const { awsIamAuthenticator, Type: AWS_MSK_IAM } = require('@jm18457/kafkajs-msk-iam-authentication-mechanism');

const brokers = (process.env.KAFKA_BROKER || '')
  .split(',')
  .map(b => b.trim())
  .filter(Boolean);

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'aitota-backend',
  brokers,
  ssl: true,
  sasl: {
    mechanism: AWS_MSK_IAM,
    authenticationProvider: awsIamAuthenticator({ region: process.env.AWS_REGION || 'ap-south-1' }),
  },
});

const producer = kafka.producer();
producer.connect().catch(() => {});
module.exports = producer;
