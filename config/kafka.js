const { Kafka, logLevel } = require('kafkajs');

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim()).filter(Boolean)
  : [];

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'demo-cluster-1',
  brokers,
  ssl: true,
  logLevel: logLevel.INFO,
});

const producer = kafka.producer();
producer.connect().catch(() => {});
module.exports = producer;
