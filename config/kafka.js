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

let producer = null;

async function getProducer() {
  try {
    if (!producer) {
      producer = kafka.producer();
      await producer.connect();
    }
  } catch (_) {
    // If connect failed, reset and try once more on demand
    try {
      producer = kafka.producer();
      await producer.connect();
    } catch (e2) {
      throw e2;
    }
  }
  return producer;
}

async function recreateProducer() {
  try {
    if (producer) {
      try { await producer.disconnect(); } catch (_) {}
    }
    producer = kafka.producer();
    await producer.connect();
    return producer;
  } catch (e) {
    throw e;
  }
}

module.exports = { getProducer, recreateProducer };
