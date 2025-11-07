const { Kafka, logLevel } = require('kafkajs');

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim()).filter(Boolean)
  : ['localhost:9092']; // default for your EC2 Kafka

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'local-campaign-cluster',
  brokers,
  ssl: false, // no SSL for local setup
  sasl: undefined, // no SASL for local
  logLevel: logLevel.INFO,
});

let producer = null;
let admin = null;
let consumer = null;

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

async function getAdmin() {
  if (!admin) {
    admin = kafka.admin();
    await admin.connect();
  }
  return admin;
}

async function getConsumer(groupId = 'campaign-consumer-group') {
  if (!consumer) {
    consumer = kafka.consumer({ groupId });
    await consumer.connect();
  }
  return consumer;
}

async function recreateConsumer(groupId = 'campaign-consumer-group') {
  try {
    if (consumer) {
      try { await consumer.disconnect(); } catch (_) {}
    }
    consumer = kafka.consumer({ groupId });
    await consumer.connect();
    return consumer;
  } catch (e) {
    throw e;
  }
}

async function ensureTopics(topicNames = []) {
  if (!Array.isArray(topicNames) || topicNames.length === 0) return;
  const admin = await getAdmin();
  try {
    const existing = await admin.listTopics();
    const toCreate = topicNames.filter(t => !existing.includes(t));
    if (toCreate.length) {
      await admin.createTopics({
        topics: toCreate.map(name => ({
          topic: name,
          numPartitions: 1,
          replicationFactor: 1, // single-node EC2 setup
        })),
        waitForLeaders: true
      });
      console.log('[kafka] created topics:', toCreate);
    }
  } catch (e) {
    console.warn('[kafka] ensureTopics failed:', e?.message || e);
  }
}

module.exports = { getProducer, recreateProducer, getAdmin, getConsumer, recreateConsumer, ensureTopics };
