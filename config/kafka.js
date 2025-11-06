const { Kafka, logLevel } = require('kafkajs');

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim()).filter(Boolean)
  : [];

  const { MSKAuth } = require('@jm18457/kafkajs-msk-iam-authentication');
  const AWS = require('aws-sdk');
  
  const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'democluster2',
    brokers,
    ssl: true,
    sasl: {
      mechanism: 'aws',
      authenticationProvider: MSKAuth(AWS),
    },
    logLevel: logLevel.INFO,
  });
  

let producer = null;
let admin = null;

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

async function ensureTopics(topicNames = []) {
  if (!Array.isArray(topicNames) || topicNames.length === 0) return;
  const admin = await getAdmin();
  try {
    const existing = await admin.listTopics();
    const toCreate = topicNames.filter(t => !existing.includes(t));
    if (toCreate.length) {
      await admin.createTopics({
        topics: toCreate.map(name => ({ topic: name, numPartitions: 1, replicationFactor: 3 })),
        waitForLeaders: true
      });
    }
  } catch (e) {
    // Log only; sending code will still retry
    console.warn('[kafka] ensureTopics failed:', e?.message || e);
  }
}

module.exports = { getProducer, recreateProducer, getAdmin, ensureTopics };
