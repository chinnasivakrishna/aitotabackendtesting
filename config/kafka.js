const { Kafka, logLevel } = require('kafkajs');

const brokers = process.env.KAFKA_BROKERS
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim()).filter(Boolean)
  : ['localhost:9092']; // default for your EC2 Kafka

// Determine SSL based on environment
const useSSL = process.env.KAFKA_SSL === 'true' || process.env.KAFKA_USE_SSL === 'true';

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'local-campaign-cluster',
  brokers,
  ssl: useSSL,
  sasl: undefined, // no SASL for local
  logLevel: logLevel.INFO,
  retry: {
    retries: 3,
    initialRetryTime: 100,
    maxRetryTime: 30000,
  },
  connectionTimeout: 10000,
  requestTimeout: 30000,
});

let producer = null;
let admin = null;
let consumer = null;
let producerConnected = false;
let consumerConnected = false;

// Helper to check if producer is actually connected
function isProducerConnected() {
  return producer && producerConnected;
}

// Helper to check if consumer is actually connected
function isConsumerConnected() {
  return consumer && consumerConnected;
}

async function getProducer() {
  try {
    if (!isProducerConnected()) {
      // Clean up old producer if it exists
      if (producer) {
        try {
          await producer.disconnect();
        } catch (e) {
          // Ignore disconnect errors
        }
        producer = null;
        producerConnected = false;
      }
      
      // Create and connect new producer
      producer = kafka.producer({
        maxInFlightRequests: 1,
        idempotent: true,
        transactionTimeout: 30000,
      });
      
      await producer.connect();
      producerConnected = true;
      console.log('[kafka] Producer connected successfully');
    }
  } catch (error) {
    producerConnected = false;
    producer = null;
    console.error('[kafka] Failed to get producer:', error?.message || error);
    throw error;
  }
  return producer;
}

async function recreateProducer() {
  try {
    // Disconnect old producer if it exists
    if (producer) {
      try {
        await producer.disconnect();
      } catch (e) {
        // Ignore disconnect errors
      }
      producer = null;
      producerConnected = false;
    }
    
    // Create new producer
    producer = kafka.producer({
      maxInFlightRequests: 1,
      idempotent: true,
      transactionTimeout: 30000,
    });
    
    await producer.connect();
    producerConnected = true;
    console.log('[kafka] Producer recreated and connected');
    
    return producer;
  } catch (e) {
    producerConnected = false;
    producer = null;
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
  try {
    if (!isConsumerConnected()) {
      // Clean up old consumer if it exists
      if (consumer) {
        try {
          await consumer.disconnect();
        } catch (e) {
          // Ignore disconnect errors
        }
        consumer = null;
        consumerConnected = false;
      }
      
      // Create and connect new consumer
      consumer = kafka.consumer({ 
        groupId,
        sessionTimeout: 30000,
        heartbeatInterval: 3000,
      });
      
      await consumer.connect();
      consumerConnected = true;
      console.log('[kafka] Consumer connected successfully');
    }
  } catch (error) {
    consumerConnected = false;
    consumer = null;
    console.error('[kafka] Failed to get consumer:', error?.message || error);
    throw error;
  }
  return consumer;
}

async function recreateConsumer(groupId = 'campaign-consumer-group') {
  try {
    // Disconnect old consumer if it exists
    if (consumer) {
      try {
        await consumer.disconnect();
      } catch (e) {
        // Ignore disconnect errors
      }
      consumer = null;
      consumerConnected = false;
    }
    
    // Create new consumer
    consumer = kafka.consumer({ 
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 3000,
    });
    
    await consumer.connect();
    consumerConnected = true;
    console.log('[kafka] Consumer recreated and connected');
    
    return consumer;
  } catch (e) {
    consumerConnected = false;
    consumer = null;
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

// Helper to mark consumer as disconnected (for cleanup)
function markConsumerDisconnected() {
  consumerConnected = false;
  consumer = null;
}

// Helper to mark producer as disconnected (for cleanup)
function markProducerDisconnected() {
  producerConnected = false;
  producer = null;
}

module.exports = { 
  getProducer, 
  recreateProducer, 
  getAdmin, 
  getConsumer, 
  recreateConsumer, 
  ensureTopics,
  isProducerConnected,
  isConsumerConnected,
  markConsumerDisconnected,
  markProducerDisconnected
};
