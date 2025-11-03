const { Kafka } = require('kafkajs');
const brokers = process.env.KAFKA_BROKER.split(',');
const kafka = new Kafka({ brokers });
const producer = kafka.producer();
producer.connect();
module.exports = producer;
