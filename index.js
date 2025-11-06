const { Kafka, Partitioners } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'democluster2',
  brokers: ['b-3.democluster2.p5hw9r.c2.kafka.ap-south-1.amazonaws.com:9098'],
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});
