const { getProducer, recreateProducer, ensureTopics } = require('../config/kafka');

async function sendWithRetry(topic, messages, attempts = 2) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const producer = await getProducer();
      await producer.send({ topic, messages });
      console.log(`[kafka] sent ${messages.length} messages to topic "${topic}"`);
      return;
    } catch (e) {
      lastErr = e;
      if (i === 0) {
        await ensureTopics([topic]);
      }
      try {
        await recreateProducer();
      } catch (_) {}
      await new Promise(r => setTimeout(r, 200 * (i + 1)));
    }
  }
  throw lastErr;
}

exports.send = async (topic, message) => {
  try {
    await sendWithRetry(topic, [{ value: JSON.stringify(message) }], 2);
  } catch (err2) {
    console.warn('[kafka] send failed after retry:', err2?.message || err2);
  }
};
