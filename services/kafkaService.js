const { getProducer, recreateProducer, ensureTopics } = require('../config/kafka');

async function sendWithRetry(topic, messages, attempts = 2) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const producer = await getProducer();
      await producer.send({ topic, messages });
      return;
    } catch (e) {
      lastErr = e;
      if (i === 0) {
        // On first failure, ensure topics exist
        await ensureTopics([topic]);
      }
      // If closed connection or network, recreate producer and retry once
      try {
        await recreateProducer();
      } catch (_) {}
      // brief backoff
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
