const producer = require('../config/kafka');

async function ensureConnected() {
  try {
    // If producer was disconnected for any reason, reconnect
    await producer.connect();
  } catch (_) {
    // ignore if already connected
  }
}

exports.send = async (topic, message) => {
  try {
    await producer.send({
      topic,
      messages: [{ value: JSON.stringify(message) }]
    });
  } catch (err) {
    try {
      // Attempt one reconnect and retry on disconnect errors
      await ensureConnected();
      await producer.send({
        topic,
        messages: [{ value: JSON.stringify(message) }]
      });
    } catch (err2) {
      // Log and continue without throwing to avoid breaking campaign flow
      console.warn('[kafka] send failed after retry:', err2?.message || err2);
    }
  }
};
