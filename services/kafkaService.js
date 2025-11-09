const { getProducer, recreateProducer, ensureTopics } = require('../config/kafka');

async function sendWithRetry(topic, messages, attempts = 3) {
  let lastErr = null;
  for (let i = 0; i < attempts; i++) {
    try {
      const producer = await getProducer();
      
      // Double-check producer is connected before sending
      if (!producer) {
        throw new Error('Producer is null');
      }
      
      await producer.send({ topic, messages });
      console.log(`[kafka] sent ${messages.length} messages to topic "${topic}"`);
      return;
    } catch (e) {
      lastErr = e;
      const errorMsg = e?.message || String(e);
      
      // Check if it's a connection error - mark producer as disconnected
      const isConnectionError = errorMsg.includes('write after end') || 
          errorMsg.includes('disconnect') || 
          errorMsg.includes('Connection') ||
          errorMsg.includes('ECONNREFUSED') ||
          errorMsg.includes('ECONNRESET') ||
          errorMsg.includes('socket hang up') ||
          errorMsg.includes('TLS connection') ||
          errorMsg.includes('secure TLS connection');
      
      // Check if it's an SSL/TLS error
      const isSSLError = errorMsg.includes('TLS') || 
          errorMsg.includes('secure TLS') || 
          errorMsg.includes('SSL') ||
          errorMsg.includes('certificate');
      
      if (isConnectionError) {
        // Special handling for SSL/TLS errors
        if (isSSLError && i === 0) {
          console.error(`[kafka] SSL/TLS connection error detected: ${errorMsg}`);
          console.error(`[kafka] 💡 TIP: If connecting to localhost, set KAFKA_SSL=false or remove KAFKA_SSL`);
          console.error(`[kafka] 💡 TIP: Local Kafka brokers typically don't use SSL`);
        } else {
          console.warn(`[kafka] Connection error detected (attempt ${i + 1}/${attempts}): ${errorMsg}`);
        }
        
        // Ensure topics exist on first retry
        if (i === 0) {
          try {
            await ensureTopics([topic]);
          } catch (topicErr) {
            console.warn('[kafka] Topic creation failed:', topicErr?.message);
          }
        }
        
        // Recreate producer (this will handle cleanup of old producer)
        try {
          await recreateProducer();
        } catch (recreateErr) {
          console.error('[kafka] Failed to recreate producer:', recreateErr?.message);
        }
        
        // Wait before retry (exponential backoff)
        await new Promise(r => setTimeout(r, 500 * (i + 1)));
      } else {
        // For other errors, just wait and retry
        await new Promise(r => setTimeout(r, 200 * (i + 1)));
      }
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
