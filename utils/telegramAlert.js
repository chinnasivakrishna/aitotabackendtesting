const axios = require('axios');

const TELEGRAM_ALERT_URL = 'https://telegram-bot-alert-module.onrender.com/api/telegram/send-text';

/**
 * Send a simple text alert to Telegram group.
 * Swallows errors to avoid impacting business flows.
 * @param {string} text
 */
async function sendTelegramAlert(text) {
  try {
    if (!text || typeof text !== 'string' || !text.trim()) return;
    await axios.post(TELEGRAM_ALERT_URL, { text });
  } catch (err) {
    // log once, but do not throw
    try {
      console.warn('Telegram alert failed:', err?.message);
    } catch (_) {}
  }
}

async function sendCampaignStartAlert({ campaignName, clientName, mode }) {
  const when = new Date().toLocaleString('en-IN', { hour12: false });
  const modeEmoji = mode === 'parallel' ? '🟦' : '🟩';
  const text = `🚀 Campaign Started ${modeEmoji}\n📛 ${campaignName}\n👤 ${clientName}\n🕒 ${when}`;
  await sendTelegramAlert(text);
}

module.exports = { sendTelegramAlert , sendCampaignStartAlert};


