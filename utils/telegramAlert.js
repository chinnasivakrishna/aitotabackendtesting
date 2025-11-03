const TelegramServiceController = require('../controllers/telegrambotcontroller');

// Create a single instance of the TelegramServiceController
const telegramService = new TelegramServiceController();

/**
 * Send a simple text alert to Telegram group.
 * Swallows errors to avoid impacting business flows.
 * @param {string} text
 */
async function sendTelegramAlert(text) {
  try {
    if (!text || typeof text !== 'string' || !text.trim()) return;
    await telegramService.sendTextMessage(text);
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

async function sendDetailedCampaignStartAlert({ 
  campaignName, 
  agentName, 
  groupName, 
  didNumber, 
  totalContacts, 
  clientName, 
  userEmail, 
  mode 
}) {
  const when = new Date().toLocaleString('en-IN', { hour12: false });
  const modeEmoji = mode === 'parallel' ? '🟦' : '🟩';
  const text = `🚀 Campaign Started ${modeEmoji}
📛 ${campaignName}
🧑‍💼 Agent: ${agentName}
👥 Group: ${groupName}
☎ DID: ${didNumber}
📦 Total Contacts: ${totalContacts}
🕒 Start: ${when}
🏳 Status: Running
🏢 Client: ${clientName}
📧 User: ${userEmail}
🏷 Mode: ${mode === 'parallel' ? 'Mode-P' : 'Mode-S'}`;
  await sendTelegramAlert(text);
}

async function sendDetailedCampaignEndAlert({ 
  campaignName, 
  runId, 
  agentName, 
  groupName, 
  didNumber, 
  totalContacts, 
  startTime, 
  endTime, 
  duration, 
  connected, 
  missed, 
  connectedPercentage, 
  clientName, 
  userEmail, 
  mode 
}) {
  const startFormatted = new Date(startTime).toLocaleString('en-IN', { hour12: false });
  const endFormatted = new Date(endTime).toLocaleString('en-IN', { hour12: false });
  const modeEmoji = mode === 'parallel' ? '🟦' : '🟩';
  
  const text = `🛑 Campaign Ended ${modeEmoji}
📛 ${campaignName}
🆔 ${runId}
🧑‍💼 Agent: ${agentName}
👥 Group: ${groupName}
☎ DID: ${didNumber}
📦 Total Contacts: ${totalContacts}
🕒 Start: ${startFormatted}
🕘 End: ${endFormatted}
⏱ Duration: ${duration}
📈 Connected: ${connected}
📉 Missed: ${missed}
📊 Connected %: ${connectedPercentage}%
🏷 Mode: ${mode === 'parallel' ? 'Mode-P' : 'Mode-S'}
🏢 Client: ${clientName}
📧 User: ${userEmail}`;
  await sendTelegramAlert(text);
}

module.exports = { 
  sendTelegramAlert, 
  sendCampaignStartAlert, 
  sendDetailedCampaignStartAlert,
  sendDetailedCampaignEndAlert
};


