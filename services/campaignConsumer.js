const { getConsumer, ensureTopics } = require('../config/kafka');
const Campaign = require('../models/Campaign');
const Agent = require('../models/Agent');
const CallLog = require('../models/CallLog');
const telephonyService = require('./telephonyService');
const kafkaService = require('./kafkaService');
const wsServer = require('../sockets/wsServer');
const { v4: uuidv4 } = require('uuid');

let consumerRunning = false;

async function processCampaignCommand(message) {
  try {
    const command = JSON.parse(message.value.toString());
    
    // Check message timestamp - ignore messages older than 5 minutes
    const messageTimestamp = message.timestamp ? new Date(parseInt(message.timestamp)) : null;
    if (messageTimestamp) {
      const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
      if (messageTimestamp < fiveMinutesAgo) {
        console.log(`⚠️ [KAFKA-CONSUMER] Ignoring old message (timestamp: ${messageTimestamp.toISOString()})`);
        return; // Skip old messages
      }
    }
    
    console.log(`📥 [KAFKA-CONSUMER] Received campaign command:`, command);

    if (command.action === 'start') {
      await handleStartCampaign(command);
    } else if (command.action === 'stop') {
      await handleStopCampaign(command);
    } else if (command.action === 'pause') {
      await handlePauseCampaign(command);
    } else if (command.action === 'resume') {
      await handleResumeCampaign(command);
    } else {
      console.warn(`⚠️ [KAFKA-CONSUMER] Unknown action: ${command.action}`);
    }
  } catch (error) {
    console.error(`❌ [KAFKA-CONSUMER] Error processing campaign command:`, error);
    throw error; // Re-throw to trigger retry mechanism
  }
}

async function handleStartCampaign(command) {
  const { campaignId, n, g, r, mode, timestamp } = command;
  
  // Ignore old messages (older than 5 minutes) - likely from before server restart
  if (timestamp) {
    const messageTime = new Date(timestamp);
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
    if (messageTime < fiveMinutesAgo) {
      console.log(`⚠️ [KAFKA-CONSUMER] Ignoring old start command for campaign ${campaignId} (timestamp: ${timestamp})`);
      return; // Skip processing old messages
    }
  }
  
  console.log(`🚀 [KAFKA-CONSUMER] Starting campaign ${campaignId} with N=${n}, G=${g}, R=${r}, mode=${mode}`);
  
  // Fetch fresh campaign state from database to avoid stale cache
  const campaign = await Campaign.findById(campaignId).lean();
  if (!campaign) {
    throw new Error(`Campaign ${campaignId} not found`);
  }
  
  // Prevent starting if already running - check fresh state
  if (campaign.isRunning) {
    // Check if campaign is actually stale (marked running but no recent activity)
    const lastUpdate = campaign.updatedAt ? new Date(campaign.updatedAt) : null;
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
    
    if (lastUpdate && lastUpdate < fiveMinutesAgo) {
      // Campaign is marked as running but hasn't been updated in 5+ minutes - likely stale
      console.log(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} is marked as running but appears stale (last update: ${lastUpdate.toISOString()})`);
      console.log(`🔄 [KAFKA-CONSUMER] Resetting campaign state and starting...`);
      
      // Reset the stale campaign state
      await Campaign.updateOne(
        { _id: campaignId },
        { 
          $set: { 
            isRunning: false,
            status: 'stopped',
            updatedAt: new Date()
          }
        }
      );
      // Continue with start process below
    } else {
      console.log(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} is already running (isRunning: ${campaign.isRunning}) - skipping start command`);
      return; // Don't start if already running and not stale
    }
  }
  
  // Re-fetch as Mongoose document for saving
  const campaignDoc = await Campaign.findById(campaignId);
  if (!campaignDoc) {
    throw new Error(`Campaign ${campaignId} not found`);
  }

  // Resolve agent reference
  const agentRef = Array.isArray(campaignDoc.agent) ? (campaignDoc.agent[0] || null) : campaignDoc.agent;
  let agent = null;
  if (agentRef) {
    agent = await Agent.findById(agentRef).lean();
    if (!agent) {
      agent = await Agent.findOne({ agentId: agentRef }).lean();
    }
  }
  
  if (!agent) {
    throw new Error('Agent not found for campaign. Please ensure campaign.agent contains a valid Agent ID or agentId');
  }

  // Batch contacts processing for N-G-R
  const contacts = campaignDoc.contacts;
  function* batch(arr, size) {
    for (let i = 0; i < arr.length; i += size) {
      yield arr.slice(i, i + size);
    }
  }
  
  // Mark campaign as running before processing - use atomic update to prevent race conditions
  const updateResult = await Campaign.updateOne(
    { _id: campaignId, isRunning: false },
    { 
      $set: { 
        isRunning: true,
        status: 'running',
        updatedAt: new Date()
      }
    }
  );
  
  if (updateResult.matchedCount === 0) {
    console.log(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} was already running (race condition) - skipping start`);
    return; // Another process already started it
  }
  
  console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} marked as running`);
  
  const contactBatches = [...batch(contacts, n || 1)];
  console.log(`📦 [KAFKA-CONSUMER] Processing ${contactBatches.length} batches for campaign ${campaignId}`);
  
  for (const batch of contactBatches) {
    if (mode === 'series') {
      for (const contact of batch) {
        await handleCall(campaignDoc, contact, agent);
      }
    } else {
      await Promise.all(batch.map(contact => handleCall(campaignDoc, contact, agent)));
    }
    
    // Wait for gap (G) between batches if specified
    if (g && g > 0 && contactBatches.indexOf(batch) < contactBatches.length - 1) {
      console.log(`⏳ [KAFKA-CONSUMER] Waiting ${g}s before next batch...`);
      await new Promise(resolve => setTimeout(resolve, g * 1000));
    }
  }
  
  // Update campaign status to stopped after all calls complete - use atomic update
  await Campaign.updateOne(
    { _id: campaignId },
    { 
      $set: { 
        status: 'stopped',
        isRunning: false,
        updatedAt: new Date()
      }
    }
  );
  
  // Fetch updated campaign for broadcasting
  const updatedCampaign = await Campaign.findById(campaignId).lean();
  
  // Broadcast completion
  wsServer.broadcastCampaignEvent(campaignId, 'stop', updatedCampaign);
  kafkaService.send('campaign-status', { campaignId: campaignId, status: 'stopped' });
  
  console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} completed successfully`);
}

async function handleCall(campaign, contact, agent) {
  const uniqueid = uuidv4();
  let res;
  
  if (agent.serviceProvider === "c-zentrix") {
    res = await telephonyService.callCzentrix({
      phone: contact.phone,
      agent,
      contact,
      campaignId: campaign._id,
      uniqueid
    });
  } else if (agent.serviceProvider === "sanpbx") {
    res = await telephonyService.callSanpbx({
      phone: contact.phone,
      agent,
      contact,
      uniqueid
    });
  }
  
  let log = null;
  let waited = 0;
  while (waited < 40000) {
    log = await CallLog.findOne({ "metadata.customParams.uniqueid": uniqueid });
    if (log) break;
    await new Promise(res => setTimeout(res, 2000));
    waited += 2000;
  }
  
  let status = "ringing";
  if (log && log.isActive) status = "ongoing";
  else if (log && !log.isActive) status = "completed";
  
  // Persist under the defined `details` array in Campaign schema
  const detail = {
    uniqueId: uniqueid,
    contactId: contact._id || contact.id,
    time: new Date(),
    status,
    runId: campaign._id.toString()
  };
  
  if (log && log.leadStatus) {
    detail.leadStatus = log.leadStatus;
  }
  
  await Campaign.findByIdAndUpdate(
    campaign._id,
    {
      $push: { details: detail },
      $set: { updatedAt: new Date() }
    },
    { new: false }
  );
  
  kafkaService.send('call-status', { campaignId: campaign._id, uniqueid, status });
  wsServer.broadcastCallEvent(campaign._id, uniqueid, status, log);
}

async function handleStopCampaign(command) {
  const { campaignId } = command;
  
  // Use atomic update to ensure proper state change
  const updateResult = await Campaign.updateOne(
    { _id: campaignId },
    { 
      $set: { 
        status: 'stopped',
        isRunning: false,
        updatedAt: new Date()
      }
    }
  );
  
  if (updateResult.matchedCount === 0) {
    throw new Error(`Campaign ${campaignId} not found`);
  }
  
  // Fetch updated campaign for broadcasting
  const campaign = await Campaign.findById(campaignId).lean();
  
  wsServer.broadcastCampaignEvent(campaignId, 'stop', campaign);
  kafkaService.send('campaign-status', { campaignId: campaignId, status: 'stopped' });
  
  console.log(`🛑 [KAFKA-CONSUMER] Campaign ${campaignId} stopped (isRunning set to false)`);
}

async function handlePauseCampaign(command) {
  const { campaignId } = command;
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) {
    throw new Error(`Campaign ${campaignId} not found`);
  }
  
  campaign.status = 'pause';
  campaign.isRunning = false;
  campaign.updatedAt = new Date();
  await campaign.save();
  
  wsServer.broadcastCampaignEvent(campaign._id, 'pause', campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: 'pause' });
  
  console.log(`⏸️ [KAFKA-CONSUMER] Campaign ${campaignId} paused`);
}

async function handleResumeCampaign(command) {
  const { campaignId } = command;
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) {
    throw new Error(`Campaign ${campaignId} not found`);
  }
  
  campaign.status = 'resume';
  campaign.isRunning = true;
  campaign.updatedAt = new Date();
  await campaign.save();
  
  wsServer.broadcastCampaignEvent(campaign._id, 'resume', campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: 'resume' });
  
  console.log(`▶️ [KAFKA-CONSUMER] Campaign ${campaignId} resumed`);
}

async function startConsumer() {
  if (consumerRunning) {
    console.log('⚠️ [KAFKA-CONSUMER] Consumer already running');
    return;
  }

  try {
    // Ensure topics exist
    await ensureTopics(['campaign-commands']);
    
    const consumerInstance = await getConsumer();
    
    await consumerInstance.subscribe({
      topic: 'campaign-commands',
      fromBeginning: false
    });
    
    console.log('✅ [KAFKA-CONSUMER] Subscribed to campaign-commands topic');
    
    await consumerInstance.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          console.log(`📥 [KAFKA-CONSUMER] Received message from topic ${topic}, partition ${partition}`);
          await processCampaignCommand(message);
        } catch (error) {
          console.error(`❌ [KAFKA-CONSUMER] Error processing message:`, error);
          // Message will be retried by Kafka if not committed
        }
      },
    });
    
    consumerRunning = true;
    console.log('✅ [KAFKA-CONSUMER] Consumer started and listening for campaign commands');
  } catch (error) {
    console.error('❌ [KAFKA-CONSUMER] Failed to start consumer:', error);
    consumerRunning = false;
    throw error;
  }
}

async function stopConsumer() {
  if (!consumerRunning) return;
  
  try {
    const { getConsumer } = require('../config/kafka');
    const consumer = await getConsumer();
    await consumer.disconnect();
    consumerRunning = false;
    console.log('🛑 [KAFKA-CONSUMER] Consumer stopped');
  } catch (error) {
    console.error('❌ [KAFKA-CONSUMER] Error stopping consumer:', error);
  }
}

module.exports = { startConsumer, stopConsumer };

