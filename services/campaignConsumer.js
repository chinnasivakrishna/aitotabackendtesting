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
    console.log(`📥 [KAFKA-CONSUMER] Received campaign command:`, command);

    // Idempotency check: Ignore messages older than 5 minutes (likely from before restart)
    if (command.timestamp) {
      const messageAge = Date.now() - new Date(command.timestamp).getTime();
      const maxAge = 5 * 60 * 1000; // 5 minutes
      if (messageAge > maxAge) {
        console.warn(`⚠️ [KAFKA-CONSUMER] Ignoring stale message (age: ${Math.round(messageAge / 1000)}s):`, command);
        return; // Skip processing but don't throw - allow offset to be committed
      }
    }

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
  const { campaignId, n, g, r, mode } = command;
  
  console.log(`🚀 [KAFKA-CONSUMER] Starting campaign ${campaignId} with N=${n}, G=${g}, R=${r}, mode=${mode}`);
  
  // Use atomic update to set running state (only if not already running)
  // This prevents race conditions if multiple start commands arrive simultaneously
  let campaign = await Campaign.findOneAndUpdate(
    {
      _id: campaignId,
      isRunning: false // Only update if not already running
    },
    {
      $set: {
        isRunning: true,
        status: 'running',
        updatedAt: new Date()
      }
    },
    {
      new: true, // Return updated document
      runValidators: true
    }
  );

  // If update returned null, campaign doesn't exist or was already running
  if (!campaign) {
    // Check if campaign exists and get its current state
    const currentState = await Campaign.findById(campaignId).lean();
    if (!currentState) {
      throw new Error(`Campaign ${campaignId} not found`);
    }
    // Log the actual state for debugging
    console.warn(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} atomic update failed. Current state:`, {
      isRunning: currentState.isRunning,
      status: currentState.status,
      updatedAt: currentState.updatedAt
    });
    
    if (currentState.isRunning) {
      console.warn(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} is already running, skipping duplicate start command`);
      return; // Don't throw - allow offset to be committed
    } else {
      // This shouldn't happen, but if isRunning is false and update failed, retry once
      console.warn(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} update failed despite isRunning=false, retrying...`);
      const retryCampaign = await Campaign.findOneAndUpdate(
        { _id: campaignId, isRunning: false },
        { $set: { isRunning: true, status: 'running', updatedAt: new Date() } },
        { new: true }
      );
      if (!retryCampaign) {
        console.error(`❌ [KAFKA-CONSUMER] Retry also failed for campaign ${campaignId}`);
        return;
      }
      // Use retry result - reassign campaign variable
      campaign = retryCampaign;
      console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} marked as running on retry, proceeding with execution`);
      // Continue with campaign processing below (campaign is now set)
    }
  }

  console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} marked as running, proceeding with execution`);

  // Resolve agent reference
  const agentRef = Array.isArray(campaign.agent) ? (campaign.agent[0] || null) : campaign.agent;
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
  const contacts = campaign.contacts;
  function* batch(arr, size) {
    for (let i = 0; i < arr.length; i += size) {
      yield arr.slice(i, i + size);
    }
  }
  
  const contactBatches = [...batch(contacts, n || 1)];
  console.log(`📦 [KAFKA-CONSUMER] Processing ${contactBatches.length} batches for campaign ${campaignId}`);
  
  for (const batch of contactBatches) {
    if (mode === 'series') {
      for (const contact of batch) {
        await handleCall(campaign, contact, agent);
      }
    } else {
      await Promise.all(batch.map(contact => handleCall(campaign, contact, agent)));
    }
    
    // Wait for gap (G) between batches if specified
    if (g && g > 0 && contactBatches.indexOf(batch) < contactBatches.length - 1) {
      console.log(`⏳ [KAFKA-CONSUMER] Waiting ${g}s before next batch...`);
      await new Promise(resolve => setTimeout(resolve, g * 1000));
    }
  }
  
  // Update campaign status to stopped after all calls complete
  campaign.status = 'stop';
  campaign.isRunning = false;
  campaign.updatedAt = new Date();
  await campaign.save();
  
  // Broadcast completion
  wsServer.broadcastCampaignEvent(campaign._id, 'stop', campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: 'stop' });
  
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
  
  // Use atomic update to ensure stop is applied
  const campaign = await Campaign.findByIdAndUpdate(
    campaignId,
    {
      $set: {
        status: 'stop',
        isRunning: false,
        updatedAt: new Date()
      }
    },
    { new: true }
  );
  
  if (!campaign) {
    throw new Error(`Campaign ${campaignId} not found`);
  }
  
  console.log(`🛑 [KAFKA-CONSUMER] Campaign ${campaignId} stopped. State:`, {
    isRunning: campaign.isRunning,
    status: campaign.status,
    updatedAt: campaign.updatedAt
  });
  
  wsServer.broadcastCampaignEvent(campaign._id, 'stop', campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: 'stop' });
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
          console.log(`📥 [KAFKA-CONSUMER] Received message from topic ${topic}, partition ${partition}, offset ${message.offset}`);
          await processCampaignCommand(message);
          // Offset will be auto-committed after successful processing
          // KafkaJS commits offsets automatically after eachMessage completes successfully
        } catch (error) {
          console.error(`❌ [KAFKA-CONSUMER] Error processing message (offset ${message.offset}):`, error);
          // Re-throw to prevent offset commit - message will be retried
          throw error;
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
    const { getConsumer, markConsumerDisconnected } = require('../config/kafka');
    const consumer = await getConsumer();
    try {
      await consumer.disconnect();
    } catch (disconnectErr) {
      console.warn('[KAFKA-CONSUMER] Error during disconnect (non-fatal):', disconnectErr?.message);
    }
    markConsumerDisconnected();
    consumerRunning = false;
    console.log('🛑 [KAFKA-CONSUMER] Consumer stopped');
  } catch (error) {
    console.error('❌ [KAFKA-CONSUMER] Error stopping consumer:', error);
    // Mark as disconnected even if there was an error
    const { markConsumerDisconnected } = require('../config/kafka');
    markConsumerDisconnected();
    consumerRunning = false;
  }
}

module.exports = { startConsumer, stopConsumer };

