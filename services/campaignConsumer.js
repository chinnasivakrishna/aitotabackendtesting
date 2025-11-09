const { getConsumer, ensureTopics } = require('../config/kafka');
const Campaign = require('../models/Campaign');
const Agent = require('../models/Agent');
const CallLog = require('../models/CallLog');
const telephonyService = require('./telephonyService');
const kafkaService = require('./kafkaService');
const wsServer = require('../sockets/wsServer');
const { v4: uuidv4 } = require('uuid');

let consumerRunning = false;

async function processCampaignCommand(message, commitOffsetCallback = null) {
  try {
    const command = JSON.parse(message.value.toString());
    console.log(`📥 [KAFKA-CONSUMER] Received campaign command:`, command);

    // Idempotency check: Ignore messages older than 5 minutes (likely from before restart)
    if (command.timestamp) {
      const messageAge = Date.now() - new Date(command.timestamp).getTime();
      const maxAge = 5 * 60 * 1000; // 5 minutes
      if (messageAge > maxAge) {
        console.warn(`⚠️ [KAFKA-CONSUMER] Ignoring stale message (age: ${Math.round(messageAge / 1000)}s):`, command);
        // Commit offset for stale messages to prevent reprocessing
        if (commitOffsetCallback) await commitOffsetCallback();
        return; // Skip processing but commit offset
      }
    }

    if (command.action === 'start') {
      await handleStartCampaign(command, commitOffsetCallback);
    } else if (command.action === 'stop') {
      await handleStopCampaign(command);
      // Commit offset immediately for stop (quick operation)
      if (commitOffsetCallback) await commitOffsetCallback();
    } else if (command.action === 'pause') {
      await handlePauseCampaign(command);
      // Commit offset immediately for pause (quick operation)
      if (commitOffsetCallback) await commitOffsetCallback();
    } else if (command.action === 'resume') {
      await handleResumeCampaign(command);
      // Commit offset immediately for resume (quick operation)
      if (commitOffsetCallback) await commitOffsetCallback();
    } else {
      console.warn(`⚠️ [KAFKA-CONSUMER] Unknown action: ${command.action}`);
      // Commit offset for unknown actions to prevent reprocessing
      if (commitOffsetCallback) await commitOffsetCallback();
    }
  } catch (error) {
    console.error(`❌ [KAFKA-CONSUMER] Error processing campaign command:`, error);
    throw error; // Re-throw to prevent offset commit - message will be retried
  }
}

async function handleStartCampaign(command, commitOffsetCallback = null) {
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
      updatedAt: currentState.updatedAt
    });
    
    if (currentState.isRunning) {
      console.warn(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} is already running, skipping duplicate start command`);
      // Commit offset for duplicate start commands to prevent reprocessing
      if (commitOffsetCallback) await commitOffsetCallback();
      return; // Don't throw - offset already committed
    } else {
      // This shouldn't happen, but if isRunning is false and update failed, retry once
      console.warn(`⚠️ [KAFKA-CONSUMER] Campaign ${campaignId} update failed despite isRunning=false, retrying...`);
      const retryCampaign = await Campaign.findOneAndUpdate(
        { _id: campaignId, isRunning: false },
        { $set: { isRunning: true, updatedAt: new Date() } },
        { new: true }
      );
      if (!retryCampaign) {
        console.error(`❌ [KAFKA-CONSUMER] Retry also failed for campaign ${campaignId}`);
        // Commit offset even on retry failure to prevent infinite reprocessing
        if (commitOffsetCallback) await commitOffsetCallback();
        return;
      }
      // Use retry result - reassign campaign variable
      campaign = retryCampaign;
      console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} marked as running on retry, proceeding with execution`);
      // Continue with campaign processing below (campaign is now set)
    }
  }

  console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} marked as running, proceeding with execution`);

  // Broadcast start event now that we've successfully marked it as running
  wsServer.broadcastCampaignEvent(campaign._id, 'start', campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: 'start' });

  // IMPORTANT: Commit offset immediately after marking campaign as running
  // This prevents reprocessing if consumer is removed from group during long campaign execution
  if (commitOffsetCallback) {
    await commitOffsetCallback();
    console.log(`✅ [KAFKA-CONSUMER] Offset committed for campaign ${campaignId} - campaign will continue processing even if consumer is removed`);
  }

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
  // Use atomic update to prevent race conditions
  await Campaign.findByIdAndUpdate(
    campaign._id,
    {
      $set: {
        isRunning: false,
        updatedAt: new Date()
      }
    }
  );
  
  // Broadcast completion
  wsServer.broadcastCampaignEvent(campaign._id, 'stop', campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: 'stop' });
  
  console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} completed successfully`);
}

async function handleCall(campaign, contact, agent) {
  const uniqueid = uuidv4();
  const phone = contact.phone;
  
  console.log(`📞 [KAFKA-CONSUMER] Initiating call for campaign ${campaign._id}, contact: ${phone}, agent: ${agent._id || agent.agentId}, provider: ${agent.serviceProvider}`);
  
  // Validate required fields
  if (!phone) {
    console.error(`❌ [KAFKA-CONSUMER] Missing phone number for contact:`, contact);
    return;
  }
  
  if (!agent.serviceProvider) {
    console.error(`❌ [KAFKA-CONSUMER] Agent ${agent._id || agent.agentId} missing serviceProvider. Available fields:`, Object.keys(agent));
    return;
  }
  
  let res;
  let callInitiated = false;
  try {
    if (agent.serviceProvider === "c-zentrix" || agent.serviceProvider === "czentrix") {
      console.log(`📞 [KAFKA-CONSUMER] Calling via C-Zentrix: ${phone}`);
      if (!agent.callerId) {
        console.error(`❌ [KAFKA-CONSUMER] Agent missing callerId for C-Zentrix call`);
        throw new Error('Agent missing callerId for C-Zentrix call');
      }
      res = await telephonyService.callCzentrix({
        phone: contact.phone,
        agent,
        contact,
        campaignId: campaign._id,
        uniqueid
      });
      console.log(`✅ [KAFKA-CONSUMER] C-Zentrix call initiated. Response:`, res);
      callInitiated = true;
      
      // Check if response indicates failure
      if (res && (res.error || res.status === 'failed')) {
        console.warn(`⚠️ [KAFKA-CONSUMER] C-Zentrix call failed:`, res.msg || res.message || res);
        // Still wait for call log in case it gets created
      }
    } else if (agent.serviceProvider === "sanpbx" || agent.serviceProvider === "snapbx") {
      console.log(`📞 [KAFKA-CONSUMER] Calling via SANPBX: ${phone}`);
      if (!agent.callerId) {
        console.error(`❌ [KAFKA-CONSUMER] Agent missing callerId for SANPBX call`);
        throw new Error('Agent missing callerId for SANPBX call');
      }
      if (!agent.accessToken) {
        console.error(`❌ [KAFKA-CONSUMER] Agent missing accessToken for SANPBX call`);
        throw new Error('Agent missing accessToken for SANPBX call');
      }
      res = await telephonyService.callSanpbx({
        phone: contact.phone,
        agent,
        contact,
        uniqueid
      });
      console.log(`✅ [KAFKA-CONSUMER] SANPBX call initiated. Response:`, res);
      callInitiated = true;
      
      // Check if response indicates failure
      if (res && (res.error || res.status === 'failed')) {
        console.warn(`⚠️ [KAFKA-CONSUMER] SANPBX call failed:`, res.msg || res.message || res);
        // Still wait for call log in case it gets created
      }
    } else {
      console.error(`❌ [KAFKA-CONSUMER] Unknown service provider: ${agent.serviceProvider}. Expected: c-zentrix, czentrix, sanpbx, or snapbx`);
      throw new Error(`Unknown service provider: ${agent.serviceProvider}`);
    }
  } catch (error) {
    console.error(`❌ [KAFKA-CONSUMER] Error initiating call to ${phone}:`, error?.message || error);
    console.error(`   Stack:`, error?.stack);
    // Mark as failed but continue to check for call log (in case it was created before error)
    callInitiated = false;
  }
  
  // Wait for call log to be created (telephony service should create it)
  let log = null;
  let waited = 0;
  const maxWait = 40000; // 40 seconds
  while (waited < maxWait) {
    log = await CallLog.findOne({ "metadata.customParams.uniqueid": uniqueid });
    if (log) {
      console.log(`✅ [KAFKA-CONSUMER] Call log found for ${phone}, uniqueid: ${uniqueid}`);
      break;
    }
    await new Promise(res => setTimeout(res, 2000));
    waited += 2000;
  }
  
  if (!log) {
    console.warn(`⚠️ [KAFKA-CONSUMER] Call log not found after ${maxWait/1000}s for ${phone}, uniqueid: ${uniqueid}. Call may have failed to initiate.`);
  }
  
  let status = "ringing";
  if (log && log.isActive) status = "ongoing";
  else if (log && !log.isActive) status = "completed";
  else if (!log) status = "failed"; // Mark as failed if no log was created
  
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
  
  console.log(`📊 [KAFKA-CONSUMER] Call detail saved for ${phone}: status=${status}, uniqueid=${uniqueid}`);
}

async function handleStopCampaign(command) {
  const { campaignId } = command;
  
  // Use atomic update to ensure stop is applied
  // Note: Campaign model may not have 'status' field, so we only update isRunning
  const campaign = await Campaign.findByIdAndUpdate(
    campaignId,
    {
      $set: {
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
          
          // Create commit callback for immediate offset commit after state update
          const commitOffset = async () => {
            try {
              await consumerInstance.commitOffsets([{
                topic,
                partition,
                offset: (parseInt(message.offset) + 1).toString() // Commit next offset
              }]);
              console.log(`✅ [KAFKA-CONSUMER] Offset ${message.offset} committed successfully`);
            } catch (commitError) {
              console.error(`❌ [KAFKA-CONSUMER] Failed to commit offset ${message.offset}:`, commitError?.message);
            }
          };
          
          // Process the command - commit happens inside for start commands (after marking as running)
          // For other commands, commit happens in processCampaignCommand
          await processCampaignCommand(message, commitOffset);
          
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

