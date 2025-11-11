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
  
  // Track all uniqueIds for calls initiated in this campaign
  const activeUniqueIds = new Set();
  
  // Process batches asynchronously (non-blocking)
  processBatchesAsync(campaign, contactBatches, agent, activeUniqueIds, n, g, mode).catch(err => {
    console.error(`[KAFKA-CONSUMER] Error processing batches for campaign ${campaignId}:`, err);
  });
  
  // Start background monitoring for campaign completion (non-blocking)
  monitorCampaignCompletion(campaign._id, activeUniqueIds).catch(err => {
    console.error(`[KAFKA-CONSUMER] Error monitoring campaign completion:`, err);
  });
  
  console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} batch processing started - running in background`);
}

// Non-blocking batch processing
async function processBatchesAsync(campaign, contactBatches, agent, activeUniqueIds, n, g, mode) {
  const campaignId = campaign._id;
  
  for (let batchIndex = 0; batchIndex < contactBatches.length; batchIndex++) {
    const batch = contactBatches[batchIndex];
    
    if (mode === 'series') {
      // Series mode: process one at a time
      for (const contact of batch) {
        const uniqueId = handleCall(campaign, contact, agent, activeUniqueIds);
        if (uniqueId) {
          // Use setTimeout to add to set asynchronously (non-blocking)
          setTimeout(() => {
            activeUniqueIds.add(uniqueId);
          }, 0);
        }
        // Small delay between calls in series mode
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    } else {
      // Parallel mode: process all in batch simultaneously (non-blocking)
      const callPromises = batch.map(contact => {
        return Promise.resolve(handleCall(campaign, contact, agent, activeUniqueIds)).then(uniqueId => {
          if (uniqueId) {
            activeUniqueIds.add(uniqueId);
          }
          return uniqueId;
        });
      });
      
      // Don't await - let them run in parallel
      Promise.all(callPromises).catch(err => {
        console.error(`[KAFKA-CONSUMER] Error in parallel batch processing:`, err);
      });
    }
    
    // Wait for gap (G) between batches if specified (non-blocking delay)
    if (g && g > 0 && batchIndex < contactBatches.length - 1) {
      console.log(`⏳ [KAFKA-CONSUMER] Waiting ${g}s before next batch...`);
      await new Promise(resolve => setTimeout(resolve, g * 1000));
    }
  }
  
  console.log(`✅ [KAFKA-CONSUMER] All batches processed for campaign ${campaignId}`);
}

// Non-blocking campaign completion monitoring
async function monitorCampaignCompletion(campaignId, activeUniqueIds) {
  const maxWaitTime = 30 * 60 * 1000; // 30 minutes max wait
  const checkInterval = 5000; // Check every 5 seconds
  const startTime = Date.now();
  
  // Wait a bit for calls to start
  await new Promise(resolve => setTimeout(resolve, 10000)); // Wait 10s for initial calls
  
  while ((Date.now() - startTime) < maxWaitTime) {
    // Check if campaign is still running
    const campaign = await Campaign.findById(campaignId).lean();
    if (!campaign || !campaign.isRunning) {
      console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} stopped externally`);
      break;
    }
    
    // Check which calls are still active (non-blocking query)
    if (activeUniqueIds.size > 0) {
      try {
        const activeCalls = await CallLog.find({
          campaignId,
          'metadata.customParams.uniqueid': { $in: Array.from(activeUniqueIds) },
          'metadata.isActive': true
        }).select('metadata.customParams.uniqueid').lean();
        
        const stillActiveUniqueIds = new Set(
          activeCalls.map(call => call.metadata?.customParams?.uniqueid).filter(Boolean)
        );
        
        // Remove finished calls from tracking
        for (const uniqueId of activeUniqueIds) {
          if (!stillActiveUniqueIds.has(uniqueId)) {
            activeUniqueIds.delete(uniqueId);
            console.log(`✅ [KAFKA-CONSUMER] Call ${uniqueId} finished, ${activeUniqueIds.size} calls remaining`);
          }
        }
        
        // If no active calls left, mark campaign as completed
        if (activeUniqueIds.size === 0) {
          console.log(`✅ [KAFKA-CONSUMER] All calls finished for campaign ${campaignId}`);
          break;
        }
      } catch (err) {
        console.error(`[KAFKA-CONSUMER] Error checking active calls:`, err);
      }
    } else {
      // No active calls to track, wait a bit more in case calls are still starting
      await new Promise(resolve => setTimeout(resolve, checkInterval * 2));
      if (activeUniqueIds.size === 0) {
        break;
      }
    }
    
    // Wait before next check (non-blocking)
    await new Promise(resolve => setTimeout(resolve, checkInterval));
  }
  
  // Mark campaign as completed (non-blocking)
  try {
    await Campaign.findByIdAndUpdate(
      campaignId,
      {
        $set: {
          isRunning: false,
          updatedAt: new Date()
        }
      }
    );
    
    // Broadcast completion (non-blocking)
    wsServer.broadcastCampaignEvent(campaignId, 'stop', { _id: campaignId });
    kafkaService.send('campaign-status', { campaignId, status: 'stop' }).catch(() => {});
    
    // Persist campaign run summary into CampaignHistory (non-blocking)
    (async () => {
      try {
        const CampaignHistory = require('../models/CampaignHistory');
        // Build contacts summary from CallLogs
        const logs = await CallLog.find({ campaignId }).lean();
        const contacts = logs.map(l => ({
          documentId: l._id ? String(l._id) : undefined,
          number: l.mobile,
          name: undefined,
          leadStatus: l.leadStatus || null,
          contactId: String(l.campaignId || '') || undefined,
          time: l.createdAt ? new Date(l.createdAt).toISOString() : undefined,
          status: l.metadata?.isActive ? 'ongoing' : (l.metadata?.callEndTime ? 'completed' : (l.leadStatus || 'not_connected')),
          duration: typeof l.duration === 'number' ? l.duration : 0,
          transcriptCount: l.metadata?.totalUpdates || 0,
          whatsappMessageSent: !!l.metadata?.whatsappMessageSent,
          whatsappRequested: !!l.metadata?.whatsappRequested
        }));
        const stats = {
          totalContacts: contacts.length,
          successfulCalls: contacts.filter(c => c.status === 'completed').length,
          failedCalls: contacts.filter(c => c.status === 'not_connected' || c.status === 'failed').length,
          totalCallDuration: contacts.reduce((a, c) => a + (c.duration || 0), 0)
        };
        stats.averageCallDuration = stats.totalContacts > 0 ? Math.round(stats.totalCallDuration / stats.totalContacts) : 0;
        // Determine instance number
        const existingCount = await CampaignHistory.countDocuments({ campaignId });
        const instanceNumber = existingCount + 1;
        const runId = `${String(campaignId)}-${Date.now()}`;
        const now = new Date();
        await CampaignHistory.create({
          campaignId,
          runId,
          instanceNumber,
          startTime: new Date(startTime).toISOString(),
          endTime: now.toISOString(),
          runTime: {
            hours: Math.floor((now - startTime) / 3600000),
            minutes: Math.floor(((now - startTime) % 3600000) / 60000),
            seconds: Math.floor(((now - startTime) % 60000) / 1000)
          },
          status: 'completed',
          contacts,
          stats,
          batchInfo: {
            isIntermediate: false
          }
        });
        console.log(`📝 [KAFKA-CONSUMER] CampaignHistory saved for campaign ${campaignId}`);
      } catch (e) {
        console.error(`[KAFKA-CONSUMER] Failed to save CampaignHistory for ${campaignId}:`, e?.message || e);
      }
    })().catch(() => {});
    
    if (activeUniqueIds.size > 0) {
      console.warn(`⚠️ [KAFKA-CONSUMER] Timeout waiting for ${activeUniqueIds.size} calls to finish. Marking campaign as completed anyway.`);
    } else {
      console.log(`✅ [KAFKA-CONSUMER] Campaign ${campaignId} completed successfully - all calls finished`);
    }
  } catch (err) {
    console.error(`[KAFKA-CONSUMER] Error marking campaign as completed:`, err);
  }
}

// Non-blocking call handler with real-time status updates
async function handleCall(campaign, contact, agent, activeUniqueIds = null) {
  const uniqueid = uuidv4();
  const phone = contact.phone;
  const campaignId = campaign._id;
  
  console.log(`📞 [KAFKA-CONSUMER] Initiating call for campaign ${campaignId}, contact: ${phone}, agent: ${agent._id || agent.agentId}, provider: ${agent.serviceProvider}`);
  
  // Validate required fields
  if (!phone) {
    console.error(`❌ [KAFKA-CONSUMER] Missing phone number for contact:`, contact);
    return null;
  }
  
  if (!agent.serviceProvider) {
    console.error(`❌ [KAFKA-CONSUMER] Agent ${agent._id || agent.agentId} missing serviceProvider. Available fields:`, Object.keys(agent));
    return null;
  }

  // Immediately create initial detail entry with "ringing" status
  const initialDetail = {
    uniqueId: uniqueid,
    contactId: contact._id || contact.id,
    time: new Date(),
    status: 'ringing',
    runId: campaignId.toString()
  };

  // Save initial detail (non-blocking - fire and forget)
  Campaign.findByIdAndUpdate(
    campaignId,
    {
      $push: { details: initialDetail },
      $set: { updatedAt: new Date() }
    },
    { new: false }
  ).catch(err => console.error(`[KAFKA-CONSUMER] Error saving initial detail:`, err));

  // Broadcast initial "ringing" status immediately
  wsServer.broadcastCallEvent(campaignId, uniqueid, 'ringing', null, phone);
  kafkaService.send('call-status', { campaignId, uniqueid, status: 'ringing', mobile: phone }).catch(() => {});

  // Start non-blocking call initiation and status tracking
  trackCallStatus(campaign, contact, agent, uniqueid, phone, activeUniqueIds).catch(err => {
    console.error(`[KAFKA-CONSUMER] Error tracking call ${uniqueid}:`, err);
  });

  // Return immediately - don't wait for call to complete
  return uniqueid;
}

// Non-blocking function to track call status in background
async function trackCallStatus(campaign, contact, agent, uniqueid, phone, activeUniqueIds = null) {
  const campaignId = campaign._id;
  let callInitiated = false;
  let res;

  try {
    // Initiate call (non-blocking)
    if (agent.serviceProvider === "c-zentrix" || agent.serviceProvider === "czentrix") {
      console.log(`📞 [KAFKA-CONSUMER] Calling via C-Zentrix: ${phone}`);
      if (!agent.callerId) {
        throw new Error('Agent missing callerId for C-Zentrix call');
      }
      res = await telephonyService.callCzentrix({
        phone: contact.phone,
        agent,
        contact,
        campaignId: campaignId,
        uniqueid
      });
      callInitiated = true;
      
      if (res && (res.error || res.status === 'failed')) {
        console.warn(`⚠️ [KAFKA-CONSUMER] C-Zentrix call failed:`, res.msg || res.message || res);
      }
    } else if (agent.serviceProvider === "sanpbx" || agent.serviceProvider === "snapbx") {
      console.log(`📞 [KAFKA-CONSUMER] Calling via SANPBX: ${phone}`);
      if (!agent.callerId) {
        throw new Error('Agent missing callerId for SANPBX call');
      }
      if (!agent.accessToken) {
        throw new Error('Agent missing accessToken for SANPBX call');
      }
      res = await telephonyService.callSanpbx({
        phone: contact.phone,
        agent,
        contact,
        uniqueid
      });
      callInitiated = true;
      
      if (res && (res.error || res.status === 'failed')) {
        console.warn(`⚠️ [KAFKA-CONSUMER] SANPBX call failed:`, res.msg || res.message || res);
      }
    } else {
      throw new Error(`Unknown service provider: ${agent.serviceProvider}`);
    }
  } catch (error) {
    console.error(`❌ [KAFKA-CONSUMER] Error initiating call to ${phone}:`, error?.message || error);
    callInitiated = false;
  }

  // Non-blocking status polling with real-time updates
  let log = null;
  let waited = 0;
  const maxWait = 40000; // 40 seconds
  const pollInterval = 2000; // Check every 2 seconds
  let lastStatus = 'ringing';

  while (waited < maxWait) {
    // Check for call log (non-blocking query)
    try {
      log = await CallLog.findOne({ "metadata.customParams.uniqueid": uniqueid }).lean();
    } catch (err) {
      console.error(`[KAFKA-CONSUMER] Error querying call log:`, err);
    }

    if (log) {
      // Call log found - determine status
      const isActive = log.metadata?.isActive || false;
      const newStatus = isActive ? 'ongoing' : 'completed';
      
      // Only broadcast if status changed
      if (newStatus !== lastStatus) {
        lastStatus = newStatus;
        console.log(`✅ [KAFKA-CONSUMER] Call log found for ${phone}, uniqueid: ${uniqueid}, status: ${newStatus}`);
        
        // Update detail in campaign
        const detail = {
          uniqueId: uniqueid,
          contactId: contact._id || contact.id,
          time: new Date(),
          status: newStatus,
          runId: campaignId.toString()
        };
        
        if (log.leadStatus) {
          detail.leadStatus = log.leadStatus;
        }

        // Update campaign detail (non-blocking)
        Campaign.findByIdAndUpdate(
          campaignId,
          {
            $set: {
              'details.$[elem].status': newStatus,
              'details.$[elem].leadStatus': log.leadStatus || undefined,
              updatedAt: new Date()
            }
          },
          {
            arrayFilters: [{ 'elem.uniqueId': uniqueid }],
            new: false
          }
        ).catch(err => console.error(`[KAFKA-CONSUMER] Error updating detail:`, err));

        // Broadcast status update (non-blocking)
        wsServer.broadcastCallEvent(campaignId, uniqueid, newStatus, log, phone);
        kafkaService.send('call-status', { 
          campaignId, 
          uniqueid, 
          status: newStatus, 
          mobile: phone,
          isActive,
          leadStatus: log.leadStatus
        }).catch(() => {});

        // If call is active, add to tracking set
        if (isActive && activeUniqueIds) {
          activeUniqueIds.add(uniqueid);
        }

        // Publish transcript update if available (non-blocking)
        if (log.transcript) {
          kafkaService.send('call-transcript-update', {
            campaignId,
            uniqueId: uniqueid,
            transcript: log.transcript,
            mobile: phone,
            status: isActive ? 'ongoing' : 'completed',
            isActive
          }).catch(() => {});
        }

        // If call is completed, stop polling
        if (!isActive) {
          break;
        }
      } else if (isActive && log.transcript) {
        // Call is ongoing - check for transcript updates (non-blocking)
        kafkaService.send('call-transcript-update', {
          campaignId,
          uniqueId: uniqueid,
          transcript: log.transcript,
          mobile: phone,
          status: 'ongoing',
          isActive: true
        }).catch(() => {});
      }
    } else if (waited >= maxWait - pollInterval) {
      // No log found after max wait - mark as not_connected
      if (lastStatus !== 'not_connected') {
        lastStatus = 'not_connected';
        console.warn(`⚠️ [KAFKA-CONSUMER] Call log not found after ${maxWait/1000}s for ${phone}, uniqueid: ${uniqueid}. Marking as not_connected.`);
        
        // Update detail status to not_connected
        Campaign.findByIdAndUpdate(
          campaignId,
          {
            $set: {
              'details.$[elem].status': 'not_connected',
              updatedAt: new Date()
            }
          },
          {
            arrayFilters: [{ 'elem.uniqueId': uniqueid }],
            new: false
          }
        ).catch(err => console.error(`[KAFKA-CONSUMER] Error updating detail to not_connected:`, err));

        // Broadcast not_connected status
        wsServer.broadcastCallEvent(campaignId, uniqueid, 'not_connected', null, phone);
        kafkaService.send('call-status', { 
          campaignId, 
          uniqueid, 
          status: 'not_connected', 
          mobile: phone 
        }).catch(() => {});
      }
      break;
    }

    // Wait before next poll (non-blocking)
    await new Promise(resolve => setTimeout(resolve, pollInterval));
    waited += pollInterval;
  }

  // Continue monitoring active calls for status changes
  if (log && log.metadata?.isActive) {
    monitorActiveCall(campaignId, uniqueid, phone, activeUniqueIds).catch(err => {
      console.error(`[KAFKA-CONSUMER] Error monitoring active call ${uniqueid}:`, err);
    });
  }
}

// Monitor active calls for status changes (completion, transcript updates)
async function monitorActiveCall(campaignId, uniqueid, phone, activeUniqueIds = null) {
  const checkInterval = 5000; // Check every 5 seconds
  let lastTranscript = '';
  
  while (true) {
    try {
      const log = await CallLog.findOne({ "metadata.customParams.uniqueid": uniqueid }).lean();
      
      if (!log) {
        // Call log disappeared - mark as completed
        break;
      }

      const isActive = log.metadata?.isActive || false;
      
      if (!isActive) {
        // Call completed
        const leadStatus = log.leadStatus || 'not_connected';
        
        // Update campaign detail
        Campaign.findByIdAndUpdate(
          campaignId,
          {
            $set: {
              'details.$[elem].status': 'completed',
              'details.$[elem].leadStatus': leadStatus,
              updatedAt: new Date()
            }
          },
          {
            arrayFilters: [{ 'elem.uniqueId': uniqueid }],
            new: false
          }
        ).catch(() => {});

        // Broadcast completion
        wsServer.broadcastCallEvent(campaignId, uniqueid, 'completed', log, phone);
        kafkaService.send('call-status', { 
          campaignId, 
          uniqueid, 
          status: 'completed', 
          mobile: phone,
          isActive: false,
          leadStatus
        }).catch(() => {});

        // Remove from active tracking
        if (activeUniqueIds) {
          activeUniqueIds.delete(uniqueid);
        }
        
        break;
      }

      // Check for transcript updates
      const currentTranscript = log.transcript || '';
      if (currentTranscript !== lastTranscript && currentTranscript) {
        lastTranscript = currentTranscript;
        
        // Broadcast transcript update
        kafkaService.send('call-transcript-update', {
          campaignId,
          uniqueId: uniqueid,
          transcript: currentTranscript,
          mobile: phone,
          status: 'ongoing',
          isActive: true
        }).catch(() => {});
      }

      // Wait before next check
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    } catch (error) {
      console.error(`[KAFKA-CONSUMER] Error monitoring call ${uniqueid}:`, error);
      // Wait before retry
      await new Promise(resolve => setTimeout(resolve, checkInterval));
    }
  }
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

