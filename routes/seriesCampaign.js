const express = require('express');
const router = express.Router();

const Campaign = require('../models/Campaign');
const CallLog = require('../models/CallLog');
const Agent = require('../models/Agent');
const { makeSingleCall } = require('../services/campaignCallingService');
const axios = require('axios');
const serviceUtils = require('../services/campaignCallingService'); // for getClientApiKey

// In-memory trackers for sequential runs
const sequentialRuns = new Map(); // campaignId -> { isRunning, currentIndex, startedAt, runId, lastHeartbeat }

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitUntilCallInactive(uniqueId, callStartTime, contactPhone = '', minNoLogWaitMs = 40000, maxWaitMs = 8 * 60 * 1000, pollMs = 3000, campaignId = null) {
  const start = Date.now();
  console.log(`⏳ SERIES: Waiting for call ${uniqueId} to become inactive (min no-log ${Math.round(minNoLogWaitMs/1000)}s)`);
  let isInactive = false;
  let everLogged = false;
  let everActive = false;
  const digitsOnly = (s) => String(s || '').replace(/\D/g, '');
  const normalizedMobile = digitsOnly(contactPhone);
  let matchedLogId = null;
  let lastHeartbeatUpdate = Date.now();
  
  while (Date.now() - start < maxWaitMs) {
    try {
      let log = null;
      if (matchedLogId) {
        log = await CallLog.findById(matchedLogId).lean();
      }
      if (!log) {
        // Try by uniqueId first
        log = await CallLog.findOne({ 'metadata.customParams.uniqueid': uniqueId })
          .sort({ updatedAt: -1 })
          .lean();
      }
      if (!log && normalizedMobile) {
        // Fallback by mobile within recent window (since callStartTime)
        const since = callStartTime ? new Date(callStartTime) : new Date(Date.now() - 60000);
        log = await CallLog.findOne({ mobile: normalizedMobile, createdAt: { $gte: since } })
          .sort({ updatedAt: -1 })
          .lean();
      }
      const active = log?.metadata?.isActive === true;
      if (log) everLogged = true;
      if (active) everActive = true;
      console.log(`🔍 SERIES: Poll uniqueId=${uniqueId} isActive=${active === true} leadStatus=${log?.leadStatus || ''}`);
      if (log && !matchedLogId) {
        matchedLogId = log._id;
      }
      if (!active) {
        isInactive = true;
        break;
      }
    } catch {}
    
    // Update heartbeat every 30 seconds to prevent stale detection
    if (campaignId && Date.now() - lastHeartbeatUpdate > 30000) {
      try {
        const state = sequentialRuns.get(campaignId);
        if (state) {
          state.lastHeartbeat = Date.now();
          sequentialRuns.set(campaignId, state);
          lastHeartbeatUpdate = Date.now();
        }
      } catch {}
    }
    
    // If we have never seen a log, ensure we wait at least minNoLogWaitMs before giving up
    if (!everLogged && Date.now() - start >= minNoLogWaitMs) {
      break;
    }
    await sleep(pollMs);
  }
  // Enforce minimum wait only if the call never became active (no conversation)
  const waited = Date.now() - start;
  if (!everLogged && waited < minNoLogWaitMs) {
    const remaining = minNoLogWaitMs - waited;
    console.log(`⏳ SERIES: Enforcing min no-log wait, sleeping extra ${Math.round(remaining/1000)}s for ${uniqueId}`);
    await sleep(remaining);
  }
  const durationSec = Math.max(0, Math.floor(((callStartTime ? new Date(callStartTime) : new Date()) - 0) / 1000));
  return { inactive: isInactive, everLogged, waitedMs: Date.now() - start, durationSec, everActive };
}

// Normalize provider names to a canonical form
function normalizeProviderName(raw) {
  const p = String(raw || '').toLowerCase().trim();
  if (!p) return '';
  // Treat common variations as the same
  if (p === 'c-zentrix' || p === 'czentrix' || p === 'c-zentrax' || p === 'czentrax') return 'czentrix';
  if (p === 'sanpbx' || p === 'snapbx') return 'sanpbx';
  return p;
}

async function runSeries({ campaignId, agentId, apiKey, clientId, minDelayMs = 40000 }) {
  const runId = `series-${campaignId}-${Date.now()}`;
  let state = sequentialRuns.get(campaignId);
  if (!state) {
    state = { isRunning: true, currentIndex: 0, startedAt: new Date(), runId, lastHeartbeat: Date.now(), inCall: false };
    sequentialRuns.set(campaignId, state);
  } else {
    // Always reset index for a fresh series run
    state.isRunning = true;
    state.currentIndex = 0;
    state.startedAt = new Date();
    state.runId = runId;
    state.lastHeartbeat = Date.now();
    state.inCall = false;
    sequentialRuns.set(campaignId, state);
  }
  let successfulCalls = 0;
  let failedCalls = 0;
  let completedCalls = 0;

  console.log(`🚀 SERIES: Starting series run for campaign=${campaignId}`);
  const campaign = await Campaign.findById(campaignId);
  if (!campaign || !Array.isArray(campaign.contacts) || campaign.contacts.length === 0) {
    console.log(`❌ SERIES: Campaign not found or no contacts`);
    state.isRunning = false;
    return { started: false, reason: 'No contacts' };
  }
  console.log(`ℹ️ SERIES: Found ${campaign.contacts.length} contacts for campaign ${campaignId}`);

  // Resolve agent if not provided
  if (!agentId) {
    const primary = Array.isArray(campaign.agent) && campaign.agent.length > 0 ? campaign.agent[0] : null;
    agentId = primary;
  }
  const agent = await Agent.findById(agentId).lean();
  if (!agent) {
    console.log(`❌ SERIES: Agent not found agentId=${agentId}`);
    state.isRunning = false;
    return { started: false, reason: 'Agent not found' };
  }

  // Resolve clientId from campaign if not provided
  if (!clientId) {
    try { clientId = campaign.clientId ? String(campaign.clientId) : null; } catch (_) { clientId = null; }
  }

  // Determine provider and validate required credentials BEFORE starting
  const provider = normalizeProviderName(agent.serviceProvider);
  let resolvedApiKey = apiKey || null;
  if (provider === 'sanpbx') {
    const hasSanCreds = !!(agent?.accessToken && agent?.accessKey && agent?.callerId);
    if (!hasSanCreds) {
      console.log(`❌ SERIES: Missing SANPBX credentials (accessToken/accessKey/callerId)`);
      state.isRunning = false;
      return { started: false, reason: 'Missing SANPBX credentials on agent (accessToken, accessKey, callerId)' };
    }
  } else if (provider === 'czentrix') {
    // Resolve API key from client or agent if not explicitly provided
    if (!resolvedApiKey) {
      try {
        const clientKey = clientId ? await serviceUtils.getClientApiKey(clientId) : null;
        if (clientKey) resolvedApiKey = clientKey;
      } catch (_) {}
    }
    if (!resolvedApiKey && agent?.X_API_KEY) {
      resolvedApiKey = agent.X_API_KEY;
    }
    if (!resolvedApiKey) {
      console.log(`❌ SERIES: Missing API key for czentrix provider`);
      state.isRunning = false;
      return { started: false, reason: 'Missing API key for czentrix provider (provide apiKey, client API key, or Agent.X_API_KEY)' };
    }
  } else {
    // Unknown or unsupported provider
    if (!provider) {
      console.log(`❌ SERIES: Agent serviceProvider not set`);
      state.isRunning = false;
      return { started: false, reason: 'Agent serviceProvider not set' };
    }
    console.log(`ℹ️ SERIES: Provider ${provider} - proceeding with default flow`);
  }

  // No API key required for SANPBX; czentrix requires apiKey
  if (!resolvedApiKey && provider !== 'sanpbx') {
    console.log(`ℹ️ SERIES: No API key provided after resolution; calls may fail`);
  }

  // Mark campaign running in DB and clear details for this run only if starting fresh
  try {
    await Campaign.updateOne({ _id: campaignId }, { $set: { isRunning: true } });
    console.log(`▶️ SERIES: Marked campaign ${campaignId} running`);
  } catch {}

  for (let i = state.currentIndex; i < campaign.contacts.length; i++) {
    // Stop if requested
    const latest = sequentialRuns.get(campaignId);
    if (!latest || !latest.isRunning) break;

    const contact = campaign.contacts[i];
    // Ensure only one call at a time per campaign
    while (true) {
      const s = sequentialRuns.get(campaignId);
      if (!s?.inCall) break;
      await sleep(1000);
    }
    // Defend against background auto-stop setting isRunning=false between calls
    try { await Campaign.updateOne({ _id: campaignId }, { $set: { isRunning: true } }); } catch {}
    console.log(`📞 SERIES: Dialing index=${i + 1}/${campaign.contacts.length} phone=${String(contact?.phone || contact?.number || '').replace(/\D/g,'').slice(-6).padStart(6,'*')}`);
    state.currentIndex = i;
    state.lastHeartbeat = Date.now();
    state.inCall = true;
    sequentialRuns.set(campaignId, state);

    // Timestamp for this call
    const initiatedAt = new Date();
    // Place call
    let callResult;
    try {
      // If SANPBX, apiKey is not used; makeSingleCall will branch by agent.serviceProvider
      callResult = await makeSingleCall(contact, agentId, resolvedApiKey || undefined, campaignId, clientId, runId, null);
      console.log(`✅ SERIES: makeSingleCall returned success=${!!callResult?.success} uniqueId=${callResult?.uniqueId || 'n/a'} provider=${callResult?.provider || ''}`);
      state.lastHeartbeat = Date.now();
      sequentialRuns.set(campaignId, state);
    } catch (err) {
      console.log(`❌ SERIES: makeSingleCall error: ${err?.message || err}`);
      callResult = { success: false, error: err?.message || 'unknown', uniqueId: `err-${Date.now()}` };
    }

    // Record call details on campaign (mirror parallel flow)
    try {
      const CampaignModel = Campaign; // reuse import
      if (callResult && callResult.success && callResult.uniqueId) {
        const callDetail = {
          uniqueId: callResult.uniqueId,
          contactId: contact._id || null,
          time: initiatedAt,
          status: 'ringing',
          lastStatusUpdate: initiatedAt,
          callDuration: 0,
          ...(runId ? { runId } : {})
        };
        await CampaignModel.updateOne(
          { _id: campaignId, 'details.uniqueId': { $ne: callResult.uniqueId } },
          { $push: { details: callDetail } }
        );
        // Schedule a status check after ~40s (similar to parallel)
        setTimeout(() => {
          try {
            console.log(`⏱️ SERIES: Scheduled 40s status check for ${callResult.uniqueId}`);
            require('../services/campaignCallingService').updateCallStatusFromLogs(campaignId, callResult.uniqueId);
          } catch {}
        }, 40000);
      } else if (callResult && callResult.uniqueId) {
        console.log(`📝 SERIES: Recording failed call ${callResult.uniqueId}`);
        const callDetail = {
          uniqueId: callResult.uniqueId,
          contactId: contact._id || null,
          time: initiatedAt,
          status: 'completed',
          lastStatusUpdate: initiatedAt,
          callDuration: 0,
          leadStatus: 'not_connected',
          ...(runId ? { runId } : {})
        };
        await CampaignModel.updateOne(
          { _id: campaignId, 'details.uniqueId': { $ne: callResult.uniqueId } },
          { $push: { details: callDetail } }
        );
      }
    } catch {}

    // If we have a uniqueId, wait for inactivity and minimum delay then update status accordingly
    if (callResult && callResult.uniqueId) {
      // Update heartbeat before starting the wait
      state.lastHeartbeat = Date.now();
      sequentialRuns.set(campaignId, state);
      
      const { inactive, everLogged, everActive } = await waitUntilCallInactive(callResult.uniqueId, initiatedAt, contact?.phone || contact?.number || '', 40000, 8 * 60 * 1000, 3000, campaignId);
      console.log(`🔚 SERIES: Call ${callResult.uniqueId} inactive=${inactive} everLogged=${everLogged} everActive=${everActive}`);
      
      // Update heartbeat after wait completes
      state.lastHeartbeat = Date.now();
      sequentialRuns.set(campaignId, state);
      
      try {
        const now = new Date();
        const duration = Math.max(0, Math.floor((now - initiatedAt) / 1000));
        if (!everLogged) {
          // No call log was ever created -> mark missed
          await Campaign.updateOne(
            { _id: campaignId, 'details.uniqueId': callResult.uniqueId },
            {
              $set: {
                'details.$.status': 'completed',
                'details.$.lastStatusUpdate': now,
                'details.$.callDuration': duration,
                'details.$.leadStatus': 'not_connected'
              }
            }
          );
          console.log(`📝 SERIES: Marked ${callResult.uniqueId} as missed (no call log found)`);
          failedCalls++;
        } else if (inactive) {
          // Call existed and ended -> mark completed with duration
          await Campaign.updateOne(
            { _id: campaignId, 'details.uniqueId': callResult.uniqueId },
            {
              $set: {
                'details.$.status': 'completed',
                'details.$.lastStatusUpdate': now,
                'details.$.callDuration': duration
              }
            }
          );
          console.log(`📝 SERIES: Marked ${callResult.uniqueId} as completed with duration=${duration}s`);
          successfulCalls++;
        }
      } catch (e) {
        console.log(`⚠️ SERIES: Post-wait status update failed for ${callResult.uniqueId}: ${e?.message || e}`);
      }
    } else {
      // No uniqueId (failed early) — still respect minimum delay before next call
      console.log(`⏳ SERIES: No uniqueId, sleeping ${Math.round(minDelayMs/1000)}s before next`);
      await sleep(minDelayMs);
      failedCalls++;
    }
    completedCalls++;
    // Release inCall lock before moving to next contact
    const after = sequentialRuns.get(campaignId) || {};
    after.inCall = false;
    after.lastHeartbeat = Date.now();
    sequentialRuns.set(campaignId, after);
  }

  state.isRunning = false;
  state.lastHeartbeat = Date.now();
  sequentialRuns.set(campaignId, state);
  // Mark campaign stopped in DB
  try {
    await Campaign.updateOne({ _id: campaignId }, { $set: { isRunning: false } });
    console.log(`⏹️ SERIES: Marked campaign ${campaignId} stopped`);
  } catch {}
  // Auto-save run history using CampaignHistory model (same as parallel campaigns)
  try {
    const CampaignHistory = require('../models/CampaignHistory');
    const fresh = await Campaign.findById(campaignId);
    if (!fresh) {
      console.log(`❌ SERIES: Campaign not found for history save: ${campaignId}`);
      return { started: true, runId };
    }

    // Get campaign details for this run
    const campaignDetails = Array.isArray(fresh.details) ? fresh.details : [];
    const runDetails = campaignDetails.filter(d => d && d.runId === runId);
    
    console.log(`💾 SERIES: Saving run history with ${runDetails.length} call details for runId=${runId}`);

    // Build contacts array for history
    const contactsById = new Map((fresh.contacts || []).map(c => [String(c._id || ''), c]));
    const contacts = runDetails.map(d => {
      const contact = contactsById.get(String(d.contactId || ''));
      return {
        contactId: d.contactId,
        phone: contact?.phone || contact?.number || '',
        name: contact?.name || '',
        uniqueId: d.uniqueId,
        status: d.status || 'completed',
        leadStatus: d.leadStatus || 'not_connected',
        callDuration: d.callDuration || 0,
        time: d.time || new Date()
      };
    });

    // Calculate stats
    const totalContacts = contacts.length;
    const successfulCalls = contacts.filter(c => c.leadStatus && c.leadStatus !== 'not_connected').length;
    const failedCalls = contacts.filter(c => !c.leadStatus || c.leadStatus === 'not_connected').length;
    const totalCallDuration = contacts.reduce((sum, c) => sum + (c.callDuration || 0), 0);
    const averageCallDuration = totalContacts > 0 ? Math.round(totalCallDuration / totalContacts) : 0;

    // Calculate run time
    const startTime = state.startedAt || new Date();
    const endTime = new Date();
    const elapsedSec = Math.max(0, Math.floor((endTime - startTime) / 1000));
    const toHms = (secs) => ({ 
      hours: Math.floor(secs / 3600), 
      minutes: Math.floor((secs % 3600) / 60), 
      seconds: secs % 60 
    });

    // Get instance number - use the same approach as parallel campaigns
    const existingCount = await CampaignHistory.countDocuments({ campaignId: String(campaignId) });
    const instanceNumber = existingCount + 1;
    
    // Double-check to ensure we get the highest instance number (handle race conditions)
    const maxInstance = await CampaignHistory.findOne(
      { campaignId: String(campaignId) },
      { instanceNumber: 1 }
    ).sort({ instanceNumber: -1 }).lean();
    
    const finalInstanceNumber = maxInstance ? Math.max(instanceNumber, maxInstance.instanceNumber + 1) : instanceNumber;

    // Save to CampaignHistory model (same as parallel campaigns)
    await CampaignHistory.findOneAndUpdate(
      { runId },
      {
        $setOnInsert: {
          campaignId: String(campaignId),
          runId,
          instanceNumber: finalInstanceNumber,
          startTime: startTime.toISOString(),
          status: 'running',
          createdAt: new Date(),
          stats: {
            totalContacts: 0,
            successfulCalls: 0,
            failedCalls: 0,
            totalCallDuration: 0,
            averageCallDuration: 0
          },
          contacts: []
        },
        $set: {
          endTime: endTime.toISOString(),
          runTime: toHms(elapsedSec),
          status: 'completed',
          updatedAt: new Date(),
          contacts,
          stats: { 
            totalContacts, 
            successfulCalls, 
            failedCalls, 
            totalCallDuration, 
            averageCallDuration 
          },
          batchInfo: { isIntermediate: false }
        }
      },
      { upsert: true, new: true }
    );

    console.log(`💾 SERIES: Auto-saved run to CampaignHistory (success=${successfulCalls}, failed=${failedCalls}) for campaign=${campaignId}`);
    
    // Send detailed campaign end telegram alert
    try {
      const { sendDetailedCampaignEndAlert } = require('../utils/telegramAlert');
      const Agent = require('../models/Agent');
      const Client = require('../models/Client');
      const Group = require('../models/Group');
      
      // Resolve agent from campaign.agent[0] or fallback
      const agentIdFromCampaign = Array.isArray(fresh.agent) && fresh.agent.length ? fresh.agent[0] : null;
      const agent = agentIdFromCampaign ? await Agent.findById(agentIdFromCampaign).lean() : null;
      
      // Resolve client
      const client = fresh.clientId ? await Client.findById(fresh.clientId).lean() : null;
      
      // Resolve group name from campaign.groupIds[0]
      let groupName = 'Default Group';
      try {
        const firstGroupId = Array.isArray(fresh.groupIds) && fresh.groupIds.length ? fresh.groupIds[0] : null;
        if (firstGroupId) {
          const groupDoc = await Group.findById(firstGroupId).lean();
          groupName = groupDoc?.groupName || groupDoc?.name || groupDoc?.title || groupName;
        }
      } catch {}
      
      const connected = successfulCalls;
      const missed = failedCalls;
      const connectedPercentage = totalContacts > 0 ? Math.round((connected / totalContacts) * 100) : 0;
      
      // Format duration as HH:MM:SS
      const hours = Math.floor(elapsedSec / 3600);
      const minutes = Math.floor((elapsedSec % 3600) / 60);
      const seconds = elapsedSec % 60;
      const durationFormatted = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
      
      await sendDetailedCampaignEndAlert({
        campaignName: fresh.name || 'Unnamed Campaign',
        runId: runId,
        agentName: agent?.agentName || 'Unknown Agent',
        groupName: groupName,
        didNumber: agent?.didNumber || 'N/A',
        totalContacts: totalContacts,
        startTime: startTime,
        endTime: endTime,
        duration: durationFormatted,
        connected: connected,
        missed: missed,
        connectedPercentage: connectedPercentage,
        clientName: client?.businessName || client?.name || client?.email || 'Unknown Client',
        userEmail: client?.email || 'N/A',
        mode: 'serial'
      });
    } catch (error) {
      console.error('❌ Failed to send series campaign end telegram alert:', error.message);
    }
    
  } catch (e) {
    console.log(`⚠️ SERIES: Auto-save failed: ${e?.message || e}`);
  }
  return { started: true, runId };
}

// Start sequential campaign calling
router.post('/start', async (req, res) => {
  try {
    const { campaignId, agentId, apiKey, clientId, minDelayMs } = req.body || {};
    if (!campaignId) return res.status(400).json({ error: 'campaignId is required' });

    // If already running, return status
    const existing = sequentialRuns.get(String(campaignId));
    if (existing?.isRunning) {
      // Consider stale if no heartbeat for > 5 minutes (series campaigns can have long waits)
      const stale = Date.now() - (existing.lastHeartbeat || 0) > 300000;
      if (!stale) {
        console.log(`ℹ️ SERIES: Start requested but already running campaign=${campaignId}`);
        return res.json({ started: false, message: 'Already running', status: existing });
      }
      console.log(`⚠️ SERIES: Detected stale run (no heartbeat), resetting state for campaign=${campaignId}`);
      sequentialRuns.delete(String(campaignId));
    }

    // Start in background and Telegram alert
    try {
      const { sendDetailedCampaignStartAlert } = require('../utils/telegramAlert');
      const Agent = require('../models/Agent');
      const Client = require('../models/Client');
      const Group = require('../models/Group');

      const clientMongoId = req.clientId || clientId || null;
      const campaignDoc = await Campaign.findById(campaignId).lean();

      // Resolve client doc robustly: try body -> campaign -> auth fallback
      let client = null;
      if (clientMongoId) {
        client = await Client.findById(clientMongoId).lean();
        if (!client) client = await Client.findOne({ _id: clientMongoId }).lean();
      }
      if (!client && campaignDoc?.clientId) {
        try { client = await Client.findById(campaignDoc.clientId).lean(); } catch (_) {}
      }
      if (!client && req.user?.userType === 'client' && req.user?.id) {
        try { client = await Client.findById(req.user.id).lean(); } catch (_) {}
      }

      // Resolve agent (from body agentId or campaign.agent[0])
      const resolvedAgentId = agentId || (Array.isArray(campaignDoc?.agent) && campaignDoc.agent.length ? campaignDoc.agent[0] : null);
      const agent = resolvedAgentId ? await Agent.findById(resolvedAgentId).lean() : null;

      // Resolve group name from campaign.groupIds[0]
      let groupName = 'Default Group';
      try {
        const firstGroupId = Array.isArray(campaignDoc?.groupIds) && campaignDoc.groupIds.length ? campaignDoc.groupIds[0] : null;
        if (firstGroupId) {
          const groupDoc = await Group.findById(firstGroupId).lean();
          groupName = groupDoc?.groupName || groupDoc?.name || groupDoc?.title || groupName;
        }
      } catch {}

      await sendDetailedCampaignStartAlert({
        campaignName: campaignDoc?.name || String(campaignId),
        agentName: agent?.agentName || 'Unknown Agent',
        groupName: groupName,
        didNumber: agent?.didNumber || 'N/A',
        totalContacts: Array.isArray(campaignDoc?.contacts) ? campaignDoc.contacts.length : 0,
        clientName: client?.businessName || client?.name || client?.email || String(clientMongoId || campaignDoc?.clientId || ''),
        userEmail: client?.email || 'N/A',
        mode: 'serial'
      });
    } catch (_) {}
    runSeries({ campaignId, agentId: agentId || null, apiKey: apiKey || null, clientId: clientId || null, minDelayMs: Math.max(5000, Number(minDelayMs) || 5000) })
      .catch(() => {})
      .finally(() => {});

    const snapshot = sequentialRuns.get(String(campaignId));
    console.log(`✅ SERIES: Started series in background for campaign=${campaignId}`);
    return res.json({ started: true, status: snapshot });
  } catch (e) {
    console.log(`❌ SERIES: Start error: ${e?.message || e}`);
    return res.status(500).json({ error: e.message });
  }
});

// Stop sequential campaign calling
router.post('/stop', async (req, res) => {
  try {
    const { campaignId } = req.body || {};
    if (!campaignId) return res.status(400).json({ error: 'campaignId is required' });
    
    const state = sequentialRuns.get(String(campaignId));
    if (state) {
      state.isRunning = false;
      sequentialRuns.set(String(campaignId), state);
      
      // Force save campaign history immediately when stopping
      try {
        const Campaign = require('../models/Campaign');
        const CampaignHistory = require('../models/CampaignHistory');
        
        const campaign = await Campaign.findById(campaignId);
        if (campaign && state.runId) {
          const campaignDetails = Array.isArray(campaign.details) ? campaign.details : [];
          const runDetails = campaignDetails.filter(d => d && d.runId === state.runId);
          
          if (runDetails.length > 0) {
            // Build contacts array for history
            const contactsById = new Map((campaign.contacts || []).map(c => [String(c._id || ''), c]));
            const contacts = runDetails.map(d => {
              const contact = contactsById.get(String(d.contactId || ''));
              return {
                documentId: d.uniqueId,
                contactId: d.contactId,
                number: contact?.phone || contact?.number || '',
                name: contact?.name || '',
                leadStatus: d.leadStatus || 'not_connected',
                time: d.time ? d.time.toISOString() : new Date().toISOString(),
                status: d.status || 'completed',
                duration: d.callDuration || 0,
                transcriptCount: 0,
                whatsappMessageSent: false,
                whatsappRequested: false
              };
            });

            // Calculate stats
            const totalContacts = contacts.length;
            const successfulCalls = contacts.filter(c => c.leadStatus && c.leadStatus !== 'not_connected').length;
            const failedCalls = contacts.filter(c => !c.leadStatus || c.leadStatus === 'not_connected').length;
            const totalCallDuration = contacts.reduce((sum, c) => sum + (c.duration || 0), 0);
            const averageCallDuration = totalContacts > 0 ? Math.round(totalCallDuration / totalContacts) : 0;

            // Calculate run time
            const startTime = state.startedAt || new Date();
            const endTime = new Date();
            const elapsedSeconds = Math.floor((endTime - startTime) / 1000);
            const hours = Math.floor(elapsedSeconds / 3600);
            const minutes = Math.floor((elapsedSeconds % 3600) / 60);
            const seconds = elapsedSeconds % 60;

            // Get instance number
            const existingCount = await CampaignHistory.countDocuments({ campaignId });
            const instanceNumber = existingCount + 1;

            // Save to campaign history
            await CampaignHistory.findOneAndUpdate(
              { runId: state.runId },
              {
                $setOnInsert: {
                  campaignId: campaignId,
                  runId: state.runId,
                  instanceNumber,
                  startTime: startTime.toISOString(),
                  status: 'running'
                },
                $set: {
                  endTime: endTime.toISOString(),
                  runTime: { hours, minutes, seconds },
                  status: 'completed',
                  contacts,
                  stats: { totalContacts, successfulCalls, failedCalls, totalCallDuration, averageCallDuration },
                  batchInfo: { isIntermediate: false }
                }
              },
              { upsert: true, new: true }
            );
            
            console.log(`💾 SERIES: Campaign history saved for run ${state.runId}`);
          }
        }
      } catch (historyError) {
        console.error(`❌ SERIES: Error saving campaign history:`, historyError);
      }
    }
    
    // Mark campaign as stopped in database
    try {
      const Campaign = require('../models/Campaign');
      await Campaign.updateOne({ _id: campaignId }, { $set: { isRunning: false } });
      console.log(`⏹️ SERIES: Marked campaign ${campaignId} stopped in database`);
    } catch (dbError) {
      console.error(`❌ SERIES: Error updating campaign database:`, dbError);
    }
    
    console.log(`⏹️ SERIES: Stop requested for campaign=${campaignId}`);
    return res.json({ stopped: true, status: state || null });
  } catch (e) {
    console.log(`❌ SERIES: Stop error: ${e?.message || e}`);
    return res.status(500).json({ error: e.message });
  }
});

// Get status
router.get('/status/:campaignId', async (req, res) => {
  const { campaignId } = req.params;
  const state = sequentialRuns.get(String(campaignId)) || null;
  return res.json({ status: state });
});

// Terminate an ongoing call (provider-specific)
router.post('/terminate', async (req, res) => {
  try {
    const { agentId, provider: rawProvider, callid, uniqueId } = req.body || {};
    let agent = null;

    // Try to infer provider if not explicitly provided
    let provider = normalizeProviderName(rawProvider || '');
    let inferredLog = null;
    if (!provider && uniqueId) {
      try {
        inferredLog = await CallLog.findOne({ 'metadata.customParams.uniqueid': uniqueId }).sort({ createdAt: -1 }).lean();
        const hasTwilio = !!(inferredLog?.metadata?.accountSid || inferredLog?.metadata?.twilio?.accountSid);
        const hasStreamOnly = !!(inferredLog?.streamSid || inferredLog?.callSid);
        if (hasTwilio || hasStreamOnly) provider = 'czentrix';
      } catch {}
    }

    // For SANPBX we require agent + creds; for czentrix we can proceed without agentId
    if (provider === 'sanpbx') {
      if (!agentId) return res.status(400).json({ success: false, error: 'agentId is required for SANPBX termination' });
      agent = await Agent.findById(agentId).lean();
      if (!agent) return res.status(404).json({ success: false, error: 'Agent not found' });
    }

    if (!provider) return res.status(400).json({ success: false, error: 'Provider not specified or could not be inferred' });

    // Try to derive callid from logs if not provided and we have uniqueId
    let finalCallId = callid;
    if (!finalCallId && uniqueId) {
      try {
        const log = await CallLog.findOne({ 'metadata.customParams.uniqueid': uniqueId }).sort({ createdAt: -1 }).lean();
        // SANPBX callid is usually stored as metadata.callid or response field depending on integration
        finalCallId = log?.metadata?.callid || log?.externalResponse?.callid || null;
      } catch {}
    }

    if (provider === 'sanpbx') {
      const accessToken = agent?.accessToken;
      const accessKey = agent?.accessKey; // e.g., "mob"
      if (!accessToken || !accessKey) {
        return res.status(400).json({ success: false, error: 'Missing SANPBX credentials on agent (accessToken/accessKey)' });
      }
      if (!finalCallId) {
        return res.status(400).json({ success: false, error: 'callid is required for SANPBX termination' });
      }
      // 1) Generate API token
      const tokenUrl = 'https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/gentoken';
      const tokenResp = await axios.post(tokenUrl, { access_key: accessKey }, { headers: { Accesstoken: accessToken } });
      const apiToken = tokenResp?.data?.Apitoken;
      if (!apiToken) {
        return res.status(502).json({ success: false, error: 'Failed to obtain SANPBX Apitoken' });
      }
      // 2) Disconnect the call
      const disconnectUrl = 'https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/calldisconnect';
      const discResp = await axios.post(disconnectUrl, { callid: finalCallId }, { headers: { Apitoken: apiToken } });
      return res.json({ success: true, provider, response: discResp.data });
    }

    if (provider === 'czentrix' || provider === 'c-zentrix') {
      // Build termination payload either from request or infer from logs by uniqueId
      let accountSid = req.body?.stop?.accountSid || req.body?.accountSid || null;
      let callSid = req.body?.stop?.callSid || req.body?.callSid || null;
      let streamSid = req.body?.streamSid || null;

      if ((!accountSid || !callSid || !streamSid) && uniqueId) {
        try {
          const log = inferredLog || await CallLog.findOne({ 'metadata.customParams.uniqueid': uniqueId }).sort({ createdAt: -1 }).lean();
          accountSid = accountSid || log?.metadata?.accountSid || log?.metadata?.twilio?.accountSid || null;
          callSid = callSid || log?.callSid || log?.metadata?.callSid || log?.metadata?.twilio?.callSid || null;
          streamSid = streamSid || log?.streamSid || log?.metadata?.streamSid || log?.metadata?.twilio?.streamSid || null;
        } catch {}
      }

      // Relax requirement: proceed if we have callSid and streamSid
      if (!callSid || !streamSid) {
        return res.status(400).json({ success: false, error: 'Missing czentrix termination fields (callSid/streamSid). Provide them or a known uniqueId.' });
      }

      const terminationPayload = {
        event: 'stop',
        sequenceNumber: 1,
        stop: { accountSid, callSid },
        streamSid
      };

      try {
        const resp = await axios.post('https://test.aitota.com/api/calls/terminate', terminationPayload, {
          headers: { 'Content-Type': 'application/json' }
        });
        return res.json({ success: true, provider, response: resp.data });
      } catch (err) {
        return res.status(502).json({ success: false, error: err?.response?.data?.message || err?.message || 'Czentrix termination failed' });
      }
    }

    return res.status(400).json({ success: false, error: `Unsupported provider: ${provider}` });
  } catch (e) {
    console.log('❌ TERMINATE ERROR:', e?.message || e);
    return res.status(500).json({ success: false, error: e?.message || 'Internal error' });
  }
});

module.exports = router;


