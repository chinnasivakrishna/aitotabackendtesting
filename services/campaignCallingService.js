// @ts-nocheck
const axios = require('axios');
const ApiKey = require('../models/ApiKey');
const Agent = require('../models/Agent');
const CallLog = require('../models/CallLog');
const mongoose = require('mongoose');

/**
 * Circuit Breaker Pattern for API calls
 */
class CircuitBreaker {
  constructor(threshold = 5, timeout = 60000) {
    this.failureCount = 0;
    this.threshold = threshold;
    this.timeout = timeout;
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    this.nextAttempt = Date.now();
  }

  async call(fn) {
    if (this.state === 'OPEN') {
      if (Date.now() < this.nextAttempt) {
        throw new Error('Circuit breaker is OPEN - API calls temporarily disabled');
      }
      this.state = 'HALF_OPEN';
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  onSuccess() {
    this.failureCount = 0;
    this.state = 'CLOSED';
  }

  onFailure() {
    this.failureCount++;
    if (this.failureCount >= this.threshold) {
      this.state = 'OPEN';
      this.nextAttempt = Date.now() + this.timeout;
      console.log(`🔴 CIRCUIT BREAKER OPEN: API calls disabled for ${this.timeout/1000} seconds`);
    }
  }

  getState() {
    return {
      state: this.state,
      failureCount: this.failureCount,
      nextAttempt: this.nextAttempt
    };
  }
}

// Global circuit breakers for different APIs
const apiCircuitBreaker = new CircuitBreaker(50, 60000); // 50 failures, 60s timeout
const sanpbxCircuitBreaker = new CircuitBreaker(50, 60000); // 50 failures, 60s timeout

/**
 * AUTOMATIC: Update call status in campaign based on isActive from call logs
 * This function runs automatically in the background to keep campaign status in sync
 */
async function updateCallStatusFromLogs(campaignId, uniqueId) {
  try {
    const Campaign = require('../models/Campaign');
    const CallLog = require('../models/CallLog');
    
    // Find the campaign
    const campaign = await Campaign.findById(campaignId);
    if (!campaign) {
      console.log(`❌ Campaign ${campaignId} not found`);
      return null;
    }
    
    // Find the call detail
    const callDetail = campaign.details.find(d => d.uniqueId === uniqueId);
    if (!callDetail) {
      console.log(`❌ Call detail with uniqueId ${uniqueId} not found in campaign ${campaignId}`);
      return null;
    }
    
    // Find the call log for this uniqueId
    const callLog = await CallLog.findOne({ 
      'metadata.customParams.uniqueid': uniqueId 
    }).sort({ createdAt: -1 }); // Get the most recent log
    
    console.log(`🔍 Checking call ${uniqueId}: CallLog found = ${!!callLog}`);
    
    if (!callLog) {
      // No call log found - check if 45 seconds have passed since call initiation
      const timeSinceInitiation = Math.floor((new Date() - callDetail.time) / 1000);
      
      console.log(`⏰ Call ${uniqueId}: No CallLog found, ${timeSinceInitiation}s since initiation`);
      
      if (timeSinceInitiation >= 45) {
        // No call log for 45+ seconds, mark as missed (not connected)
        if (callDetail.status !== 'completed') {
          const now = new Date();
          await Campaign.updateOne(
            { _id: campaignId, 'details.uniqueId': uniqueId },
            {
              $set: {
                'details.$.status': 'completed',
                'details.$.lastStatusUpdate': now,
                'details.$.callDuration': timeSinceInitiation,
                'details.$.leadStatus': 'not_connected'
              }
            }
          );
          console.log(`✅ Call ${uniqueId}: Marked as completed (no CallLog for ${timeSinceInitiation}s)`);
          return 'missed';
        } else {
          // Already completed, but log for debugging
          console.log(`✅ Call ${uniqueId}: Already completed (no CallLog for ${timeSinceInitiation}s)`);
          return 'already_completed';
        }
      } else {
        // Still within 45 seconds, keep as ringing
        console.log(`⏳ Call ${uniqueId} still ringing (${timeSinceInitiation}s since initiation)`);
        return null;
      }
    }
    
    // Call log found - check isActive status
    const isActive = callLog.metadata?.isActive;
    const timeSinceCallStart = Math.floor((new Date() - callDetail.time) / 1000);
    
    console.log(`📞 Call ${uniqueId}: CallLog found, isActive = ${isActive}, current status = ${callDetail.status}, time since start = ${timeSinceCallStart}s`);
    
    // ENHANCED STATUS LOGIC: Check isActive and add timeout mechanism
    let newStatus;
    
    if (isActive === true) {
      // Call is active - check if it's been too long (8 minutes = 480 seconds)
      if (timeSinceCallStart >= 480) {
        // Call has been "active" for too long, mark as completed
        newStatus = 'completed';
        console.log(`🔄 Call ${uniqueId}: isActive=true but ${timeSinceCallStart}s passed, marking as completed (8min timeout)`);
        
        // Also update the CallLog to mark it as inactive
        try {
          await CallLog.findByIdAndUpdate(callLog._id, {
            'metadata.isActive': false,
            'metadata.callEndTime': new Date(),
            leadStatus: 'not_connected'
          });
        } catch (error) {
          console.error(`❌ Error updating CallLog:`, error);
        }
      } else {
        // Call is active and within reasonable time, keep as ongoing
        newStatus = 'ongoing';
      }
    } else if (isActive === false) {
      // Call is not active - mark as completed
      newStatus = 'completed';
    } else {
      // isActive is undefined/null - check if 45 seconds passed
      if (timeSinceCallStart >= 45) {
        newStatus = 'completed';
      } else {
        // Still within 40 seconds, keep current status
        return null;
      }
    }

    // Update campaign details with new status
    if (callDetail.status !== newStatus) {
      const oldStatus = callDetail.status;
      const now = new Date();
      const setOps = {
        'details.$.status': newStatus,
        'details.$.lastStatusUpdate': now
      };
      if (newStatus === 'completed') {
        setOps['details.$.callDuration'] = timeSinceCallStart;
        try {
          const { deductCreditsForCall } = require('./creditUsageService');
          const clientId = campaign.clientId || callLog?.clientId;
          const uid = callDetail.uniqueId;
          if (clientId && uid) {
            await deductCreditsForCall({ clientId, uniqueId: uid });
          }
        } catch (e) {
          console.error('Credit deduction failed:', e.message);
        }
      }
      await Campaign.updateOne(
        { _id: campaignId, 'details.uniqueId': uniqueId },
        { $set: setOps }
      );
      // Optionally refresh minimal doc to recompute running status
      try {
        const refreshed = await Campaign.findById(campaignId).select('_id details isRunning updatedAt createdAt');
        await updateCampaignRunningStatus(refreshed);
      } catch {}
      return {
        uniqueId,
        oldStatus,
        newStatus,
        isActive,
        leadStatus: callLog.leadStatus,
        timeSinceCallStart
      };
    }

    return null;
  } catch (error) {
    console.error('Error updating call status from logs:', error);
    return null;
  }
}

/**
 * Auto-update campaign isRunning field based on actual call status
 * This prevents campaigns from getting stuck in running state
 */
async function updateCampaignRunningStatus(campaign) {
  try {
    if (!campaign || !campaign.details) {
      return;
    }

    const details = Array.isArray(campaign.details) ? campaign.details : [];
    const hasActive = details.some(d => d && (d.status === 'ringing' || d.status === 'ongoing'));
    const initiatedCount = details.length;
    const completedCount = details.filter(d => d && d.status === 'completed').length;
    const allCallsFinalized = initiatedCount > 0 && !hasActive && completedCount === initiatedCount;

    let shouldUpdate = false;
    let newIsRunning = campaign.isRunning;

    // Check if campaign is waiting for calls to complete
    const progress = campaignCallingProgress.get(campaign._id.toString());
    const isWaitingForCalls = progress && progress.waitingForCallsToComplete;

    if (campaign.isRunning && allCallsFinalized) {
      // Campaign is marked as running but all calls are finalized - should be stopped
      newIsRunning = false;
      shouldUpdate = true;
      console.log(`🔄 Auto-stopping campaign ${campaign._id}: all calls finalized`);
      
      // Persist run to history on auto-stop to ensure history is saved in serial mode
      try {
        await autoSaveCampaignRun(campaign, progress || { currentIndex: campaign.details?.length || 0 });
      } catch (e) {
        console.error('❌ Auto-save campaign run failed on auto-stop:', e?.message);
      }
      
      // FALLBACK: Schedule cleanup after 5 minutes for automatically stopped campaigns
      setTimeout(async () => {
        try {
          console.log('🧹 FALLBACK: Running cleanup after 5 minutes (auto-stop)...');
          await cleanupCompletedCampaignsWithDetails();
        } catch (error) {
          console.error('❌ FALLBACK: Error in 5-minute cleanup:', error);
        }
      }, 5 * 60 * 1000); // 5 minutes
    } else if (isWaitingForCalls && allCallsFinalized) {
      // Campaign was waiting for calls to complete and now all are done
      newIsRunning = false;
      shouldUpdate = true;
      console.log(`✅ Campaign ${campaign._id}: All calls completed, marking as finished`);
      
      // Clear the waiting flag
      if (progress) {
        progress.waitingForCallsToComplete = false;
        campaignCallingProgress.set(campaign._id.toString(), progress);
      }

      // Auto-save campaign run when all calls are completed after manual stop
      try {
        await autoSaveCampaignRun(campaign, progress);
        
        // MANUAL: Schedule cleanup after 5 minutes (only for manual stop)
        setTimeout(async () => {
          try {
            console.log('🧹 MANUAL: Running cleanup after 5 minutes...');
            await cleanupCompletedCampaignsWithDetails();
          } catch (error) {
            console.error('❌ MANUAL: Error in 5-minute cleanup:', error);
          }
        }, 5 * 60 * 1000); // 5 minutes
        
      } catch (error) {
        console.error(`❌ Error auto-saving campaign run for ${campaign._id}:`, error);
      }
    }

    if (shouldUpdate) {
      campaign.isRunning = newIsRunning;
      await campaign.save();
      console.log(`✅ Campaign ${campaign._id} isRunning updated to: ${newIsRunning}`);
    }
  } catch (error) {
    console.error('Error updating campaign running status:', error);
  }
}

// In-memory storage for campaign calling progress
const campaignCallingProgress = new Map();
const activeCampaigns = new Map();

/**
 * Rate Limiter for API calls
 */
class RateLimiter {
  constructor() {
    this.rateLimits = {
      perMinute: 50,    // Increased from 20 to 50
      perHour: 2000,    // Increased from 1000 to 2000
      perDay: 20000     // Increased from 10000 to 20000
    };
    this.callHistory = [];
    this.errorCount = 0;
    this.successCount = 0;
  }

  async checkRateLimit() {
    const now = Date.now();
    const oneMinuteAgo = now - 60000;
    const oneHourAgo = now - 3600000;
    const oneDayAgo = now - 86400000;

    // Count calls in different time windows
    const callsLastMinute = this.callHistory.filter(call => call.timestamp > oneMinuteAgo).length;
    const callsLastHour = this.callHistory.filter(call => call.timestamp > oneHourAgo).length;
    const callsLastDay = this.callHistory.filter(call => call.timestamp > oneDayAgo).length;

    // IMPROVED: More intelligent rate limiting
    // Allow burst calls if overall rate is good
    const successRate = this.successCount / (this.successCount + this.errorCount) || 0;
    const adjustedMinuteLimit = successRate > 0.8 ? this.rateLimits.perMinute * 1.5 : this.rateLimits.perMinute;
    
    // Check if any limit is exceeded
    if (callsLastMinute >= adjustedMinuteLimit) {
      // Calculate wait time based on oldest call in current minute
      const oldestCallInMinute = this.callHistory
        .filter(call => call.timestamp > oneMinuteAgo)
        .sort((a, b) => a.timestamp - b.timestamp)[0];
      
      const waitTime = oldestCallInMinute ? (60000 - (now - oldestCallInMinute.timestamp)) : 60000;
      return { allowed: false, reason: 'minute_limit', retryAfter: Math.max(waitTime, 1000) };
    }
    
    if (callsLastHour >= this.rateLimits.perHour) {
      return { allowed: false, reason: 'hour_limit', retryAfter: 3600000 };
    }
    
    if (callsLastDay >= this.rateLimits.perDay) {
      return { allowed: false, reason: 'day_limit', retryAfter: 86400000 };
    }

    return { allowed: true };
  }

  recordCall(success = true) {
    this.callHistory.push({ timestamp: Date.now(), success });
    
    if (success) {
      this.successCount++;
    } else {
      this.errorCount++;
    }

    // Clean old history (keep only last 24 hours)
    const oneDayAgo = Date.now() - 86400000;
    this.callHistory = this.callHistory.filter(call => call.timestamp > oneDayAgo);

    // Adjust rate limits based on error rate
    this.adjustRateLimits();
  }

  adjustRateLimits() {
    const totalCalls = this.errorCount + this.successCount;
    if (totalCalls === 0) return;

    const errorRate = this.errorCount / totalCalls;
    
    if (errorRate > 0.1) { // More than 10% errors
      this.rateLimits.perMinute = Math.max(10, this.rateLimits.perMinute - 2);
      this.rateLimits.perHour = Math.max(500, this.rateLimits.perHour - 100);
      console.log(`🔻 RATE LIMIT REDUCED: Error rate ${(errorRate * 100).toFixed(1)}%`);
    } else if (errorRate < 0.05 && totalCalls > 100) { // Less than 5% errors
      this.rateLimits.perMinute = Math.min(30, this.rateLimits.perMinute + 1);
      this.rateLimits.perHour = Math.min(1500, this.rateLimits.perHour + 50);
      console.log(`🔺 RATE LIMIT INCREASED: Error rate ${(errorRate * 100).toFixed(1)}%`);
    }
  }

  getStats() {
    return {
      rateLimits: this.rateLimits,
      errorCount: this.errorCount,
      successCount: this.successCount,
      errorRate: this.errorCount / (this.errorCount + this.successCount) || 0,
      callsLastMinute: this.callHistory.filter(call => call.timestamp > Date.now() - 60000).length
    };
  }
}

// Global rate limiter
const rateLimiter = new RateLimiter();

/**
 * Resource Monitor for system health
 */
/* Resource Monitor removed */
class ResourceMonitor {
  constructor() {
    this.maxMemoryUsage = 90; // 90% of available memory (T3.Medium has 4GB RAM)
    this.maxCpuUsage = 85; // 85% of available CPU (2 vCPUs available)
    this.maxCampaigns = 5; // Max campaigns per server (T3.Medium can handle 3 safely)
    this.maxConcurrentCalls = 50; // Max concurrent calls (T3.Medium: 2 vCPUs + 4GB RAM - proven to work)
    this.memoryHistory = []; // Track memory usage over time
  }

  checkResources() {
    const memoryUsage = process.memoryUsage();
    const cpuUsage = process.cpuUsage();
    const activeCampaignsCount = activeCampaigns.size;
    
    // FIXED: Count ACTIVE/RINGING calls, not completed calls
    let totalActiveCalls = 0;
    for (const [campaignId, progress] of campaignCallingProgress.entries()) {
      if (progress.isRunning && progress.details) {
        const activeCalls = progress.details.filter(detail => 
          detail.status === 'ringing' || detail.status === 'ongoing'
        ).length;
        totalActiveCalls += activeCalls;
      }
    }
    
    console.log(`📊 CONCURRENT CALLS: ${totalActiveCalls} active calls out of ${this.maxConcurrentCalls} limit`);

    // IMPROVED: Better memory calculation
    const totalMemory = memoryUsage.rss; // Resident Set Size (actual memory used)
    const heapUsed = memoryUsage.heapUsed;
    const heapTotal = memoryUsage.heapTotal;
    
    // Calculate memory usage as percentage of heap vs total memory
    const memoryUsagePercent = heapTotal > 0 ? (heapUsed / heapTotal) * 100 : (heapUsed / totalMemory) * 100;
    
    // Track memory history
    this.memoryHistory.push({
      timestamp: Date.now(),
      heapUsed: heapUsed,
      heapTotal: heapTotal,
      rss: totalMemory,
      percent: memoryUsagePercent
    });
    
    // Keep only last 10 measurements
    if (this.memoryHistory.length > 10) {
      this.memoryHistory = this.memoryHistory.slice(-10);
    }

    const warnings = [];
    
    // IMPROVED: More intelligent memory checking
    if (memoryUsagePercent > this.maxMemoryUsage) {
      warnings.push(`High memory usage: ${memoryUsagePercent.toFixed(1)}% (${Math.round(heapUsed/1024/1024)}MB/${Math.round(heapTotal/1024/1024)}MB)`);
    }
    
    if (activeCampaignsCount > this.maxCampaigns) {
      warnings.push(`Too many campaigns: ${activeCampaignsCount}`);
    }
    
    if (totalActiveCalls > this.maxConcurrentCalls) {
      warnings.push(`Too many concurrent calls: ${totalActiveCalls}`);
    }

    if (warnings.length > 0) {
      console.warn(`⚠️ RESOURCE WARNING: ${warnings.join(', ')}`);
      return false;
    }

    return true;
  }

  getStats() {
    const memoryUsage = process.memoryUsage();
    const heapUsed = memoryUsage.heapUsed;
    const heapTotal = memoryUsage.heapTotal;
    const rss = memoryUsage.rss;
    
    return {
      memoryUsage: {
        heapUsed: heapUsed,
        heapTotal: heapTotal,
        rss: rss,
        percent: heapTotal > 0 ? (heapUsed / heapTotal) * 100 : (heapUsed / rss) * 100,
        heapUsedMB: Math.round(heapUsed / 1024 / 1024),
        heapTotalMB: Math.round(heapTotal / 1024 / 1024),
        rssMB: Math.round(rss / 1024 / 1024)
      },
      activeCampaigns: activeCampaigns.size,
      totalProgress: campaignCallingProgress.size,
      circuitBreakers: {
        api: apiCircuitBreaker.getState(),
        sanpbx: sanpbxCircuitBreaker.getState()
      },
      rateLimiter: rateLimiter.getStats(),
      memoryHistory: this.memoryHistory.slice(-5) // Last 5 measurements
    };
  }
}

// Resource monitor disabled
// const resourceMonitor = new ResourceMonitor();

// AUTOMATIC: Background service to monitor and update call statuses
let statusUpdateInterval = null;

/**
 * Start automatic background status updates for all campaigns
 */
function startAutomaticStatusUpdates() {
  if (statusUpdateInterval) {
    clearInterval(statusUpdateInterval);
  }
  
  // Check and update call statuses every 3 seconds
  statusUpdateInterval = setInterval(async () => {
    try {
      await updateAllCampaignCallStatuses();
    } catch (error) {
      console.error('❌ Error in automatic status update:', error);
    }
  }, 3000); // 3 seconds
  
}

/**
 * Stop automatic background status updates
 */
function stopAutomaticStatusUpdates() {
  if (statusUpdateInterval) {
    clearInterval(statusUpdateInterval);
    statusUpdateInterval = null;
  }
}

/**
 * AUTOMATIC: Update call statuses for all campaigns based on isActive
 */
async function updateAllCampaignCallStatuses() {
  try {
    const Campaign = require('../models/Campaign');
    const CallLog = require('../models/CallLog');
    
    // Find all campaigns with ringing or ongoing calls
    const campaigns = await Campaign.find({
      'details.status': { $in: ['ringing', 'ongoing'] }
    }).lean();
    
    if (campaigns.length === 0) {
      return;
    }
    
    // Filter out manually stopped campaigns (but still process campaigns waiting for calls to complete)
    const activeCampaignsList = campaigns.filter(campaign => {
      const progress = campaignCallingProgress.get(campaign._id.toString());
      // Process if not manually stopped OR if waiting for calls to complete
      return !progress || !progress.manuallyStopped || progress.waitingForCallsToComplete;
    });
    
    if (activeCampaignsList.length === 0) {
      return;
    }
    
    
    // Silent: avoid noisy logging of all campaigns
    
    let totalUpdates = 0;
    
    for (const campaign of activeCampaignsList) {
      const activeCalls = campaign.details.filter(d => d.status === 'ringing' || d.status === 'ongoing');
      
      for (const callDetail of activeCalls) {
        try {
          // Use the same logic as updateCallStatusFromLogs
          const updateResult = await updateCallStatusFromLogs(campaign._id, callDetail.uniqueId);
          if (updateResult) {
            totalUpdates++;
          }
        } catch (error) {
          console.error(`❌ Error updating call ${callDetail.uniqueId} in campaign ${campaign._id}:`, error);
        }
      }
      
      // Also check and update campaign running status for each campaign
      try {
        const Campaign = require('../models/Campaign');
        const fullCampaign = await Campaign.findById(campaign._id);
        if (fullCampaign) {
          await updateCampaignRunningStatus(fullCampaign);
        }
      } catch (error) {
        console.error(`❌ Error updating running status for campaign ${campaign._id}:`, error);
      }
    }
    
    // Suppress routine logs
    
  } catch (error) {
    console.error('❌ Error in updateAllCampaignCallStatuses:', error);
  }
}

/**
 * MANUAL: Trigger immediate status update for testing/debugging
 */
async function triggerManualStatusUpdate(campaignId = null) {
  try {
    console.log('🔧 MANUAL: Triggering immediate status update...');
    
    if (campaignId) {
      // Update specific campaign
      const Campaign = require('../models/Campaign');
      const campaign = await Campaign.findById(campaignId);
      if (campaign) {
        const activeCalls = campaign.details.filter(d => d.status === 'ringing' || d.status === 'ongoing');
        console.log(`🔧 MANUAL: Found ${activeCalls.length} active calls in campaign ${campaignId}`);
        
        for (const callDetail of activeCalls) {
          await updateCallStatusFromLogs(campaignId, callDetail.uniqueId);
        }
      }
    } else {
      // Update all campaigns
      await updateAllCampaignCallStatuses();
    }
    
    console.log('🔧 MANUAL: Status update completed');
  } catch (error) {
    console.error('❌ Error in manual status update:', error);
  }
}

/**
 * Get client API key from database
 */
async function getClientApiKey(clientId) {
  try {
    const apiKey = await ApiKey.findOne({ clientId, isActive: true });
    return apiKey ? apiKey.key : null;
  } catch (error) {
    console.error('Error fetching API key:', error);
    return null;
  }
}

/**
 * Generate unique ID for call tracking
 */
function generateUniqueId() {
  return `aidial-${Date.now()}-${performance.now().toString(36).replace(".", "")}-${Math.random().toString(36).substr(2, 9)}`;
}

/**
 * Make a single call to a contact
 */
async function makeSingleCall(contact, agentId, apiKey, campaignId, clientId, runId = null, providedUniqueId = null, extraCustomParams = {}) {
  const uniqueId = providedUniqueId || generateUniqueId(); // Use provided uniqueId or generate new one
  
  console.log(`📞 MAKING CALL: Contact=${contact?.phone}, AgentId=${agentId}, ApiKey=${!!apiKey}, ClientId=${clientId}`);
  
  try {
    // Check resource availability

    // Load agent to branch by provider
    const agent = await Agent.findById(agentId).lean();
    const provider = String(agent?.serviceProvider || '').toLowerCase();

    // SANPBX provider flow
    if (provider === 'snapbx' || provider === 'sanpbx') {
      const accessToken = agent?.accessToken;
      const accessKey = agent?.accessKey;
      const callerId = agent?.callerId;
      if (!accessToken || !accessKey || !callerId) {
        throw new Error('Telephony_MISSING_FIELDS');
      }

      // Normalize phone and ensure it starts with '0'
      const normalizedDigits = String(contact?.phone || '').replace(/[^\d]/g, '');
      const callTo = normalizedDigits.startsWith('0') ? normalizedDigits : `0${normalizedDigits}`;

      // 1) Get API token (access token in header) with circuit breaker and retry
      const tokenUrl = 'https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/gentoken';
      let tokenResp;
      let retryCount = 0;
      const maxRetries = 3;
      
      while (retryCount < maxRetries) {
        try {
          tokenResp = await sanpbxCircuitBreaker.call(async () => {
            return await axios.post(tokenUrl, { access_key: accessKey }, { 
              headers: { Accesstoken: accessToken }, 
              timeout: 15000 
            });
          });
          break; // Success, exit retry loop
        } catch (error) {
          retryCount++;
          if (retryCount >= maxRetries) {
            throw error; // Re-throw after max retries
          }
          console.log(`🔄 API Token retry ${retryCount}/${maxRetries}: ${error.message}`);
          await new Promise(resolve => setTimeout(resolve, 1000 * retryCount)); // Exponential backoff
        }
      }
      const sanToken = tokenResp?.data?.Apitoken;
      if (!sanToken) {
        throw new Error('SANPBX_TOKEN_FAILED');
      }

      // 2) Dial call (apitoken in header) with circuit breaker and retry
      const dialUrl = 'https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/dialcall';
      const dialBody = { appid: 2, call_to: callTo, caller_id: callerId, custom_field: { uniqueid: uniqueId, name: contact.name , runId, ...(extraCustomParams || {}) } };
      let response;
      retryCount = 0;
      
      while (retryCount < maxRetries) {
        try {
          response = await sanpbxCircuitBreaker.call(async () => {
            return await axios.post(dialUrl, dialBody, { 
              headers: { Apitoken: sanToken }, 
              timeout: 20000 
            });
          });
          break; // Success, exit retry loop
        } catch (error) {
          retryCount++;
          if (retryCount >= maxRetries) {
            throw error; // Re-throw after max retries
          }
          console.log(`🔄 Dial Call retry ${retryCount}/${maxRetries}: ${error.message}`);
          await new Promise(resolve => setTimeout(resolve, 1000 * retryCount)); // Exponential backoff
        }
      }

      return {
        success: true,
        uniqueId,
        contact,
        timestamp: new Date(),
        externalResponse: response.data,
        provider: 'sanpbx'
      };
    }

    // Check if API key is available
    if (!apiKey) {
      throw new Error('API_KEY_MISSING');
    }

    // Sanitize name and phone
    const rawName = (contact && contact.name) ? String(contact.name).trim() : '';
    const digitsOnly = (str) => (str || '').replace(/\D/g, '');
    const sanitizedPhone = digitsOnly(contact?.phone || '');
    if (!sanitizedPhone) {
      throw new Error('Missing phone');
    }
    
    console.log(`📞 CALL DETAILS: Phone=${sanitizedPhone}, Name=${rawName}, ApiKey=${apiKey.substring(0, 10)}...`);
    const isNumberLike = rawName && digitsOnly(rawName).length >= 6 && (
      !isNaN(Number(digitsOnly(rawName)))
    );
    const sameAsPhone = rawName && digitsOnly(rawName) === digitsOnly(contact.phone || '');
    const safeName = (rawName && !isNumberLike && !sameAsPhone) ? rawName : '';

    const callPayload = {
      transaction_id: "CTI_BOT_DIAL",
      phone_num: sanitizedPhone,
      uniqueid: uniqueId,
      callerid: "168353225",
      uuid: clientId || "client-uuid-001",
      custom_param: {
        uniqueid: uniqueId,
        name: safeName,
        contact_name: safeName,
        clientUserId: contact?.clientUserId || null,
        runId: runId,
        ...(extraCustomParams || {})
      },
      resFormat: 3,
    };

    // Make call to external API with circuit breaker
    console.log(`📞 EXTERNAL API CALL: Sending to clicktobot API`);
    const response = await apiCircuitBreaker.call(async () => {
      return await axios.post(
        'https://3neysomt18.execute-api.us-east-1.amazonaws.com/dev/clicktobot',
        callPayload,
        {
          headers: {
            'X-CLIENT': 'czobd',
            'X-API-KEY': apiKey,
            'Content-Type': 'application/json',
          },
          timeout: 10000 // 10 second timeout
        }
      );
    });

    console.log(`✅ EXTERNAL API SUCCESS: Response status=${response.status}`);
    return {
      success: true,
      uniqueId,
      contact,
      timestamp: new Date(),
      externalResponse: response.data,
      provider: 'clicktobot'
    };

  } catch (error) {
    console.error('❌ CALL FAILED:', error?.response?.status, error?.response?.data || error?.message);    
    return {
      success: false,
      uniqueId, // Return uniqueId even for failed calls
      error: error.message,
      status: error?.response?.status || null,
      responseData: error?.response?.data || null,
      provider: 'czentrix',
      contact,
      timestamp: new Date()
    };
  }
}

/**
 * Start campaign calling process with NGR support
 */
async function startCampaignCalling(campaign, agentId, apiKey, delayBetweenCalls, clientId, runId, agentConfig = null) {
  const campaignId = campaign._id.toString();
  
  // Initialize progress tracking
  const progress = {
    campaignId,
    totalContacts: campaign.contacts.length,
    currentIndex: 0,
    completedCalls: 0,
    successfulCalls: 0,
    failedCalls: 0,
    startTime: new Date(),
    isRunning: true,
    lastCallTime: null,
    runId: runId || `run-${Date.now()}`, // Store runId for tracking
    callingMode: agentConfig?.mode || 'serial' // Track calling mode
  };

  campaignCallingProgress.set(campaignId, progress);
  activeCampaigns.set(campaignId, true);

  // Determine calling strategy based on agent configuration
  if (agentConfig && agentConfig.mode === 'parallel' && agentConfig.items && agentConfig.items.length > 0) {
    // Use NGR pattern for parallel mode
    const defaultConfig = agentConfig.items.find(item => item.isDefault) || agentConfig.items[0];
    const ngrConfig = {
      n: defaultConfig.n || 1,
      g: defaultConfig.g || 5,
      rSec: defaultConfig.rSec || 30
    };
    
    console.log(`🚀 NGR CAMPAIGN START: Using NGR pattern with N=${ngrConfig.n}, G=${ngrConfig.g}s, R=${ngrConfig.rSec}s`);
    
    // Process calls using NGR pattern
    processCampaignCallsNGR(campaign, agentId, apiKey, clientId, progress, runId, ngrConfig).catch(error => {
      console.error(`❌ NGR Background campaign processing failed:`, error);
      progress.isRunning = false;
      progress.endTime = new Date();
      progress.error = error.message;
      campaignCallingProgress.set(campaignId, progress);
    });
  } else {
    // Use legacy batch processing for series mode or when no NGR config
    console.log(`🚀 LEGACY CAMPAIGN START: Using legacy batch processing`);
    processCampaignCalls(campaign, agentId, apiKey, delayBetweenCalls, clientId, progress, runId).catch(error => {
      console.error(`❌ Background campaign processing failed:`, error);
      progress.isRunning = false;
      progress.endTime = new Date();
      progress.error = error.message;
      campaignCallingProgress.set(campaignId, progress);
    });
  }
}

/**
 * Process campaign calls using NGR pattern for parallel mode
 */
async function processCampaignCallsNGR(campaign, agentId, apiKey, clientId, progress, runId, ngrConfig) {
  const campaignId = campaign._id.toString();
  const { n = 1, g = 5, rSec = 30 } = ngrConfig;
  
  console.log(`🚀 NGR CAMPAIGN: Starting with N=${n} concurrent, G=${g} numbers per batch, R=${rSec}s rest`);
  
  try {
    const totalContacts = campaign.contacts.length;
    let currentIndex = 0;
    
    while (currentIndex < totalContacts && progress.isRunning) {
      // Check if campaign should stop
      const callActive = activeCampaigns.get(campaignId);
      if (!callActive) {
        console.log(`🛑 NGR CAMPAIGN STOPPED: Campaign ${campaignId} removed from active campaigns`);
        break;
      }
      
      const callProgress = campaignCallingProgress.get(campaignId);
      if (callProgress && callProgress.manuallyStopped) {
        console.log(`🛑 NGR MANUAL STOP: Campaign ${campaignId} was manually stopped`);
        break;
      }
      
      // Process a batch of G numbers (contacts)
      const batchEnd = Math.min(currentIndex + g, totalContacts);
      const batchContacts = campaign.contacts.slice(currentIndex, batchEnd);
      
      console.log(`📞 NGR BATCH: Processing ${batchContacts.length} numbers (${currentIndex + 1}-${batchEnd}/${totalContacts}) with up to N=${n} concurrent calls`);

      // Run the batch with a max concurrency of N (no per-call gaps)
      for (let i = 0; i < batchContacts.length; i += n) {
        const chunk = batchContacts.slice(i, i + n);
        await Promise.all(
          chunk.map(async (contact, idxInChunk) => {
            const globalIndex = currentIndex + i + idxInChunk; // 0-based

            // Update progress index to reflect the call being initiated
            progress.currentIndex = globalIndex;
            campaignCallingProgress.set(campaignId, progress);

            try {
              console.log(`📞 NGR CALL: Making call ${globalIndex + 1}/${totalContacts} to ${contact.phone}`);
              const callResult = await makeSingleCall(contact, agentId, apiKey, campaign._id, clientId, runId, null);

              if (callResult.success) {
                progress.successfulCalls++;
                console.log(`✅ NGR SUCCESS: Call ${globalIndex + 1} completed successfully`);
                // Append call detail to campaign (same structure as legacy path)
                if (callResult.uniqueId) {
                  const initiatedAt = new Date();
                  const callDetail = {
                    uniqueId: callResult.uniqueId,
                    contactId: contact._id || null,
                    time: initiatedAt,
                    status: 'ringing',
                    lastStatusUpdate: initiatedAt,
                    callDuration: 0,
                    ...(runId ? { runId } : {})
                  };
                  try {
                    const Campaign = require('../models/Campaign');
                    await Campaign.updateOne(
                      { _id: campaign._id, 'details.uniqueId': { $ne: callResult.uniqueId } },
                      { $push: { details: callDetail } }
                    );
                    // Schedule a status check after ~40s to mirror frontend behavior
                    setTimeout(() => {
                      updateCallStatusFromLogs(campaign._id, callResult.uniqueId).catch(() => {});
                    }, 40000);
                  } catch (e) {
                    console.error(`❌ NGR: Failed to append success call detail ${callResult.uniqueId}:`, e.message);
                  }
                }
              } else {
                progress.failedCalls++;
                console.log(`❌ NGR FAILED: Call ${globalIndex + 1} failed`);
                // Append failed call detail so UI shows it as completed/not connected
                if (callResult.uniqueId) {
                  const initiatedAt = new Date();
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
                  try {
                    const Campaign = require('../models/Campaign');
                    await Campaign.updateOne(
                      { _id: campaign._id, 'details.uniqueId': { $ne: callResult.uniqueId } },
                      { $push: { details: callDetail } }
                    );
                  } catch (e) {
                    console.error(`❌ NGR: Failed to append failed call detail ${callResult.uniqueId}:`, e.message);
                  }
                }
              }

              progress.completedCalls++;
              progress.lastCallTime = new Date();
              campaignCallingProgress.set(campaignId, progress);

              return callResult;
            } catch (error) {
              progress.failedCalls++;
              progress.completedCalls++;
              progress.lastCallTime = new Date();
              campaignCallingProgress.set(campaignId, progress);
              console.error(`❌ NGR ERROR: Call ${globalIndex + 1} error:`, error.message);
              // Append failed call detail without uniqueId (fallback)
              try {
                const initiatedAt = new Date();
                const Campaign = require('../models/Campaign');
                const fallbackDetail = {
                  uniqueId: `error-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
                  contactId: contact._id || null,
                  time: initiatedAt,
                  status: 'completed',
                  lastStatusUpdate: initiatedAt,
                  callDuration: 0,
                  leadStatus: 'not_connected',
                  ...(runId ? { runId } : {})
                };
                await Campaign.updateOne(
                  { _id: campaign._id, 'details.uniqueId': { $ne: fallbackDetail.uniqueId } },
                  { $push: { details: fallbackDetail } }
                );
              } catch (e) {
                // swallow
              }
              return { success: false, error: error.message };
            }
          })
        );
      }
      
      currentIndex = batchEnd;
      
      // Add R-second rest after completing G numbers (except for the last batch)
      if (currentIndex < totalContacts) {
        console.log(`⏳ NGR REST: Waiting ${rSec} seconds before next batch`);
        await new Promise(resolve => setTimeout(resolve, rSec * 1000));
      }
    }
    
    // Mark campaign as completed
    progress.isRunning = false;
    progress.endTime = new Date();
    progress.currentIndex = totalContacts;
    campaignCallingProgress.set(campaignId, progress);
    activeCampaigns.delete(campaignId);
    
    console.log(`✅ NGR CAMPAIGN COMPLETED: ${campaignId} finished with ${progress.successfulCalls} successful calls`);
    
  } catch (error) {
    console.error(`❌ NGR CAMPAIGN ERROR:`, error);
    progress.isRunning = false;
    progress.endTime = new Date();
    progress.error = error.message;
    campaignCallingProgress.set(campaignId, progress);
    activeCampaigns.delete(campaignId);
  }
}

/**
 * Process campaign calls in background with batch processing (legacy method)
 */
async function processCampaignCalls(campaign, agentId, apiKey, delayBetweenCalls, clientId, progress, runId) {
  const campaignId = campaign._id.toString();
  const BATCH_SIZE = parseInt(process.env.CAMPAIGN_BATCH_SIZE) || 25; // Reduced from 100 to 25 for safety
  const MAX_CONCURRENT_CAMPAIGNS = 3; // Max 3 campaigns at once
  const SAFE_DELAY_BETWEEN_CALLS = Math.max(delayBetweenCalls, 2000); // Minimum 2 seconds
  
  try {
    // Check campaign limits
    if (activeCampaigns.size >= MAX_CONCURRENT_CAMPAIGNS) {
      console.log(`⚠️ CAMPAIGN LIMIT: Maximum ${MAX_CONCURRENT_CAMPAIGNS} campaigns allowed, current: ${activeCampaigns.size}`);
      progress.isRunning = false;
      progress.endTime = new Date();
      campaignCallingProgress.set(campaignId, progress);
      return;
    }

    // Resume from where we left off if this is a continuation
    let startIndex = progress.currentIndex || 0;
    
    console.log(`🚀 BATCH PROCESSING: Starting from index ${startIndex}/${campaign.contacts.length}`);
    
    // Process in batches
    for (let batchStart = startIndex; batchStart < campaign.contacts.length; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, campaign.contacts.length);
      const batchNumber = Math.floor(batchStart / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(campaign.contacts.length / BATCH_SIZE);
      
      console.log(`📦 BATCH ${batchNumber}/${totalBatches}: Processing calls ${batchStart + 1}-${batchEnd} of ${campaign.contacts.length}`);
      
      // CRITICAL: Check if campaign was manually stopped or instance created
      const batchProgress = campaignCallingProgress.get(campaignId);
      if (batchProgress && batchProgress.manuallyStopped) {
        console.log(`🛑 MANUAL STOP DETECTED: Campaign ${campaignId} was manually stopped, stopping batch processing`);
        break;
      }
      
      // CRITICAL: Check if campaign is still active
      const campaignActive = activeCampaigns.get(campaignId);
      if (!campaignActive) {
        console.log(`🛑 CAMPAIGN STOPPED: Campaign ${campaignId} removed from active campaigns, stopping batch processing`);
        break;
      }
      
      // CRITICAL: Check if campaign is still running in database (cached check)
      const Campaign = require('../models/Campaign');
      // Only check database every 5 batches to reduce delay
      if (batchNumber % 5 === 1) {
        const currentCampaign = await Campaign.findById(campaignId).lean();
        if (!currentCampaign || !currentCampaign.isRunning) {
          console.log(`🛑 DATABASE STOP: Campaign ${campaignId} is not running in database, stopping batch processing`);
          // Remove from active campaigns
          activeCampaigns.delete(campaignId);
          break;
        }
      }
      
      try {
        // Process this batch with safe delay
        await processBatch(campaign, agentId, apiKey, SAFE_DELAY_BETWEEN_CALLS, clientId, progress, runId, batchStart, batchEnd);
      } catch (batchError) {
        console.error(`❌ BATCH ${batchNumber} FAILED:`, batchError);
        
        // Continue with next batch instead of stopping entire campaign
        console.log(`🔄 CONTINUING: Skipping batch ${batchNumber}, moving to next batch`);
        continue;
      }
      
      // Update progress index to end of current batch
      progress.currentIndex = batchEnd;
      campaignCallingProgress.set(campaignId, progress);
      
      // Save progress after each batch
      await saveBatchProgress(campaign, progress, runId, batchNumber, totalBatches);
      
      // Check if campaign should continue
      const isActive = activeCampaigns.get(campaignId);
      if (!isActive) {
        console.log(`🛑 CAMPAIGN STOPPED: Campaign ${campaignId} removed from active campaigns after batch ${batchNumber}`);
        break;
      }
      
      const currentProgress = campaignCallingProgress.get(campaignId);
      if (currentProgress && currentProgress.manuallyStopped) {
        console.log(`🛑 MANUAL STOP DETECTED: Campaign ${campaignId} was manually stopped after batch ${batchNumber}`);
        break;
      }
      
      // Small delay between batches to prevent overwhelming the system
      if (batchEnd < campaign.contacts.length) {
        console.log(`⏳ BATCH COMPLETE: Waiting 2 seconds before next batch...`);
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    // Final completion
    progress.isRunning = false;
    progress.endTime = new Date();
    progress.waitingForCallsToComplete = true;
    campaignCallingProgress.set(campaignId, progress);
    
    console.log(`✅ CAMPAIGN COMPLETED: All ${campaign.contacts.length} calls processed`);
    
    // Watchdog: after a short window, force-finalize history if all calls appear finalized
    try {
      const FINALIZE_AFTER_MS = 60000; // 60s buffer to allow last logs to land
      setTimeout(async () => {
        try {
          const Campaign = require('../models/Campaign');
          const refreshed = await Campaign.findById(campaignId).select('_id details isRunning updatedAt createdAt clientId').lean();
          const details = Array.isArray(refreshed?.details) ? refreshed.details : [];
          const initiatedCount = details.length;
          const completedCount = details.filter(d => d && d.status === 'completed').length;
          const hasActive = details.some(d => d && (d.status === 'ringing' || d.status === 'ongoing'));
          const allCallsFinalized = initiatedCount > 0 && !hasActive && completedCount === initiatedCount;
          if (allCallsFinalized) {
            try {
              await autoSaveCampaignRun({ ...campaign, details }, campaignCallingProgress.get(campaignId) || progress);
            } catch (e) {
              console.error('❌ Watchdog auto-save failed:', e?.message);
            }
          }
        } catch (e) {
          console.error('❌ Watchdog finalize check failed:', e?.message);
        }
      }, FINALIZE_AFTER_MS);
    } catch {}
     
  } catch (error) {
    console.error(`❌ Error in campaign calling process for ${campaignId}:`, error);
    progress.isRunning = false;
    progress.endTime = new Date();
    campaignCallingProgress.set(campaignId, progress);
  }
}

/**
 * Process a single batch of calls
 */
async function processBatch(campaign, agentId, apiKey, delayBetweenCalls, clientId, progress, runId, batchStart, batchEnd) {
  const campaignId = campaign._id.toString();
  
  try {
    // Check if campaign still exists and is valid
    if (!campaign || !campaign.contacts || campaign.contacts.length === 0) {
      console.log(`⚠️ BATCH SKIP: Campaign ${campaignId} is invalid or has no contacts`);
      return;
    }
    for (let i = batchStart; i < batchEnd; i++) {
      // Check if campaign should stop (manual stop or removed from active campaigns)
      const callActive = activeCampaigns.get(campaignId);
      console.log(`🔄 PROCESS CALL ${i + 1}/${campaign.contacts.length}: activeCampaigns.get(${campaignId}) = ${callActive}`);
      
      if (!callActive) {
        console.log(`🛑 CAMPAIGN STOPPED: Campaign ${campaignId} removed from active campaigns, stopping at contact ${i + 1}/${campaign.contacts.length}`);
        break;
      }
      
      // Check if campaign was manually stopped
      const callProgress = campaignCallingProgress.get(campaignId);
      console.log(`🔄 PROCESS CALL ${i + 1}/${campaign.contacts.length}: progress.manuallyStopped = ${callProgress?.manuallyStopped}`);
      
      if (callProgress && callProgress.manuallyStopped) {
        console.log(`🛑 MANUAL STOP DETECTED: Campaign ${campaignId} was manually stopped, stopping at contact ${i + 1}/${campaign.contacts.length}`);
        break;
      }

      // Update progress
      progress.currentIndex = i;
      campaignCallingProgress.set(campaignId, progress);

      const contact = campaign.contacts[i];
      

      // CRITICAL: Add delay BEFORE making call (except first call in batch)
      if (i > batchStart) {
        console.log(`⏳ WAITING ${delayBetweenCalls}ms before call ${i + 1}...`);
        await new Promise(resolve => setTimeout(resolve, delayBetweenCalls));
      }
      
      // Make the call with individual error handling
      console.log(`🔄 MAKING CALL ${i + 1}/${campaign.contacts.length}: ${contact.phone}`);
      if (i === batchStart) {
        console.log(`🚀 FIRST CALL IN BATCH: Starting immediately without delay`);
      }
      let callResult;
      try {
        callResult = await makeSingleCall(contact, agentId, apiKey, campaign._id, clientId, runId, null);
      } catch (callError) {
        console.error(`❌ INDIVIDUAL CALL ERROR: ${callError.message}`);
        callResult = {
          success: false,
          error: callError.message,
          contact,
          uniqueId: `error-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          timestamp: new Date()
        };
      }
      
      // Update progress
      progress.completedCalls++;
      progress.lastCallTime = new Date();
      
      if (callResult.success) {
        progress.successfulCalls++;
        console.log(`✅ CALL SUCCESS: ${callResult.uniqueId}`);
        
        // Add call details to campaign with new structure
        if (callResult.uniqueId) {
          const initiatedAt = new Date();
          const callDetail = {
            uniqueId: callResult.uniqueId,
            contactId: contact._id || null,
            time: initiatedAt,
            status: 'ringing', // Start with 'ringing' status when call is initiated
            lastStatusUpdate: initiatedAt,
            callDuration: 0,
            ...(runId ? { runId } : {})
          };
          
          // Atomically push if missing to avoid version conflicts
          const Campaign = require('../models/Campaign');
          await Campaign.updateOne(
            { _id: campaign._id, 'details.uniqueId': { $ne: callResult.uniqueId } },
            { $push: { details: callDetail } }
          );
          console.log(`📝 ADDED CALL DETAIL (atomic): ${callResult.uniqueId}`);
          // Schedule a status check after ~40s to mirror frontend behavior
          setTimeout(() => {
            updateCallStatusFromLogs(campaign._id, callResult.uniqueId).catch(() => {});
          }, 40000);
        }
        // Do not wait for call to complete; proceed to next number immediately
      } else {
        progress.failedCalls++;
        console.log(`❌ CALL FAILED: ${callResult.error}`);
        
        // Add failed call details to campaign so it shows up in the UI
        if (callResult.uniqueId) {
          const initiatedAt = new Date();
          const callDetail = {
            uniqueId: callResult.uniqueId,
            contactId: contact._id || null,
            time: initiatedAt,
            status: 'completed', // Mark failed calls as completed immediately
            lastStatusUpdate: initiatedAt,
            callDuration: 0,
            leadStatus: 'not_connected',
            ...(runId ? { runId } : {})
          };
          
          // Atomically push if missing to avoid version conflicts
          try {
            const Campaign = require('../models/Campaign');
            await Campaign.updateOne(
              { _id: campaign._id, 'details.uniqueId': { $ne: callResult.uniqueId } },
              { $push: { details: callDetail } }
            );
            console.log(`📝 ADDED FAILED CALL DETAIL (atomic): ${callResult.uniqueId}`);
          } catch (dbError) {
            console.error(`❌ DATABASE ERROR: Failed to save call detail ${callResult.uniqueId}:`, dbError.message);
            // Continue processing even if database save fails
          }
        }
      }

      campaignCallingProgress.set(campaignId, progress);

      // Wait before next call (except for last call in batch)
      if (i < batchEnd - 1) {
        await new Promise(resolve => setTimeout(resolve, delayBetweenCalls));
      }
    }
  } catch (error) {
    console.error(`❌ Error processing batch ${batchStart}-${batchEnd} for campaign ${campaignId}:`, error);
    throw error;
  }
}

/**
 * Save batch progress and create intermediate campaign history
 */
async function saveBatchProgress(campaign, progress, runId, batchNumber, totalBatches) {
  try {
    const campaignId = campaign._id.toString();
    const CampaignHistory = require('../models/CampaignHistory');
    
    // CRITICAL: Check if campaign is still running before saving
    const Campaign = require('../models/Campaign');
    const currentCampaign = await Campaign.findById(campaign._id).lean();
    if (!currentCampaign || !currentCampaign.isRunning) {
      console.log(`🛑 SAVE SKIP: Campaign ${campaignId} is not running, skipping batch save`);
      return;
    }
    
    // CRITICAL: Check if campaign was manually stopped
    const currentProgress = campaignCallingProgress.get(campaignId);
    if (currentProgress && currentProgress.manuallyStopped) {
      console.log(`🛑 SAVE SKIP: Campaign ${campaignId} was manually stopped, skipping batch save`);
      return;
    }
    
    console.log(`💾 SAVING BATCH ${batchNumber}/${totalBatches} PROGRESS (appending to run ${runId})...`);
    
    // Determine the slice of contacts for this batch (100 per batch)
    const batchStartIndex = (batchNumber - 1) * 100;
    const batchEndIndex = Math.min(batchStartIndex + 100, campaign.contacts.length);
    
    // Reload the latest campaign to get up-to-date details
    const updatedCampaign = await Campaign.findById(campaign._id);
    const campaignDetails = Array.isArray(updatedCampaign.details) ? updatedCampaign.details : [];
    
    // Filter details for this runId and take the slice for this batch
    const runDetails = campaignDetails.filter(d => d && d.uniqueId && d.runId === runId);
    const batchCallDetails = runDetails.slice(batchStartIndex, batchEndIndex);
    
    // Map contacts for this batch
    const contactsForBatch = batchCallDetails.map((detail, index) => {
      const contactIndex = batchStartIndex + index;
      const contact = (updatedCampaign.contacts || [])[contactIndex] || {};
      return {
        documentId: detail.uniqueId,
        number: contact.phone || contact.number || '',
        name: contact.name || '',
        leadStatus: detail.leadStatus || 'not_connected',
        contactId: detail.contactId || contact._id || '',
        time: detail.time || new Date().toISOString(),
        status: detail.status || 'completed',
        duration: detail.callDuration || 0,
        transcriptCount: 0,
        whatsappMessageSent: false,
        whatsappRequested: false
      };
    });
    
    const successfulInBatch = batchCallDetails.filter(d => (d.leadStatus || '').toLowerCase() === 'connected').length;
    const totalDurationInBatch = batchCallDetails.reduce((sum, d) => sum + (d.callDuration || 0), 0);
    const totalInBatch = contactsForBatch.length;
    const failedInBatch = totalInBatch - successfulInBatch;
    
    const now = new Date();
    const startDate = progress && progress.startTime instanceof Date ? progress.startTime : new Date(progress && progress.startTime || now);
    const elapsedSeconds = Math.max(0, Math.floor((now - startDate) / 1000));
    const toHms = (secs) => ({
      hours: Math.floor(secs / 3600),
      minutes: Math.floor((secs % 3600) / 60),
      seconds: secs % 60
    });
    
    // Ensure a single CampaignHistory per runId. Upsert and append contacts + stats.
    const existingHistory = await CampaignHistory.findOne({ runId }).lean();
    let instanceNumber = 1;
    if (!existingHistory) {
      const existingCount = await CampaignHistory.countDocuments({ campaignId });
      instanceNumber = existingCount + 1;
    }
    
    await CampaignHistory.findOneAndUpdate(
      { runId },
      {
        $setOnInsert: {
          campaignId: campaign._id,
          runId,
          instanceNumber,
          startTime: (startDate && startDate.toISOString) ? startDate.toISOString() : String(startDate),
          status: 'running',
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
          endTime: now.toISOString(),
          runTime: toHms(elapsedSeconds),
          updatedAt: now
        },
        $push: {
          contacts: { $each: contactsForBatch }
        },
        $inc: {
          'stats.totalContacts': totalInBatch,
          'stats.successfulCalls': successfulInBatch,
          'stats.failedCalls': failedInBatch,
          'stats.totalCallDuration': totalDurationInBatch
        }
      },
      { upsert: true, new: true }
    );
    
    // Optionally recompute averageCallDuration based on current totals
    const updatedHistory = await CampaignHistory.findOne({ runId }, { 'stats.totalContacts': 1, 'stats.totalCallDuration': 1 }).lean();
    if (updatedHistory && updatedHistory.stats) {
      const { totalContacts, totalCallDuration } = updatedHistory.stats;
      const avg = totalContacts > 0 ? Math.round(totalCallDuration / totalContacts) : 0;
      await CampaignHistory.updateOne(
        { runId },
        { $set: { 'stats.averageCallDuration': avg } }
      );
    }
    
    console.log(`✅ BATCH ${batchNumber}/${totalBatches} APPENDED: ${progress.completedCalls} calls processed (run ${runId})`);
    
    // CRITICAL: If this is the final batch, do not force stop here; let series/other flows manage isRunning
    
    // Update progress in memory
    campaignCallingProgress.set(campaignId, progress);
    
  } catch (error) {
    console.error(`❌ Error saving batch ${batchNumber} progress:`, error);
    // Don't throw error to prevent stopping the campaign
  }
}

/**
 * Stop campaign calling process
 */
function stopCampaignCalling(campaignId) {
  
  // Remove from active campaigns to stop new calls
  activeCampaigns.delete(campaignId);
  
  const progress = campaignCallingProgress.get(campaignId);
  
  if (progress) {
    progress.isRunning = false;
    progress.endTime = new Date();
    progress.manuallyStopped = true; // Mark as manually stopped
    campaignCallingProgress.set(campaignId, progress);
    console.log(`✅ MANUAL STOP: Progress updated:`, {
      isRunning: progress.isRunning,
      manuallyStopped: progress.manuallyStopped,
      endTime: progress.endTime
    });
  } else {
    console.log(`⚠️ MANUAL STOP: No progress found for campaign ${campaignId}`);
  }
  
  // Note: Ongoing calls will continue until they naturally complete
  // The automatic status updates will handle marking them as completed
  // when their CallLog shows isActive: false
}

/**
 * Get campaign calling progress
 */
function getCampaignCallingProgress(campaignId) {
  return campaignCallingProgress.get(campaignId) || null;
}

/**
 * Get all active campaigns
 */
function getActiveCampaigns() {
  return Array.from(activeCampaigns.keys());
}

/**
 * Clean up completed campaigns (run periodically)
 */
function cleanupCompletedCampaigns() {
  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000); // 1 hour ago

  for (const [campaignId, progress] of campaignCallingProgress.entries()) {
    if (!progress.isRunning && progress.endTime && progress.endTime < oneHourAgo) {
      campaignCallingProgress.delete(campaignId);
    }
  }
}

// Clean up completed campaigns every hour
setInterval(cleanupCompletedCampaigns, 60 * 60 * 1000);

// Clean up stale active calls every 10 minutes
setInterval(cleanupStaleActiveCalls, 10 * 60 * 1000);

// Memory cleanup interval removed

// Graceful shutdown handler
process.on('SIGTERM', async () => {
  console.log('🛑 SIGTERM received, shutting down gracefully...');
  
  try {
    // Stop all active campaigns
    for (const [campaignId, progress] of campaignCallingProgress.entries()) {
      if (progress.isRunning) {
        progress.isRunning = false;
        progress.endTime = new Date();
        progress.manuallyStopped = true;
        campaignCallingProgress.set(campaignId, progress);
        console.log(`🛑 Stopped campaign ${campaignId}`);
      }
    }
    
    // Clear active campaigns
    activeCampaigns.clear();
    
    // Stop status update interval
    if (statusUpdateInterval) {
      clearInterval(statusUpdateInterval);
      statusUpdateInterval = null;
    }
    
    console.log('✅ Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during graceful shutdown:', error);
    process.exit(1);
  }
});

process.on('SIGINT', async () => {
  console.log('🛑 SIGINT received, shutting down gracefully...');
  
  try {
    // Stop all active campaigns
    for (const [campaignId, progress] of campaignCallingProgress.entries()) {
      if (progress.isRunning) {
        progress.isRunning = false;
        progress.endTime = new Date();
        progress.manuallyStopped = true;
        campaignCallingProgress.set(campaignId, progress);
        console.log(`🛑 Stopped campaign ${campaignId}`);
      }
    }
    
    // Clear active campaigns
    activeCampaigns.clear();
    
    // Stop status update interval
    if (statusUpdateInterval) {
      clearInterval(statusUpdateInterval);
      statusUpdateInterval = null;
    }
    
    console.log('✅ Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    console.error('❌ Error during graceful shutdown:', error);
    process.exit(1);
  }
});

/**
 * DEBUG: Check current call status for a specific uniqueId
 */
async function debugCallStatus(uniqueId) {
  try {
    const CallLog = require('../models/CallLog');
    
    // Find the most recent call log for this uniqueId
    const callLog = await CallLog.findOne(
      { 'metadata.customParams.uniqueid': uniqueId },
      {},
      { sort: { updatedAt: -1 } }
    ).lean();

    if (!callLog) {
      return null;
    }

    const isActive = callLog.metadata?.isActive;
    const leadStatus = callLog.leadStatus;
    const createdAt = callLog.createdAt;
    const callDuration = Math.floor((new Date() - createdAt) / 1000);
    
    // Determine expected status based on SIMPLE logic
    let expectedStatus = 'ringing';
    if (isActive === true) {
      expectedStatus = 'ongoing';
    } else if (isActive === false) {
      expectedStatus = 'completed';
    } else {
      // isActive is undefined/null - check if 60 seconds passed (increased from 40s)
      if (callDuration >= 60) {
        expectedStatus = 'completed';
      }
    }
    
    
    
    return {
      uniqueId,
      isActive,
      leadStatus,
      createdAt,
      callDuration,
      expectedStatus,
      shouldMarkAsCompleted: callDuration >= 60
    };
  } catch (error) {
    console.error('❌ Error in debug call status:', error);
    return null;
  }
}

/**
 * MANUAL: Fix stuck calls that have been "active" for too long
 */
async function fixStuckCalls() {
  try {
    const CallLog = require('../models/CallLog');
    const Campaign = require('../models/Campaign');
    
    
    // Find all CallLogs that have been "active" for more than 8 minutes
    const eightMinutesAgo = new Date(Date.now() - 8 * 60 * 1000);
    const stuckCallLogs = await CallLog.find({
      'metadata.isActive': true,
      createdAt: { $lt: eightMinutesAgo }
    }).lean();
    
    
    for (const callLog of stuckCallLogs) {
      const uniqueId = callLog.metadata?.customParams?.uniqueid;
      if (!uniqueId) continue;
      
      
      // Update CallLog to mark as inactive
      await CallLog.findByIdAndUpdate(callLog._id, {
        'metadata.isActive': false,
        'metadata.callEndTime': new Date(),
        leadStatus: 'not_connected'
      });
      
      // Find and update campaign details
      const campaigns = await Campaign.find({
        'details.uniqueId': uniqueId
      });
      
      for (const campaign of campaigns) {
        const callDetail = campaign.details.find(d => d.uniqueId === uniqueId);
        if (callDetail && callDetail.status !== 'completed') {
          const now = new Date();
          await Campaign.updateOne(
            { _id: campaign._id, 'details.uniqueId': uniqueId },
            {
              $set: {
                'details.$.status': 'completed',
                'details.$.lastStatusUpdate': now,
                'details.$.callDuration': Math.floor((now - callDetail.time) / 1000)
              }
            }
          );
          console.log(`✅ MANUAL: Updated campaign ${campaign._id} call ${uniqueId} to completed`);
        }
      }
    }    
  } catch (error) {
    console.error('❌ Error fixing stuck calls:', error);
  }
}

/**
 * AUTOMATIC: Cleanup stale active calls (runs every 10 minutes)
 */
async function cleanupStaleActiveCalls() {
  try {
    const CallLog = require('../models/CallLog');
    
    
    
    // Find all CallLogs that have been "active" for more than 8 minutes
    const eightMinutesAgo = new Date(Date.now() - 8 * 60 * 1000);
    const staleCallLogs = await CallLog.find({
      'metadata.isActive': true,
      createdAt: { $lt: eightMinutesAgo }
    });
    
    if (staleCallLogs.length === 0) {
      return;
    }
    
    
    
    // Update all stale calls to inactive
    const updateResult = await CallLog.updateMany(
      {
        'metadata.isActive': true,
        createdAt: { $lt: eightMinutesAgo }
      },
      {
        $set: {
          'metadata.isActive': false,
          'metadata.callEndTime': new Date(),
          leadStatus: 'not_connected'
        }
      }
    );
    
    
    
  } catch (error) {
    console.error('❌ Error cleaning up stale active calls:', error);
  }
}

/**
 * SERVER RESTART: Cleanup campaigns that might be stuck in running state
 */
async function cleanupStuckCampaignsOnRestart() {
  try {    
    const Campaign = require('../models/Campaign');
    
    // Find campaigns that are marked as running but have no active progress
    const stuckCampaigns = await Campaign.find({
      isRunning: true
    }).lean();
    
    if (stuckCampaigns.length === 0) {
      console.log('✅ SERVER RESTART: No stuck campaigns found');
      return;
    }
    
    console.log(`🔍 SERVER RESTART: Found ${stuckCampaigns.length} potentially stuck campaigns`);
    
    for (const campaign of stuckCampaigns) {
      const campaignId = campaign._id.toString();
      const progress = campaignCallingProgress.get(campaignId);
      
      // If no progress exists in memory, this campaign is likely stuck
      if (!progress) {
        console.log(`⚠️ SERVER RESTART: Campaign ${campaignId} is marked as running but has no progress - marking as stopped`);
        
        // Update campaign to not running
        await Campaign.findByIdAndUpdate(campaignId, {
          isRunning: false
        });
        
        // Remove from active campaigns if it exists
        activeCampaigns.delete(campaignId);
        
        console.log(`✅ SERVER RESTART: Campaign ${campaignId} marked as stopped`);
      } else {
        console.log(`ℹ️ SERVER RESTART: Campaign ${campaignId} has progress - leaving as is`);
      }
    }
    
    console.log('✅ SERVER RESTART: Stuck campaigns cleanup completed');
    
  } catch (error) {
    console.error('❌ SERVER RESTART: Error cleaning up stuck campaigns:', error);
  }
}

/**
 * SIMPLE: Cleanup completed campaigns with details (manual only)
 */
async function cleanupCompletedCampaignsWithDetails() {
  try {
    console.log('🧹 MANUAL: Checking for completed campaigns with details...');
    
    const Campaign = require('../models/Campaign');
    
    // Find campaigns that are not running but still have details
    const completedCampaignsWithDetails = await Campaign.find({
      isRunning: false,
      details: { $exists: true, $ne: [], $not: { $size: 0 } }
    }).lean();
    
    console.log(`🧹 MANUAL: Found ${completedCampaignsWithDetails.length} completed campaigns with details`);
    
    for (const campaign of completedCampaignsWithDetails) {
      try {
        console.log(`🧹 MANUAL: Processing completed campaign ${campaign._id} with ${campaign.details.length} details`);
        
        // Create progress object for auto-save
        const progress = {
          campaignId: campaign._id.toString(),
          totalContacts: campaign.contacts?.length || 0,
          currentIndex: campaign.details?.length || 0,
          completedCalls: campaign.details?.filter(d => d?.status === 'completed').length || 0,
          successfulCalls: 0,
          failedCalls: 0,
          startTime: new Date(campaign.updatedAt || campaign.createdAt),
          isRunning: false,
          endTime: new Date(),
          waitingForCallsToComplete: false,
          runId: `cleanup-${Date.now()}`,
          recovered: true
        };
        
        // Auto-save the campaign run
        const fullCampaign = await Campaign.findById(campaign._id);
        if (fullCampaign) {
          await autoSaveCampaignRun(fullCampaign, progress);
          console.log(`✅ MANUAL: Saved details for completed campaign ${campaign._id}`);
        }
        
      } catch (error) {
        console.error(`❌ MANUAL: Error processing completed campaign ${campaign._id}:`, error);
      }
    }
    
    console.log(`✅ MANUAL: Completed campaigns cleanup finished`);
  } catch (error) {
    console.error('❌ MANUAL: Error during completed campaigns cleanup:', error);
  }
}

/**
 * Auto-save campaign run when all calls are completed after manual stop
 */
async function autoSaveCampaignRun(campaign, progress) {
  try {
    if (!progress || !progress.startTime) {
      console.log(`⚠️ No progress or startTime found for campaign ${campaign._id}`);
      return;
    }

    const now = new Date();
    const startTime = progress.startTime;
    const endTime = now;
    const runTime = Math.floor((endTime - startTime) / 1000); // in seconds
    const runId = progress.runId || `run-${Date.now()}`;

    console.log(`💾 AUTO-SAVE: Saving campaign run for ${campaign._id}, runTime: ${runTime}s`);

    // Fetch call logs for this run
    const CallLog = require('../models/CallLog');
    const callLogs = await CallLog.find({
      campaignId: campaign._id,
      'metadata.customParams.runId': runId
    }).lean();

    console.log(`💾 AUTO-SAVE: Found ${callLogs.length} call logs for run ${runId}`);

    // Create campaign history entry
    const historyEntry = {
      runId,
      startTime,
      endTime,
      runTime,
      callLogs: callLogs.map(log => ({
        uniqueId: log.metadata?.customParams?.uniqueid,
        phone: log.mobile,
        duration: log.duration,
        leadStatus: log.leadStatus,
        statusText: log.statusText,
        time: log.time
      })),
      totalCalls: callLogs.length,
      successfulCalls: callLogs.filter(log => log.leadStatus && log.leadStatus !== 'not_connected').length,
      failedCalls: callLogs.filter(log => !log.leadStatus || log.leadStatus === 'not_connected').length
    };

    // Add to campaign history
    if (!campaign.history) {
      campaign.history = [];
    }
    campaign.history.push(historyEntry);
    await campaign.save();

    console.log(`✅ AUTO-SAVE: Campaign run saved successfully for ${campaign._id}`);
  } catch (error) {
    console.error(`❌ AUTO-SAVE: Error saving campaign run for ${campaign._id}:`, error);
  }
}



/**
 * Reset circuit breakers (for debugging)
 */
function resetCircuitBreakers() {
  apiCircuitBreaker.failureCount = 0;
  apiCircuitBreaker.state = 'CLOSED';
  sanpbxCircuitBreaker.failureCount = 0;
  sanpbxCircuitBreaker.state = 'CLOSED';
  console.log('🔄 Circuit breakers reset');
}


async function migrateMissedToCompleted() {
  try {
    const Campaign = require('../models/Campaign');
    
    // Find all campaigns with 'missed' status
    const campaignsWithMissed = await Campaign.find({
      'details.status': 'missed'
    });
    
    if (campaignsWithMissed.length === 0) {
      return;
    }
    
    console.log(`🔄 MIGRATION: Found ${campaignsWithMissed.length} campaigns with "missed" status, converting to "completed"...`);
    
    let totalConverted = 0;
    
    for (const campaign of campaignsWithMissed) {
      const missedDetails = campaign.details.filter(d => d.status === 'missed');
      
      for (const detail of missedDetails) {
        detail.status = 'completed';
        detail.lastStatusUpdate = new Date();
        // Calculate call duration
        detail.callDuration = Math.floor((new Date() - detail.time) / 1000);
        totalConverted++;
      }
      
      await campaign.save();
      console.log(`✅ MIGRATION: Converted ${missedDetails.length} "missed" calls to "completed" in campaign ${campaign._id}`);
    }
    
    console.log(`✅ MIGRATION: Completed! Converted ${totalConverted} total calls from "missed" to "completed"`);
    
  } catch (error) {
    console.error('❌ MIGRATION: Error converting missed to completed:', error);
  }
}

// AUTOMATIC: Start the background status update service after all functions are defined
console.log('🚀 Starting automatic campaign call status update service...');

// Run migrations first, then fix stuck calls, then start automatic updates
migrateMissedToCompleted().then(() => {
  return fixStuckCalls();
}).then(() => {
  // Run campaign validation fix
  const { exec } = require('child_process');
  return new Promise((resolve) => {
    exec('node scripts/fixCampaignValidation.js', (error, stdout, stderr) => {
      if (error) {
        console.log('⚠️ Campaign validation fix failed:', error.message);
      } else {
      }
      resolve();
    });
  });
}).then(() => {
  startAutomaticStatusUpdates();
});


module.exports = {
  getClientApiKey,
  makeSingleCall,
  startCampaignCalling,
  stopCampaignCalling,
  getCampaignCallingProgress,
  getActiveCampaigns,
  cleanupCompletedCampaigns,
  updateCallStatusFromLogs,
  updateCampaignRunningStatus,
  startAutomaticStatusUpdates,
  stopAutomaticStatusUpdates,
  updateAllCampaignCallStatuses,
  triggerManualStatusUpdate,
  debugCallStatus,
  migrateMissedToCompleted,
  fixStuckCalls,
  cleanupStaleActiveCalls,
  cleanupStuckCampaignsOnRestart,
  saveBatchProgress,
  autoSaveCampaignRun,
  cleanupCompletedCampaignsWithDetails,
  resetCircuitBreakers,
  processCampaignCallsNGR,
};

/**
 * MIGRATION: Convert any existing 'missed' status to 'completed'
 */
