const socketio = require('socket.io');
const mongoose = require('mongoose');
const CallLog = require('../models/CallLog');
const Campaign = require('../models/Campaign');

let io = null;
const campaignPollers = new Map(); // campaignId -> { refCount, timer, running, lastState: Map }
const DEFAULT_POLL_INTERVAL_MS = 3000;

function toPlain(doc) {
  if (!doc) return null;
  if (typeof doc.toObject === 'function') {
    return doc.toObject({ virtuals: false, getters: true });
  }
  if (doc._doc) {
    return { ...doc._doc };
  }
  return doc;
}

function parseTranscriptLines(transcriptText = '') {
  return String(transcriptText)
    .split('\n')
    .map(line => line.trim())
    .filter(Boolean)
    .map(line => {
      const match = line.match(/^\[(.*?)\]\s*(.*?)\s*(?:\((.*?)\))?\s*:\s*(.*)$/);
      if (!match) {
        return {
          raw: line,
          text: line
        };
      }
      const [, timestamp, speakerLabel, language, text] = match;
      const speakerParts = speakerLabel.split(/\s+/);
      const speaker = speakerParts.shift() || speakerLabel;
      const role = speaker.toLowerCase().includes('ai') ? 'assistant' : (speaker.toLowerCase().includes('user') ? 'user' : speaker.toLowerCase());
      return {
        timestamp,
        speaker: speakerLabel.trim(),
        role,
        language: language ? language.trim() : null,
        text: text.trim(),
      };
    });
}

function deriveCallStatus(log) {
  if (!log || !log.metadata) return 'unknown';
  if (log.metadata.isActive) return 'ongoing';
  if (log.metadata.callEndTime) return 'completed';
  return log.leadStatus || 'unknown';
}

function formatCallLogEntry(rawLog) {
  if (!rawLog) return null;
  const log = toPlain(rawLog);
  
  // Get transcript from various possible locations
  const transcriptText = log.transcript || 
                        log?.metadata?.transcript || 
                        log?.transcriptText || 
                        '';
  
  const uniqueId = log?.metadata?.customParams?.uniqueid || 
                   log?.metadata?.uniqueid || 
                   log?.uniqueId || 
                   log?.uniqueid ||
                   log?.metadata?.customParams?.uniqueId ||
                   null;
  
  // Parse transcript segments
  const transcriptSegments = parseTranscriptLines(transcriptText);
  
  // Build comprehensive call log entry with ALL fields
  const formatted = {
    // Basic identifiers
    id: log._id ? String(log._id) : null,
    campaignId: log.campaignId ? String(log.campaignId) : null,
    agentId: log.agentId ? String(log.agentId) : null,
    clientId: log.clientId || null,
    uniqueId,
    
    // Contact information
    mobile: log.mobile || 
            log?.metadata?.customParams?.customercontact || 
            log?.metadata?.customParams?.phone ||
            log?.phone ||
            null,
    
    // Call status and disposition
    leadStatus: log.leadStatus || 'unknown',
    status: deriveCallStatus(log),
    disposition: log.disposition || null,
    subDisposition: log.subDisposition || null,
    dispositionId: log.dispositionId || null,
    subDispositionId: log.subDispositionId || null,
    
    // Timing information
    durationSeconds: typeof log.duration === 'number' ? log.duration : null,
    time: log.time || null,
    startedAt: log.createdAt || log.time || null,
    updatedAt: log.updatedAt || log?.metadata?.lastUpdated || null,
    
    // Media and recording
    recordingUrl: log.audioUrl || null,
    streamSid: log.streamSid || null,
    callSid: log.callSid || null,
    
    // Transcript information (comprehensive)
    transcript: {
      text: transcriptText,
      segments: transcriptSegments,
      segmentCount: transcriptSegments.length,
      hasContent: transcriptText.length > 0,
      lastUpdated: log.updatedAt || log?.metadata?.lastUpdated || null,
    },
    
    // Complete metadata (all fields)
    metadata: {
      isActive: !!(log.metadata && log.metadata.isActive),
      callEndTime: log.metadata?.callEndTime || null,
      callStartTime: log.createdAt || log.time || null,
      callDirection: log.metadata?.callDirection || 'outbound',
      languages: Array.isArray(log.metadata?.languages) ? log.metadata.languages : [],
      customParams: log.metadata?.customParams || {},
      totalUpdates: log.metadata?.totalUpdates || 0,
      userTranscriptCount: log.metadata?.userTranscriptCount || 0,
      aiResponseCount: log.metadata?.aiResponseCount || 0,
      callerId: log.metadata?.callerId || null,
      averageResponseTime: log.metadata?.averageResponseTime || null,
      sttProvider: log.metadata?.sttProvider || 'deepgram',
      ttsProvider: log.metadata?.ttsProvider || 'sarvam',
      llmProvider: log.metadata?.llmProvider || 'openai',
      whatsappRequested: log.metadata?.whatsappRequested || false,
      whatsappMessageSent: log.metadata?.whatsappMessageSent || false,
      lastUpdated: log.metadata?.lastUpdated || log.updatedAt || null,
    },
    
    // Raw fields (for debugging/completeness)
    raw: {
      streamSid: log.streamSid || null,
      callSid: log.callSid || null,
      time: log.time || null,
    }
  };
  
  return formatted;
}

async function buildCampaignTranscriptSnapshot(campaignId, { limit = 200 } = {}) {
  if (!campaignId) {
    throw new Error('campaignId is required');
  }

  if (!mongoose.Types.ObjectId.isValid(campaignId)) {
    throw new Error('Invalid campaignId');
  }

  const [campaignDoc, callLogs] = await Promise.all([
    Campaign.findById(campaignId).lean(),
    CallLog.find({ campaignId })
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean()
  ]);

  if (!campaignDoc) {
    throw new Error('Campaign not found');
  }

  const formattedCalls = callLogs.map(formatCallLogEntry);
  const activeCalls = formattedCalls.filter(call => call?.metadata?.isActive).length;

  return {
    campaignId: String(campaignDoc._id),
    campaign: {
      id: String(campaignDoc._id),
      name: campaignDoc.name,
      status: campaignDoc.status || (campaignDoc.isRunning ? 'running' : 'idle'),
      isRunning: !!campaignDoc.isRunning,
      totalContacts: Array.isArray(campaignDoc.contacts) ? campaignDoc.contacts.length : 0,
      updatedAt: campaignDoc.updatedAt,
      createdAt: campaignDoc.createdAt,
    },
    totals: {
      callsReturned: formattedCalls.length,
      activeCalls,
      completedCalls: formattedCalls.filter(call => call?.status === 'completed').length,
    },
    calls: formattedCalls,
    fetchedAt: new Date().toISOString(),
  };
}

async function emitCampaignSnapshot(socket, campaignId, options = {}) {
  try {
    const snapshot = await buildCampaignTranscriptSnapshot(campaignId, options);
    socket.emit('campaign-transcripts', snapshot);
  } catch (error) {
    console.error('[wsServer] Failed to emit campaign snapshot:', error?.message || error);
    socket.emit('campaign-transcripts-error', {
      campaignId,
      message: error?.message || 'Failed to fetch campaign transcripts',
    });
  }
}

function ensureCampaignPoller(campaignId) {
  if (!campaignId) return;
  const existing = campaignPollers.get(campaignId);
  if (existing) {
    existing.refCount += 1;
    return existing;
  }

  const poller = {
    refCount: 1,
    running: false,
    timer: null,
    lastState: new Map(),
  };

  poller.timer = setInterval(() => {
    pollCampaignUpdates(campaignId, poller).catch(err => {
      console.error('[wsServer] campaign poller error:', err?.message || err);
    });
  }, DEFAULT_POLL_INTERVAL_MS);

  campaignPollers.set(campaignId, poller);
  return poller;
}

function releaseCampaignPoller(campaignId) {
  const poller = campaignPollers.get(campaignId);
  if (!poller) return;

  poller.refCount -= 1;
  if (poller.refCount <= 0) {
    if (poller.timer) clearInterval(poller.timer);
    campaignPollers.delete(campaignId);
  }
}

async function pollCampaignUpdates(campaignId, poller) {
  if (!io) return;
  if (!poller || poller.running) return;
  poller.running = true;

  try {
    // Fetch call logs sorted by most recently updated first
    const callLogs = await CallLog.find({ campaignId })
      .sort({ updatedAt: -1 })
      .lean();

    const nextState = new Map();
    const room = 'campaign-' + campaignId;
    let updateCount = 0;

    for (const log of callLogs) {
      const formatted = formatCallLogEntry(log);
      if (!formatted) continue;
      const key = formatted.uniqueId || formatted.id;
      if (!key) continue;

      const serialized = JSON.stringify(formatted);
      nextState.set(key, serialized);

      const previous = poller.lastState.get(key);
      let hasChanged = false;
      let changeReason = '';
      
      if (!previous) {
        // New call
        hasChanged = true;
        changeReason = 'new call';
      } else {
        // Compare specific fields that matter for updates
        try {
          const prevFormatted = JSON.parse(previous);
          
          // Check if transcript changed
          const transcriptChanged = formatted.transcript?.text !== prevFormatted.transcript?.text;
          const statusChanged = formatted.status !== prevFormatted.status;
          const metadataChanged = JSON.stringify(formatted.metadata) !== JSON.stringify(prevFormatted.metadata);
          const updatedAtChanged = formatted.updatedAt?.toString() !== prevFormatted.updatedAt?.toString();
          
          if (transcriptChanged || statusChanged || metadataChanged || updatedAtChanged) {
            hasChanged = true;
            const reasons = [];
            if (transcriptChanged) reasons.push('transcript');
            if (statusChanged) reasons.push('status');
            if (metadataChanged) reasons.push('metadata');
            if (updatedAtChanged) reasons.push('updatedAt');
            changeReason = reasons.join(', ');
          }
        } catch (e) {
          // If parsing fails, assume changed
          hasChanged = true;
          changeReason = 'parse error';
        }
      }
      
      if (hasChanged) {
        updateCount++;
        const transcriptLength = formatted.transcript?.text?.length || 0;
        const segmentCount = formatted.transcript?.segmentCount || 0;
        
        console.log(`📤 [SOCKET.IO] Emitting call-transcript-update for campaign ${campaignId}`);
        console.log(`   UniqueId: ${formatted.uniqueId || formatted.id}`);
        console.log(`   Change: ${changeReason}`);
        console.log(`   Status: ${formatted.status}, Transcript: ${transcriptLength} chars, ${segmentCount} segments`);
        console.log(`   Mobile: ${formatted.mobile || 'N/A'}`);
        console.log(`   Updated: ${formatted.updatedAt}`);
        
        io.to(room).emit('call-transcript-update', {
          campaignId: String(campaignId),
          uniqueId: formatted.uniqueId || formatted.id,
          type: 'upsert',
          call: formatted,
          timestamp: new Date().toISOString(),
          changeReason: changeReason,
        });
      }
    }
    
    if (updateCount > 0) {
      console.log(`📊 [SOCKET.IO] Polled campaign ${campaignId}: ${updateCount} updates, ${callLogs.length} total calls`);
    }

    // Detect removed calls
    for (const [key] of poller.lastState.entries()) {
      if (!nextState.has(key)) {
        io.to(room).emit('call-transcript-update', {
          campaignId: String(campaignId),
          uniqueId: key,
          type: 'remove',
        });
      }
    }

    poller.lastState = nextState;
  } catch (error) {
    console.error('[wsServer] pollCampaignUpdates failed:', error?.message || error);
  } finally {
    poller.running = false;
  }
}

function init(server) {
  try {
    io = socketio(server, { 
      cors: { 
        origin: '*',
        methods: ['GET', 'POST'],
        credentials: true
      },
      transports: ['websocket', 'polling'],
      allowEIO3: true, // Support older Socket.IO clients
      pingTimeout: 60000,
      pingInterval: 25000
    });
    
    console.log('✅ [SOCKET.IO] Campaign transcript WebSocket server initialized');
    console.log('   - Path: /socket.io/');
    console.log('   - Transports: websocket, polling');
    console.log('   - CORS: enabled for all origins');
  } catch (error) {
    console.error('❌ [SOCKET.IO] Failed to initialize:', error?.message || error);
    throw error;
  }
  
  // Log all connection attempts
  io.engine.on('connection', (req) => {
    console.log(`🔗 [SOCKET.IO] Connection attempt from ${req.socket.remoteAddress || 'unknown'}`);
  });
  
  io.engine.on('connection_error', (err) => {
    console.error('❌ [SOCKET.IO] Engine connection error:', err?.message || err);
    console.error('   Error details:', {
      type: err?.type,
      description: err?.description,
      context: err?.context
    });
  });

  io.engine.on('upgrade', () => {
    console.log('⬆️ [SOCKET.IO] Transport upgraded to WebSocket');
  });

  io.engine.on('upgradeError', (err) => {
    console.error('❌ [SOCKET.IO] Upgrade error (falling back to polling):', err?.message || err);
  });

  io.on('connection', socket => {
    console.log(`🔌 [SOCKET.IO] Client connected: ${socket.id}`);
    console.log(`   Transport: ${socket.conn?.transport?.name || 'unknown'}`);
    socket.joinedCampaigns = new Set();

    socket.on('join-campaign', async campaignId => {
      if (!campaignId) {
        console.warn(`⚠️ [SOCKET.IO] join-campaign called without campaignId`);
        return;
      }
      console.log(`📥 [SOCKET.IO] Client ${socket.id} joining campaign: ${campaignId}`);
      const room = 'campaign-' + campaignId;
      socket.join(room);
      if (!socket.joinedCampaigns.has(campaignId)) {
        socket.joinedCampaigns.add(campaignId);
        ensureCampaignPoller(campaignId);
        console.log(`✅ [SOCKET.IO] Started poller for campaign: ${campaignId}`);
      }
      await emitCampaignSnapshot(socket, campaignId);
      console.log(`📤 [SOCKET.IO] Sent initial snapshot to ${socket.id} for campaign: ${campaignId}`);
    });

    socket.on('leave-campaign', campaignId => {
      if (!campaignId) return;
      socket.leave('campaign-' + campaignId);
       if (socket.joinedCampaigns?.has(campaignId)) {
        socket.joinedCampaigns.delete(campaignId);
        releaseCampaignPoller(campaignId);
      }
    });

    socket.on('get-campaign-transcripts', async payload => {
      const { campaignId, limit } = typeof payload === 'object' ? payload : { campaignId: payload };
      if (!campaignId) return;
      await emitCampaignSnapshot(socket, campaignId, {
        limit: typeof limit === 'number' && limit > 0 ? limit : undefined,
      });
    });

    socket.on('disconnect', () => {
      console.log(`🔌 [SOCKET.IO] Client disconnected: ${socket.id}`);
      if (!socket.joinedCampaigns) return;
      for (const campaignId of socket.joinedCampaigns) {
        releaseCampaignPoller(campaignId);
      }
      socket.joinedCampaigns.clear();
    });
  });
}

function broadcastCampaignEvent(campaignId, event, payload) {
  if (!io) return;
  const room = 'campaign-' + campaignId;
  io.to(room).emit('campaign-status', { event, ...payload });
}

function broadcastCallEvent(campaignId, uniqueId, status, callLog) {
  if (!io) return;
  const room = 'campaign-' + campaignId;
  const formattedLog = formatCallLogEntry(callLog);
  io.to(room).emit('call-status', { uniqueId, status, callLog: formattedLog });
  if (formattedLog) {
    io.to(room).emit('call-transcript-update', {
      campaignId: String(campaignId),
      call: formattedLog,
      uniqueId,
      status,
    });
  }
}

function getStatus() {
  if (!io) {
    return { initialized: false, connectedClients: 0, activePollers: 0 };
  }
  
  const connectedClients = io.sockets.sockets.size;
  const activePollers = campaignPollers.size;
  const pollerDetails = Array.from(campaignPollers.entries()).map(([campaignId, poller]) => ({
    campaignId,
    refCount: poller.refCount,
    running: poller.running
  }));
  
  return {
    initialized: true,
    connectedClients,
    activePollers,
    pollerDetails
  };
}

module.exports = { init, broadcastCampaignEvent, broadcastCallEvent, buildCampaignTranscriptSnapshot, getStatus };
