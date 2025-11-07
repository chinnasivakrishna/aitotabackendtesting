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
  const transcriptText = log.transcript || '';
  const uniqueId = log?.metadata?.customParams?.uniqueid || log?.metadata?.uniqueid || log?.uniqueId || log?.uniqueid;
  return {
    id: log._id ? String(log._id) : null,
    campaignId: log.campaignId ? String(log.campaignId) : null,
    agentId: log.agentId ? String(log.agentId) : null,
    clientId: log.clientId || null,
    uniqueId,
    mobile: log.mobile || log?.metadata?.customParams?.customercontact || null,
    leadStatus: log.leadStatus || 'unknown',
    status: deriveCallStatus(log),
    durationSeconds: typeof log.duration === 'number' ? log.duration : null,
    recordingUrl: log.audioUrl || null,
    startedAt: log.createdAt || null,
    updatedAt: log.updatedAt || log?.metadata?.lastUpdated || null,
    metadata: {
      isActive: !!(log.metadata && log.metadata.isActive),
      callEndTime: log.metadata?.callEndTime || null,
      languages: Array.isArray(log.metadata?.languages) ? log.metadata.languages : [],
      customParams: log.metadata?.customParams || {},
      totalUpdates: log.metadata?.totalUpdates || 0,
    },
    transcript: {
      text: transcriptText,
      segments: parseTranscriptLines(transcriptText),
    },
  };
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
    const callLogs = await CallLog.find({ campaignId })
      .sort({ updatedAt: -1 })
      .lean();

    const nextState = new Map();
    const room = 'campaign-' + campaignId;

    for (const log of callLogs) {
      const formatted = formatCallLogEntry(log);
      if (!formatted) continue;
      const key = formatted.uniqueId || formatted.id;
      if (!key) continue;

      const serialized = JSON.stringify(formatted);
      nextState.set(key, serialized);

      const previous = poller.lastState.get(key);
      if (previous !== serialized) {
        console.log(`📤 [SOCKET.IO] Emitting call-transcript-update for campaign ${campaignId}, uniqueId: ${formatted.uniqueId || formatted.id}`);
        io.to(room).emit('call-transcript-update', {
          campaignId: String(campaignId),
          uniqueId: formatted.uniqueId || formatted.id,
          type: 'upsert',
          call: formatted,
        });
      }
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
  } catch (error) {
    console.error('❌ [SOCKET.IO] Failed to initialize:', error?.message || error);
    throw error;
  }
  
  io.on('connection', socket => {
    console.log(`🔌 [SOCKET.IO] Client connected: ${socket.id}`);
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
