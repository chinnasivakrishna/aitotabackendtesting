const socketio = require('socket.io');
const mongoose = require('mongoose');
const CallLog = require('../models/CallLog');
const Campaign = require('../models/Campaign');

let io = null;
const uniqueIdPollers = new Map(); // socketId -> { uniqueId, timer, running, lastTranscript }
const DEFAULT_POLL_INTERVAL_MS = 2000; // Poll every 2 seconds for transcript updates

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


// Send transcript update for a specific uniqueId
async function sendTranscriptUpdate(socket, uniqueId) {
  try {
    const callLog = await CallLog.findOne({ 
      'metadata.customParams.uniqueid': uniqueId 
    }).lean();

    if (!callLog) {
      socket.emit('transcript-update', {
        uniqueId,
        found: false,
        message: 'Call log not found'
      });
      return;
    }

    const formatted = formatCallLogEntry(callLog);
    if (formatted) {
      socket.emit('transcript-update', {
        uniqueId,
        found: true,
        call: formatted,
        transcript: formatted.transcript,
        mobile: formatted.mobile,
        status: formatted.status,
        isActive: formatted.metadata?.isActive || false,
        updatedAt: formatted.updatedAt
      });
    }
  } catch (error) {
    console.error('[wsServer] sendTranscriptUpdate failed:', error?.message || error);
    socket.emit('error', { message: 'Failed to fetch transcript', error: error?.message });
  }
}

// Start polling for a uniqueId
function startUniqueIdPoller(socketId, uniqueId, socket) {
  // Stop existing poller if any
  stopUniqueIdPoller(socketId);
  
  const poller = {
    uniqueId,
    running: false,
    timer: null,
    lastTranscript: null
  };

  const poll = async () => {
    if (poller.running || !socket.connected) return;
    poller.running = true;

    try {
      await sendTranscriptUpdate(socket, uniqueId);
    } catch (error) {
      console.error(`[wsServer] Poll error for ${uniqueId}:`, error?.message);
    } finally {
      poller.running = false;
    }
  };

  // Poll immediately
  poll();
  
  // Then poll at intervals
  poller.timer = setInterval(() => {
    if (socket.connected) {
      poll();
    } else {
      stopUniqueIdPoller(socketId);
    }
  }, DEFAULT_POLL_INTERVAL_MS);

  uniqueIdPollers.set(socketId, poller);
  console.log(`✅ [SOCKET.IO] Started poller for uniqueId: ${uniqueId}, socket: ${socketId}`);
}

// Stop polling for a uniqueId
function stopUniqueIdPoller(socketId) {
  const poller = uniqueIdPollers.get(socketId);
  if (poller) {
    if (poller.timer) {
      clearInterval(poller.timer);
    }
    uniqueIdPollers.delete(socketId);
    console.log(`🛑 [SOCKET.IO] Stopped poller for socket: ${socketId}`);
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
    
    console.log('✅ [SOCKET.IO] Transcript WebSocket server initialized');
    console.log('   - Path: /socket.io/');
    console.log('   - Transports: websocket, polling');
    console.log('   - CORS: enabled for all origins');
    console.log('   - Events: start-transcript, stop-transcript');
  } catch (error) {
    console.error('❌ [SOCKET.IO] Failed to initialize:', error?.message || error);
    throw error;
  }
  
  // Log all connection attempts
  io.engine.on('connection', (req) => {
    const remoteAddress = req?.socket?.remoteAddress || req?.headers?.['x-forwarded-for'] || req?.connection?.remoteAddress || 'unknown';
    console.log(`🔗 [SOCKET.IO] Connection attempt from ${remoteAddress}`);
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

    // Start tracking transcript for a uniqueId
    socket.on('start-transcript', async uniqueId => {
      if (!uniqueId) {
        socket.emit('error', { message: 'uniqueId is required' });
        return;
      }
      
      console.log(`📥 [SOCKET.IO] Client ${socket.id} started tracking transcript for uniqueId: ${uniqueId}`);
      
      // Stop any existing poller for this socket
      stopUniqueIdPoller(socket.id);
      
      // Start new poller
      startUniqueIdPoller(socket.id, uniqueId, socket);
      
      // Send initial transcript if available
      await sendTranscriptUpdate(socket, uniqueId);
    });

    // Stop tracking transcript
    socket.on('stop-transcript', () => {
      console.log(`🛑 [SOCKET.IO] Client ${socket.id} stopped tracking transcript`);
      stopUniqueIdPoller(socket.id);
      socket.emit('transcript-stopped', { message: 'Transcript tracking stopped' });
    });

    socket.on('disconnect', () => {
      console.log(`🔌 [SOCKET.IO] Client disconnected: ${socket.id}`);
      stopUniqueIdPoller(socket.id);
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
  // Find all sockets tracking this uniqueId and send update
  for (const [socketId, poller] of uniqueIdPollers.entries()) {
    if (poller.uniqueId === uniqueId) {
      const socket = io.sockets.sockets.get(socketId);
      if (socket && socket.connected) {
        const formattedLog = formatCallLogEntry(callLog);
        if (formattedLog) {
          socket.emit('transcript-update', {
            uniqueId,
            found: true,
            call: formattedLog,
            transcript: formattedLog.transcript,
            mobile: formattedLog.mobile,
            status: formattedLog.status,
            isActive: formattedLog.metadata?.isActive || false,
            updatedAt: formattedLog.updatedAt
          });
        }
      }
    }
  }
}

function getStatus() {
  if (!io) {
    return { initialized: false, connectedClients: 0, activePollers: 0 };
  }
  
  const connectedClients = io.sockets.sockets.size;
  const activePollers = uniqueIdPollers.size;
  const pollerDetails = Array.from(uniqueIdPollers.entries()).map(([socketId, poller]) => ({
    socketId,
    uniqueId: poller.uniqueId,
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
