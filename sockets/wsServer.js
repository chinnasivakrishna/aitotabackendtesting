const WebSocket = require('ws');
const mongoose = require('mongoose');
const CallLog = require('../models/CallLog');
const Campaign = require('../models/Campaign');

let wss = null;
const uniqueIdPollers = new Map(); // connectionId -> { uniqueId, timer, running, lastTranscript, ws }
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

// Send transcript update for a specific uniqueId
async function sendTranscriptUpdate(ws, uniqueId) {
  try {
    const callLog = await CallLog.findOne({ 
      'metadata.customParams.uniqueid': uniqueId 
    }).lean();

    if (!callLog) {
      sendMessage(ws, {
        event: 'transcript-update',
        uniqueId,
        found: false,
        message: 'Call log not found'
      });
      return;
    }

    const formatted = formatCallLogEntry(callLog);
    if (formatted) {
      sendMessage(ws, {
        event: 'transcript-update',
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
    sendMessage(ws, {
      event: 'error',
      message: 'Failed to fetch transcript',
      error: error?.message
    });
  }
}

// Helper to send WebSocket message
function sendMessage(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try {
      ws.send(JSON.stringify(data));
    } catch (error) {
      console.error('[wsServer] Error sending message:', error?.message);
    }
  }
}

// Start polling for a uniqueId
function startUniqueIdPoller(connectionId, uniqueId, ws) {
  // Stop existing poller if any
  stopUniqueIdPoller(connectionId);
  
  const poller = {
    uniqueId,
    ws,
    running: false,
    timer: null,
    lastTranscript: null
  };

  const poll = async () => {
    if (poller.running || ws.readyState !== WebSocket.OPEN) return;
    poller.running = true;

    try {
      await sendTranscriptUpdate(ws, uniqueId);
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
    if (ws.readyState === WebSocket.OPEN) {
      poll();
    } else {
      stopUniqueIdPoller(connectionId);
    }
  }, DEFAULT_POLL_INTERVAL_MS);

  uniqueIdPollers.set(connectionId, poller);
  console.log(`✅ [WEBSOCKET] Started poller for uniqueId: ${uniqueId}, connection: ${connectionId}`);
}

// Stop polling for a uniqueId
function stopUniqueIdPoller(connectionId) {
  const poller = uniqueIdPollers.get(connectionId);
  if (poller) {
    if (poller.timer) {
      clearInterval(poller.timer);
    }
    uniqueIdPollers.delete(connectionId);
    console.log(`🛑 [WEBSOCKET] Stopped poller for connection: ${connectionId}`);
  }
}

function init(server) {
  try {
    // Create WebSocket server using noServer to avoid conflicts with VoiceChatWebSocketServer
    wss = new WebSocket.Server({ noServer: true });
    
    // Store the original upgrade handler if it exists
    const existingUpgradeHandlers = server.listeners('upgrade').slice();
    server.removeAllListeners('upgrade');
    
    // Handle upgrade requests - route based on path
    server.on('upgrade', (request, socket, head) => {
      const pathname = new URL(request.url, `http://${request.headers.host}`).pathname;
      
      if (pathname === '/transcript') {
        // Handle transcript WebSocket connections
        wss.handleUpgrade(request, socket, head, (ws) => {
          wss.emit('connection', ws, request);
        });
      } else {
        // Pass to other WebSocket servers (VoiceChatWebSocketServer)
        // Call existing handlers
        for (const handler of existingUpgradeHandlers) {
          handler(request, socket, head);
        }
      }
    });
    
    console.log('✅ [WEBSOCKET] Transcript WebSocket server initialized');
    console.log('   - Path: /transcript');
    console.log('   - Events: start-transcript, stop-transcript');
    
    wss.on('connection', (ws, req) => {
      const connectionId = `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const remoteAddress = req.socket?.remoteAddress || req.headers?.['x-forwarded-for'] || 'unknown';
      
      console.log(`🔌 [WEBSOCKET] Client connected: ${connectionId} from ${remoteAddress}`);
      
      // Store connection ID on ws object
      ws.connectionId = connectionId;
      
      // Send connection confirmation
      sendMessage(ws, {
        event: 'connected',
        connectionId,
        message: 'Connected to transcript WebSocket server',
        events: ['start-transcript', 'stop-transcript']
      });
      
      // Handle incoming messages
      ws.on('message', async (message) => {
        try {
          const data = JSON.parse(message.toString());
          
          if (data.event === 'start-transcript') {
            const uniqueId = data.uniqueId || data.data;
            if (!uniqueId) {
              sendMessage(ws, {
                event: 'error',
                message: 'uniqueId is required'
              });
              return;
            }
            
            console.log(`📥 [WEBSOCKET] Client ${connectionId} started tracking transcript for uniqueId: ${uniqueId}`);
            
            // Stop any existing poller for this connection
            stopUniqueIdPoller(connectionId);
            
            // Start new poller
            startUniqueIdPoller(connectionId, uniqueId, ws);
            
            // Send initial transcript if available
            await sendTranscriptUpdate(ws, uniqueId);
            
          } else if (data.event === 'stop-transcript') {
            console.log(`🛑 [WEBSOCKET] Client ${connectionId} stopped tracking transcript`);
            stopUniqueIdPoller(connectionId);
            sendMessage(ws, {
              event: 'transcript-stopped',
              message: 'Transcript tracking stopped'
            });
          } else {
            sendMessage(ws, {
              event: 'error',
              message: `Unknown event: ${data.event}`
            });
          }
        } catch (error) {
          console.error(`❌ [WEBSOCKET] Error handling message:`, error?.message || error);
          sendMessage(ws, {
            event: 'error',
            message: 'Invalid message format',
            error: error?.message
          });
        }
      });
      
      // Handle connection close
      ws.on('close', (code, reason) => {
        console.log(`🔌 [WEBSOCKET] Client disconnected: ${connectionId}, code: ${code}, reason: ${reason || 'unknown'}`);
        stopUniqueIdPoller(connectionId);
      });
      
      // Handle errors
      ws.on('error', (error) => {
        console.error(`❌ [WEBSOCKET] Connection error for ${connectionId}:`, error?.message || error);
        stopUniqueIdPoller(connectionId);
      });
    });
    
    wss.on('error', (error) => {
      console.error('❌ [WEBSOCKET] Server error:', error?.message || error);
    });
    
  } catch (error) {
    console.error('❌ [WEBSOCKET] Failed to initialize:', error?.message || error);
    console.error('   Stack:', error?.stack);
    throw error;
  }
}

function broadcastCampaignEvent(campaignId, event, payload) {
  // Not used with native WebSocket - kept for compatibility
  console.log(`[WEBSOCKET] broadcastCampaignEvent called (not implemented for native WebSocket): ${campaignId}, ${event}`);
}

function broadcastCallEvent(campaignId, uniqueId, status, callLog) {
  if (!wss) return;
  
  // Find all connections tracking this uniqueId and send update
  for (const [connectionId, poller] of uniqueIdPollers.entries()) {
    if (poller.uniqueId === uniqueId && poller.ws && poller.ws.readyState === WebSocket.OPEN) {
      const formattedLog = formatCallLogEntry(callLog);
      if (formattedLog) {
        sendMessage(poller.ws, {
          event: 'transcript-update',
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

function getStatus() {
  if (!wss) {
    return { initialized: false, connectedClients: 0, activePollers: 0 };
  }
  
  const connectedClients = wss.clients.size;
  const activePollers = uniqueIdPollers.size;
  const pollerDetails = Array.from(uniqueIdPollers.entries()).map(([connectionId, poller]) => ({
    connectionId,
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
