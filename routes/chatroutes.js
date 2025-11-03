const express = require('express');
const router = express.Router();

const Chat = require('../models/Chat');
const mongoose = require('mongoose');
let Agent;
try {
  Agent = require('../models/Agent');
} catch (_) {
  Agent = null;
}

// GET /api/v1/chat/agents/:clientId
// Returns unique agentIds (and optional agent details) that have chats with the client
router.get('/agents/:clientId', async (req, res) => {
  try {
    const { clientId } = req.params;
    if (!clientId) {
      return res.status(400).json({ success: false, message: 'clientId is required' });
    }

    // Support clientId saved as String or ObjectId (some datasets mix types)
    let clientObjectId = null;
    try { clientObjectId = new mongoose.Types.ObjectId(String(clientId)); } catch {}

    const clientMatch = clientObjectId
      ? { $or: [ { clientId: String(clientId) }, { clientId: clientObjectId } ] }
      : { clientId: String(clientId) };

    // Use distinct to reliably deduplicate ObjectId values
    const distinctIds = await Chat.distinct('agentId', { ...clientMatch, agentId: { $ne: null } });
    // Also fetch lastActivity per agent
    const byAgent = await Chat.aggregate([
      { $match: { ...clientMatch, agentId: { $ne: null } } },
      { $group: { _id: '$agentId', lastActivityAt: { $max: '$lastActivityAt' }, sessions: { $addToSet: '$sessionId' } } },
      { $sort: { lastActivityAt: -1 } }
    ]);

    const agentIds = distinctIds.map(id => id && id.toString()).filter(Boolean);

    let agents = byAgent.map(r => ({
      agentId: r._id ? r._id.toString() : null,
      lastActivityAt: r.lastActivityAt,
      sessions: r.sessions
    })).filter(a => a.agentId);

    // Optionally attach agent details if Agent model exists
    if (Agent && agentIds.length > 0) {
      const docs = await Agent.find({ _id: { $in: agentIds } }).select('_id name email username phone');
      const byId = new Map(docs.map(d => [String(d._id), d]));
      agents = agents.map(a => ({
        ...a,
        agent: byId.get(String(a.agentId)) || null
      }));
    }

    return res.json({ success: true, data: { clientId, agentIds, agents } });
  } catch (error) {
    console.error('GET /chat/agents error:', error);
    return res.status(500).json({ success: false, message: 'Failed to fetch agents', error: error.message });
  }
});

// GET /api/v1/chat/merge?clientId=...&agentId=...
// Merges all chats for a given clientId and agentId into a single conversation
router.get('/merge', async (req, res) => {
  try {
    const { clientId, agentId } = req.query || {};
    if (!clientId || !agentId) {
      return res.status(400).json({ success: false, message: 'clientId and agentId are required' });
    }

    // Ensure agentId is treated as ObjectId; but support stored as string too
    let agentObjectId = null;
    try { agentObjectId = new mongoose.Types.ObjectId(String(agentId)); } catch {}

    // Support clientId saved as String or ObjectId
    let clientObjectId = null;
    try { clientObjectId = new mongoose.Types.ObjectId(String(clientId)); } catch {}

    const query = {
      $and: [
        clientObjectId ? { $or: [ { clientId: String(clientId) }, { clientId: clientObjectId } ] } : { clientId: String(clientId) },
        agentObjectId ? { $or: [ { agentId: agentObjectId }, { agentId: String(agentId) } ] } : { agentId: String(agentId) }
      ]
    };

    const chats = await Chat.find(query).lean();
    if (!chats || chats.length === 0) {
      return res.json({ success: true, data: { clientId, agentId, sessions: [], messages: [], messageCount: 0 } });
    }

    const sessions = Array.from(new Set(chats.map(c => c.sessionId).filter(Boolean)));

    const messages = chats.flatMap(c => (c.messages || []).map(m => ({
      ...m,
      // annotate with source chat/session for traceability
      _source: { sessionId: c.sessionId, chatId: c._id }
    })));

    // Sort messages chronologically using createdAt fallback to Date.now()
    messages.sort((a, b) => new Date(a.createdAt || 0) - new Date(b.createdAt || 0));

    const startedAt = new Date(Math.min(...chats.map(c => new Date(c.startedAt || Date.now()))));
    const endedAt = new Date(Math.max(...chats.map(c => new Date(c.endedAt || c.lastActivityAt || c.startedAt || Date.now()))));
    const lastActivityAt = new Date(Math.max(...chats.map(c => new Date(c.lastActivityAt || c.endedAt || c.startedAt || Date.now()))));

    const data = {
      clientId: String(clientId),
      agentId: String(agentId),
      sessions,
      startedAt,
      endedAt,
      lastActivityAt,
      messageCount: messages.length,
      messages
    };

    return res.json({ success: true, data });
  } catch (error) {
    console.error('GET /chat/merge error:', error);
    return res.status(500).json({ success: false, message: 'Failed to merge chats', error: error.message });
  }
});

module.exports = router;


