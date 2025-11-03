const express = require('express');
const router = express.Router();
const mongoose = require("mongoose");
const { loginClient, registerClient, getClientProfile, getAllUsers, getUploadUrlCustomization, getUploadUrl,getUploadUrlMyBusiness, googleLogin, getHumanAgents, createHumanAgent, updateHumanAgent, deleteHumanAgent, getHumanAgentById, loginHumanAgent, getUploadUrlKnowledgeBase, getFileUrlByKey, createKnowledgeItem, getKnowledgeItems, updateKnowledgeItem, deleteKnowledgeItem, embedKnowledgeItem } = require('../controllers/clientcontroller');
const { authMiddleware, verifyAdminTokenOnlyForRegister, verifyAdminToken, verifyClientToken, verifyClientOrHumanAgentToken, verifyClientOrAdminAndExtractClientId } = require('../middlewares/authmiddleware');
const { verifyGoogleToken } = require('../middlewares/googleAuth');
const Client = require("../models/Client")
const ClientApiService = require("../services/ClientApiService")
const { generateClientApiKey, getActiveClientApiKey, copyActiveClientApiKey } = require("../controllers/clientApiKeyController")
const { getobject, getobjectFor, getobjectForWithRegion } = require('../utils/s3')
const Agent = require('../models/Agent');
const VoiceService = require('../services/voiceService');
const voiceService = new VoiceService();
const CallLog = require('../models/CallLog');
const WaChat = require('../models/WaChat');
const AgentSettings = require('../models/AgentSettings');
const Group = require('../models/Group');
const Campaign = require('../models/Campaign');
const jwt = require('jsonwebtoken');
const Business = require('../models/BusinessInfo');
const Contacts = require('../models/Contacts');
const MyBusiness = require('../models/MyBussiness');
const MyDials = require('../models/MyDials');
const User = require('../models/User'); // Added User model import
const CampaignHistory = require('../models/CampaignHistory');
const { generateBusinessHash } = require('../utils/hashUtils');
const crypto = require('crypto');
const PaytmConfig = require('../config/paytm');
const PaytmChecksum = require('paytmchecksum');
const CashfreeConfig = require('../config/cashfree');
const {
  startCampaignCalling,
  stopCampaignCalling,
  getCampaignCallingProgress,
  triggerManualStatusUpdate,
  debugCallStatus,
  migrateMissedToCompleted,
  makeSingleCall,
  updateCallStatusFromLogs,
  getClientApiKey,
  getSystemHealth,
  resetCircuitBreakers,
  getSafeLimits
} = require('../services/campaignCallingService');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');


const clientApiService = new ClientApiService()

// Middleware to extract client ID from token or fallback to headers/query
const extractClientId = (req, res, next) => {
  try {
    if(!req.headers.authorization)
    {
      return res.status(401).json({ success: false, error: 'Authorization header is required' });
    }
    
    // First try to extract from JWT token
    if (req.headers.authorization && req.headers.authorization.startsWith('Bearer')) {
      const token = req.headers.authorization.split(' ')[1];
      try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET);
        if (decoded.userType === 'client' && decoded.id) {
          req.clientId = decoded.id;
          console.log('Using clientId from token:', req.clientId);
          next();
          return;
        } else {
          return res.status(401).json({ error: 'Invalid token: userType must be client' });
        }
      } catch (tokenError) {
        console.log('Token verification failed:', tokenError.message);
        return res.status(401).json({ error: 'Token expired or invalid' });
      }
    }
  } catch (error) {
    console.error('Error in extractClientId middleware:', error);
    return res.status(401).json({ error: 'Token expired or invalid' });
  }
}

// Get or create client
router.get("/", extractClientId, async (req, res) => {
  try {
    // Fetch actual client by _id from token. Do not create here to avoid schema validation errors.
    const client = await Client.findById(req.clientId).lean();
    if (!client) {
      return res.status(404).json({ success: false, error: "Client not found" });
    }
    // Return a lightweight view with clientId added for frontend compatibility
    const responseClient = {
      ...client,
      clientId: String(client._id),
    };
    return res.json({ success: true, data: responseClient });
  } catch (error) {
    console.error("Error fetching client:", error);
    return res.status(500).json({ error: "Failed to fetch client information" });
  }
})


// Update client information
router.put("/", extractClientId, async (req, res) => {
  try {
    const { clientName, email, settings } = req.body
    const client = await Client.findOneAndUpdate(
      { clientId: req.clientId },
      { clientName, email, settings, updatedAt: new Date() },
      { new: true, upsert: true },
    )
    res.json({ success: true, data: client, message: "Client information updated successfully" })
  } catch (error) {
    console.error("Error updating client:", error)
    res.status(500).json({ error: "Failed to update client information" })
  }
})

// Update client information (currently supports clientType update)
router.put("/client-type", extractClientId, async (req, res) => {
  try {
    const { clientType } = req.body

    if (!clientType) {
      return res.status(400).json({ error: "clientType is required" })
    }

    const client = await Client.findByIdAndUpdate(
      req.clientId,
      { clientType, updatedAt: new Date() },
      { new: true },
    )

    if (!client) {
      return res.status(404).json({ error: "Client not found" })
    }

    return res.json({ success: true, data: client, message: "Client information updated successfully" })
  } catch (error) {
    console.error("Error updating client:", error)
    return res.status(500).json({ error: "Failed to update client information" })
  }
})

// Get all API keys for client
router.get("/api-keys", extractClientId, async (req, res) => {
  try {
    const result = await clientApiService.getClientApiKeys(req.clientId)
    if (result.success) {
      res.json(result)
    } else {
      res.status(500).json(result)
    }
  } catch (error) {
    console.error("Error fetching API keys:", error)
    res.status(500).json({ error: "Failed to fetch API keys" })
  }
})

// Add or update API key
router.post("/api-keys/:provider", extractClientId, async (req, res) => {
  try {
    const { provider } = req.params
    const keyData = req.body
    const result = await clientApiService.setApiKey(req.clientId, provider, keyData)
    if (result.success) {
      res.json(result)
    } else {
      res.status(400).json(result)
    }
  } catch (error) {
    console.error("Error setting API key:", error)
    res.status(500).json({ error: "Failed to set API key" })
  }
})

// Test API key
router.post("/api-keys/:provider/test", extractClientId, async (req, res) => {
  try {
    const { provider } = req.params
    const { key, configuration } = req.body
    const result = await clientApiService.testApiKey(provider, key, configuration)
    res.json(result)
  } catch (error) {
    console.error("Error testing API key:", error)
    res.status(500).json({ error: "Failed to test API key" })
  }
})

// Delete API key
router.delete("/api-keys/:provider", extractClientId, async (req, res) => {
  try {
    const { provider } = req.params
    const result = await clientApiService.deleteApiKey(req.clientId, provider)
    if (result.success) {
      res.json(result)
    } else {
      res.status(404).json(result)
    }
  } catch (error) {
    console.error("Error deleting API key:", error)
    res.status(500).json({ error: "Failed to delete API key" })
  }
})

// Get provider configurations
router.get("/providers", (req, res) => {
  try {
    const providers = clientApiService.getProviderConfigs()
    res.json({ success: true, data: providers })
  } catch (error) {
    console.error("Error fetching providers:", error)
    res.status(500).json({ error: "Failed to fetch provider configurations" })
  }
})

// Generate per-client API key used for public API access
router.post("/api-key/generate", extractClientId, generateClientApiKey)

// Get current active API key metadata for the client
router.get("/api-key", extractClientId, getActiveClientApiKey)

// Copy full active API key (server decrypts and returns temporarily)
router.post("/api-key/copy", extractClientId, copyActiveClientApiKey)

router.get('/upload-url',getUploadUrl);

router.get('/upload-url-mybusiness',getUploadUrlMyBusiness);

router.get('/upload-url-customization',getUploadUrlCustomization);

router.post('/login', loginClient);

router.post('/human-agent/login', loginHumanAgent);

router.post('/google-login',verifyGoogleToken, googleLogin);

router.post('/register',verifyAdminTokenOnlyForRegister, registerClient);

router.get('/profile', authMiddleware, getClientProfile);

// Knowledge Base uploads and file access
router.get('/upload-url-knowledge-base', getUploadUrlKnowledgeBase);
router.get('/file-url', getFileUrlByKey);

// Stream call audio via backend proxy to avoid S3 CORS/signature issues
router.get('/campaigns/:id/call-audio', async (req, res) => {
  try {
    const { id } = req.params;
    const { documentId } = req.query;
    if (!documentId) return res.status(400).json({ success: false, error: 'documentId is required' });

    // Find CallLog by uniqueid first (more reliable)
    let latest = await CallLog.findOne({
      'metadata.customParams.uniqueid': String(documentId)
    }).sort({ createdAt: -1 }).lean();

    // If not found, try with campaignId filter as well
    if (!latest) {
      try {
        const campaignIdObj = new mongoose.Types.ObjectId(id);
        latest = await CallLog.findOne({
          campaignId: campaignIdObj,
          'metadata.customParams.uniqueid': String(documentId)
        }).sort({ createdAt: -1 }).lean();
      } catch (e) {
        // If ObjectId conversion fails, try string match
        latest = await CallLog.findOne({
          campaignId: id,
          'metadata.customParams.uniqueid': String(documentId)
        }).sort({ createdAt: -1 }).lean();
      }
    }

    if (!latest || !latest.audioUrl) {
      console.log('Audio not found:', { documentId, campaignId: id, found: !!latest });
      return res.status(404).json({ success: false, error: 'Recording not found' });
    }

    let bucket = process.env.AWS_BUCKET_NAME;
    let key;
    let region = process.env.AWS_REGION;
    try {
      const u = new URL(latest.audioUrl);
      key = decodeURIComponent(u.pathname.replace(/^\//, ''));
      const host = String(u.hostname || '');
      if (host.includes('.s3')) {
        bucket = host.split('.s3')[0];
        if (host.includes('.s3.')) {
          region = host.split('.s3.')[1].split('.')[0] || region;
        }
      }
    } catch (_) {
      key = latest.audioUrl; // treat as raw key
    }

    const client = new S3Client({
      region: region || process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const s3Resp = await client.send(command);

    // Default headers for audio
    res.setHeader('Content-Type', s3Resp.ContentType || 'audio/wav');
    if (s3Resp.ContentLength) res.setHeader('Content-Length', String(s3Resp.ContentLength));
    // Allow range requests for audio seeking
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'private, max-age=0, no-cache, no-store');

    if (s3Resp.Body && typeof s3Resp.Body.pipe === 'function') {
      s3Resp.Body.pipe(res);
    } else {
      res.status(500).json({ success: false, error: 'Failed to stream audio' });
    }
  } catch (error) {
    console.error('Audio proxy error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch audio' });
  }
});

// Stream agent call audio via backend proxy to avoid S3 CORS/signature issues
// Note: This route accepts token from query parameter for audio element compatibility
router.get('/agents/:id/call-audio', async (req, res) => {
  try {
    const { id } = req.params;
    const { callLogId, token } = req.query;
    if (!callLogId) return res.status(400).json({ success: false, error: 'callLogId is required' });

    // Extract clientId from token (query param or header)
    let clientId = null;
    const authHeader = req.headers.authorization;
    const authToken = token || (authHeader && authHeader.startsWith('Bearer') ? authHeader.split(' ')[1] : null);
    
    if (authToken) {
      try {
        const decoded = jwt.verify(authToken, process.env.JWT_SECRET);
        if (decoded.userType === 'client' && decoded.id) {
          clientId = decoded.id;
        }
      } catch (tokenError) {
        // Token verification failed, will validate via callLog ownership instead
      }
    }

    // Find CallLog by callLogId and agentId
    const callLog = await CallLog.findOne({
      _id: callLogId,
      agentId: id
    }).lean();

    if (!callLog || !callLog.audioUrl) {
      console.log('Audio not found:', { callLogId, agentId: id, found: !!callLog });
      return res.status(404).json({ success: false, error: 'Recording not found' });
    }

    // Validate ownership: if we have clientId from token, verify it matches
    if (clientId && callLog.clientId !== clientId) {
      return res.status(403).json({ success: false, error: 'Access denied' });
    }

    // If no token but we found the callLog, validate agent belongs to the callLog's client
    if (!clientId) {
      const agent = await Agent.findOne({ _id: id, clientId: callLog.clientId }).lean();
      if (!agent) {
        return res.status(403).json({ success: false, error: 'Access denied' });
      }
    } else {
      // Validate agent exists and belongs to client
      const agent = await Agent.findOne({ _id: id, clientId: clientId }).lean();
      if (!agent) {
        return res.status(404).json({ success: false, error: 'Agent not found' });
      }
    }

    let bucket = process.env.AWS_BUCKET_NAME;
    let key;
    let region = process.env.AWS_REGION;
    try {
      const u = new URL(callLog.audioUrl);
      key = decodeURIComponent(u.pathname.replace(/^\//, ''));
      const host = String(u.hostname || '');
      if (host.includes('.s3')) {
        bucket = host.split('.s3')[0];
        if (host.includes('.s3.')) {
          region = host.split('.s3.')[1].split('.')[0] || region;
        }
      }
    } catch (_) {
      key = callLog.audioUrl; // treat as raw key
    }

    const client = new S3Client({
      region: region || process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const s3Resp = await client.send(command);

    // Default headers for audio
    res.setHeader('Content-Type', s3Resp.ContentType || 'audio/wav');
    if (s3Resp.ContentLength) res.setHeader('Content-Length', String(s3Resp.ContentLength));
    // Allow range requests for audio seeking
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader('Cache-Control', 'private, max-age=0, no-cache, no-store');

    if (s3Resp.Body && typeof s3Resp.Body.pipe === 'function') {
      s3Resp.Body.pipe(res);
    } else {
      res.status(500).json({ success: false, error: 'Failed to stream audio' });
    }
  } catch (error) {
    console.error('Agent audio proxy error:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch audio' });
  }
});

// Knowledge Base CRUD operations
router.post('/knowledge-base', authMiddleware, createKnowledgeItem);
router.get('/knowledge-base/:agentId', authMiddleware, getKnowledgeItems);
router.put('/knowledge-base/:id', authMiddleware, updateKnowledgeItem);
router.delete('/knowledge-base/:id', authMiddleware, deleteKnowledgeItem);
// Knowledge Base embed (process) operation
router.post('/knowledge-base/:id/embed', authMiddleware, embedKnowledgeItem);

// Create new agent with multiple starting messages and default selection
router.post('/agents', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    console.log('🚀 Creating agent - Request data:', {
      userType: req.user?.userType,
      clientId: req.clientId,
      adminId: req.adminId,
      bodyKeys: Object.keys(req.body)
    });
    
    const { startingMessages, defaultStartingMessageIndex, ...agentData } = req.body;

    // Map optional socials payload to schema fields if present
    const mapSocials = (src) => {
      if (!src || typeof src !== 'object') return;
      console.log('🔧 Backend received socials data (create):', src);
      
      const platforms = ['whatsapp', 'telegram', 'email', 'sms'];
      platforms.forEach((p) => {
        const enabledKey = `${p}Enabled`;
        if (typeof src[enabledKey] === 'boolean') {
          agentData[enabledKey] = src[enabledKey];
          console.log(`🔧 ${p} enabled (create):`, agentData[enabledKey]);
          if (!agentData[enabledKey]) {
            // Also clear the convenience link field for WhatsApp
            if (p === 'whatsapp') {
              agentData.whatsapplink = undefined;
            }
          }
        }
        if (Array.isArray(src[p])) {
          // Normalize to [{ link }]
          const filteredArr = src[p]
            .filter((it) => it && typeof it.link === 'string' && it.link.trim().length > 0)
            .map((it) => ({ link: it.link.trim() }));
          
          // Set to undefined if no valid links, to be consistent with model pre-save hook
          agentData[p] = filteredArr.length > 0 ? filteredArr : undefined;
          console.log(`🔧 ${p} array (create):`, agentData[p]);
          
          // Update the convenience link field for WhatsApp
          if (p === 'whatsapp' && filteredArr.length > 0) {
            agentData.whatsapplink = filteredArr[0].link;
            console.log(`🔧 whatsapplink updated (create):`, agentData.whatsapplink);
          }
        }
      });
    };
    mapSocials(req.body);
    
    // Validate required fields
    if (!agentData.agentName || !agentData.agentName.trim()) {
      return res.status(400).json({ error: 'Agent name is required.' });
    }
    if (!agentData.description || !agentData.description.trim()) {
      return res.status(400).json({ error: 'Description is required.' });
    }
    if (!agentData.systemPrompt || !agentData.systemPrompt.trim()) {
      return res.status(400).json({ error: 'System prompt is required.' });
    }
    if (!Array.isArray(startingMessages) || startingMessages.length === 0) {
      return res.status(400).json({ error: 'At least one starting message is required.' });
    }
    if (
      typeof defaultStartingMessageIndex !== 'number' ||
      defaultStartingMessageIndex < 0 ||
      defaultStartingMessageIndex >= startingMessages.length
    ) {
      return res.status(400).json({ error: 'Invalid default starting message index.' });
    }
    
    // Set default firstMessage 
    agentData.firstMessage = startingMessages[defaultStartingMessageIndex].text;
    agentData.startingMessages = startingMessages;
    
    // Set the appropriate ID based on token type
    console.log('🔧 Setting agent IDs:', { userType: req.user.userType, clientId: req.clientId, adminId: req.adminId });
    
    if (req.user.userType === 'client') {
      // If client token, store client ID in createdBy and set clientId
      if (!req.clientId) {
        return res.status(400).json({ error: 'Client ID is required for client tokens' });
      }
      agentData.clientId = req.clientId;
      agentData.createdBy = req.clientId; // Store client ID in createdBy
      agentData.createdByType = 'client';
      console.log('✅ Client agent - IDs set:', { clientId: agentData.clientId, createdBy: agentData.createdBy, createdByType: agentData.createdByType });
    } else if (req.user.userType === 'admin') {
      // If admin token, store admin ID in createdBy and clientId is optional
      if (req.clientId) {
        agentData.clientId = req.clientId;
        console.log('✅ Admin agent with clientId:', { clientId: agentData.clientId });
      } else {
        // For admin tokens, clientId is optional - allow creating agents without client association
        agentData.clientId = undefined;
        console.log('ℹ️ Admin creating agent without clientId - agent will be unassigned');
      }
      agentData.createdBy = req.adminId; // Store admin ID in createdBy
      agentData.createdByType = 'admin';
      console.log('✅ Admin agent - IDs set:', { clientId: agentData.clientId, createdBy: agentData.createdBy, createdByType: agentData.createdByType });
    } else {
      return res.status(400).json({ error: 'Invalid user type' });
    }
    
    // Validate that createdBy is set
    if (!agentData.createdBy) {
      return res.status(400).json({ error: 'Failed to set createdBy field' });
    }

    // If creating as active with an accountSid, deactivate others first to satisfy unique index
    const willBeActive = agentData.isActive !== false; // default true per schema
    if (req.clientId && willBeActive && agentData.accountSid) {
      await Agent.updateMany(
        {
          clientId: req.clientId,
          accountSid: agentData.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      );
    } else if (req.user.userType === 'admin' && !req.clientId && willBeActive && agentData.accountSid) {
      // For admin-created agents without clientId, only check accountSid uniqueness
      await Agent.updateMany(
        {
          clientId: { $exists: false },
          accountSid: agentData.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      );
    }

    const agent = new Agent(agentData);
    // Generate agentKey using pre-assigned _id (Mongoose assigns _id on instantiation)
    const plainAgentKey = Agent.generateAgentKey(agent._id);
    agent.agentKey = plainAgentKey;

    const savedAgent = await agent.save();

    // If this agent is active and has accountSid, deactivate others with same (clientId, accountSid)
    if (req.clientId && savedAgent.isActive && savedAgent.accountSid) {
      await Agent.updateMany(
        {
          _id: { $ne: savedAgent._id },
          clientId: req.clientId,
          accountSid: savedAgent.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      )
    } else if (req.user.userType === 'admin' && !req.clientId && savedAgent.isActive && savedAgent.accountSid) {
      // For admin-created agents without clientId, only check accountSid uniqueness
      await Agent.updateMany(
        {
          _id: { $ne: savedAgent._id },
          clientId: { $exists: false },
          accountSid: savedAgent.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      )
    }

    console.log('🔑 Generated agentKey:', plainAgentKey);

    
    const responseAgent = savedAgent.toObject();
    delete responseAgent.audioBytes;

     // Include the plain agentKey in response (only returned once during creation)
     responseAgent.agentKey = plainAgentKey;

    res.status(201).json(responseAgent);
  } catch (error) {
    console.error('❌ Error creating agent:', error);
    console.error('Request data:', {
      userType: req.user?.userType,
      clientId: req.clientId,
      adminId: req.adminId,
      agentData: {
        agentName: agentData.agentName,
        serviceProvider: agentData.serviceProvider,
        accountSid: agentData.accountSid
      }
    });
    
    // Provide more specific error messages
    if (error.name === 'ValidationError') {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: error.message,
        missingFields: Object.keys(error.errors).map(key => key)
      });
    }
    
    res.status(500).json({ error: 'Failed to create agent', details: error.message });
  }
});

// Save campaign run data to history
router.post('/campaigns/:id/save-run', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const { startTime, endTime, runTime, callLogs, runId } = req.body || {};

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    const toNum = (v) => {
      const n = Number(v);
      return Number.isFinite(n) ? n : 0;
    };
    const isSuccess = (c) => (String(c.status || '').toLowerCase() === 'completed') || (String(c.leadStatus || '').toLowerCase() === 'connected');
    const isFailed = (c) => ['missed', 'not_connected', 'failed'].includes(String(c.status || '').toLowerCase());

    const existingCount = await CampaignHistory.countDocuments({ campaignId: id });
    const instanceNumber = existingCount + 1;

    // Upsert a single CampaignHistory document per runId. If it exists, finalize it; otherwise, create it.
    const existingHistory = await CampaignHistory.findOne({ runId }).lean();
    let finalInstanceNumber = instanceNumber;
    if (existingHistory && typeof existingHistory.instanceNumber === 'number') {
      finalInstanceNumber = existingHistory.instanceNumber;
    }

    const historyDoc = await CampaignHistory.findOneAndUpdate(
      { runId },
      {
        $setOnInsert: {
          campaignId: id,
          runId: runId || `run_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          instanceNumber: finalInstanceNumber,
          startTime: startTime || '00:00:00',
          contacts: [],
          stats: {
            totalContacts: 0,
            successfulCalls: 0,
            failedCalls: 0,
            totalCallDuration: 0,
            averageCallDuration: 0
          }
        },
        $set: {
          endTime: endTime || '00:00:00',
          runTime: {
            hours: toNum(runTime && runTime.hours),
            minutes: toNum(runTime && runTime.minutes),
            seconds: toNum(runTime && runTime.seconds)
          },
          status: 'completed'
        }
      },
      { upsert: true, new: true }
    );

    // After completion: snapshot merged calls for this run and REPLACE contacts (avoid duplicates)
    try {
      const finalRunId = historyDoc.runId;
      let runDetails = Array.isArray(campaign.details) ? campaign.details.filter(Boolean) : [];
      if (finalRunId) runDetails = runDetails.filter(d => d && d.runId === finalRunId);
      const allUniqueIds = runDetails.map(d => d && d.uniqueId).filter(Boolean);

      // If the history doc already has the same count, skip to avoid double-save
      const existingHistory = await CampaignHistory.findById(historyDoc._id).lean();
      if (existingHistory?.contacts?.length && existingHistory.contacts.length >= allUniqueIds.length) {
        // Still update status and runtime but do not duplicate contacts
      } else {
        const getDurationByUniqueId = async (uid) => {
          try {
            if (!uid) return 0;
            let logsForUid = await CallLog.find({ 'metadata.customParams.uniqueid': uid, ...(req.clientId ? { clientId: req.clientId } : {}) }).sort({ createdAt: 1 }).lean();
            if (!logsForUid || logsForUid.length === 0) logsForUid = await CallLog.find({ 'metadata.customParams.uniqueid': uid }).sort({ createdAt: 1 }).lean();
            if (!logsForUid || logsForUid.length === 0) return 0;
            const maxExplicit = logsForUid.reduce((m, l) => Math.max(m, typeof l.duration === 'number' ? l.duration : 0), 0);
            const first = logsForUid[0];
            const last = logsForUid[logsForUid.length - 1];
            const startMs = first?.createdAt ? new Date(first.createdAt).getTime() : 0;
            const endCandidate = last?.metadata?.callEndTime || last?.updatedAt || last?.time || last?.createdAt;
            const endMs = endCandidate ? new Date(endCandidate).getTime() : 0;
            const derived = Math.max(0, Math.round((endMs - startMs) / 1000));
            const best = Math.max(maxExplicit, derived);
            return Number.isFinite(best) ? best : 0;
          } catch { return 0; }
        };

        // Load all logs needed in one go for faster mapping
        let logs = await CallLog.find({ clientId: req.clientId, 'metadata.customParams.uniqueid': { $in: allUniqueIds } }).sort({ createdAt: -1 }).lean();
        if (!logs || logs.length === 0) logs = await CallLog.find({ 'metadata.customParams.uniqueid': { $in: allUniqueIds } }).sort({ createdAt: -1 }).lean();
        const latestByUid = new Map();
        for (const log of logs) {
          const uid = log?.metadata?.customParams?.uniqueid; if (!uid) continue;
          if (!latestByUid.has(uid)) latestByUid.set(uid, log);
        }

        // Build full contacts array preserving original order in details
        const contacts = [];
        for (const detail of runDetails) {
          const uid = detail.uniqueId; if (!uid) continue;
          const log = latestByUid.get(uid) || null;
          let contactName = '';
          let contactNumber = '';
          let contactLeadStatus = '';
          if (detail.contactId) {
            const campaignContact = campaign.contacts.find(c => (c._id ? c._id.toString() : '') === (detail.contactId ? detail.contactId.toString() : ''));
            if (campaignContact) {
              contactName = campaignContact.name || '';
              contactNumber = campaignContact.phone || campaignContact.mobile || '';
            }
          }
          const number = log?.mobile || contactNumber;
          const name = log?.metadata?.customParams?.name || contactName;
          const leadStatus = log?.leadStatus || contactLeadStatus;
          const duration = await getDurationByUniqueId(uid);
          let status;
          if (log?.metadata?.isActive === true) status = 'ongoing';
          else if (log?.metadata?.isActive === false) status = 'completed';
          else { status = 'missed'; contactLeadStatus = 'not_connected'; }
          contacts.push({
            documentId: uid,
            number,
            name,
            leadStatus: leadStatus || contactLeadStatus,
            contactId: detail.contactId || '',
            time: detail.time || '',
            status,
            duration,
            audioUrl: log?.audioUrl || null
          });
        }

        // Replace contacts in one go (dedup by documentId before set)
        const deduped = Array.from(new Map(contacts.map(c => [c.documentId, c])).values());
        await CampaignHistory.updateOne(
          { _id: historyDoc._id },
          { $set: { contacts: deduped } }
        );
      }

      // Recompute stats from final contacts
      const finalHistory = await CampaignHistory.findById(historyDoc._id).lean();
      const allContacts = finalHistory?.contacts || [];
      const totalContacts = allContacts.length;
      const successfulCalls = allContacts.reduce((acc, c) => acc + (isSuccess(c) ? 1 : 0), 0);
      const failedCalls = allContacts.reduce((acc, c) => acc + (isFailed(c) ? 1 : 0), 0);
      const totalCallDuration = allContacts.reduce((acc, c) => acc + toNum(c.duration), 0);
      const averageCallDuration = totalContacts > 0 ? Math.round(totalCallDuration / totalContacts) : 0;
      await CampaignHistory.updateOne(
        { _id: historyDoc._id },
        { $set: { stats: { totalContacts, successfulCalls, failedCalls, totalCallDuration, averageCallDuration } } }
      );

      // Update CallLog entries to match the campaign history data
      // This ensures the frontend call logs table shows the correct status
      for (const contact of allContacts) {
        try {
          const uniqueId = contact.documentId;
          if (uniqueId) {
            // Get contact details from campaign contacts using contactId
            let contactName = contact.name;
            let contactNumber = contact.number;
            
            if (contact.contactId && (!contactName || !contactNumber)) {
              // Find the contact in campaign contacts
              const campaignContact = campaign.contacts.find(c => c._id && c._id.toString() === contact.contactId);
              if (campaignContact) {
                contactName = contactName || campaignContact.name || '';
                contactNumber = contactNumber || campaignContact.phone || campaignContact.mobile || '';
              }
            }

            // Find the CallLog entry for this uniqueId
            const callLog = await CallLog.findOne({
              'metadata.customParams.uniqueid': uniqueId,
              clientId: req.clientId
            });

            if (callLog) {
              // Update the CallLog with the correct status and leadStatus
              await CallLog.updateOne(
                { _id: callLog._id },
                {
                  $set: {
                    mobile: contactNumber,
                    leadStatus: contact.leadStatus || 'not_connected',
                    duration: contact.duration,
                    'metadata.isActive': false, // Mark as not active
                    'metadata.callEndTime': new Date(),
                    'metadata.customParams.name': contactName
                  }
                }
              );
            } else {
              // Note: CallLog entries are now handled by external system
              // No longer creating CallLog entries for campaign calls
            }
          }
        } catch (error) {
          console.error(`Error updating CallLog for contact ${contact.documentId}:`, error);
        }
      }
    } catch (e) {
      console.error('❌ Error snapshotting merged calls into history:', e?.message);
    }

    // Clear transient details after snapshotting (success path only)
    // NOTE: Per requirement, do NOT clear details until the above snapshot finishes
    try {
      await Campaign.updateOne({ _id: id, clientId: req.clientId }, { $set: { details: [] } });
    } catch (e) {
      console.warn('Warning: failed clearing campaign details after save-run:', e?.message);
    }

    return res.json({ success: true, data: historyDoc, clearedDetails: true });
  } catch (error) {
    console.error('Error saving campaign run:', error);
    if (error && error.code === 11000) {
      return res.status(409).json({ success: false, error: 'Duplicate runId' });
    }
    return res.status(500).json({ success: false, error: 'Failed to save campaign run' });
  }
});

// MANUAL: Force recovery of stuck campaigns
router.post('/campaigns/force-recovery', extractClientId, async (req, res) => {
  try {
    const { forceRecoverStuckCampaigns } = require('../services/campaignCallingService');
    
    console.log('🔧 MANUAL RECOVERY: Force recovery requested by client', req.clientId);
    await forceRecoverStuckCampaigns();
    
    res.json({
      success: true,
      message: 'Stuck campaigns recovery completed successfully'
    });
  } catch (error) {
    console.error('❌ MANUAL RECOVERY: Error during force recovery:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to recover stuck campaigns',
      message: error.message
    });
  }
});

// MANUAL: Cleanup completed campaigns with details
router.post('/campaigns/cleanup-completed', extractClientId, async (req, res) => {
  try {
    const { cleanupCompletedCampaignsWithDetails } = require('../services/campaignCallingService');
    
    console.log('🧹 MANUAL CLEANUP: Cleanup completed campaigns requested by client', req.clientId);
    await cleanupCompletedCampaignsWithDetails();
    
    res.json({
      success: true,
      message: 'Completed campaigns cleanup completed successfully'
    });
  } catch (error) {
    console.error('❌ MANUAL CLEANUP: Error during cleanup:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to cleanup completed campaigns',
      message: error.message
    });
  }
});

// Fetch campaign run history
router.get('/campaigns/:id/history', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId }).select('_id');
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Exclude intermediate batch snapshots for clean, contiguous history
    const rows = await CampaignHistory.find({
      campaignId: id,
      $or: [
        { batchInfo: { $exists: false } },
        { 'batchInfo.isIntermediate': { $ne: true } }
      ]
    })
      .sort({ createdAt: -1 })
      .lean();

    // Normalize instance numbers in descending order without changing stored data
    let nextNumber = rows.length;
    const normalized = rows.map((r) => ({
      ...r,
      instanceNumber: nextNumber--
    }));

    return res.json({ success: true, data: normalized });
  } catch (error) {
    console.error('Error fetching campaign history:', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch campaign history' });
  }
});
// Update agent with multiple starting messages and default selection
router.put('/agents/:id', extractClientId, async (req, res) => {
  try {
    const { startingMessages, defaultStartingMessageIndex, ...agentData } = req.body;

    // Map optional socials payload to schema fields if present
    const mapSocialsUpdate = (src) => {
      if (!src || typeof src !== 'object') return;
      console.log('🔧 Backend received socials data:', src);
      
      const platforms = ['whatsapp', 'telegram', 'email', 'sms'];
      platforms.forEach((p) => {
        const enabledKey = `${p}Enabled`;
        if (Object.prototype.hasOwnProperty.call(src, enabledKey)) {
          agentData[enabledKey] = !!src[enabledKey];
          console.log(`🔧 ${p} enabled:`, agentData[enabledKey]);
          if (!agentData[enabledKey]) {
            agentData[p] = undefined;
            // Also clear the convenience link field for WhatsApp
            if (p === 'whatsapp') {
              agentData.whatsapplink = undefined;
            }
          }
        }
        if (Object.prototype.hasOwnProperty.call(src, p)) {
          const arr = Array.isArray(src[p]) ? src[p] : [];
          const filteredArr = arr
            .filter((it) => it && typeof it.link === 'string' && it.link.trim().length > 0)
            .map((it) => ({ link: it.link.trim() }));
          
          // Set to undefined if no valid links, to be consistent with model pre-save hook
          agentData[p] = filteredArr.length > 0 ? filteredArr : undefined;
          console.log(`🔧 ${p} array:`, agentData[p]);
          
          // Update the convenience link field for WhatsApp
          if (p === 'whatsapp' && filteredArr.length > 0) {
            agentData.whatsapplink = filteredArr[0].link;
            console.log(`🔧 whatsapplink updated:`, agentData.whatsapplink);
          }
        }
      });
    };
    mapSocialsUpdate(req.body);
    if (!Array.isArray(startingMessages) || startingMessages.length === 0) {
      return res.status(400).json({ error: 'At least one starting message is required.' });
    }
    if (
      typeof defaultStartingMessageIndex !== 'number' ||
      defaultStartingMessageIndex < 0 ||
      defaultStartingMessageIndex >= startingMessages.length
    ) {
      return res.status(400).json({ error: 'Invalid default starting message index.' });
    }
    // Set default firstMessage and audioBytes
    agentData.firstMessage = startingMessages[defaultStartingMessageIndex].text;
    agentData.startingMessages = startingMessages;

    // If we are activating this agent, deactivate others first to satisfy unique index
    let agent;
    if (agentData.isActive === true && req.clientId) {
      const current = await Agent.findOne({ _id: req.params.id, clientId: req.clientId });
      if (!current) {
        return res.status(404).json({ error: 'Agent not found' });
      }
      if (current.accountSid) {
        await Agent.updateMany(
          {
            _id: { $ne: current._id },
            clientId: req.clientId,
            accountSid: current.accountSid,
            isActive: true,
          },
          { $set: { isActive: false, updatedAt: new Date() } }
        );
      }
    }

    agent = await Agent.findOneAndUpdate(
      req.clientId ? { _id: req.params.id, clientId: req.clientId } : { _id: req.params.id },
      agentData,
      { new: true, runValidators: true }
    );
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    // If this agent is active and has accountSid, deactivate others with same (clientId, accountSid)
    if (req.clientId && agent && agent.isActive && agent.accountSid) {
      await Agent.updateMany(
        {
          _id: { $ne: agent._id },
          clientId: req.clientId,
          accountSid: agent.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      )
    }

    const responseAgent = agent.toObject();
    delete responseAgent.audioBytes;
    res.json(responseAgent);
  } catch (error) {
    console.error('❌ Error updating agent:', error);
    res.status(500).json({ error: 'Failed to update agent' });
  }
});

router.put('/agents/mob/:id', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const { id } = req.params;
    const { firstMessage, voiceSelection, startingMessages } = req.body;
    
    // Only allow updating firstMessage, voiceSelection, and startingMessages
    const updateData = {};
    
    if (firstMessage !== undefined) {
      updateData.firstMessage = firstMessage;
    }
    
    if (voiceSelection !== undefined) {
      updateData.voiceSelection = voiceSelection;
    }
    
    if (startingMessages !== undefined) {
      // Get the current agent to access existing startingMessages
      const currentAgent = await Agent.findOne({ _id: id, clientId: clientId });
      
      if (!currentAgent) {
        return res.status(404).json({ error: 'Agent not found' });
      }
      
      // Get existing startingMessages or initialize empty array
      const existingMessages = currentAgent.startingMessages || [];
      
      // Transform new startingMessages to proper format if they're strings
      const newMessages = startingMessages.map(msg => {
        if (typeof msg === 'string') {
          return {
            text: msg,
          };
        }
        return msg;
      });
      
      // Combine existing and new messages
      const combinedMessages = [...existingMessages, ...newMessages];
      
      updateData.startingMessages = combinedMessages;
    }
    
    // Find and update the agent
    const agent = await Agent.findOneAndUpdate(
      { _id: id, clientId: clientId },
      updateData,
      { new: true, runValidators: true }
    );
    
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }
    
    const responseAgent = agent.toObject();
    delete responseAgent.audioBytes; // Don't send audio bytes in response
    
    res.json(responseAgent);
  }catch(error){
    console.error('❌ Error updating agent:', error);
    res.status(500).json({ error: 'Failed to update agent' });
  }
});

router.delete('/agents/:id', verifyClientOrAdminAndExtractClientId, async (req, res)=>{
  try{
    const { id } = req.params;
    const agent = await Agent.findOneAndDelete({ 
      _id: id, 
      clientId: req.clientId 
    });
    if(!agent)
    {
      return res.status(404).json({error:"Agent not found"});
    }
    res.json({message:"Agent deleted successfully"})
  }catch(error){
    console.error('❌ Error deleting agent:', error);
    res.status(500).json({error:"Failed to delete agent"})
  }
});

// Get all agents for client
router.get('/agents', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const filter = req.clientId ? { clientId: req.clientId } : {};
    const agents = await Agent.find(filter)
      .select('-audioBytes') // Don't send audio bytes in list view
      .sort({ createdAt: -1 });
    res.json({success: true, data: agents});
  } catch (error) {
    console.error("Error fetching agents:", error);
    res.status(500).json({ success: false, error: "Failed to fetch agents" });
  }
});

// Get all agents for client without audio data
router.get('/agents/no-audio', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const filter = req.clientId ? { clientId: req.clientId } : {};
    const agents = await Agent.find(filter)
      .select('-audioBytes -audioFile -audioMetadata -defaultTemplate -whatsappTemplates')
      .sort({ createdAt: -1 });
    
    // Remove audioBase64 from startingMessages array
    const agentsWithoutAudio = agents.map(agent => {
      const agentObj = agent.toObject();
      if (agentObj.startingMessages && Array.isArray(agentObj.startingMessages)) {
        agentObj.startingMessages = agentObj.startingMessages.map(message => {
          const { audioBase64, ...messageWithoutAudio } = message;
          return messageWithoutAudio;
        });
      }
      return agentObj;
    });
    
    res.json({success: true, data: agentsWithoutAudio});
  } catch (error) {
    console.error("Error fetching agents without audio:", error);
    res.status(500).json({ success: false, error: "Failed to fetch agents" });
  }
});

// Get all agents created by admin
router.get('/agents/admin', verifyAdminToken, async (req, res) => {
  try {
    const adminId = req.adminId;
    
    if (!adminId) {
      return res.status(400).json({ 
        success: false, 
        error: 'Admin ID not found in token' 
      });
    }

    // Find all agents created by this admin
    const agents = await Agent.find({ 
      createdBy: adminId,
      createdByType: 'admin'
    })
    .select('-audioBytes') // Don't send audio bytes in list view
    .sort({ createdAt: -1 })
    .lean();

    console.log(`🔍 Admin ${adminId} fetching their agents. Found: ${agents.length}`);

    res.json({
      success: true, 
      data: agents,
      totalCount: agents.length
    });

  } catch (error) {
    console.error('❌ Error fetching admin agents:', error);
    res.status(500).json({
      success: false, 
      error: 'Failed to fetch agents',
      details: error.message
    });
  }
});

// Get agent audio
router.get('/agents/:id/audio', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const query = req.clientId ? { _id: id, clientId: req.clientId } : { _id: id };
    const agent = await Agent.findOne(query);
    
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    if (!agent.audioBytes) {
      return res.status(404).json({ error: 'No audio available for this agent' });
    }

    // Convert base64 to buffer
    const audioBuffer = Buffer.from(agent.audioBytes, 'base64');
    
    // Set appropriate headers
    res.set({
      'Content-Type': 'audio/mpeg',
      'Content-Length': audioBuffer.length,
      'Cache-Control': 'public, max-age=3600'
    });
    
    res.send(audioBuffer);
  } catch (error) {
    console.error('Error fetching agent audio:', error);
    res.status(500).json({ error: 'Failed to fetch agent audio' });
  }
});

// Generate audio from text endpoint - returns both buffer and base64
router.post('/voice/synthesize', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { text, language = "en", speaker, serviceProvider = "sarvam" } = req.body;
    if (!text || !text.trim()) {
      return res.status(400).json({ error: "Text is required" });
    }

    // Generate audio using the voice service
    const audioResult = await voiceService.textToSpeech(text, language, speaker, serviceProvider);

    // Return both for frontend: buffer for playback, base64 for DB
    res.set({
      "Content-Type": "application/json",
    });
    res.json({
      audioBuffer: audioResult.audioBuffer.toString('base64'), // for compatibility
      audioBase64: audioResult.audioBase64, // for database storage
      format: audioResult.format,
      size: audioResult.audioBuffer.length,
      sampleRate: audioResult.sampleRate,
      channels: audioResult.channels,
      usedSpeaker: audioResult.usedSpeaker,
      targetLanguage: audioResult.targetLanguage,
      serviceProvider: audioResult.serviceProvider || serviceProvider
    });
  } catch (error) {
    console.error("❌ Voice synthesis error:", error);
    // Surface clearer error payloads to the frontend
    const message = typeof error?.message === 'string' ? error.message : String(error)
    res.status(500).json({ error: message });
  }
});

// Stream agent's firstMessage as generated audio without saving
router.get('/agents/:id/first-message/audio', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const query = req.clientId ? { _id: id, clientId: req.clientId } : { _id: id };
    const agent = await Agent.findOne(query).lean();
    if (!agent) {
      return res.status(404).json({ success: false, message: 'Agent not found' });
    }
    const text = (agent.firstMessage || '').trim();
    if (!text) {
      return res.status(400).json({ success: false, message: 'Agent has no firstMessage' });
    }

    const language = agent.language || 'en';
    const speaker = agent.voiceSelection || agent.voiceId;
    const serviceProvider = agent.ttsSelection || agent.voiceServiceProvider || 'sarvam';

    const audioResult = await voiceService.textToSpeech(text, language, speaker, serviceProvider);
    let buf = audioResult.audioBuffer;
    if (!buf && audioResult.audioBase64) buf = Buffer.from(audioResult.audioBase64, 'base64');
    if (!buf) {
      return res.status(500).json({ success: false, message: 'Audio generation failed' });
    }

    const format = (audioResult.format || 'mp3').toLowerCase();
    const mime = format === 'wav' ? 'audio/wav' : (format === 'ogg' ? 'audio/ogg' : 'audio/mpeg');
    res.set({ 'Content-Type': mime, 'Content-Length': buf.length });
    return res.send(buf);
  } catch (error) {
    console.error('❌ First message audio error:', error);
    const message = typeof error?.message === 'string' ? error.message : String(error);
    return res.status(500).json({ success: false, message });
  }
});

// Inbound Reports
router.get('/inbound/report', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query - filter for inbound calls only
    const query = { 
      clientId, 
      'metadata.callDirection': "inbound",
      ...dateFilter 
    };
        
    const logs = await CallLog.find(query);
    const totalCalls = logs.length;
    const totalConnected = logs.filter(l => l.leadStatus !== 'not_connected').length;
    const totalNotConnected = logs.filter(l => l.leadStatus === 'not_connected').length;
    const totalConversationTime = logs.reduce((sum, l) => sum + (l.duration || 0), 0);
    const avgCallDuration = totalCalls ? totalConversationTime / totalCalls : 0;
    
    res.json({ 
      success: true, 
      data: {
        clientId,
        totalCalls, 
        totalConnected, 
        totalNotConnected, 
        totalConversationTime, 
        avgCallDuration 
      },
      filter: {
        applied: filter || 'all',
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error in /inbound/report:', error);
    res.status(500).json({ error: 'Failed to fetch report' });
  }
});

// Outbound Report
router.get('/outbound/report', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query - filter for outbound calls only
    const query = { 
      clientId, 
      'metadata.callDirection': "outbound",
      ...dateFilter 
    };
        
    const logs = await CallLog.find(query);
    const totalCalls = logs.length;
    const totalConnected = logs.filter(l => l.leadStatus !== 'not_connected').length;
    const totalNotConnected = logs.filter(l => l.leadStatus === 'not_connected').length;
    const totalConversationTime = logs.reduce((sum, l) => sum + (l.duration || 0), 0);
    const avgCallDuration = totalCalls ? totalConversationTime / totalCalls : 0;
    
    res.json({ 
      success: true, 
      data: {
        clientId,
        totalCalls, 
        totalConnected, 
        totalNotConnected, 
        totalConversationTime, 
        avgCallDuration 
      },
      filter: {
        applied: filter || 'all',
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error in /outbound/report:', error);
    res.status(500).json({ error: 'Failed to fetch report' });
  }
});

// Inbound Logs/Conversation
router.get('/inbound/logs', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate, page = 1, limit = 20 } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query - filter for inbound calls only
    const query = { 
      clientId, 
      'metadata.callDirection': "inbound",
      ...dateFilter
    };
    
    // Pagination parameters
    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    const clientName = await Client.findOne({ _id: clientId }).select('name');
    
    // Get total count for pagination
    const totalCount = await CallLog.countDocuments(query);
    
    // Get paginated logs
    const logs = await CallLog.find(query)
      .sort({ createdAt: -1 })
      .populate('agentId', 'agentName')
      .skip(skip)
      .limit(limitNum)
      .lean();
      
    const logsWithAgentName = logs.map(l => ({
      ...l,
      agentName: l.agentId && l.agentId.agentName ? l.agentId.agentName : null,
    }));
    
    // Calculate pagination info
    const totalPages = Math.ceil(totalCount / limitNum);
    const hasNextPage = pageNum < totalPages;
    const hasPrevPage = pageNum > 1;
    
    res.json({
      success: true, 
      clientName: clientName,
      data: logsWithAgentName,
      pagination: {
        currentPage: pageNum,
        totalPages: totalPages,
        totalItems: totalCount,
        itemsPerPage: limitNum,
        hasNextPage: hasNextPage,
        hasPrevPage: hasPrevPage,
        nextPage: hasNextPage ? pageNum + 1 : null,
        prevPage: hasPrevPage ? pageNum - 1 : null
      }
    });
  } catch (error) {
    console.error('Error in /inbound/logs:', error);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// Outbound logs API
router.get('/outbound/logs', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate, page = 1, limit = 20 } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query - filter for outbound calls only
    const query = { 
      clientId, 
      'metadata.callDirection': "outbound",
      ...dateFilter
    };
    
    // Pagination parameters
    const pageNum = parseInt(page);
    const limitNum = parseInt(limit);
    const skip = (pageNum - 1) * limitNum;

    const clientName = await Client.findOne({ _id: clientId }).select('name');
    
    // Get total count for pagination
    const totalCount = await CallLog.countDocuments(query);
    
    // Get paginated logs
    const logs = await CallLog.find(query)
      .sort({ createdAt: -1 })
      .populate('agentId', 'agentName')
      .skip(skip)
      .limit(limitNum)
      .lean();
      
    const logsWithAgentName = logs.map(l => ({
      ...l,
      agentName: l.agentId && l.agentId.agentName ? l.agentId.agentName : null,
    }));
    
    // Calculate pagination info
    const totalPages = Math.ceil(totalCount / limitNum);
    const hasNextPage = pageNum < totalPages;
    const hasPrevPage = pageNum > 1;
    
    res.json({
      success: true, 
      clientName: clientName,
      data: logsWithAgentName,
      pagination: {
        currentPage: pageNum,
        totalPages: totalPages,
        totalItems: totalCount,
        itemsPerPage: limitNum,
        hasNextPage: hasNextPage,
        hasPrevPage: hasPrevPage,
        nextPage: hasNextPage ? pageNum + 1 : null,
        prevPage: hasPrevPage ? pageNum - 1 : null
      }
    });
  } catch (error) {
    console.error('Error in /outbound/logs:', error);
    res.status(500).json({ error: 'Failed to fetch logs' });
  }
});

// Inbound Leads
router.get('/inbound/leads', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }

    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query - filter for inbound calls only
    const query = { 
      clientId, 
      'metadata.callDirection': "inbound",
      ...dateFilter 
    };    
    const logs = await CallLog.find(query).sort({ createdAt: -1 });
    
    // Group leads according to the new leadStatus structure
    const leads = {
      // Connected - Interested
      veryInterested: {
        data: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very_interested'),
        count: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very_interested').length
      },
      maybe: {
        data: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium'),
        count: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium').length
      },
      enrolled: {
        data: logs.filter(l => l.leadStatus === 'enrolled'),
        count: logs.filter(l => l.leadStatus === 'enrolled').length
      },
      
      // Connected - Not Interested
      junkLead: {
        data: logs.filter(l => l.leadStatus === 'junk_lead'),
        count: logs.filter(l => l.leadStatus === 'junk_lead').length
      },
      notRequired: {
        data: logs.filter(l => l.leadStatus === 'not_required'),
        count: logs.filter(l => l.leadStatus === 'not_required').length
      },
      enrolledOther: {
        data: logs.filter(l => l.leadStatus === 'enrolled_other'),
        count: logs.filter(l => l.leadStatus === 'enrolled_other').length
      },
      decline: {
        data: logs.filter(l => l.leadStatus === 'decline'),
        count: logs.filter(l => l.leadStatus === 'decline').length
      },
      notEligible: {
        data: logs.filter(l => l.leadStatus === 'not_eligible'),
        count: logs.filter(l => l.leadStatus === 'not_eligible').length
      },
      wrongNumber: {
        data: logs.filter(l => l.leadStatus === 'wrong_number'),
        count: logs.filter(l => l.leadStatus === 'wrong_number').length
      },
      
      // Connected - Followup
      hotFollowup: {
        data: logs.filter(l => l.leadStatus === 'hot_followup'),
        count: logs.filter(l => l.leadStatus === 'hot_followup').length
      },
      coldFollowup: {
        data: logs.filter(l => l.leadStatus === 'cold_followup'),
        count: logs.filter(l => l.leadStatus === 'cold_followup').length
      },
      schedule: {
        data: logs.filter(l => l.leadStatus === 'schedule'),
        count: logs.filter(l => l.leadStatus === 'schedule').length
      },
      
      // Not Connected
      notConnected: {
        data: logs.filter(l => l.leadStatus === 'not_connected'),
        count: logs.filter(l => l.leadStatus === 'not_connected').length
      }
    };

    res.json({ 
      success: true, 
      data: leads,
      filter: {
        applied: filter || 'all',
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error in /inbound/leads:', error);
    res.status(500).json({ error: 'Failed to fetch leads' });
  }
});

// Outbound Leads
router.get('/outbound/leads', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }

    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query - filter for outbound calls only
    const query = { 
      clientId, 
      'metadata.callDirection': "outbound",
      ...dateFilter 
    };    
    const logs = await CallLog.find(query).sort({ createdAt: -1 });
    
    // Group leads according to the new leadStatus structure
    const leads = {
      // Connected - Interested
      veryInterested: {
        data: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very_interested'),
        count: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very_interested').length
      },
      maybe: {
        data: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium'),
        count: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium').length
      },
      enrolled: {
        data: logs.filter(l => l.leadStatus === 'enrolled'),
        count: logs.filter(l => l.leadStatus === 'enrolled').length
      },
      
      // Connected - Not Interested
      junkLead: {
        data: logs.filter(l => l.leadStatus === 'junk_lead'),
        count: logs.filter(l => l.leadStatus === 'junk_lead').length
      },
      notRequired: {
        data: logs.filter(l => l.leadStatus === 'not_required'),
        count: logs.filter(l => l.leadStatus === 'not_required').length
      },
      enrolledOther: {
        data: logs.filter(l => l.leadStatus === 'enrolled_other'),
        count: logs.filter(l => l.leadStatus === 'enrolled_other').length
      },
      decline: {
        data: logs.filter(l => l.leadStatus === 'decline'),
        count: logs.filter(l => l.leadStatus === 'decline').length
      },
      notEligible: {
        data: logs.filter(l => l.leadStatus === 'not_eligible'),
        count: logs.filter(l => l.leadStatus === 'not_eligible').length
      },
      wrongNumber: {
        data: logs.filter(l => l.leadStatus === 'wrong_number'),
        count: logs.filter(l => l.leadStatus === 'wrong_number').length
      },
      
      // Connected - Followup
      hotFollowup: {
        data: logs.filter(l => l.leadStatus === 'hot_followup'),
        count: logs.filter(l => l.leadStatus === 'hot_followup').length
      },
      coldFollowup: {
        data: logs.filter(l => l.leadStatus === 'cold_followup'),
        count: logs.filter(l => l.leadStatus === 'cold_followup').length
      },
      schedule: {
        data: logs.filter(l => l.leadStatus === 'schedule'),
        count: logs.filter(l => l.leadStatus === 'schedule').length
      },
      
      // Not Connected
      notConnected: {
        data: logs.filter(l => l.leadStatus === 'not_connected'),
        count: logs.filter(l => l.leadStatus === 'not_connected').length
      }
    };

    res.json({ 
      success: true, 
      data: leads,
      filter: {
        applied: filter || 'all',
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error in /outbound/leads:', error);
    res.status(500).json({ error: 'Failed to fetch leads' });
  }
});

// Inbound Settings (GET/PUT)
router.get('/inbound/settings', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const settings = await AgentSettings.findOne({ clientId });
    res.json(settings);
  } catch (error) {
    console.error('Error in /inbound/settings GET:', error);
    res.status(500).json({ error: 'Failed to fetch settings' });
  }
});

//Inbound Settings
router.put('/inbound/settings', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const update = req.body;
    const settings = await AgentSettings.findOneAndUpdate({ clientId }, update, { new: true, upsert: true });
    res.json(settings);
  } catch (error) {
    console.error('Error in /inbound/settings PUT:', error);
    res.status(500).json({ error: 'Failed to update settings' });
  }
});
// ==================== Sync Contacts =================

// Bulk contact addition
router.post('/sync/contacts', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { contacts } = req.body;

    if (!contacts || !Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ 
        status: false, 
        message: 'contacts array is required and must not be empty' 
      });
    }

    // Validate each contact (require phone; name optional)
    for (let i = 0; i < contacts.length; i++) {
      const contact = contacts[i];
      if (!contact || !contact.phone || !String(contact.phone).trim()) {
        return res.status(400).json({ 
          status: false, 
          message: `Contact at index ${i}: phone is required` 
        });
      }
    }

    const results = {
      success: [],
      duplicates: [],
      errors: []
    };

    // Process each contact
    for (let i = 0; i < contacts.length; i++) {
      const contact = contacts[i];
      try {
        // Clean phone number: remove spaces and country code
        let cleanPhone = contact.phone.trim().replace(/\s+/g, ''); // Remove all spaces
        let countryCode = ''; // Default empty country code
        
        // Remove common country codes if present and save the country code
        const countryCodes = ['+91', '+1', '+44', '+61', '+86', '+81', '+49', '+33', '+39', '+34', '+7', '+55', '+52', '+31', '+46', '+47', '+45', '+358', '+46', '+47', '+45', '+358'];
        for (const code of countryCodes) {
          if (cleanPhone.startsWith(code)) {
            countryCode = code; // Save the country code
            cleanPhone = cleanPhone.substring(code.length);
            break;
          }
        }
        
        // Handle US numbers without + prefix (like 964-339-5853)
        // If phone number is 10 digits and starts with common US area codes, assume it's US
        if (!countryCode && cleanPhone.length === 10 && /^[2-9]\d{9}$/.test(cleanPhone)) {
          countryCode = '+1';
        }
        
        // Handle 11-digit US numbers starting with 1
        if (!countryCode && cleanPhone.length === 11 && cleanPhone.startsWith('1')) {
          countryCode = '+1';
          cleanPhone = cleanPhone.substring(1);
        }
        
        // If phone starts with 0, remove it (common in many countries)
        if (cleanPhone.startsWith('0')) {
          cleanPhone = cleanPhone.substring(1);
        }
        
        // Check if contact already exists with cleaned phone
        const existingContact = await Contacts.findOne({ 
          clientId, 
          phone: cleanPhone 
        });

        if (existingContact) {
          results.duplicates.push({
            index: i,
            input: contact,
            existing: {
              name: existingContact.name,
              phone: existingContact.phone
            }
          });
          continue;
        }

        // Create new contact with cleaned phone and country code
        const rawName = (contact.name || '').toString().trim();
        const looksLikeNumber = /^\+?\d[\d\s-]*$/.test(rawName);
        const safeName = (!rawName || looksLikeNumber) ? '' : rawName;

        const newContact = new Contacts({
          name: safeName,
          phone: cleanPhone,
          countyCode: countryCode, // Save the country code
          email: contact.email?.toString().trim() || '',
          clientId
        });

        const savedContact = await newContact.save();
        results.success.push({
          index: i,
          data: savedContact
        });

      } catch (error) {
        results.errors.push({
          index: i,
          input: contact,
          error: error.message
        });
      }
    }

    // Determine response status
    const hasSuccess = results.success.length > 0;
    const hasDuplicates = results.duplicates.length > 0;
    const hasErrors = results.errors.length > 0;

    let statusCode = 200;
    let message = '';

    if (hasSuccess && !hasDuplicates && !hasErrors) {
      statusCode = 201;
      message = `Successfully added ${results.success.length} contacts`;
    } else if (hasSuccess && (hasDuplicates || hasErrors)) {
      statusCode = 207; // Multi-Status
      message = `Partially successful: ${results.success.length} added, ${results.duplicates.length} duplicates, ${results.errors.length} errors`;
    } else if (!hasSuccess && hasDuplicates) {
      statusCode = 409;
      message = `All contacts already exist (${results.duplicates.length} duplicates)`;
    } else if (!hasSuccess && hasErrors) {
      statusCode = 400;
      message = `Failed to add contacts: ${results.errors.length} errors`;
    }

    res.status(statusCode).json({
      status: hasSuccess,
      message,
      results
    });

  } catch (error) {
    console.error('Error in bulk contact addition:', error);
    res.status(500).json({ 
      status: false, 
      message: 'Internal server error during bulk contact addition' 
    });
  }
});

// ==================== GROUPS API ====================

// Get all groups for client
router.get('/groups', extractClientId, async (req, res) => {
  try {
    const groups = await Group.aggregate([
      { $match: { clientId: req.clientId } },
      { $sort: { createdAt: -1 } },
      {
        $project: {
          name: 1,
          category: 1,
          description: 1,
          clientId: 1,
          createdAt: 1,
          updatedAt: 1,
          contactsCount: { $size: { $ifNull: ["$contacts", []] } }
        }
      }
    ]);
    res.json({ success: true, data: groups });
  } catch (error) {
    console.error('Error fetching groups:', error);
    res.status(500).json({ error: 'Failed to fetch groups' });
  }
});

// Create new group
router.post('/groups', extractClientId, async (req, res) => {
  try {
    const { name, category, description } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Group name is required' });
    }

    // Check for duplicate group name within the same client
    const groupNameMatch = await Group.findOne({
      name: { $regex: name.trim(), $options: 'i' },
      clientId: req.clientId
    });
    if (groupNameMatch) {
      return res.status(400).json({ success : false,error: 'Group name already exists' });
    }

    const group = new Group({
      name: name.trim(),
      category: category?.trim() || '',
      description: description?.trim() || '',
      clientId: req.clientId,
      contacts: []
    });

    await group.save();
    res.status(201).json({ success: true, data: group });
  } catch (error) {
    console.error('Error creating group:', error);
    res.status(500).json({ error: 'Failed to create group' });
  }
});

// Get single group by ID
router.get('/groups/:id', extractClientId, async (req, res) => {
  try {
    const group = await Group.findOne({ _id: req.params.id, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ error: 'Group not found' });
    }
    res.json({ success: true, data: group });
  } catch (error) {
    console.error('Error fetching group:', error);
    res.status(500).json({ error: 'Failed to fetch group' });
  }
});

// Update group
router.put('/groups/:id', extractClientId, async (req, res) => {
  try {
    const { name, category, description } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Group name is required' });
    }

    // Check for duplicate group name within the same client (excluding current group)
    const groupNameMatch = await Group.findOne({
      name: { $regex: name.trim(), $options: 'i' },
      clientId: req.clientId,
      _id: { $ne: req.params.id }
    });
    if (groupNameMatch) {
      return res.status(400).json({ success : false, error: 'Group name already exists' });
    }

    const group = await Group.findOneAndUpdate(
      { _id: req.params.id, clientId: req.clientId },
      { 
        name: name.trim(), 
        category: category?.trim() || '',
        description: description?.trim() || '',
        updatedAt: new Date()
      },
      { new: true }
    );

    if (!group) {
      return res.status(404).json({ error: 'Group not found' });
    }

    res.json({ success: true, data: group });
  } catch (error) {
    console.error('Error updating group:', error);
    res.status(500).json({ error: 'Failed to update group' });
  }
});

// Delete group
router.delete('/groups/:id', extractClientId, async (req, res) => {
  try {
    const group = await Group.findOneAndDelete({ _id: req.params.id, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ error: 'Group not found' });
    }
    res.json({ success: true, message: 'Group deleted successfully' });
  } catch (error) {
    console.error('Error deleting group:', error);
    res.status(500).json({ error: 'Failed to delete group' });
  }
});

// Add contact to group
router.post('/groups/:id/contacts', extractClientId, async (req, res) => {
  try {
    const { name, phone, email } = req.body;
    
    if (!phone || !phone.trim()) {
      return res.status(400).json({ 
        success: false,
        error: 'phone is required' 
      });
    }

    // Function to normalize phone number for duplicate detection
    const normalizePhoneNumber = (phoneNumber) => {
      if (!phoneNumber) return '';
      
      // Convert to string and trim
      let normalized = phoneNumber.toString().trim();
      
      // Remove all spaces, dashes, dots, and parentheses
      normalized = normalized.replace(/[\s\-\.\(\)]/g, '');
      
      // Remove country codes (common patterns)
      // Remove +91, +1, +44, etc. (any + followed by 1-3 digits)
      normalized = normalized.replace(/^\+\d{1,3}/, '');
      
      // Remove leading zeros
      normalized = normalized.replace(/^0+/, '');
      
      // If the number starts with 91 and is longer than 10 digits, remove 91
      if (normalized.startsWith('91') && normalized.length > 10) {
        normalized = normalized.substring(2);
      }
      
      // If the number starts with 1 and is longer than 10 digits, remove 1
      if (normalized.startsWith('1') && normalized.length > 10) {
        normalized = normalized.substring(1);
      }
      
      return normalized;
    };

    const group = await Group.findOne({ _id: req.params.id, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ 
        success: false,
        error: 'Group not found' 
      });
    }

    // Normalize the input phone number
    const normalizedInputPhone = normalizePhoneNumber(phone);
    
    // Check if normalized phone number already exists in the group
    const existingContact = group.contacts.find(contact => {
      const normalizedContactPhone = normalizePhoneNumber(contact.phone);
      return normalizedContactPhone === normalizedInputPhone;
    });
    
    if (existingContact) {
      return res.status(409).json({ 
        success: false,
        error: 'Phone number already exists in this group',
        existingContact: {
          name: existingContact.name,
          phone: existingContact.phone,
          email: existingContact.email || '',
          createdAt: existingContact.createdAt
        },
        message: `Phone number ${phone.trim()} is already assigned to contact "${existingContact.name}" in group "${group.name}" (normalized: ${normalizedInputPhone})`
      });
    }

    const contact = {
      name: typeof name === 'string' ? name.trim() : '',
      phone: phone.trim(),
      normalizedPhone: normalizedInputPhone, // Store normalized version for future comparisons
      email: typeof email === 'string' ? email.trim() : '',
      createdAt: new Date()
    };

    group.contacts.push(contact);
    await group.save();

    // Sync campaigns including this group (add contact's phone)
    try {
      await syncCampaignContactsForGroup({
        clientId: req.clientId,
        groupId: group._id,
        phonesAdded: [contact.phone]
      });
    } catch (e) {
      console.error('Campaign sync after add failed:', e);
    }

    res.status(201).json({ 
      success: true, 
      data: contact,
      message: 'Contact added successfully to group'
    });
  } catch (error) {
    console.error('Error adding contact:', error);
    res.status(500).json({ 
      success: false,
      error: 'Failed to add contact' 
    });
  }
});

// Delete contact from group
router.delete('/groups/:groupId/contacts/:contactId', extractClientId, async (req, res) => {
  try {
    const { groupId, contactId } = req.params;
    
    const group = await Group.findOne({ _id: groupId, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ error: 'Group not found' });
    }

    // Find the contact slated for deletion
    const contactToDelete = (Array.isArray(group.contacts) && group.contacts.find(c => c && c._id && c._id.toString() === contactId)) || null;

    // Before deleting, store it in Contacts using the same normalization as /sync/contacts
    if (contactToDelete && contactToDelete.phone) {
      try {
        let cleanPhone = String(contactToDelete.phone).trim().replace(/\s+/g, '');
        let countryCode = '';
        const countryCodes = ['+91', '+1', '+44', '+61', '+86', '+81', '+49', '+33', '+39', '+34', '+7', '+55', '+52', '+31', '+46', '+47', '+45', '+358', '+46', '+47', '+45', '+358'];

        for (const code of countryCodes) {
          if (cleanPhone.startsWith(code)) {
            countryCode = code;
            cleanPhone = cleanPhone.substring(code.length);
            break;
          }
        }

        if (!countryCode && cleanPhone.length === 10 && /^[2-9]\d{9}$/.test(cleanPhone)) {
          countryCode = '+1';
        }

        if (!countryCode && cleanPhone.length === 11 && cleanPhone.startsWith('1')) {
          countryCode = '+1';
          cleanPhone = cleanPhone.substring(1);
        }

        if (cleanPhone.startsWith('0')) {
          cleanPhone = cleanPhone.substring(1);
        }

        const exists = await Contacts.findOne({ clientId: req.clientId, phone: cleanPhone });
        if (!exists) {
          const rawName = (contactToDelete.name || '').toString().trim();
          const looksLikeNumber = /^\+?\d[\d\s-]*$/.test(rawName);
          const safeName = (!rawName || looksLikeNumber) ? '' : rawName;
          const newContact = new Contacts({
            name: safeName,
            phone: cleanPhone,
            countyCode: countryCode,
            email: (contactToDelete.email || '').toString().trim(),
            clientId: req.clientId
          });
          await newContact.save();
        }
      } catch (syncError) {
        // Do not block deletion on sync failure
        console.error('Failed to sync contact before deletion:', syncError);
      }
    }

    // Proceed with deletion from group
    const deleted = group.contacts.find(contact => contact._id.toString() === contactId);
    group.contacts = group.contacts.filter(contact => contact._id.toString() !== contactId);
    await group.save();

    // Sync campaigns including this group for removed phone (if no other group has it)
    try {
      if (deleted && deleted.phone) {
        await syncCampaignContactsForGroup({
          clientId: req.clientId,
          groupId: group._id,
          phonesRemoved: [deleted.phone]
        });
      }
    } catch (e) {
      console.error('Campaign sync after delete failed:', e);
    }

    res.json({ success: true, message: 'Contact deleted successfully' });
  } catch (error) {
    console.error('Error deleting contact:', error);
    res.status(500).json({ error: 'Failed to delete contact' });
  }
});
// Bulk add contacts to a group in a single request
// Body: { contacts: [{ name?: string, phone: string, email?: string }] }
router.post('/groups/:groupId/contacts/bulk-add', extractClientId, async (req, res) => {
  try {
    const { groupId } = req.params;
    const { contacts } = req.body || {};

    if (!Array.isArray(contacts) || contacts.length === 0) {
      return res.status(400).json({ success: false, error: 'contacts must be a non-empty array' });
    }

    // Soft cap to prevent excessively large payloads
    if (contacts.length > 5000) {
      return res.status(400).json({ success: false, error: 'Maximum 5000 contacts per request' });
    }

    const group = await Group.findOne({ _id: groupId, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ success: false, error: 'Group not found' });
    }

    // Build set of existing normalized phones in this group
    const existingSet = new Set(
      (Array.isArray(group.contacts) ? group.contacts : []).map(c => {
        const raw = c && (c.normalizedPhone || c.phone) ? String(c.normalizedPhone || c.phone) : '';
        return normalizePhoneNumber(raw);
      }).filter(Boolean)
    );

    // Prepare new contacts
    const toInsert = [];
    const seenIncoming = new Set();

    for (const c of contacts) {
      if (!c || !c.phone || !String(c.phone).trim()) continue;

      const inputPhone = String(c.phone).trim();
      const normalized = normalizePhoneNumber(inputPhone);
      if (!normalized) continue;

      // Deduplicate within request and against existing group
      if (existingSet.has(normalized) || seenIncoming.has(normalized)) continue;
      seenIncoming.add(normalized);

      const rawName = (c.name || '').toString().trim();
      const looksLikeNumber = /^\+?\d[\d\s-]*$/.test(rawName);
      const safeName = (!rawName || looksLikeNumber) ? '' : rawName;

      const safeEmail = (c.email || '').toString().trim();

      toInsert.push({
        name: safeName,
        phone: inputPhone,
        normalizedPhone: normalized,
        email: safeEmail,
        createdAt: new Date()
      });
    }

    if (toInsert.length === 0) {
      return res.status(200).json({ success: true, message: 'No new contacts to add', added: 0, duplicates: contacts.length });
    }

    // Atomic push using $each
    const updateResult = await Group.updateOne(
      { _id: group._id, clientId: req.clientId },
      { $push: { contacts: { $each: toInsert } } }
    );

    // Sync campaigns including this group for added phones
    try {
      await syncCampaignContactsForGroup({
        clientId: req.clientId,
        groupId: group._id,
        phonesAdded: toInsert.map(c => c.phone)
      });
    } catch (e) {
      console.error('Campaign sync after bulk-add failed:', e);
    }

    return res.status(201).json({
      success: true,
      message: `Added ${toInsert.length} contact(s) to group`,
      added: toInsert.length,
      requested: contacts.length,
      modifiedCount: updateResult && typeof updateResult.modifiedCount === 'number' ? updateResult.modifiedCount : undefined
    });
  } catch (error) {
    console.error('Error bulk-adding contacts to group:', error);
    return res.status(500).json({ success: false, error: 'Failed to bulk add contacts' });
  }
});

// Bulk delete contacts from group (with pre-sync to Contacts)
// Body: { contactIds: string[] }
router.post('/groups/:groupId/contacts/bulk-delete', extractClientId, async (req, res) => {
  try {
    const { groupId } = req.params;
    const { contactIds } = req.body || {};

    if (!Array.isArray(contactIds) || contactIds.length === 0) {
      return res.status(400).json({ success: false, error: 'contactIds must be a non-empty array' });
    }

    const group = await Group.findOne({ _id: groupId, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ success: false, error: 'Group not found' });
    }

    // Build set of string ids for quick lookup
    const idsSet = new Set(contactIds.map(id => String(id)));
    const contactsToRemove = (Array.isArray(group.contacts) ? group.contacts : []).filter(c => c && c._id && idsSet.has(String(c._id)));

    // Pre-sync: normalize phones and upsert missing into Contacts in bulk
    const toNormalize = [];
    for (const c of contactsToRemove) {
      if (c && c.phone) {
        let cleanPhone = String(c.phone).trim().replace(/\s+/g, '');
        let countryCode = '';
        const countryCodes = ['+91', '+1', '+44', '+61', '+86', '+81', '+49', '+33', '+39', '+34', '+7', '+55', '+52', '+31', '+46', '+47', '+45', '+358', '+46', '+47', '+45', '+358'];
        for (const code of countryCodes) {
          if (cleanPhone.startsWith(code)) {
            countryCode = code;
            cleanPhone = cleanPhone.substring(code.length);
            break;
          }
        }
        if (!countryCode && cleanPhone.length === 10 && /^[2-9]\d{9}$/.test(cleanPhone)) {
          countryCode = '+1';
        }
        if (!countryCode && cleanPhone.length === 11 && cleanPhone.startsWith('1')) {
          countryCode = '+1';
          cleanPhone = cleanPhone.substring(1);
        }
        if (cleanPhone.startsWith('0')) {
          cleanPhone = cleanPhone.substring(1);
        }
        const rawName = (c.name || '').toString().trim();
        const looksLikeNumber = /^\+?\d[\d\s-]*$/.test(rawName);
        const safeName = (!rawName || looksLikeNumber) ? '' : rawName;
        toNormalize.push({
          name: safeName,
          email: (c.email || '').toString().trim(),
          phone: cleanPhone,
          countyCode: countryCode
        });
      }
    }

    // Deduplicate by phone
    const phoneSet = new Set(toNormalize.map(x => x.phone).filter(Boolean));
    const uniquePhones = Array.from(phoneSet);

    if (uniquePhones.length > 0) {
      try {
        const existing = await Contacts.find({ clientId: req.clientId, phone: { $in: uniquePhones } }, { phone: 1 }).lean();
        const existingPhones = new Set((existing || []).map(e => String(e.phone)));
        const toInsert = toNormalize
          .filter(x => x.phone && !existingPhones.has(String(x.phone)))
          .map(x => ({
            name: x.name,
            phone: x.phone,
            countyCode: x.countyCode,
            email: x.email,
            clientId: req.clientId
          }));

        if (toInsert.length > 0) {
          await Contacts.insertMany(toInsert, { ordered: false });
        }
      } catch (syncErr) {
        // Log but do not block deletion
        console.error('Bulk sync before deletion failed:', syncErr);
      }
    }

    // Perform atomic bulk removal
    const mongoose = require('mongoose');
    const objectIds = contactIds
      .map(id => (mongoose.Types.ObjectId.isValid(id) ? new mongoose.Types.ObjectId(id) : null))
      .filter(Boolean);

    // Pull by ObjectId matches
    const updateResultObj = objectIds.length > 0
      ? await Group.updateOne(
          { _id: group._id, clientId: req.clientId },
          { $pull: { contacts: { _id: { $in: objectIds } } } }
        )
      : { modifiedCount: 0 };

    // Also attempt pull by raw string ids in case subdocument _id are stored as strings
    const stringIds = contactIds.map(id => String(id));
    const updateResultStr = stringIds.length > 0
      ? await Group.updateOne(
          { _id: group._id, clientId: req.clientId },
          { $pull: { contacts: { _id: { $in: stringIds } } } }
        )
      : { modifiedCount: 0 };

    // Reload counts for response
    const updatedGroup = await Group.findById(group._id).select('contacts');
    const remaining = Array.isArray(updatedGroup && updatedGroup.contacts) ? updatedGroup.contacts.length : 0;

    // Sync campaigns for removed phones (compute from contactsToRemove)
    try {
      const removedPhones = contactsToRemove.map(c => c && c.phone).filter(Boolean);
      if (removedPhones.length > 0) {
        await syncCampaignContactsForGroup({
          clientId: req.clientId,
          groupId: group._id,
          phonesRemoved: removedPhones
        });
      }
    } catch (e) {
      console.error('Campaign sync after bulk-delete failed:', e);
    }

    return res.json({
      success: true,
      message: 'Contacts deleted successfully',
      deletedRequested: contactIds.length,
      actuallyMatchedInGroup: contactsToRemove.length,
      modifiedCountObjectId: updateResultObj && typeof updateResultObj.modifiedCount === 'number' ? updateResultObj.modifiedCount : undefined,
      modifiedCountStringId: updateResultStr && typeof updateResultStr.modifiedCount === 'number' ? updateResultStr.modifiedCount : undefined,
      remainingContactsInGroup: remaining
    });
  } catch (error) {
    console.error('Error bulk-deleting contacts:', error);
    return res.status(500).json({ success: false, error: 'Failed to bulk delete contacts' });
  }
});

// Normalize phone number helper used in multiple routes
function normalizePhoneNumber(phone) {
  if (!phone || typeof phone !== 'string') return '';
  return phone.replace(/\D/g, '').replace(/^0+/, '');
}

// Sync helper: ensure campaign.contacts reflects group contacts for campaigns containing the group
async function syncCampaignContactsForGroup({ clientId, groupId, phonesAdded = [], phonesRemoved = [] }) {
  try {
    const campaigns = await Campaign.find({ clientId, groupIds: groupId });
    if (!campaigns || campaigns.length === 0) return;

    const normalizedAdded = Array.from(new Set(phonesAdded.map(p => normalizePhoneNumber(String(p || ''))).filter(Boolean)));
    const normalizedRemoved = Array.from(new Set(phonesRemoved.map(p => normalizePhoneNumber(String(p || ''))).filter(Boolean)));

    for (const campaign of campaigns) {
      // Handle additions: push missing contacts based on normalized phone
      if (normalizedAdded.length > 0) {
        const existingPhones = new Set(
          (campaign.contacts || []).map(c => normalizePhoneNumber(String(c && c.phone || ''))).filter(Boolean)
        );

        const toAdd = [];
        for (const norm of normalizedAdded) {
          if (!existingPhones.has(norm)) {
            // We only have normalized phone; store as digits; name/email left empty
            toAdd.push({ name: '', phone: norm, status: 'default', email: '', addedAt: new Date() });
          }
        }

        if (toAdd.length > 0) {
          await Campaign.updateOne(
            { _id: campaign._id },
            { $push: { contacts: { $each: toAdd } }, $set: { updatedAt: new Date() } }
          );
        }
      }

      // Handle removals: remove a phone only if it is not present in any other group of this campaign
      if (normalizedRemoved.length > 0) {
        if (Array.isArray(campaign.groupIds) && campaign.groupIds.length > 0) {
          // Load all groups that feed this campaign
          const groups = await Group.find({ _id: { $in: campaign.groupIds }, clientId });
          const phonesStillPresent = new Set();
          for (const g of groups) {
            const contacts = Array.isArray(g && g.contacts) ? g.contacts : [];
            for (const c of contacts) {
              const np = normalizePhoneNumber(String(c && c.phone || ''));
              if (np) phonesStillPresent.add(np);
            }
          }

          const toRemove = normalizedRemoved.filter(p => !phonesStillPresent.has(p));
          if (toRemove.length > 0) {
            await Campaign.updateOne(
              { _id: campaign._id },
              { $pull: { contacts: { phone: { $in: toRemove } } }, $set: { updatedAt: new Date() } }
            );
          }
        }
      }
    }
  } catch (e) {
    console.error('Error syncing campaign contacts for group:', e);
  }
}

// POST: Update contact status within groups linked to a campaign by phone
// Body: { campaignId, phone, status }
// NOTE: This route must be declared AFTER extractClientId middleware definition.
router.post('/groups/mark-contact-status', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { campaignId, phone, status } = req.body;

    const allowed = ['default', 'interested', 'maybe', 'not interested'];
    if (!allowed.includes(status)) {
      return res.status(400).json({ error: 'Invalid status value' });
    }

    if (!campaignId || !phone) {
      return res.status(400).json({ error: 'campaignId and phone are required' });
    }

    const campaign = await Campaign.findOne({ _id: campaignId, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    if (!Array.isArray(campaign.groupIds) || campaign.groupIds.length === 0) {
      return res.status(400).json({ error: 'Campaign has no groups' });
    }

    const normalizedTarget = normalizePhoneNumber(String(phone));
    if (!normalizedTarget) {
      return res.status(400).json({ error: 'Invalid phone' });
    }

    // Load groups for this client and campaign
    const groups = await Group.find({ _id: { $in: campaign.groupIds }, clientId: req.clientId });

    let updated = false;
    for (const group of groups) {
      if (!Array.isArray(group.contacts)) continue;
      for (const contact of group.contacts) {
        const normalized = normalizePhoneNumber(String(contact && contact.phone));
        if (normalized && normalized === normalizedTarget) {
          contact.status = status;
          updated = true;
          break;
        }
      }
      if (updated) {
        await group.save();
        break;
      }
    }

    if (!updated) {
      return res.status(404).json({ error: 'Contact not found in campaign groups' });
    }

    // Also update the status in campaign.contacts if present
    let campaignUpdated = false;
    if (Array.isArray(campaign.contacts) && campaign.contacts.length > 0) {
      for (const c of campaign.contacts) {
        const norm = normalizePhoneNumber(String(c && c.phone));
        if (norm && norm === normalizedTarget) {
          c.status = status;
          campaignUpdated = true;
          break;
        }
      }
      if (campaignUpdated) {
        await campaign.save();
      }
    }

    return res.json({ success: true });
  } catch (err) {
    console.error('Error marking contact status:', err);
    return res.status(500).json({ error: 'Failed to mark contact status' });
  }
});

// ==================== CAMPAIGNS API ====================

// Get all campaigns for client
router.get('/campaigns', extractClientId, async (req, res) => {
  try {
    // Exclude heavy/sensitive fields from list response
    const campaigns = await Campaign.find({ clientId: req.clientId })
      .select('-details -uniqueIds -contacts')
      .populate('groupIds', 'name description')
      .sort({ createdAt: -1 });

    res.json({ success: true, data: campaigns });
  } catch (error) {
    console.error('Error fetching campaigns:', error);
    res.status(500).json({ error: 'Failed to fetch campaigns' });
  }
});

// Create new campaign
router.post('/campaigns', extractClientId, async (req, res) => {
  try {
    const { name, description, groupIds, category, agent, isRunning } = req.body;
    
    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Campaign name is required' });
    }

    // Check for duplicate campaign name within the same client
    const campaignNameMatch = await Campaign.findOne({
      name: { $regex: name.trim(), $options: 'i' },
      clientId: req.clientId
    });
    if (campaignNameMatch) {
      return res.status(400).json({ error: 'Campaign name already exists' });
    }

    let agentArray = [];
    if (Array.isArray(agent)) {
      agentArray = agent
        .filter((v) => typeof v === 'string')
        .map((v) => v.trim())
        .filter(Boolean);
    } else if (typeof agent === 'string') {
      const val = agent.trim();
      agentArray = val ? [val] : [];
    }

    const campaign = new Campaign({
      name: name.trim(),
      description: description?.trim() || '',
      groupIds: groupIds || [],
      clientId: req.clientId,
      category: category?.trim() || '',
      agent: agentArray,
      isRunning: isRunning || false
    });

    await campaign.save();
    res.status(201).json({ success: true, data: campaign });
  } catch (error) {
    console.error('Error creating campaign:', error);
    res.status(500).json({ error: 'Failed to create campaign' });
  }
});

// Get single campaign by ID
router.get('/campaigns/:id', extractClientId, async (req, res) => {
  try {
    const campaign = await Campaign.findOne({ _id: req.params.id, clientId: req.clientId })
      .populate('groupIds', 'name description contacts')
      .select('-details -uniqueIds -contacts');
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    res.json({ success: true, data: campaign });
  } catch (error) {
    console.error('Error fetching campaign:', error);
    res.status(500).json({ error: 'Failed to fetch campaign' });
  }
});

// Update campaign
router.put('/campaigns/:id', extractClientId, async (req, res) => {
  try {
    const { name, description, groupIds, category, agent, isRunning } = req.body;

    // Check for duplicate campaign name within the same client (excluding current campaign)
    if (name && name.trim()) {
      const campaignNameMatch = await Campaign.findOne({
        name: { $regex: name.trim(), $options: 'i' },
        clientId: req.clientId,
        _id: { $ne: req.params.id }
      });
      if (campaignNameMatch) {
        return res.status(400).json({ error: 'Campaign name already exists' });
      }
    }

    const updateData = {
      updatedAt: new Date()
    };

    if (name !== undefined && name.trim()) {
      updateData.name = name.trim();
    }

    if (description !== undefined) {
      updateData.description = description?.trim() || '';
    }

    if (groupIds !== undefined) {
      updateData.groupIds = groupIds;
    }

    if (category !== undefined) {
      updateData.category = category?.trim() || '';
    }

    if (agent !== undefined) {
      if (Array.isArray(agent)) {
        updateData.agent = agent
          .filter((v) => typeof v === 'string')
          .map((v) => v.trim())
          .filter(Boolean);
      } else if (typeof agent === 'string') {
        const val = agent.trim();
        updateData.agent = val ? [val] : [];
      } else {
        updateData.agent = [];
      }
    }

    if (isRunning !== undefined) {
      updateData.isRunning = isRunning;
    }

    const campaign = await Campaign.findOneAndUpdate(
      { _id: req.params.id, clientId: req.clientId },
      updateData,
      { new: true }
    ).populate('groupIds', 'name description');

    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    res.json({ success: true, data: campaign });
  } catch (error) {
    console.error('Error updating campaign:', error);
    res.status(500).json({ error: 'Failed to update campaign' });
  }
});

// Delete campaign
router.delete('/campaigns/:id', extractClientId, async (req, res) => {
  try {
    const campaign = await Campaign.findOneAndDelete({ _id: req.params.id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    res.json({ success: true, message: 'Campaign deleted successfully' });
  } catch (error) {
    console.error('Error deleting campaign:', error);
    res.status(500).json({ error: 'Failed to delete campaign' });
  }
});

// Add groups to campaign
router.post('/campaigns/:id/groups', extractClientId, async (req, res) => {
  try {
    const { groupIds } = req.body;
    
    if (!groupIds || !Array.isArray(groupIds)) {
      return res.status(400).json({ error: 'groupIds array is required' });
    }

    const campaign = await Campaign.findOne({ _id: req.params.id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    // Verify all groups belong to the client
    const groups = await Group.find({ _id: { $in: groupIds }, clientId: req.clientId });
    if (groups.length !== groupIds.length) {
      return res.status(400).json({ error: 'Some groups not found or don\'t belong to client' });
    }

    campaign.groupIds = groupIds;
    await campaign.save();

    const updatedCampaign = await Campaign.findById(campaign._id)
      .populate('groupIds', 'name description');

    res.json({ success: true, data: updatedCampaign });
  } catch (error) {
    console.error('Error adding groups to campaign:', error);
    res.status(500).json({ error: 'Failed to add groups to campaign' });
  }
});

// Add subset of a group's contacts to a campaign by index range
  // Also persist the selection (with group name) in campaign.groupSelections
router.post('/campaigns/:id/groups/:groupId/contacts-range', extractClientId, async (req, res) => {
  try {
    const { id, groupId } = req.params;
    let { startIndex, endIndex, replace, selectedIndices } = req.body;

    // Ensure mongoose is available for ObjectId generation
    const mongoose = require('mongoose');

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    const group = await Group.findOne({ _id: groupId, clientId: req.clientId });
    if (!group) {
      return res.status(404).json({ success: false, error: 'Group not found' });
    }

    const total = Array.isArray(group.contacts) ? group.contacts.length : 0;
    if (total === 0) {
      return res.json({ success: true, data: { added: 0, totalCampaignContacts: campaign.contacts?.length || 0 }, message: 'No contacts in group' });
    }

    // Normalize range
    startIndex = Number.isInteger(startIndex) ? startIndex : 0;
    endIndex = Number.isInteger(endIndex) ? endIndex : total; // exclusive
    if (startIndex < 0) startIndex = 0;
    if (endIndex > total) endIndex = total;
    if (endIndex < startIndex) [startIndex, endIndex] = [startIndex, startIndex];

    let slice;
    if (Array.isArray(selectedIndices) && selectedIndices.length > 0) {
      // Use explicit indices (within 0..total-1)
      const validSet = new Set();
      for (const idx of selectedIndices) {
        const i = Number(idx);
        if (Number.isInteger(i) && i >= 0 && i < total) validSet.add(i);
      }
      slice = Array.from(validSet)
        .sort((a,b)=>a-b)
        .map(i => group.contacts[i]);
    } else {
      // Fallback to range selection
      slice = group.contacts.slice(startIndex, endIndex);
    }

    // Guard against sparse/null entries producing undefined contacts
    slice = (Array.isArray(slice) ? slice : []).filter(c => c && c.phone);

    // Helper to normalize phone
    const normalizePhoneNumber = (phoneNumber) => {
      if (!phoneNumber) return '';
      let normalized = phoneNumber.toString().trim();
      normalized = normalized.replace(/[\s\-\.\(\)]/g, '');
      normalized = normalized.replace(/^\+\d{1,3}/, '');
      normalized = normalized.replace(/^0+/, '');
      if (normalized.startsWith('91') && normalized.length > 10) normalized = normalized.substring(2);
      if (normalized.startsWith('1') && normalized.length > 10) normalized = normalized.substring(1);
      return normalized;
    };

    // Build a set of this group's phone numbers to support group-specific replace behavior
    const groupPhoneSet = new Set(
      (group.contacts || [])
        .map((gc) => normalizePhoneNumber(gc && gc.phone))
        .filter(Boolean)
    );

    // Ensure campaign.contacts is an array before manipulating
    campaign.contacts = Array.isArray(campaign.contacts) ? campaign.contacts : [];

    // If replace mode is enabled, remove only contacts that belong to THIS group (by matching phone numbers)
    if (replace && groupPhoneSet.size > 0) {
      campaign.contacts = campaign.contacts.filter((c) => {
        const normalized = normalizePhoneNumber(c && c.phone);
        return !normalized || !groupPhoneSet.has(normalized);
      });
    }

    // Prepare existingPhones for dedupe: use remaining campaign contacts after potential group-specific removal
    const existingPhones = new Set(
      (campaign.contacts || []).map((c) => normalizePhoneNumber(c && c.phone)).filter(Boolean)
    );

    let added = 0;
    const newContacts = [];
    for (const contact of slice) {
      if (!contact || !contact.phone) continue;
      const phoneNorm = normalizePhoneNumber(contact.phone);
      if (!phoneNorm || existingPhones.has(phoneNorm)) continue;
      existingPhones.add(phoneNorm);
      const rawName = contact.name && String(contact.name).trim() ? String(contact.name).trim() : '';
      const looksLikeNumber = /^\+?\d[\d\s-]*$/.test(rawName);
      const safeName = (!rawName || looksLikeNumber) ? '' : rawName;
      newContacts.push({
        _id: new mongoose.Types.ObjectId(),
        name: safeName,
        phone: contact.phone,
        email: contact.email || '',
        addedAt: new Date()
      });
      added += 1;
    }

    // Always append newContacts; if replace was requested, we already removed this group's previous contacts above
    campaign.contacts = Array.isArray(campaign.contacts) ? campaign.contacts : [];
    campaign.contacts.push(...newContacts);

    // Persist selection metadata for visibility across app
    try {
      const groupName = group?.name || '';
      const count = newContacts.length;
      const sel = {
        groupId: group._id,
        groupName,
        startIndex,
        endIndex,
        selectedIndices: Array.isArray(selectedIndices) ? [...new Set(selectedIndices.filter((x)=>Number.isInteger(Number(x))).map((x)=>Number(x)).filter((i)=>i>=0 && i<total))] : [],
        count,
        replace: !!replace,
        addedAt: new Date()
      };
      campaign.groupSelections = Array.isArray(campaign.groupSelections) ? campaign.groupSelections : [];
      const idx = campaign.groupSelections.findIndex(gs => String(gs.groupId) === String(group._id));
      if (idx >= 0) {
        campaign.groupSelections[idx] = sel;
      } else {
        campaign.groupSelections.push(sel);
      }
    } catch (_) {}

    await campaign.save();

    // Also persist the human-friendly range into CampaignHistory.selectedRanges for this run if possible
    try {
      const runId = req.body.runId || (campaign.details && campaign.details[0] && campaign.details[0].runId) || null;
      if (runId) {
        const CampaignHistory = require('../models/CampaignHistory');
        const Group = require('../models/Group');
        const grp = await Group.findOne({ _id: groupId }).lean();
        const friendly = {
          groupId: String(groupId),
          groupName: grp?.name || '',
          start: startIndex + 1, // store 1-based inclusive
          end: endIndex,         // endIndex is exclusive → already human inclusive when used as label
          selectedAt: new Date()
        };
        await CampaignHistory.updateOne(
          { runId },
          { $push: { selectedRanges: friendly } },
          { upsert: true }
        );
      }
    } catch (e) {
      console.warn('Warning: failed to persist selected range to history:', e?.message);
    }

    return res.json({
      success: true,
      data: {
        added,
        totalSelected: slice.length,
        range: { startIndex, endIndex },
        usedSelectedIndices: Array.isArray(selectedIndices) ? selectedIndices : undefined,
        totalCampaignContacts: campaign.contacts.length,
        groupSelections: campaign.groupSelections
      },
      message: `Added ${added} contacts to campaign from range ${startIndex}-${endIndex}${replace ? ' (replaced existing contacts)' : ''}`
    });
  } catch (error) {
    console.error('Error adding contacts by range:', error);
    res.status(500).json({ success: false, error: 'Failed to add contacts by range' });
  }
});

//Delete group from campaign
router.delete('/campaigns/:id/groups/:groupId', extractClientId, async (req, res) => {
  try {
    const { id, groupId } = req.params;
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }
    campaign.groupIds = campaign.groupIds.filter((id) => id.toString() !== groupId);
    await campaign.save();
    res.json({ success: true, message: 'Group deleted from campaign' });
  }catch (error) {
    console.error('Error deleting group from campaign:', error);
    res.status(500).json({ error: 'Failed to delete group from campaign' });
  }
})

// Get all groups associated with a campaign
router.get('/campaigns/:id/groups', extractClientId, async (req, res) => {
  try {
    // Find the campaign and verify it belongs to the client
    const campaign = await Campaign.findOne({ _id: req.params.id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    // If no groups are associated with the campaign
    if (!campaign.groupIds || campaign.groupIds.length === 0) {
      return res.json({ success: true, data: [], message: 'No groups associated with this campaign' });
    }

    // Fetch all groups associated with the campaign
    const groups = await Group.find({ 
      _id: { $in: campaign.groupIds }, 
      clientId: req.clientId 
    }).populate('contacts', 'name email phone');

    res.json({ 
      success: true, 
      campaignName: campaign.name,
      totalGroups: groups.length,
      data: groups,
      campaignId: campaign._id,
    });
  } catch (error) {
    console.error('Error fetching campaign groups:', error);
    res.status(500).json({ error: 'Failed to fetch campaign groups' });
  }
});
// Add unique ID to campaign (for tracking campaign calls)
router.post('/campaigns/:id/unique-ids', extractClientId, async (req, res) => {
  try {
    const { uniqueId, runId } = req.body;
    
    if (!uniqueId || typeof uniqueId !== 'string') {
      return res.status(400).json({ error: 'uniqueId is required and must be a string' });
    }

    const campaign = await Campaign.findOne({ _id: req.params.id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ error: 'Campaign not found' });
    }

    // Add unique ID to campaign details if it doesn't already exist
    const existingDetail = Array.isArray(campaign.details) && 
      campaign.details.find(detail => detail.uniqueId === uniqueId);
    
    if (!existingDetail) {
      const callDetail = {
        uniqueId: uniqueId,
        contactId: req.body.contactId || null,
        time: new Date(),
        status: 'ringing', // Start with 'ringing' status
        lastStatusUpdate: new Date(),
        callDuration: 0,
        ...(runId ? { runId } : {})
      };
      
      if (!Array.isArray(campaign.details)) {
        campaign.details = [];
      }
      
      campaign.details.push(callDetail);

      // Ensure uniqueIds array is maintained for efficient lookup
      if (!Array.isArray(campaign.uniqueIds)) {
        campaign.uniqueIds = [];
      }
      if (!campaign.uniqueIds.includes(uniqueId)) {
        campaign.uniqueIds.push(uniqueId);
      }

      await campaign.save();
      console.log(`✅ Added unique ID ${uniqueId} to campaign ${campaign._id} with contactId: ${req.body.contactId || 'null'}`);
    }

    res.json({ 
      success: true, 
      message: 'Unique ID added to campaign',
      data: { 
        uniqueId, 
        totalDetails: Array.isArray(campaign.details) ? campaign.details.length : 0 
      }
    });
  } catch (error) {
    console.error('Error adding unique ID to campaign:', error);
    res.status(500).json({ error: 'Failed to add unique ID to campaign' });
  }
});

// Get call logs for a campaign using stored uniqueIds
router.get('/campaigns/:id/call-logs-dashboard', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const page = parseInt(req.query.page || '1', 10);
    const limit = parseInt(req.query.limit || '50', 10);
    const sortBy = req.query.sortBy || 'createdAt';
    const sortOrder = (req.query.sortOrder || 'desc').toLowerCase() === 'asc' ? 1 : -1;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // If a specific documentId is provided, return only its logs (convenience path)
    const documentId = req.query.documentId;
    if (documentId) {
      const idsFromArray = Array.isArray(campaign.uniqueIds) ? campaign.uniqueIds.filter(Boolean) : [];
      const idsFromDetails = Array.isArray(campaign.details)
        ? campaign.details.map(d => d && d.uniqueId).filter(Boolean)
        : [];
      const knownIds = new Set([...idsFromArray, ...idsFromDetails]);
      if (!knownIds.has(documentId)) {
        return res.status(404).json({ success: false, error: 'Document ID not found in this campaign' });
      }

      let logsByDoc = await CallLog.find({
        clientId: req.clientId,
        campaignId: campaign._id,
        'metadata.customParams.uniqueid': documentId
      })
        .sort({ createdAt: sortOrder })
        .populate('campaignId', 'name description')
        .populate('agentId', 'agentName')
        .lean();

      // Replace with backend proxy URL to avoid S3 CORS/signature issues
      try {
        const base = `${req.protocol}://${req.get('host')}`;
        logsByDoc = (logsByDoc || []).map((l) => {
          const uid = l?.metadata?.customParams?.uniqueid;
          if (uid) {
            l.audioUrl = `${base}/api/v1/client/campaigns/${campaign._id}/call-audio?documentId=${encodeURIComponent(uid)}`;
          }
          return l;
        });
      } catch (_) {}

      return res.json({
        success: true,
        data: logsByDoc,
        campaign: { _id: campaign._id, name: campaign.name },
        documentId
      });
    }

    const idsFromArray = Array.isArray(campaign.uniqueIds) ? campaign.uniqueIds.filter(Boolean) : [];
    const idsFromDetails = Array.isArray(campaign.details)
      ? campaign.details.map(d => d && d.uniqueId).filter(Boolean)
      : [];
    const uniqueIds = Array.from(new Set([...idsFromArray, ...idsFromDetails]));
    if (uniqueIds.length === 0) {
      return res.json({
        success: true,
        data: [],
        campaign: { _id: campaign._id, name: campaign.name, uniqueIdsCount: 0 },
        pagination: { currentPage: page, totalPages: 0, totalLogs: 0, hasNextPage: false, hasPrevPage: false }
      });
    }

    const query = {
      clientId: req.clientId,
      'metadata.customParams.uniqueid': { $in: uniqueIds }
    };

    const totalLogs = await CallLog.countDocuments(query);
    const skip = (page - 1) * limit;
    const sortSpec = { [sortBy]: sortOrder };

    const logs = await CallLog.find(query)
      .sort(sortSpec)
      .skip(skip)
      .limit(limit)
      .populate('campaignId', 'name description')
      .populate('agentId', 'agentName')
      .lean();

    // Build placeholders for uniqueIds without logs
    const loggedUniqueIds = new Set(
      (logs || [])
        .map(l => l && l.metadata && l.metadata.customParams && l.metadata.customParams.uniqueid)
        .filter(Boolean)
    );
    const missingUniqueIds = uniqueIds.filter(uid => !loggedUniqueIds.has(uid));

    const placeholderLogs = missingUniqueIds.map(uid => ({
      _id: new mongoose.Types.ObjectId(),
      clientId: req.clientId,
      campaignId: { _id: campaign._id, name: campaign.name },
      agentId: null,
      mobile: null,
      duration: 0,
      callType: 'outbound',
      leadStatus: 'not_connected',
      statusText: 'Not Accepted / Busy / Disconnected',
      createdAt: null,
      time: null,
      metadata: { customParams: { uniqueid: uid }, isActive: false }
    }));

    let allLogs = [...logs, ...placeholderLogs];

    // Use backend proxy URL to avoid S3 CORS/signature issues
    try {
      const base = `${req.protocol}://${req.get('host')}`;
      allLogs = (allLogs || []).map((l) => {
        const uid = l?.metadata?.customParams?.uniqueid;
        if (uid) {
          l.audioUrl = `${base}/api/v1/client/campaigns/${campaign._id}/call-audio?documentId=${encodeURIComponent(uid)}`;
        }
        return l;
      });
    } catch (_) {}

    return res.json({
      success: true,
      data: allLogs,
      campaign: {
        _id: campaign._id,
        name: campaign.name,
        uniqueIdsCount: uniqueIds.length,
        missingUniqueIdsCount: missingUniqueIds.length
      },
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(totalLogs / limit),
        totalLogs,
        hasNextPage: skip + logs.length < totalLogs,
        hasPrevPage: page > 1
      }
    });
  } catch (error) {
    console.error('Error fetching campaign call logs:', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch campaign call logs' });
  }
});

// GET campaign contacts
router.get('/campaigns/:id/contacts', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    res.json({
      success: true,
      data: campaign.contacts || []
    });
  } catch (error) {
    console.error('Error fetching campaign contacts:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch campaign contacts' });
  }
});

// POST new contact to campaign
router.post('/campaigns/:id/contacts', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const { name, phone, email } = req.body;

    if (!name || !phone) {
      return res.status(400).json({ success: false, error: 'Name and phone are required' });
    }

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Check if phone number already exists in campaign contacts
    const existingContact = campaign.contacts.find(contact => contact.phone === phone);
    if (existingContact) {
      return res.status(400).json({ success: false, error: 'Phone number already exists in this campaign' });
    }

    // Add new contact (MongoDB will auto-generate _id)
    campaign.contacts.push({
      _id: new mongoose.Types.ObjectId(),
      name,
      phone,
      email: email || '',
      bookmarked: false,
      addedAt: new Date()
    });

    await campaign.save();

    res.json({
      success: true,
      data: campaign.contacts[campaign.contacts.length - 1],
      message: 'Contact added successfully'
    });
  } catch (error) {
    console.error('Error adding contact to campaign:', error);
    res.status(500).json({ success: false, error: 'Failed to add contact to campaign' });
  }
});

// DELETE campaign contact
router.delete('/campaigns/:id/contacts/:contactId', extractClientId, async (req, res) => {
  try {
    const { id, contactId } = req.params;
    
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Remove the contact using ObjectId
    const initialLength = campaign.contacts.length;
    campaign.contacts = campaign.contacts.filter(contact => 
      contact._id.toString() !== contactId
    );
    
    if (campaign.contacts.length === initialLength) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    await campaign.save();

    res.json({
      success: true,
      message: 'Contact removed successfully'
    });
  } catch (error) {
    console.error('Error removing campaign contact:', error);
    res.status(500).json({ success: false, error: 'Failed to remove campaign contact' });
  }
});

// PATCH toggle bookmark for a campaign contact
router.patch('/campaigns/:id/contacts/:contactId/bookmark', extractClientId, async (req, res) => {
  try {
    const { id, contactId } = req.params;
    const { bookmarked } = req.body;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    const idx = (campaign.contacts || []).findIndex(c => c._id && c._id.toString() === contactId);
    if (idx === -1) {
      return res.status(404).json({ success: false, error: 'Contact not found' });
    }

    const value = Boolean(bookmarked);
    campaign.contacts[idx].bookmarked = value;
    await campaign.save();

    return res.json({ success: true, data: campaign.contacts[idx] });
  } catch (error) {
    console.error('Error toggling bookmark:', error);
    return res.status(500).json({ success: false, error: 'Failed to toggle bookmark' });
  }
});

// PATCH toggle bookmark for a campaign contact by phone (digits)
router.patch('/campaigns/:id/contacts/bookmark-by-phone', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    let { phone, bookmarked } = req.body;
    if (!phone) return res.status(400).json({ success: false, error: 'phone is required' });
    const digits = String(phone).replace(/\D/g, '');
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }
    const idx = (campaign.contacts || []).findIndex(c => String(c.phone).replace(/\D/g, '') === digits);
    if (idx === -1) {
      return res.status(404).json({ success: false, error: 'Contact not found for phone' });
    }
    campaign.contacts[idx].bookmarked = Boolean(bookmarked);
    await campaign.save();
    return res.json({ success: true, data: campaign.contacts[idx] });
  } catch (error) {
    console.error('Error toggling bookmark by phone:', error);
    return res.status(500).json({ success: false, error: 'Failed to toggle bookmark by phone' });
  }
});

// POST sync contacts from groups
router.post('/campaigns/:id/sync-contacts', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    if (!campaign.groupIds || campaign.groupIds.length === 0) {
      // No groups remain: clear campaign contacts to prevent stale entries
      const removedCount = Array.isArray(campaign.contacts) ? campaign.contacts.length : 0;
      campaign.contacts = [];
      await campaign.save();
      return res.json({
        success: true,
        data: {
          totalContacts: 0,
          totalGroups: 0,
          newContactsAdded: 0,
          contactsRemoved: removedCount,
          totalContactsInCampaign: 0
        },
        message: `Cleared ${removedCount} contacts because campaign has no groups`
      });
    }

    // Local helper: normalize phone numbers for consistent comparison
    const normalizePhoneNumber = (phoneNumber) => {
      if (!phoneNumber) return '';
      let normalized = phoneNumber.toString().trim();
      normalized = normalized.replace(/[\s\-\.\(\)]/g, '');
      normalized = normalized.replace(/^\+\d{1,3}/, '');
      normalized = normalized.replace(/^0+/, '');
      if (normalized.startsWith('91') && normalized.length > 10) {
        normalized = normalized.substring(2);
      }
      if (normalized.startsWith('1') && normalized.length > 10) {
        normalized = normalized.substring(1);
      }
      return normalized;
    };

    // Fetch all groups and their contacts in the same order as campaign.groupIds
    const groupsFromDb = await Group.find({ _id: { $in: campaign.groupIds } });
    const groups = campaign.groupIds
      .map(id => groupsFromDb.find(g => String(g._id) === String(id)))
      .filter(Boolean);
    // Ensure contacts array exists to avoid runtime errors
    if (!Array.isArray(campaign.contacts)) {
      campaign.contacts = [];
    }
    
    let totalContacts = 0;
    let totalGroups = groups.length;
    const newContacts = [];
    const contactsToRemove = [];

    // Collect all valid (normalized) phone numbers from groups
    const validPhoneNumbers = new Set();
    
    for (const group of groups) {
      if (group && Array.isArray(group.contacts) && group.contacts.length > 0) {
        for (const groupContact of group.contacts) {
          const normalizedGroupPhone = normalizePhoneNumber(groupContact && groupContact.phone);
          // Skip invalid/empty phone numbers
          if (!normalizedGroupPhone) continue;

          validPhoneNumbers.add(normalizedGroupPhone);
          
          // Check if phone number already exists in campaign contacts (normalized compare)
          const existingContact = campaign.contacts.find(contact => {
            const normalizedExisting = normalizePhoneNumber(contact && contact.phone);
            return normalizedExisting && normalizedExisting === normalizedGroupPhone;
          });
          if (!existingContact) {
            const safeName = (groupContact && typeof groupContact.name === 'string' && groupContact.name.trim())
              ? groupContact.name.trim()
              : (groupContact && groupContact.phone) || '';
            newContacts.push({
              _id: new mongoose.Types.ObjectId(),
              name: safeName,
              phone: groupContact.phone,
              email: groupContact.email || '',
              addedAt: new Date()
            });
            totalContacts++;
          }
        }
      }
    }

    // Find contacts to remove (contacts that are no longer in any group)
    for (const campaignContact of campaign.contacts) {
      const normalizedCampaignPhone = normalizePhoneNumber(campaignContact && campaignContact.phone);
      if (normalizedCampaignPhone && !validPhoneNumbers.has(normalizedCampaignPhone)) {
        contactsToRemove.push(campaignContact);
      }
    }

    // Remove contacts that are no longer in groups (compare by normalized phone)
    if (contactsToRemove.length > 0) {
      const phonesToRemove = new Set(
        contactsToRemove.map(c => normalizePhoneNumber(c.phone)).filter(Boolean)
      );
      campaign.contacts = (campaign.contacts || []).filter(contact => {
        const normalized = normalizePhoneNumber(contact && contact.phone);
        return !normalized || !phonesToRemove.has(normalized);
      });
    }

    // Add new contacts to campaign
    if (newContacts.length > 0) {
      campaign.contacts.push(...newContacts);
    }

    // Save campaign even if there are no changes to ensure consistency with zero-group clearing etc.
    await campaign.save();

    res.json({
      success: true,
      data: {
        totalContacts: totalContacts,
        totalGroups: totalGroups,
        newContactsAdded: newContacts.length,
        contactsRemoved: contactsToRemove.length,
        totalContactsInCampaign: campaign.contacts.length
      },
      message: `Synced ${newContacts.length} new contacts and removed ${contactsToRemove.length} contacts from ${totalGroups} groups`
    });
  } catch (error) {
    console.error('Error syncing contacts from groups:', error);
    res.status(500).json({ success: false, error: 'Failed to sync contacts from groups' });
  }
});

// Get minimal leads list for a campaign: documentId, number, name, leadStatus
router.get('/campaigns/:id/leads', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const page = parseInt(req.query.page || '1', 10);
    const limit = parseInt(req.query.limit || '50', 10);

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Use the new details structure instead of uniqueIds
    const details = Array.isArray(campaign.details) ? campaign.details.filter(Boolean) : [];
    const totalItems = details.length;

    if (totalItems === 0) {
      return res.json({
        success: true,
        data: [],
        campaign: { _id: campaign._id, name: campaign.name, detailsCount: 0 },
        pagination: { currentPage: page, totalPages: 0, totalItems: 0, hasNextPage: false, hasPrevPage: false }
      });
    }

    const skip = (page - 1) * limit;
    const pagedDetails = details.slice(skip, skip + limit);

    // Extract uniqueIds from the paged details
    const pagedUniqueIds = pagedDetails.map(detail => detail.uniqueId);

    // Fetch logs for the paged uniqueIds and map latest log per id
    const logs = await CallLog.find({
      clientId: req.clientId,
      'metadata.customParams.uniqueid': { $in: pagedUniqueIds }
    })
      .sort({ createdAt: -1 })
      .lean();

    const latestLogByUid = new Map();
    for (const log of logs) {
      const uid = log && log.metadata && log.metadata.customParams && log.metadata.customParams.uniqueid;
      if (uid && !latestLogByUid.has(uid)) {
        latestLogByUid.set(uid, log);
      }
    }

    const minimal = pagedDetails.map(detail => {
      const log = latestLogByUid.get(detail.uniqueId);
      const name = log && (log.contactName || (log.metadata && log.metadata.customParams && log.metadata.customParams.name));
      const number = log && (log.mobile || (log.metadata && log.metadata.callerId));
      const leadStatus = (log && log.leadStatus) || 'not_connected';
      const duration = (log && typeof log.duration === 'number') ? log.duration : 0;
      return {
        documentId: detail.uniqueId,
        number: number || null,
        name: name || null,
        leadStatus,
        contactId: detail.contactId,
        time: detail.time,
        status: detail.status,
        duration
      };
    });

    return res.json({
      success: true,
      data: minimal,
      campaign: {
        _id: campaign._id,
        name: campaign.name,
        detailsCount: totalItems
      },
      pagination: {
        currentPage: page,
        totalPages: Math.ceil(totalItems / limit),
        totalItems,
        hasNextPage: skip + pagedUniqueIds.length < totalItems,
        hasPrevPage: page > 1
      }
    });
  } catch (error) {
    console.error('Error fetching minimal leads list:', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch minimal leads list' });
  }
});
// Get merged call logs (completed + missed calls) with deduplication
router.get('/campaigns/:id/merged-calls', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const page = parseInt(req.query.page || '1', 10);
    const limit = parseInt(req.query.limit || '50', 10);
    const runId = req.query.runId || null;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      console.log('Campaign not found');
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // 1. Get all campaign details (uniqueIds)
    let details = Array.isArray(campaign.details) ? campaign.details.filter(Boolean) : [];
    
    // Filter by runId if provided
    if (runId) {
      details = details.filter(detail => detail.runId === runId);
    }
    
    const totalDetails = details.length;


    if (totalDetails === 0) {
      return res.json({
        success: true,
        data: [],
        campaign: { _id: campaign._id, name: campaign.name, detailsCount: 0 },
        pagination: { currentPage: page, totalPages: 0, totalItems: 0, hasNextPage: false, hasPrevPage: false }
      });
    }

    // 2. Get all uniqueIds
    const allUniqueIds = details.map(d => d.uniqueId).filter(Boolean);

    // Build a contacts map for O(1) lookups
    const contactsMap = new Map((campaign.contacts || []).map(c => [String(c._id), c]));

    // 3. Aggregation to fetch latest log per uniqueId, compute duration, isOngoing,
    //    and sum transcript counts across all logs (fallback to transcript array size)
    const agg = await CallLog.aggregate([
      { $match: { clientId: req.clientId, 'metadata.customParams.uniqueid': { $in: allUniqueIds } } },
      { $sort: { createdAt: -1 } },
      { $group: {
          _id: '$metadata.customParams.uniqueid',
          latest: { $first: '$$ROOT' },
          maxExplicitDuration: { $max: { $ifNull: ['$duration', 0] } },
          earliestCreatedAt: { $last: '$createdAt' },
          lastTimestamp: { $first: { $ifNull: ['$metadata.callEndTime', { $ifNull: ['$updatedAt', { $ifNull: ['$time', '$createdAt'] }] }] } },
          transcriptSum: { $sum: { $add: [
            { $ifNull: ['$metadata.userTranscriptCount', 0] },
            { $ifNull: ['$metadata.aiResponseCount', 0] },
            { $cond: [ { $isArray: '$transcript' }, { $size: '$transcript' }, 0 ] }
          ] } }
        }
      },
      { $project: {
          _id: 0,
          uniqueid: '$_id',
          latest: {
            _id: '$latest._id',
            createdAt: '$latest.createdAt',
            updatedAt: '$latest.updatedAt',
            time: '$latest.time',
            duration: '$latest.duration',
            audioUrl: '$latest.audioUrl',
            status: '$latest.status',
            leadStatus: '$latest.leadStatus',
            mobile: '$latest.mobile',
            metadata: {
              isActive: '$latest.metadata.isActive',
              callerId: '$latest.metadata.callerId',
              userTranscriptCount: { $ifNull: ['$latest.metadata.userTranscriptCount', 0] },
              aiResponseCount: { $ifNull: ['$latest.metadata.aiResponseCount', 0] },
              customParams: {
                uniqueid: '$_id',
                contact_name: '$latest.metadata.customParams.contact_name',
                name: '$latest.metadata.customParams.name',
                whatsappMessageSent: '$latest.metadata.customParams.whatsappMessageSent',
                whatsappRequested: '$latest.metadata.customParams.whatsappRequested'
              }
            },
            contactName: '$latest.contactName'
          },
          computedDerivedDuration: {
            $toInt: {
              $max: [
                0,
                { $divide: [ { $subtract: [ { $toDate: '$lastTimestamp' }, { $toDate: '$earliestCreatedAt' } ] }, 1000 ] }
              ]
            }
          },
          maxExplicitDuration: 1,
          transcriptSum: 1
        }
      },
      { $addFields: {
          duration: { $max: ['$maxExplicitDuration', '$computedDerivedDuration'] },
          isOngoing: {
            $or: [
              { $eq: ['$latest.metadata.isActive', true] },
              { $in: ['$latest.status', ['ongoing', 'ringing']] },
              { $eq: ['$latest.leadStatus', 'maybe'] }
            ]
          },
          transcriptCount: { $max: [ '$transcriptSum', { $add: ['$latest.metadata.userTranscriptCount', '$latest.metadata.aiResponseCount'] } ] }
        }
      }
    ], { allowDiskUse: true });

    const uniqueIdToAgg = new Map(agg.map(a => [a.uniqueid, a]));

    // Helper: sanitize display name to avoid numbers posing as names
    const sanitizeName = (raw, fallbackNumber) => {
      try {
        const name = (raw || '').toString().trim();
        if (!name) return '';
        const digits = name.replace(/\D/g, '');
        const phoneDigits = (fallbackNumber || '').toString().replace(/\D/g, '');
        const numberLike = digits.length >= 6 && /^\d+$/.test(digits);
        const sameAsPhone = phoneDigits && digits === phoneDigits;
        return (!numberLike && !sameAsPhone) ? name : '';
      } catch (_) {
        return '';
      }
    };

    // 5. Build merged list with deduplication

    const mergedCalls = [];
    const processedUniqueIds = new Set();

    // First, add completed calls (these have priority)
    for (const detail of details) {
      const uniqueId = detail.uniqueId;
      if (!uniqueId || processedUniqueIds.has(uniqueId)) continue;

      const aggEntry = uniqueIdToAgg.get(uniqueId);
      if (aggEntry && aggEntry.latest) {
        const log = aggEntry.latest;
        // SIMPLE RULES:
        // - If there is a CallLog: status from metadata.isActive (true -> ongoing, else completed)
        // - Missed is only decided in the no-logs branch below
        const isOngoingFlag = !!aggEntry.isOngoing;
        // Show 'ongoin' while the call is actively in progress
        let callStatus = isOngoingFlag ? 'ongoing' : 'completed';
        
        // Use precomputed duration from aggregation
        const computedDuration = typeof aggEntry.duration === 'number' ? aggEntry.duration : (typeof log.duration === 'number' ? log.duration : 0);

        // Build a clean display name
        const fallbackNumber = log.mobile || log.metadata?.callerId || null;
        const displayName = sanitizeName(
          log.contactName ||
          log.metadata?.customParams?.contact_name ||
          log.metadata?.customParams?.name,
          fallbackNumber
        ) || null;

        // Compute transcript count from metadata if available
        const transcriptCount = typeof aggEntry.transcriptCount === 'number' ? aggEntry.transcriptCount : 0;

        // This is either a completed call or an ongoing call
        mergedCalls.push({
          documentId: uniqueId,
          number: log.mobile || log.metadata?.callerId || null,
          name: displayName,
          leadStatus: log.leadStatus || 'not_connected',
          contactId: detail.contactId,
          time: detail.time || detail.createdAt,
          status: callStatus,
          duration: computedDuration,
          audioUrl: log.audioUrl || null,
          isMissed: false,
          isOngoing: isOngoingFlag,
          transcriptCount,
          whatsappMessageSent: log.metadata?.customParams?.whatsappMessageSent ||
                            log.metadata?.whatsappMessageSent ||
                            detail.whatsappMessageSent || 
                            false ,
          whatsappRequested: log.metadata?.customParams?.whatsappRequested ||
                            log.metadata?.whatsappRequested ||
                            detail.whatsappRequested || 
                            false 
        });
        
        
        processedUniqueIds.add(uniqueId);
      }
    }

    // Then, add items with no logs yet (derive status/duration from detail fields)
    for (const detail of details) {
      const uniqueId = detail.uniqueId;
      if (!uniqueId || processedUniqueIds.has(uniqueId)) continue;

      // No CallLog found for this uniqueId yet – fall back to campaign.details status
      const contact = contactsMap.get(String(detail.contactId));
      // Default: ringing immediately after initiation until timeout threshold
      let fallbackStatus = 'ringing';

      // If there are no logs for this uniqueId then:
      // - if age < 40s => ringing
      // - else => missed
      try {
        const startTs = detail.time || detail.createdAt;
        const startMs = startTs ? new Date(startTs).getTime() : 0;
        const ageSec = startMs ? Math.round((Date.now() - startMs) / 1000) : 0;
        fallbackStatus = ageSec >= 40 ? 'missed' : 'ringing';
      } catch (_) {}
      const isMissedDerived = fallbackStatus === 'missed';
      // Compute a best-effort duration from detail if available
      let computedDetailDuration = (() => {
        try {
          if (typeof detail.callDuration === 'number' && detail.callDuration > 0) {
            return detail.callDuration;
          }
          const endTs = detail.lastStatusUpdate || detail.updatedAt || detail.time;
          const startTs = detail.time || detail.createdAt;
          const endMs = endTs ? new Date(endTs).getTime() : 0;
          const startMs = startTs ? new Date(startTs).getTime() : 0;
          const diffSec = Math.max(0, Math.round((endMs - startMs) / 1000));
          return Number.isFinite(diffSec) ? diffSec : 0;
        } catch (_) {
          return 0;
        }
      })();
      // No extra DB lookup here; keep computedDetailDuration as-is to avoid N+1 queries
      // Build a clean display name from contact when logs are absent
      const fallbackNumber2 = contact?.phone || null;
      const displayName2 = sanitizeName(contact?.name || '', fallbackNumber2) || null;

      mergedCalls.push({
        documentId: uniqueId,
        number: contact?.phone || null,
        name: displayName2,
        leadStatus: 'not_connected',
        contactId: detail.contactId,
        time: detail.time || detail.createdAt,
        status: fallbackStatus,
        duration: computedDetailDuration,
        isMissed: isMissedDerived,
        transcriptCount: 0,
        whatsappMessageSent: detail.whatsappMessageSent || false,
        whatsappRequested: detail.whatsappRequested || false  
      });
      processedUniqueIds.add(uniqueId);
    }

    // 6. Sort by time (most recent first)
    mergedCalls.sort((a, b) => {
      const timeA = new Date(a.time || 0).getTime();
      const timeB = new Date(b.time || 0).getTime();
      return timeB - timeA;
    });

    
    // Debug: Log status distribution
    const statusCounts = {};
    const ongoingCounts = {};
    mergedCalls.forEach(call => {
      statusCounts[call.status] = (statusCounts[call.status] || 0) + 1;
      ongoingCounts[call.isOngoing] = (ongoingCounts[call.isOngoing] || 0) + 1;
    });
    
    

    // 7. Apply pagination
    const totalItems = mergedCalls.length;
    const totalPages = Math.ceil(totalItems / limit);
    const skip = (page - 1) * limit;
    let pagedCalls = mergedCalls.slice(skip, skip + limit);

    // Use backend proxy URL to avoid S3 CORS/signature issues
    try {
      const base = `${req.protocol}://${req.get('host')}`;
      pagedCalls = (pagedCalls || []).map((c) => {
        const uid = c?.documentId;
        if (uid) {
          c.audioUrl = `${base}/api/v1/client/campaigns/${campaign._id}/call-audio?documentId=${encodeURIComponent(uid)}`;
        }
        return c;
      });
    } catch (_) {}


    // Calculate totals from all mergedCalls (before pagination)
    const totals = mergedCalls.reduce((acc, call) => {
      // Add duration for all calls (including ongoing ones)
      if (call.duration > 0) {
        acc.totalDuration += call.duration;
      }
      
      // Count connected calls (completed calls with duration > 0 or status indicating connection)
      if ((call.status === 'completed' || call.leadStatus === 'connected') && call.duration > 0) {
        acc.totalConnected += 1;
      }
      // Count missed calls
      else if (call.status === 'missed' || call.isMissed) {
        acc.totalMissed += 1;
      }

      // Count ongoing and ringing totals
      if (call.isOngoing === true || call.status === 'ongoing') {
        acc.totalOngoing += 1;
      }
      if (call.status === 'ringing') {
        acc.totalRinging += 1;
      }
      
      return acc;
    }, {
      totalConnected: 0,
      totalMissed: 0,
      totalDuration: 0,
      totalOngoing: 0,
      totalRinging: 0
    });


    return res.json({
      success: true,
      data: pagedCalls,
      campaign: {
        _id: campaign._id,
        name: campaign.name,
        detailsCount: totalDetails,
        groupSelections: Array.isArray(campaign.groupSelections) ? campaign.groupSelections : []
      },
      totals: {
        totalItems: totalDetails,
        totalConnected: totals.totalConnected,
        totalMissed: totals.totalMissed,
        totalDuration: totals.totalDuration,
        totalOngoing: totals.totalOngoing,
        totalRinging: totals.totalRinging
      },
      pagination: {
        currentPage: page,
        totalPages,
        totalItems,
        hasNextPage: skip + pagedCalls.length < totalItems,
        hasPrevPage: page > 1
      }
    });

  } catch (error) {
    console.error('Error fetching merged calls:', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch merged calls' });
  }
});

// Fetch transcript for a call by uniqueId (documentId)
router.get('/campaigns/:id/logs/:documentId', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const { documentId } = req.params;

    if (!documentId) {
      return res.status(400).json({ success: false, error: 'documentId query parameter is required' });
    }

    // Step 1: Strict lookup (by clientId + uniqueid) using the compound index
    let latest = await CallLog.findOne({
      clientId: req.clientId,
      'metadata.customParams.uniqueid': documentId
    })
      .hint({ 'metadata.customParams.uniqueid': 1, clientId: 1, createdAt: -1 })
      .sort({ createdAt: -1 })
      .select('transcript createdAt')
      .lean();

    // Step 2: Fallback lookup (by uniqueid only) using the fallback index
    if (!latest) {
      latest = await CallLog.findOne({
        'metadata.customParams.uniqueid': documentId
      })
        .hint({ 'metadata.customParams.uniqueid': 1, createdAt: -1 })
        .sort({ createdAt: -1 })
        .select('transcript createdAt')
        .lean();
    }

    return res.json({
      success: true,
      transcript: latest && typeof latest.transcript === 'string' ? latest.transcript : '',
      documentId
    });
  } catch (error) {
    console.error('Error fetching transcript by documentId (alias route):', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch transcript' });
  }
});

// check missed call or not connected calls
router.get('/campaigns/:id/missed-calls', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId }).lean();
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // 1) Collect all uniqueIds from campaign.details
    const detailEntries = Array.isArray(campaign.details) ? campaign.details : [];
    const allUniqueIds = detailEntries
      .map(d => d && d.uniqueId)
      .filter(u => typeof u === 'string' && u.trim().length > 0);

    if (allUniqueIds.length === 0) {
      return res.status(200).json({ success: true, data: [], meta: { totalDetails: 0, totalWithLogs: 0, totalMissing: 0 } });
    }

    // 2) Find which of those uniqueIds have CallLogs
    const logs = await CallLog.find({
      clientId: req.clientId,
      'metadata.customParams.uniqueid': { $in: allUniqueIds }
    }, { 'metadata.customParams.uniqueid': 1 }).lean();

    const uniqueIdsWithLogs = new Set((logs || []).map(l => l?.metadata?.customParams?.uniqueid).filter(Boolean));
    const missingUniqueIds = allUniqueIds.filter(u => !uniqueIdsWithLogs.has(u));

    if (missingUniqueIds.length === 0) {
      return res.status(200).json({ success: true, data: [], meta: { totalDetails: allUniqueIds.length, totalWithLogs: uniqueIdsWithLogs.size, totalMissing: 0 } });
    }

    // 3) Map missing uniqueIds to contactIds and their timestamps from details
    const uniqueIdToContactId = new Map();
    const uniqueIdToTime = new Map();
    for (const d of detailEntries) {
      if (d && missingUniqueIds.includes(d.uniqueId)) {
        uniqueIdToContactId.set(d.uniqueId, d.contactId ? String(d.contactId) : null);
        uniqueIdToTime.set(d.uniqueId, d.time || d.createdAt || null);
      }
    }

    // 4) Build lookup map for campaign contacts by stringified _id
    const contacts = Array.isArray(campaign.contacts) ? campaign.contacts : [];
    const contactIdToContact = new Map();
    for (const c of contacts) {
      if (c && c._id) {
        contactIdToContact.set(String(c._id), c);
      }
    }

    // 5) Compose response list: for each missing uniqueId, include its contact and timestamp if available
    const responseItems = missingUniqueIds.map(u => {
      const contactIdStr = uniqueIdToContactId.get(u);
      const contact = contactIdStr ? contactIdToContact.get(contactIdStr) : null;
      const time = uniqueIdToTime.get(u) || null;
      return { uniqueId: u, contactId: contactIdStr || null, contact: contact || null, time };
    });

    return res.status(200).json({
      success: true,
      data: responseItems,
      meta: {
        totalDetails: allUniqueIds.length,
        totalWithLogs: uniqueIdsWithLogs.size,
        totalMissing: responseItems.length
      }
    });
  } catch (e) {
    console.error('Error fetching campaign missed-calls:', e);
    return res.status(500).json({ success: false, error: 'Failed to fetch missed-calls' });
  }
});

// Call all missed contacts for a campaign (server-side dialing)
router.post('/campaigns/:id/call-missed', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
     const { agentId, delayBetweenCalls = 2000, runId } = req.body || {};

    if (!agentId) {
      return res.status(400).json({ success: false, error: 'AGENT_REQUIRED', message: 'agentId is required' });
    }

    const Campaign = require('../models/Campaign');
    const CallLog = require('../models/CallLog');
    const Agent = require('../models/Agent');
    const { makeSingleCall, updateCallStatusFromLogs } = require('../services/campaignCallingService');

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Build retry list: only numbers missed in THIS runId (or overall if runId not provided)
    const details = Array.isArray(campaign.details) ? campaign.details : [];

    // Build allowlist of uniqueIds from CallLog for this runId (covers older details without runId)
    let uniqueIdsForRun = new Set();
    if (runId) {
      try {
        const runLogs = await CallLog.find({ 'metadata.customParams.runId': runId }, { 'metadata.customParams.uniqueid': 1, mobile: 1 }).lean();
        uniqueIdsForRun = new Set((runLogs || []).map(l => l?.metadata?.customParams?.uniqueid).filter(Boolean));
      } catch (_) {}
    }

    const missedDetails = details.filter(d => {
      if (!d) return false;
      if (runId && d.runId !== runId && !uniqueIdsForRun.has(d.uniqueId)) return false;
      const status = String(d.status || '').toLowerCase();
      const lead = String(d.leadStatus || '').toLowerCase();
      // Consider missed if not completed OR completed with not_connected
      return status !== 'completed' || lead === 'not_connected';
    });

    // Resolve phone for each missed detail; prefer campaign.contacts by contactId, fallback to CallLog
    const idToContact = new Map((campaign.contacts || []).map(c => [String(c._id || ''), c]));
    const seenPhones = new Set();
    let retryContacts = [];
    for (const d of missedDetails) {
      let phone = null, name = '';
      const byId = d.contactId && idToContact.get(String(d.contactId));
      if (byId && byId.phone) {
        phone = byId.phone; name = byId.name || '';
      } else if (d.uniqueId) {
        const log = await CallLog.findOne({ 'metadata.customParams.uniqueid': d.uniqueId }, { mobile: 1 }).lean();
        phone = log?.mobile || null;
      }
      if (!phone) continue;
      const key = String(phone).replace(/[^\d]/g, '');
      if (seenPhones.has(key)) continue;
      seenPhones.add(key);
      retryContacts.push({ phone, name, _id: d.contactId || null });
    }

    // Fallback: if nothing found in details (e.g., after history save cleared details), use CampaignHistory for this runId
    if (retryContacts.length === 0 && runId) {
      try {
        const CampaignHistory = require('../models/CampaignHistory');
        const hist = await CampaignHistory.findOne({ runId }).lean();
        const contactsFromHistory = Array.isArray(hist?.contacts) ? hist.contacts : [];
        for (const c of contactsFromHistory) {
          const isMissed = String(c.status || '').toLowerCase() !== 'completed' || String(c.leadStatus || '').toLowerCase() === 'not_connected';
          if (!isMissed) continue;
          const phone = c.number || null;
          if (!phone) continue;
          const key = String(phone).replace(/[^\d]/g, '');
          if (seenPhones.has(key)) continue;
          seenPhones.add(key);
          retryContacts.push({ phone, name: c.name || '', _id: c.contactId || null });
        }
      } catch (_) {}
    }

    if (retryContacts.length === 0) {
      return res.json({ success: true, started: false, count: 0 });
    }

    // Ensure retry contacts are reflected in campaign.contacts by REPLACING prior entries
    try {
      const contactsArr = Array.isArray(campaign.contacts) ? campaign.contacts : [];
      const digits = (v) => String(v || '').replace(/[^\d]/g, '');

      // Index existing contacts by contactId and by phone
      const byId = new Map();
      const byPhone = new Map();
      for (let i = 0; i < contactsArr.length; i++) {
        const c = contactsArr[i];
        const cid = String(c._id || '').trim();
        const ph = digits(c.phone);
        if (cid) byId.set(cid, i);
        if (ph) byPhone.set(ph, i);
      }

      // Apply replacements/upserts
      for (const rc of retryContacts) {
        const ph = digits(rc.phone);
        const cid = String(rc._id || '').trim();
        if (!ph) continue;

        if (cid && byId.has(cid)) {
          // Replace phone/name on the existing record for this contactId
          const idx = byId.get(cid);
          contactsArr[idx].phone = rc.phone;
          if (rc.name) contactsArr[idx].name = rc.name;
          byPhone.set(ph, idx);
        } else if (byPhone.has(ph)) {
          // Update name on the existing phone record
          const idx = byPhone.get(ph);
          if (rc.name) contactsArr[idx].name = rc.name;
        } else {
          // Insert a single new entry (no duplicates)
          contactsArr.push({ phone: rc.phone, name: rc.name || '' });
          const newIdx = contactsArr.length - 1;
          if (cid) byId.set(cid, newIdx);
          byPhone.set(ph, newIdx);
        }
      }

      // De-duplicate: keep only one record per phone (last write wins)
      const seen = new Set();
      const deduped = [];
      for (let i = contactsArr.length - 1; i >= 0; i--) {
        const ph = digits(contactsArr[i].phone);
        if (!ph || seen.has(ph)) continue;
        seen.add(ph);
        deduped.push(contactsArr[i]);
      }
      campaign.contacts = deduped.reverse();
      await campaign.save();
    } catch (_) {}

    // Resolve API key from Agent (same approach as start-calling)
    const agent = await Agent.findById(agentId).lean();
    if (!agent) {
      return res.status(404).json({ success: false, error: 'Agent not found' });
    }
    const apiKey = agent.X_API_KEY || '';
    if (!apiKey) {
      return res.status(400).json({ success: false, error: 'NO_API_KEY', message: 'No API key found on agent' });
    }

    // Mark campaign running while we schedule calls
    campaign.isRunning = true;
    await campaign.save();

    // Fire-and-forget async loop
    (async () => {
      try {
        for (let i = 0; i < retryContacts.length; i++) {
          const contact = retryContacts[i];
          const result = await makeSingleCall({ phone: contact.phone, name: contact.name }, agentId, apiKey, campaign._id, req.clientId, runId, null);
          if (result && result.uniqueId) {
            const initiatedAt = new Date();
            const callDetail = {
              uniqueId: result.uniqueId,
              contactId: contact._id || null,
              time: initiatedAt,
              status: 'ringing',
              lastStatusUpdate: initiatedAt,
              callDuration: 0,
              ...(runId ? { runId } : {})
            };
            const exists = campaign.details.find(d => d.uniqueId === result.uniqueId);
            if (!exists) {
              campaign.details.push(callDetail);
              await campaign.save();
            }
            setTimeout(() => {
              updateCallStatusFromLogs(campaign._id, result.uniqueId).catch(() => {});
            }, 40000);
          }
          if (i < retryContacts.length - 1) {
            await new Promise(r => setTimeout(r, Number(delayBetweenCalls) || 2000));
          }
        }
        campaign.isRunning = false;
        await campaign.save();
      } catch (e) {
        try { campaign.isRunning = false; await campaign.save(); } catch {}
      }
    })();

    return res.json({ success: true, started: true, count: retryContacts.length });
  } catch (e) {
    console.error('Error calling missed contacts:', e);
    return res.status(500).json({ success: false, error: 'Failed to call missed contacts' });
  }
});

// Start campaign calling process
router.post('/campaigns/:id/start-calling', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const { agentId, delayBetweenCalls = 2000 } = req.body;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Check if client has sufficient credits before starting
    try {
      const Credit = require('../models/Credit');
      const creditRecord = await Credit.getOrCreateCreditRecord(req.clientId);
      const currentBalance = Number(creditRecord?.currentBalance || 0);
      console.log(currentBalance);
      if (currentBalance <= 0) {
        return res.status(402).json({
          success: false,
          error: 'INSUFFICIENT_CREDITS',
          message: 'Not sufficient credits. Please recharge first to start calling.'
        });
      }
    } catch (e) {
      console.error('Credit check failed:', e);
      return res.status(500).json({ success: false, error: 'Credit check failed' });
    }

    if (!campaign.contacts || campaign.contacts.length === 0) {
      return res.status(400).json({ success: false, error: 'No contacts in campaign to call' });
    }

    // Check if campaign is already running
    if (campaign.isRunning) {
      return res.status(400).json({ success: false, error: 'Campaign is already running' });
    }

    // Resolve API key from Agent instead of Client
    const agent = await Agent.findById(agentId).lean();
    if (!agent) {
      return res.status(404).json({ success: false, error: 'Agent not found' });
    }
    const apiKey = agent.X_API_KEY || '';
    if (!apiKey && String(agent?.serviceProvider || '').toLowerCase() !== 'sanpbx' && String(agent?.serviceProvider || '').toLowerCase() !== 'snapbx') {
      return res.status(400).json({ success: false, error: 'No API key found on agent' });
    }

    // Fetch agent configuration for NGR pattern
    const AgentConfig = require('../models/AgentConfig');
    const agentConfig = await AgentConfig.findOne({ agentId }).lean();
    console.log(`🔧 CAMPAIGN START: Agent config for ${agentId}:`, agentConfig);

    // Preflight for SANPBX/SNAPBX provider to surface errors early
    try {
      const provider = String(agent?.serviceProvider || '').toLowerCase();
      if (provider === 'sanpbx' || provider === 'snapbx') {
        const accessToken = agent?.accessToken;
        const accessKey = agent?.accessKey;
        const callerId = agent?.callerId;
        if (!accessToken || !accessKey || !callerId) {
          return res.status(400).json({ success: false, error: 'TELEPHONY MISSING FIELDS', message: 'accessToken, accessKey and callerId are required on agent for SANPBX' });
        }
        const axios = require('axios');
        await axios.post(
          'https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/gentoken',
          { access_key: accessKey },
          { headers: { Accesstoken: accessToken }, timeout: 8000 }
        ).then(r => {
          if (!r?.data?.Apitoken) {
            throw new Error('SANPBX_TOKEN_FAILED');
          }
        });
      }
    } catch (e) {
      const isTimeout = (e?.code === 'ETIMEDOUT') || /timeout/i.test(e?.message || '');
      return res.status(502).json({ success: false, error: 'PROVIDER_UNREACHABLE', message: isTimeout ? 'Dialer gateway timeout while generating token' : (e?.response?.data?.message || e?.message || 'Dialer preflight failed') });
    }

    // Update campaign status
    campaign.isRunning = true;
    await campaign.save();

    // Start calling process in background with a runId for this instance
    const runId = `run_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
    startCampaignCalling(campaign, agentId, apiKey, delayBetweenCalls, req.clientId, runId, agentConfig);

    res.json({
      success: true,
      message: 'Campaign calling started',
      data: {
        campaignId: campaign._id,
        totalContacts: campaign.contacts.length,
        status: 'started',
        runId
      }
    });

  } catch (error) {
    console.error('Error starting campaign calling:', error);
    res.status(500).json({ success: false, error: 'Failed to start campaign calling' });
  }
});
// Save or update WhatsApp chat history for a phone number (upsert by clientId+phoneNumber)
router.post('/wa/chat/save', extractClientId, async (req, res) => {
  try {
    const { phoneNumber, contactName, messages } = req.body || {};
    if (!phoneNumber || !Array.isArray(messages)) {
      return res.status(400).json({ success: false, error: 'phoneNumber and messages[] are required' });
    }

    // Normalize incoming messages to schema
    const normalized = messages.map((m) => ({
      messageId: m.messageId || m.id || undefined,
      direction: m.direction || (m.side === 'right' ? 'sent' : 'received'),
      text: m.text || m.message || '',
      status: m.status || '',
      type: m.type || 'text',
      timestamp: m.timestamp ? new Date(m.timestamp) : (m.time ? new Date(m.time) : new Date()),
    }));

    // Upsert document. Only append messages that are new
    const existing = await WaChat.findOne({ clientId: req.clientId, phoneNumber });
    if (!existing) {
      const doc = await WaChat.create({
        clientId: req.clientId,
        phoneNumber,
        contactName: contactName || '',
        messages: normalized,
        lastSyncedAt: new Date(),
      });
      return res.json({ success: true, created: true, count: doc.messages.length });
    }

    // Build sets for dedupe: prefer messageId if present, else use timestamp+text heuristic
    const existingIdSet = new Set(
      (existing.messages || [])
        .map((m) => m.messageId)
        .filter(Boolean)
    );
    const existingComposite = new Set(
      (existing.messages || []).map((m) => `${new Date(m.timestamp).getTime()}|${(m.text || '').slice(0, 50)}`)
    );
    const toAppend = normalized.filter((m) => {
      if (m.messageId && existingIdSet.has(m.messageId)) return false;
      const comp = `${new Date(m.timestamp).getTime()}|${(m.text || '').slice(0, 50)}`;
      if (existingComposite.has(comp)) return false;
      return true;
    });

    if (contactName && !existing.contactName) existing.contactName = contactName;
    if (toAppend.length > 0) {
      existing.messages.push(...toAppend);
      existing.lastSyncedAt = new Date();
      await existing.save();
    }

    return res.json({ success: true, created: false, appended: toAppend.length, total: existing.messages.length });
  } catch (e) {
    console.error('Error saving WA chat:', e);
    return res.status(500).json({ success: false, error: 'Failed to save chat' });
  }
});

// Validate agent credentials before making calls
router.post('/agents/:agentId/validate-credentials', extractClientId, async (req, res) => {
  try {
    const { agentId } = req.params;
    const agent = await Agent.findById(agentId).lean();
    
    if (!agent) {
      return res.status(404).json({ 
        success: false, 
        error: 'Agent not found',
        missingFields: ['Agent not found']
      });
    }

    const provider = String(agent?.serviceProvider || '').toLowerCase();
    const missingFields = [];
    const validationErrors = [];

    // Check if agent is active
    if (!agent.isActive) {
      validationErrors.push('Agent is inactive');
    }

    // Provider-specific validation
    if (provider === 'snapbx' || provider === 'sanpbx') {
      // SANPBX validation
      if (!agent.accessToken) missingFields.push('Access Token');
      if (!agent.accessKey) missingFields.push('Access Key');
      if (!agent.callerId) missingFields.push('Caller ID');
      if (!agent.didNumber) missingFields.push('DID Number');
    } else if (provider === 'c-zentrix') {
      // C-Zentrix validation
      if (!agent.X_API_KEY) missingFields.push('API Key');
      if (!agent.callerId) missingFields.push('Caller ID');
      if (!agent.didNumber) missingFields.push('DID Number');
    } else if (provider === 'tata') {
      // Tata validation
      if (!agent.X_API_KEY) missingFields.push('API Key');
      if (!agent.callingNumber) missingFields.push('Calling Number');
    } else {
      // Generic validation for other providers
      if (!agent.X_API_KEY) missingFields.push('API Key');
      if (!agent.callingNumber && !agent.callerId) missingFields.push('Calling Number or Caller ID');
    }

    // Check for service provider
    if (!agent.serviceProvider) {
      missingFields.push('Service Provider');
    }

    // Check client credits
    try {
      const Credit = require('../models/Credit');
      // Lightweight read: only fetch currentBalance, do not load large arrays
      const creditDoc = await Credit.findOne({ clientId: req.clientId })
        .select('currentBalance')
        .lean();
      const currentBalance = Number(creditDoc?.currentBalance || 0);
      if (currentBalance <= 0) {
        validationErrors.push('Insufficient credits - Please recharge your account');
      }
    } catch (e) {
      validationErrors.push('Unable to verify account credits');
    }

    const isValid = missingFields.length === 0 && validationErrors.length === 0;
    
    return res.json({
      success: isValid,
      isValid,
      missingFields,
      validationErrors,
      provider,
      agentName: agent.agentName,
      message: isValid 
        ? 'All credentials are valid' 
        : `Missing: ${missingFields.join(', ')}${validationErrors.length > 0 ? ` | Issues: ${validationErrors.join(', ')}` : ''}`
    });

  } catch (error) {
    console.error('Validation error:', error);
    return res.status(500).json({ 
      success: false, 
      error: 'Validation failed',
      missingFields: ['System error during validation']
    });
  }
});

// Make single call
router.post('/calls/single', extractClientId, async (req, res) => {
  const _startTs = Date.now();
  const reqId = `SINGLE_${_startTs}_${Math.random().toString(36).slice(2, 8)}`;
  const log = (stage, details = {}) => {
    try {
      console.log('[CALLS/SINGLE]', JSON.stringify({
        reqId,
        stage,
        clientId: req.clientId,
        ts: new Date().toISOString(),
        ...details
      }));
    } catch (_) {}
  };
  const maskPhone = (p) => {
    try {
      const s = String(p || '');
      if (s.length <= 4) return '****';
      return `${'*'.repeat(Math.max(0, s.length - 4))}${s.slice(-4)}`;
    } catch { return '****'; }
  };
  try {
    const { contact, agentId, apiKey, campaignId, custom_field, uniqueid } = req.body || {};
    log('request.received', {
      hasContact: !!contact,
      hasAgentId: !!agentId,
      hasApiKey: !!apiKey,
      hasCampaignId: !!campaignId,
      hasCustomField: !!custom_field
    });
    console.log(req.body)
    // Allow either a plain phone string or a contact object { phone, name }
    const contactPhoneRaw =
      typeof contact === 'string' ? contact : (contact && (contact.phone || contact.number));
    const contactNameRaw = typeof contact === 'object' && contact ? (contact.name || contact.fullName || '') : '';
    // Also accept name when contact is a plain string: from top-level body or custom_field
    const topLevelName = (typeof contact !== 'object')
      ? ((req.body && req.body.name) || (custom_field && (custom_field.name || custom_field.contact_name)) || '')
      : '';
    const resolvedName = contactNameRaw || topLevelName || '';
    if (!contactPhoneRaw) {
      log('validation.failed', { reason: 'Missing contact (phone)' });
      return res.status(400).json({ success: false, error: 'Missing contact (phone)' });
    }
    if (!agentId) {
      log('validation.failed', { reason: 'Missing agentId' });
      return res.status(400).json({ success: false, error: 'Missing agentId' });
    }

    // Credit check (lightweight: fetch only currentBalance)
    try {
      const Credit = require('../models/Credit');
      const creditSlim = await Credit.findOne({ clientId: req.clientId })
        .select({ clientId: 1, currentBalance: 1 })
        .lean();
      const currentBalance = Number(creditSlim?.currentBalance || 0);
      log('credit.checked', { currentBalance });
      if (currentBalance <= 0) {
        log('credit.insufficient', { currentBalance });
        return res.status(402).json({
          success: false,
          error: 'INSUFFICIENT_CREDITS',
          message: 'Not sufficient credits. Please recharge first to start calling.'
        });
      }
    } catch (e) {
      console.error('Credit check failed:', e);
      log('credit.error', { error: e?.message });
      return res.status(500).json({ success: false, error: 'Credit check failed' });
    }

    // Load agent for provider-specific dialing
    const agent = await Agent.findById(agentId).lean();
    if (!agent) {
      log('agent.missing', { agentId });
      return res.status(404).json({ success: false, error: 'Agent not found' });
    }
    log('agent.loaded', { agentId, provider: agent?.provider, hasXApiKey: !!agent?.X_API_KEY });

    // Helper: normalize phone to digits only, trim common prefixes
    const normalizePhone = (raw) => {
      try {
        if (!raw) return '';
        let p = String(raw).replace(/[^\d]/g, '');
        // Drop leading country code if longer than 10-12 digits
        if (p.length > 12 && p.startsWith('91')) p = p.slice(2);
        if (p.length > 11 && p.startsWith('1')) p = p.slice(1);
        if (p.startsWith('0')) p = p.replace(/^0+/, '');
        return p;
      } catch (_) { return String(raw || ''); }
    };
    const normalizedDigits = normalizePhone(contactPhoneRaw);
    if (!normalizedDigits) {
      log('phone.invalid', { provided: maskPhone(contactPhoneRaw), normalized: normalizedDigits });
      return res.status(400).json({ success: false, error: 'Invalid phone' });
    }
    log('phone.normalized', { provided: maskPhone(contactPhoneRaw), normalizedMasked: maskPhone(normalizedDigits) });

    // Use makeSingleCall service for all providers (including SANPBX)
    // Resolve API key: only required for C-Zentrix, not for SANPBX
    const provider = String(agent?.serviceProvider || '').toLowerCase();
    let resolvedApiKey = apiKey;
    
    if (provider === 'snapbx' || provider === 'sanpbx') {
      // SANPBX doesn't require API key
      log('apikey.skipped', { reason: 'SANPBX provider does not require API key', agentId });
    } else {
      // C-Zentrix and other providers require API key
      if (!resolvedApiKey) {
        resolvedApiKey = agent.X_API_KEY || '';
        if (!resolvedApiKey) {
          // fallback to client-level key if agent key missing
          resolvedApiKey = await getClientApiKey(req.clientId);
        }
        if (!resolvedApiKey) {
          log('apikey.missing', { agentId, clientId: req.clientId });
          return res.status(400).json({ success: false, error: 'No API key found for agent or client' });
        }
      }
      log('apikey.resolved', { via: apiKey ? 'request' : (agent?.X_API_KEY ? 'agent' : 'client') });
    }

    log('call.initiating', { agentId, campaignId: campaignId || null });
    const result = await makeSingleCall(
      {
        name: resolvedName,
        phone: normalizedDigits,
      },
      agentId,
      resolvedApiKey,
      campaignId || null,
      req.clientId,
      null, // runId
      uniqueid // providedUniqueId
    );
    log('call.initiated', { success: !!result?.success, uniqueId: result?.uniqueId || null });

    // If tied to a campaign and call successfully initiated, update campaign asynchronously (non-blocking)
    if (result.success && campaignId && result.uniqueId) {
      // Run campaign update in background to avoid blocking the response
      setImmediate(async () => {
        try {
          const Campaign = require('../models/Campaign');
          const campaign = await Campaign.findById(campaignId);
          if (campaign) {
            const initiatedAt = new Date();
            // Prefer contactId from request; otherwise best-effort resolve by phone
            let resolvedContactId = contact.contactId || null;
            if (!resolvedContactId) {
              try {
                const matched = (campaign.contacts || []).find((c) => {
                  const p1 = (c.phone || c.number || '').replace(/\D/g, '');
                  const p2 = (contact.phone || '').replace(/\D/g, '');
                  return p1.endsWith(p2) || p2.endsWith(p1);
                });
                if (matched && matched._id) resolvedContactId = matched._id;
              } catch (_) {}
            }

            const callDetail = {
              uniqueId: result.uniqueId,
              contactId: resolvedContactId,
              time: initiatedAt,
              status: 'ringing',
              lastStatusUpdate: initiatedAt,
              callDuration: 0,
            };
            const exists = campaign.details.find((d) => d.uniqueId === result.uniqueId);
            if (!exists) {
              campaign.details.push(callDetail);
              await campaign.save();
              log('campaign.detail.appended', { campaignId, uniqueId: result.uniqueId });
            }
            
          }
        } catch (e) {
          console.error('Failed to append single-call detail to campaign:', e);
          log('campaign.detail.error', { campaignId, error: e?.message });
        }
      });
    }

    const durationMs = Date.now() - _startTs;
    log('response.success', { durationMs });
    return res.status(200).json({ success: true, data: result });
  } catch (error) {
    console.error('Single call error:', error);
    const durationMs = Date.now() - _startTs;
    log('response.error', { error: error?.message, durationMs });
    return res.status(500).json({ success: false, error: 'Failed to initiate single call' });
  }
});

// Stop campaign calling process
router.post('/campaigns/:id/stop-calling', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Update campaign status
    campaign.isRunning = false;
    await campaign.save();

    // Stop the calling process
    stopCampaignCalling(campaign._id.toString());

    // Force save campaign history immediately when stopping
    try {
      const CampaignHistory = require('../models/CampaignHistory');
      const CallLog = require('../models/CallLog');
      
      // Get the latest runId from campaign details
      const campaignDetails = Array.isArray(campaign.details) ? campaign.details : [];
      const latestDetail = campaignDetails
        .filter(d => d && d.runId)
        .sort((a, b) => new Date(b.time || 0) - new Date(a.time || 0))[0];
      
      if (latestDetail && latestDetail.runId) {
        const runId = latestDetail.runId;
        
        // Get call logs for this run
        const callLogs = await CallLog.find({
          campaignId: campaign._id,
          'metadata.customParams.runId': runId
        }).lean();

        // Build contacts array for history
        const contactsById = new Map((campaign.contacts || []).map(c => [String(c._id || ''), c]));
        const runDetails = campaignDetails.filter(d => d && d.runId === runId);
        
        let contacts = runDetails.map(d => {
          const contact = contactsById.get(String(d.contactId || ''));
          const callLog = callLogs.find(log => log.metadata?.customParams?.uniqueid === d.uniqueId);
          
          return {
            documentId: d.uniqueId,
            contactId: d.contactId,
            number: contact?.phone || contact?.number || '',
            name: contact?.name || '',
            leadStatus: d.leadStatus || 'not_connected',
            time: d.time ? d.time.toISOString() : new Date().toISOString(),
            status: d.status || 'completed',
            duration: d.callDuration || 0,
            audioUrl: callLog?.audioUrl || null,
            transcriptCount: callLog?.transcriptCount || 0,
            whatsappMessageSent: callLog?.whatsappMessageSent || false,
            whatsappRequested: callLog?.whatsappRequested || false
          };
        });

        // Replace with backend proxy URL to avoid S3 CORS/signature issues
        try {
          const base = `${req.protocol}://${req.get('host')}`;
          contacts = (contacts || []).map((c) => {
            const uid = c?.documentId;
            if (uid) {
              c.audioUrl = `${base}/api/v1/client/campaigns/${campaign._id}/call-audio?documentId=${encodeURIComponent(uid)}`;
            }
            return c;
          });
        } catch (_) {}

        // Calculate stats
        const totalContacts = contacts.length;
        const successfulCalls = contacts.filter(c => c.leadStatus && c.leadStatus !== 'not_connected').length;
        const failedCalls = contacts.filter(c => !c.leadStatus || c.leadStatus === 'not_connected').length;
        const totalCallDuration = contacts.reduce((sum, c) => sum + (c.duration || 0), 0);
        const averageCallDuration = totalContacts > 0 ? Math.round(totalCallDuration / totalContacts) : 0;

        // Calculate run time
        const startTime = runDetails.reduce((min, d) => {
          const t = new Date(d.time || 0).getTime();
          return Math.min(min, t);
        }, Number.POSITIVE_INFINITY);
        const endTime = new Date();
        const elapsedSeconds = Math.floor((endTime - new Date(startTime)) / 1000);
        const hours = Math.floor(elapsedSeconds / 3600);
        const minutes = Math.floor((elapsedSeconds % 3600) / 60);
        const seconds = elapsedSeconds % 60;

        // Get instance number
        const existingCount = await CampaignHistory.countDocuments({ campaignId: campaign._id });
        const instanceNumber = existingCount + 1;

        // Save to campaign history
        await CampaignHistory.findOneAndUpdate(
          { runId },
          {
            $setOnInsert: {
              campaignId: campaign._id,
              runId,
              instanceNumber,
              startTime: new Date(startTime).toISOString(),
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
        
        console.log(`💾 PARALLEL: Campaign history saved for run ${runId}`);
      }
    } catch (historyError) {
      console.error(`❌ PARALLEL: Error saving campaign history:`, historyError);
    }

    res.json({
      success: true,
      message: 'Campaign calling stopped',
      data: {
        campaignId: campaign._id,
        status: 'stopped'
      }
    });

  } catch (error) {
    console.error('Error stopping campaign calling:', error);
    res.status(500).json({ success: false, error: 'Failed to stop campaign calling' });
  }
});

// Get campaign calling status
router.get('/campaigns/:id/calling-status', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Get calling progress from memory
    const callingProgress = getCampaignCallingProgress(campaign._id.toString());

    // Derive whether all calls are finalized (no ringing/ongoing; all initiated have status 'completed')
    // Note: progress.completedCalls counts initiated calls, not finalized, so do NOT use it here
    const details = Array.isArray(campaign.details) ? campaign.details : [];
    const initiatedCount = details.length;
    const ringingCount = details.filter(d => d && d.status === 'ringing').length;
    const ongoingCount = details.filter(d => d && d.status === 'ongoing').length;
    const hasActive = details.some(d => d && (d.status === 'ringing' || d.status === 'ongoing'));
    const completedCount = details.filter(d => d && d.status === 'completed').length;
    const allCallsFinalized = initiatedCount > 0 && !hasActive && completedCount === initiatedCount;

    // Determine if campaign is actually running
    const isActuallyRunning = campaign.isRunning && (hasActive || (callingProgress && callingProgress.isRunning));

    // Compute latestRunId and inferred runStartTime from details/progress
    let latestRunId = null;
    let runStartTime = null;
    if (initiatedCount > 0) {
      // Pick the most recent detail by time/lastStatusUpdate (guard against nulls)
      const sorted = [...details]
        .filter((d) => d && (d.lastStatusUpdate || d.time))
        .sort((a, b) => {
          const at = new Date((a && (a.lastStatusUpdate || a.time)) || 0).getTime();
          const bt = new Date((b && (b.lastStatusUpdate || b.time)) || 0).getTime();
          return bt - at;
        });
      const mostRecent = sorted[0] || null;
      latestRunId = mostRecent && mostRecent.runId ? mostRecent.runId : null;
      if (latestRunId) {
        const sameRun = details.filter(d => d && d.runId === latestRunId);
        // Earliest time in that run
        const earliest = sameRun.reduce((min, d) => {
          const t = new Date((d && (d.time || d.lastStatusUpdate)) || 0).getTime();
          return Math.min(min, t);
        }, Number.POSITIVE_INFINITY);
        if (isFinite(earliest) && earliest !== Number.POSITIVE_INFINITY) {
          runStartTime = new Date(earliest);
        }
      }
    }
    if (!runStartTime && callingProgress && callingProgress.startTime) {
      runStartTime = new Date(callingProgress.startTime);
    }

    // Auto-update isRunning based on actual call status to prevent stuck campaigns
    // BUT respect manual stops - don't auto-start if campaign was manually stopped
    let shouldUpdateIsRunning = false;
    let newIsRunning = campaign.isRunning;
    
    if (campaign.isRunning && allCallsFinalized) {
      // Campaign is marked as running but all calls are finalized - should be stopped
      newIsRunning = false;
      shouldUpdateIsRunning = true;
      console.log(`🔄 BACKEND: Auto-stopping campaign ${id} - all calls finalized`);
    }
    // REMOVED: Auto-start logic that was overriding manual stops
    // Only auto-stop when all calls are finalized, don't auto-start
    
    // Update database if needed
    if (shouldUpdateIsRunning) {
      campaign.isRunning = newIsRunning;
      await campaign.save();
    }

    res.json({
      success: true,
      data: {
        campaignId: campaign._id,
        isRunning: newIsRunning,
        isActuallyRunning,
        allCallsFinalized,
        hasActiveCalls: hasActive,
        initiatedCount,
        completedCount,
        ringingCount,
        ongoingCount,
        totalContacts: campaign.contacts.length,
        progress: callingProgress,
        latestRunId,
        runStartTime: runStartTime ? runStartTime.toISOString() : null
      }
    });

  } catch (error) {
    console.error('Error getting campaign calling status:', error);
    res.status(500).json({ success: false, error: 'Failed to get campaign calling status' });
  }
});


// Get campaign history
router.get('/campaigns/:id/history', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;

    const history = await CampaignHistory.find({ campaignId: id })
      .sort({ instanceNumber: -1 })
      .lean();

    res.json({
      success: true,
      data: history
    });

  } catch (error) {
    console.error('Error fetching campaign history:', error);
    res.status(500).json({ success: false, error: 'Failed to fetch campaign history' });
  }
});

// ==================== BUSINESS INFO API ====================
//create client business id
router.post('/business-info', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const {text} = req.body;

    if (!text || !text.trim()) {
      return res.status(400).json({success: false, message: "Text is required"});
    }

    const businessInfo = await Business.create({clientId: clientId, text: text.trim()});
    res.status(201).json({success: true, data: businessInfo});
  }catch(error){
    console.error('Error creating business info:', error);
    res.status(500).json({success: false, message: "Failed to create business info"});
  }
});

//Get client's business id
router.get('/business-info/:id', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const { id } = req.params;

    const businessInfo = await Business.findOne({ _id: id, clientId: clientId });
    
    if (!businessInfo) {
      return res.status(404).json({success: false, message: "Business info not found"});
    }

    res.status(200).json({success: true, data: businessInfo});
  }catch(error){
    console.error('Error fetching business info:', error);
    res.status(500).json({success: false, message: "Failed to fetch business info"});
  }
});

//update client's business id
router.put('/business-info/:id', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const { id } = req.params;
    const { text } = req.body;

    if (!text || !text.trim()) {
      return res.status(400).json({success: false, message: "Text is required"});
    }

    const businessInfo = await Business.findOneAndUpdate(
      { _id: id, clientId: clientId },
      { text: text.trim(), updatedAt: Date.now() },
      { new: true, runValidators: true }
    );

    if (!businessInfo) {
      return res.status(404).json({success: false, message: "Business info not found"});
    }

    res.status(200).json({success: true, data: businessInfo});
  }catch(error){
    console.error('Error updating business info:', error);
    res.status(500).json({success: false, message: "Failed to update business info"});
  }
});

//===================== My Business ===========================

// CREATE MyBusiness
router.post('/business', extractClientId, async(req, res)=>{
  try{
    console.log(req.body);
    const clientId = req.clientId;
    const { title, category, type, image, documents, videoLink, link, description, mrp, offerPrice } = req.body;

    // Validate required fields
    if(!title || !category || !type || !image || !image.key || !description || mrp === undefined) {
      return res.status(400).json({success: false, message: "Missing required fields. Required: title, category, type, image.key, description, mrp"});
    }

    // Validate image structure
    if(typeof image !== 'object' || !image.key) {
      return res.status(400).json({success: false, message: "Image must be an object with a 'key' property."});
    }

    // Validate documents structure if provided
    if(documents && (typeof documents !== 'object' || !documents.key)) {
      return res.status(400).json({success: false, message: "Documents must be an object with a 'key' property if provided."});
    }

    // Validate mrp and offerPrice
    if(isNaN(Number(mrp)) || (offerPrice !== undefined && offerPrice !== null && isNaN(Number(offerPrice)))) {
      return res.status(400).json({success: false, message: "mrp and offerPrice must be numbers."});
    }

     // Generate S3 URLs using getobject function
     const { getobject } = require('../utils/s3');
    
     let imageWithUrl = { ...image };
     let documentsWithUrl = documents ? { ...documents } : undefined;
     
     try {
       // Generate URL for image
       const imageUrl = await getobject(image.key);
       imageWithUrl.url = imageUrl;
       
       // Generate URL for documents if provided
       if (documents && documents.key) {
         const documentsUrl = await getobject(documents.key);
         documentsWithUrl.url = documentsUrl;
       }
     } catch (s3Error) {
       console.error('Error generating S3 URLs:', s3Error);
       return res.status(500).json({success: false, message: "Error generating file URLs"});
     }
 
     // Generate unique hash for the business
     let hash;
     let isHashUnique = false;
     let attempts = 0;
     const maxAttempts = 10;
 
     while (!isHashUnique && attempts < maxAttempts) {
       hash = generateBusinessHash();
       const existingBusiness = await MyBusiness.findOne({ hash });
       if (!existingBusiness) {
         isHashUnique = true;
       }
       attempts++;
     }
 
     if (!isHashUnique) {
       return res.status(500).json({ success: false, message: "Failed to generate unique hash for business" });
     } 

     // Generate share link using the hash
     const baseUrl = 'https://aitotafrontend.vercel.app' || 'http://localhost:5173';
     const slug = title
       .toLowerCase()
       .replace(/[^a-z0-9]+/g, "-")
       .replace(/(^-|-$)/g, "");
     const shareLink = `${baseUrl}/${slug}-${hash}`;

    const business = await MyBusiness.create({
      clientId,
      title,
      category,
      type,
      image: imageWithUrl,
      documents: documentsWithUrl,
      videoLink,
      link,
      description,
      mrp: Number(mrp),
      offerPrice: offerPrice !== undefined && offerPrice !== null ? Number(offerPrice) : null,
      hash,
      Sharelink: shareLink
    });
    res.status(201).json({success: true, data: business});
  }catch(error){
    console.error('Error creating business:', error);
    res.status(500).json({success: false, message: "Failed to create business"});
  }
})

// READ: Get all businesses for a client
router.get('/business', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    let businesses = await MyBusiness.find({ clientId }).sort({ createdAt: -1 }); // Sort by creation date, most recent first
    
    // Import S3 utility for generating fresh URLs
    const { getobject } = require('../utils/s3');
    
    // Ensure image and documents always have fresh url and key fields
    businesses = await Promise.all(businesses.map(async (business) => {
      let imageUrl = '';
      let documentsUrl = '';
      
      // Generate fresh presigned URL for image if key exists
      if (business.image && business.image.key) {
        try {
          imageUrl = await getobject(business.image.key);
        } catch (error) {
          console.error('Error generating image URL:', error);
          imageUrl = '';
        }
      }
      
      // Generate fresh presigned URL for documents if key exists
      if (business.documents && business.documents.key) {
        try {
          documentsUrl = await getobject(business.documents.key);
        } catch (error) {
          console.error('Error generating documents URL:', error);
          documentsUrl = '';
        }
      }
      
      return {
        ...business.toObject(),
        image: {
          url: imageUrl,
          key: business.image && business.image.key ? business.image.key : ''
        },
        documents: {
          url: documentsUrl,
          key: business.documents && business.documents.key ? business.documents.key : ''
        }
      };
    }));
    
    res.status(200).json({ success: true, data: businesses });
  } catch (error) {
    console.error('Error fetching businesses:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch businesses' });
  }
});

// READ: Get a single business by ID
router.get('/business/:id', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { id } = req.params;
    let business = await MyBusiness.findOne({ _id: id, clientId });
    if (!business) {
      return res.status(404).json({ success: false, message: 'Business not found' });
    }
    
    // Import S3 utility for generating fresh URLs
    const { getobject } = require('../utils/s3');
    
    let imageUrl = '';
    let documentsUrl = '';
    
    // Generate fresh presigned URL for image if key exists
    if (business.image && business.image.key) {
      try {
        imageUrl = await getobject(business.image.key);
      } catch (error) {
        console.error('Error generating image URL:', error);
        imageUrl = '';
      }
    }
    
    // Generate fresh presigned URL for documents if key exists
    if (business.documents && business.documents.key) {
      try {
        documentsUrl = await getobject(business.documents.key);
      } catch (error) {
        console.error('Error generating documents URL:', error);
        documentsUrl = '';
      }
    }
    
    business = {
      ...business.toObject(),
      image: {
        url: imageUrl,
        key: business.image && business.image.key ? business.image.key : ''
      },
      documents: {
        url: documentsUrl,
        key: business.documents && business.documents.key ? business.documents.key : ''
      }
    };
    res.status(200).json({ success: true, data: business });
  } catch (error) {
    console.error('Error fetching business:', error);
    res.status(500).json({ success: false, message: 'Failed to fetch business' });
  }
});

// UPDATE: Update a business by ID
router.put('/business/:id', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { id } = req.params;
    const updateData = req.body;
    
    // If title is being updated, regenerate the share link
    if (updateData.title) {
      const business = await MyBusiness.findById(id);
      if (business && business.hash) {
        const baseUrl = process.env.FRONTEND_BASE_URL || 'http://localhost:3000';
        const slug = updateData.title
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/(^-|-$)/g, "");
        updateData.Sharelink = `${baseUrl}/${slug}-${business.hash}`;
      }
    }
    
    const business = await MyBusiness.findOneAndUpdate(
      { _id: id, clientId },
      { ...updateData, updatedAt: Date.now() },
      { new: true, runValidators: true }
    );
    if (!business) {
      return res.status(404).json({ success: false, message: 'Business not found' });
    }
    res.status(200).json({ success: true, data: business });
  } catch (error) {
    console.error('Error updating business:', error);
    res.status(500).json({ success: false, message: 'Failed to update business' });
  }
});
// DELETE: Delete a business by ID
router.delete('/business/:id', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { id } = req.params;
    const business = await MyBusiness.findOneAndDelete({ _id: id, clientId });
    if (!business) {
      return res.status(404).json({ success: false, message: 'Business not found' });
    }
    res.status(200).json({ success: true, message: 'Business deleted successfully' });
  } catch (error) {
    console.error('Error deleting business:', error);
    res.status(500).json({ success: false, message: 'Failed to delete business' });
  }
});

//===================== MY Dial ===============================

router.post('/dials', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const {category, phoneNumber, leadStatus ,contactName, date, other} = req.body;

    if(!category || !phoneNumber || !contactName){
      return res.status(400).json({success: false, message: "Missing required fields. Required: category, phoneNumber, contactName"});
    }

    const dial = await MyDials.create({
      clientId : clientId,
      category,
      leadStatus,
      phoneNumber,
      contactName,
      date,
      other
    });
    res.status(201).json({success: true, data: dial});

  }catch(error){
    console.log(error);
    return json.status(400)({sucess: true, message: "Failed to add dials"})
  }
});

router.get('/dials/report', extractClientId, async (req, res) => {
  try {
    const clientId = req.clientId;
    const { filter, startDate, endDate } = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query
    const query = { clientId, ...dateFilter };
        
    const logs = await MyDials.find(query);
    const totalCalls = logs.length;
    const totalConnected = logs.filter(l => l.category === 'connected').length;
    const totalNotConnected = logs.filter(l => l.category === 'not connected').length;
    const totalConversationTime = logs.reduce((sum, l) => sum + (l.duration || 0), 0);
    const avgCallDuration = totalCalls ? totalConversationTime / totalCalls : 0;
    
    res.json({ 
      success: true, 
      data: {
        clientId,
        totalCalls, 
        totalConnected, 
        totalNotConnected, 
        totalConversationTime, 
        avgCallDuration 
      },
      filter: {
        applied: filter || 'all',
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error in /dials/report', error);
    res.status(500).json({ error: 'Failed to fetch report' });
  }
});

router.get('/dials/leads', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const {filter, startDate, endDate} = req.query;
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query
    const query = { clientId, ...dateFilter };    
    const logs = await MyDials.find(query).sort({ createdAt: -1 });
    
    // Group leads according to the new leadStatus structure
    const leads = {
      // Connected - Interested
      veryInterested: {
        data: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very interested'),
        count: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very interested').length
      },
      maybe: {
        data: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium'),
        count: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium').length
      },
      enrolled: {
        data: logs.filter(l => l.leadStatus === 'enrolled'),
        count: logs.filter(l => l.leadStatus === 'enrolled').length
      },
      
      // Connected - Not Interested
      junkLead: {
        data: logs.filter(l => l.leadStatus === 'junk lead'),
        count: logs.filter(l => l.leadStatus === 'junk lead').length
      },
      notRequired: {
        data: logs.filter(l => l.leadStatus === 'not required'),
        count: logs.filter(l => l.leadStatus === 'not required').length
      },
      enrolledOther: {
        data: logs.filter(l => l.leadStatus === 'enrolled other'),
        count: logs.filter(l => l.leadStatus === 'enrolled other').length
      },
      decline: {
        data: logs.filter(l => l.leadStatus === 'decline'),
        count: logs.filter(l => l.leadStatus === 'decline').length
      },
      notEligible: {
        data: logs.filter(l => l.leadStatus === 'not eligible'),
        count: logs.filter(l => l.leadStatus === 'not eligible').length
      },
      wrongNumber: {
        data: logs.filter(l => l.leadStatus === 'wrong number'),
        count: logs.filter(l => l.leadStatus === 'wrong number').length
      },
      
      // Connected - Followup
      hotFollowup: {
        data: logs.filter(l => l.leadStatus === 'hot followup'),
        count: logs.filter(l => l.leadStatus === 'hot followup').length
      },
      coldFollowup: {
        data: logs.filter(l => l.leadStatus === 'cold followup'),
        count: logs.filter(l => l.leadStatus === 'cold followup').length
      },
      schedule: {
        data: logs.filter(l => l.leadStatus === 'schedule'),
        count: logs.filter(l => l.leadStatus === 'schedule').length
      },
      
      // Not Connected
      notConnected: {
        data: logs.filter(l => l.leadStatus === 'not connected'),
        count: logs.filter(l => l.leadStatus === 'not connected').length
      },
      
      // Other - leads that don't match any predefined category
      other: {
        data: logs.filter(l => {
          const predefinedStatuses = [
            'vvi', 'very interested', 'maybe', 'medium', 'enrolled', 
            'junk lead', 'not required', 'enrolled other', 'decline', 
            'not eligible', 'wrong number', 'hot followup', 'cold followup', 
            'schedule', 'not connected'
          ];
          return !predefinedStatuses.includes(l.leadStatus);
        }),
        count: logs.filter(l => {
          const predefinedStatuses = [
            'vvi', 'very interested', 'maybe', 'medium', 'enrolled', 
            'junk lead', 'not required', 'enrolled other', 'decline', 
            'not eligible', 'wrong number', 'hot followup', 'cold followup', 
            'schedule', 'not connected'
          ];
          return !predefinedStatuses.includes(l.leadStatus);
        }).length
      }
    };

    res.json({ 
      success: true, 
      data: leads,
      filter: {
        applied: filter,
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error in /inbound/leads:', error);
    res.status(500).json({ error: 'Failed to fetch leads' });
  }
});

router.get('/dials/done', extractClientId, async(req,res)=>{
  try{
    const clientId = req.clientId;
    const {filter, startDate, endDate} = req.query;
    
    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days'];
    if (filter && !allowedFilters.includes(filter) && !startDate && !endDate) {
      return res.status(400).json({ error: 'Invalid filter parameter' });
    }
    
    let dateFilter = {};
    
    // Apply date filtering based on filter parameter
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today);
      yesterday.setDate(today.getDate() - 1);
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today);
      sevenDaysAgo.setDate(today.getDate() - 7);
      
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query
    const query = { 
      clientId, 
      category: 'sale_done',
      ...dateFilter 
    };
    
    const data = await MyDials.find(query).sort({ createdAt: -1 });
    res.json({
      success: true,
      data: data,
      filter: {
        applied: filter,
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch done dials' });
  }
});

// ==================== HUMAN AGENT ROUTES ====================

// Get all human agents for a client
router.get('/human-agents', extractClientId,  getHumanAgents);

// Create new human agent
router.post('/human-agents', extractClientId,  createHumanAgent);

// Get single human agent
router.get('/human-agents/:agentId', extractClientId,  getHumanAgentById);

// Update human agent
router.put('/human-agents/:agentId', extractClientId,  updateHumanAgent);

// Delete human agent
router.delete('/human-agents/:agentId', extractClientId,  deleteHumanAgent);

//client assigned agent
router.get('/staff/agent', verifyClientOrHumanAgentToken, async(req,res)=>{
  try{
    const humanAgent = req.humanAgent;
    const clientId = humanAgent.clientId;
    
    // Check if human agent has assigned agents
    if (!humanAgent.agentIds || humanAgent.agentIds.length === 0) {
      return res.status(404).json({ 
        success: false, 
        error: 'No agents assigned to this human agent' 
      });
    }
    
    // Fetch the first assigned agent (assuming one agent per human agent for now)
    const agentId = humanAgent.agentIds[0];
    const agent = await Agent.findById(agentId);
    
    if (!agent) {
      return res.status(404).json({ 
        success: false, 
        error: 'Assigned agent not found' 
      });
    }
    
    res.json({success: true, data: agent});
  }
  catch(error){
    console.error('Error in /staff/agent:', error);
    res.status(500).json({ error: 'Failed to fetch agent' });
  }
});

// Get agent by ID (public route for mobile users)
router.get('/agents/:id/public', async (req, res) => {
  try {
    const { id } = req.params;
    const agent = await Agent.findById(id).select('-audioBytes');
    
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    const agentObj = agent.toObject();
    try {
      if (agentObj.uiImage && typeof agentObj.uiImage === 'string') {
        agentObj.uiImageUrl = await getobject(agentObj.uiImage);
      }
      if (agentObj.backgroundImage && typeof agentObj.backgroundImage === 'string') {
        agentObj.backgroundImageUrl = await getobject(agentObj.backgroundImage);
      }
    } catch (e) {
      console.warn('Failed to sign public customization URLs:', e.message);
    }

    res.json({ success: true, data: agentObj });
  } catch (error) {
    console.error('Error fetching agent by ID:', error);
    res.status(500).json({ error: 'Failed to fetch agent' });
  }
});

// Public route to fetch minimal client details for mobile display
router.get('/public/:clientId', async (req, res) => {
  try {
    const { clientId } = req.params;

    let client = null;
    // Try by Mongo _id if valid
    const mongoose = require('mongoose');
    if (mongoose.Types.ObjectId.isValid(clientId)) {
      client = await Client.findById(clientId).lean();
    }
    // Fallbacks for installations using different id fields
    if (!client) {
      client = await Client.findOne({ clientId: clientId }).lean();
    }
    if (!client) {
      client = await Client.findOne({ userId: clientId }).lean();
    }

    if (!client) {
      return res.status(404).json({ success: false, error: 'Client not found' });
    }

    let logoUrl = client.businessLogoUrl || null;
    try {
      if (!logoUrl && client.businessLogoKey) {
        logoUrl = await getobject(client.businessLogoKey);
      }
    } catch (e) {
      console.warn('Failed to sign client business logo URL:', e.message);
    }

    const minimal = {
      id: client._id,
      name: client.name || client.clientName || 'Client',
      email: client.email || null,
      businessName: client.businessName || null,
      businessLogoUrl: logoUrl,
      websiteUrl: client.websiteUrl || null,
    };

    return res.json({ success: true, data: minimal });
  } catch (error) {
    console.error('Error fetching public client details:', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch client details' });
  }
});

// Get agent by ID (authenticated route)
router.get('/agents/:id', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const query = req.clientId ? { _id: id, clientId: req.clientId } : { _id: id };
    const agent = await Agent.findOne(query).select('-audioBytes');
    
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    // Generate fresh signed URLs for customization images if keys are present
    const agentObj = agent.toObject();
    try {
      if (agentObj.uiImage && typeof agentObj.uiImage === 'string') {
        agentObj.uiImageUrl = await getobject(agentObj.uiImage);
      }
      if (agentObj.backgroundImage && typeof agentObj.backgroundImage === 'string') {
        agentObj.backgroundImageUrl = await getobject(agentObj.backgroundImage);
      }
    } catch (e) {
      console.warn('Failed to sign customization URLs:', e.message);
    }

    res.json({ success: true, data: agentObj });
  } catch (error) {
    console.error('Error fetching agent by ID:', error);
    res.status(500).json({ error: 'Failed to fetch agent' });
  }
});

// Get only agent name by ID (public, independent of clientId)
router.get('/agents/:id/name', async (req, res) => {
  try {
    const { id } = req.params;
    const agent = await Agent.findById(id).select('agentName name fullName email');

    if (!agent) {
      return res.status(404).json({ success: false, error: 'Agent not found' });
    }

    const name = agent.agentName || agent.name || agent.fullName || agent.email || '';
    return res.json({ success: true, data: { id, name } });
  } catch (error) {
    console.error('Error fetching agent name by ID:', error);
    return res.status(500).json({ success: false, error: 'Failed to fetch agent name' });
  }
});

// Get call logs by agent ID
router.get('/agents/:id/call-logs', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const { filter, startDate, endDate, page = 1, limit = 20 } = req.query;
    
    // Validate agent exists and belongs to client
    const agent = await Agent.findOne({ _id: id, clientId: req.clientId });
    if (!agent) {
      return res.status(404).json({ error: 'Agent not found' });
    }

    // Validate filter parameter
    const allowedFilters = ['today', 'yesterday', 'last7days', 'last30days'];
    if (filter && !allowedFilters.includes(filter) && (!startDate || !endDate)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid filter parameter',
        message: `Filter must be one of: ${allowedFilters.join(', ')} or provide both startDate and endDate`,
        allowedFilters: allowedFilters
      });
    }
    
    // Build date filter based on parameters
    let dateFilter = {};
    
    if (filter === 'today') {
      const today = new Date();
      const startOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate());
      const endOfDay = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfDay,
          $lte: endOfDay
        }
      };
    } else if (filter === 'yesterday') {
      const today = new Date();
      const yesterday = new Date(today.getTime() - (24 * 60 * 60 * 1000));
      const startOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate());
      const endOfYesterday = new Date(yesterday.getFullYear(), yesterday.getMonth(), yesterday.getDate(), 23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: startOfYesterday,
          $lte: endOfYesterday
        }
      };
    } else if (filter === 'last7days') {
      const today = new Date();
      const sevenDaysAgo = new Date(today.getTime() - (7 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: sevenDaysAgo,
          $lte: today
        }
      };
    } else if (filter === 'last30days') {
      const today = new Date();
      const thirtyDaysAgo = new Date(today.getTime() - (30 * 24 * 60 * 60 * 1000));
      dateFilter = {
        createdAt: {
          $gte: thirtyDaysAgo,
          $lte: today
        }
      };
    } else if (startDate && endDate) {
      const start = new Date(startDate);
      const end = new Date(endDate);
      end.setHours(23, 59, 59, 999);
      dateFilter = {
        createdAt: {
          $gte: start,
          $lte: end
        }
      };
    }
    
    // Build the complete query
    const query = { 
      clientId: req.clientId, 
      agentId: id, 
      ...dateFilter 
    };
    
    // Calculate pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);
    
    // Get total count for pagination
    const totalLogs = await CallLog.countDocuments(query);
    
    // Fetch call logs with pagination
    let logs = await CallLog.find(query)
      .sort({ time: -1 })
      .skip(skip)
      .limit(parseInt(limit))
      .populate('campaignId', 'name description')
      .lean();
    
    // Replace with backend proxy URL to avoid S3 CORS/signature issues
    // Always generate proxy URL if we have a callLogId, even if audioUrl is empty (audio might be available)
    try {
      const base = `${req.protocol}://${req.get('host')}`;
      logs = (logs || []).map((l) => {
        if (l._id) {
          // Generate proxy URL for all logs with an ID, regardless of whether audioUrl exists
          // The proxy route will check if audio actually exists when requested
          l.audioUrl = `${base}/api/v1/client/agents/${id}/call-audio?callLogId=${encodeURIComponent(l._id)}`;
        }
        return l;
      });
    } catch (_) {}
    
    // Calculate statistics
    const totalCalls = totalLogs;
    const totalConnected = logs.filter(l => l.leadStatus !== 'not_connected').length;
    const totalNotConnected = logs.filter(l => l.leadStatus === 'not_connected').length;
    const totalConversationTime = logs.reduce((sum, l) => sum + (l.duration || 0), 0);
    const avgCallDuration = totalCalls ? totalConversationTime / totalCalls : 0;
    
    // Group by lead status for detailed breakdown
    const leadStatusBreakdown = {
      veryInterested: logs.filter(l => l.leadStatus === 'vvi' || l.leadStatus === 'very_interested').length,
      maybe: logs.filter(l => l.leadStatus === 'maybe' || l.leadStatus === 'medium').length,
      enrolled: logs.filter(l => l.leadStatus === 'enrolled').length,
      junkLead: logs.filter(l => l.leadStatus === 'junk_lead').length,
      notRequired: logs.filter(l => l.leadStatus === 'not_required').length,
      enrolledOther: logs.filter(l => l.leadStatus === 'enrolled_other').length,
      decline: logs.filter(l => l.leadStatus === 'decline').length,
      notEligible: logs.filter(l => l.leadStatus === 'not_eligible').length,
      wrongNumber: logs.filter(l => l.leadStatus === 'wrong_number').length,
      hotFollowup: logs.filter(l => l.leadStatus === 'hot_followup').length,
      coldFollowup: logs.filter(l => l.leadStatus === 'cold_followup').length,
      schedule: logs.filter(l => l.leadStatus === 'schedule').length,
      notConnected: logs.filter(l => l.leadStatus === 'not_connected').length
    };
    
    res.json({ 
      success: true, 
      data: {
        agent: {
          _id: agent._id,
          agentName: agent.agentName,
          category: agent.category,
          personality: agent.personality
        },
        logs,
        statistics: {
          totalCalls,
          totalConnected,
          totalNotConnected,
          totalConversationTime,
          avgCallDuration,
          leadStatusBreakdown
        },
        pagination: {
          currentPage: parseInt(page),
          totalPages: Math.ceil(totalLogs / parseInt(limit)),
          totalLogs,
          hasNextPage: skip + logs.length < totalLogs,
          hasPrevPage: parseInt(page) > 1
        }
      },
      filter: {
        applied: filter || 'all',
        startDate: dateFilter.createdAt?.$gte,
        endDate: dateFilter.createdAt?.$lte
      }
    });
  } catch (error) {
    console.error('Error fetching agent call logs:', error);
    res.status(500).json({ error: 'Failed to fetch call logs' });
  }
});

// Public route for user registration (for mobile users)
router.post('/register-user', async (req, res) => {
  try {
    const { name, mobileNumber, email, clientId, sessionId } = req.body;
    
    if (!name || !mobileNumber || !clientId || !sessionId) {
      return res.status(400).json({ 
        success: false, 
        error: 'Name, mobile number, client ID, and session ID are required' 
      });
    }

    // Find existing user by sessionId
    let user = await User.findOne({ sessionId });
    
    if (user) {
      // Update existing user
      user.name = name;
      user.mobileNumber = mobileNumber;
      user.email = email || null;
      user.isRegistered = true;
      user.registrationAttempts = 0;
      user.lastRegistrationPrompt = null;
      await user.save();
    } else {
      // Create new user
      user = new User({
        clientId,
        name,
        mobileNumber,
        email: email || null,
        isRegistered: true,
        sessionId,
        conversations: []
      });
      await user.save();
    }

    res.json({ 
      success: true, 
      message: 'User registered successfully',
      data: {
        userId: user._id,
        name: user.name,
        mobileNumber: user.mobileNumber,
        isRegistered: user.isRegistered
      }
    });
  } catch (error) {
    console.error('Error registering user:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to register user' 
    });
  }
});
// Public route to get user by session ID
router.get('/user/:sessionId', async (req, res) => {
  try {
    const { sessionId } = req.params;
    const user = await User.findOne({ sessionId });
    
    if (!user) {
      return res.status(404).json({ 
        success: false, 
        error: 'User not found' 
      });
    }

    res.json({ 
      success: true, 
      data: {
        userId: user._id,
        name: user.name,
        mobileNumber: user.mobileNumber,
        isRegistered: user.isRegistered,
        clientId: user.clientId
      }
    });
  } catch (error) {
    console.error('Error fetching user:', error);
    res.status(500).json({ 
      success: false, 
      error: 'Failed to fetch user' 
    });
  }
});

// Add this route to your client routes file (usually clientroutes.js)

// Toggle agent active status
router.patch('/agents/:agentId/toggle-active', async (req, res) => {
  try {
    const { agentId } = req.params;
    const { isActive } = req.body;
    const clientId = req.query.clientId;

    // Validate required fields
    if (!agentId) {
      return res.status(400).json({
        success: false,
        message: 'Agent ID is required'
      });
    }

    if (!clientId) {
      return res.status(400).json({
        success: false,
        message: 'Client ID is required'
      });
    }

    if (typeof isActive !== 'boolean') {
      return res.status(400).json({
        success: false,
        message: 'isActive must be a boolean value'
      });
    }

    // Find the agent first
    const current = await Agent.findOne({ _id: agentId, clientId: clientId });
    if (!current) {
      return res.status(404).json({
        success: false,
        message: 'Agent not found or you do not have permission to modify this agent'
      });
    }

    // If activating, deactivate others first to satisfy unique index
    if (isActive === true && current.accountSid) {
      await Agent.updateMany(
        {
          _id: { $ne: current._id },
          clientId: clientId,
          accountSid: current.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      );
    }

    // Now update this agent's active status
    const agent = await Agent.findOneAndUpdate(
      { _id: agentId, clientId: clientId },
      { isActive: isActive, updatedAt: new Date() },
      { new: true, runValidators: true }
    );

    if (!agent) {
      return res.status(404).json({
        success: false,
        message: 'Agent not found or you do not have permission to modify this agent'
      });
    }

    console.log(`✅ [AGENT-TOGGLE] Agent ${agent.agentName} (${agentId}) ${isActive ? 'activated' : 'deactivated'} by client ${clientId}`);

    // If activating this agent and it has accountSid, deactivate others with same (clientId, accountSid)
    if (agent.isActive && agent.accountSid) {
      await Agent.updateMany(
        {
          _id: { $ne: agent._id },
          clientId: clientId,
          accountSid: agent.accountSid,
          isActive: true,
        },
        { $set: { isActive: false, updatedAt: new Date() } }
      )
    }

    res.json({
      success: true,
      message: `Agent ${isActive ? 'activated' : 'deactivated'} successfully`,
      data: {
        agentId: agent._id,
        agentName: agent.agentName,
        isActive: agent.isActive,
        updatedAt: agent.updatedAt
      }
    });

  } catch (error) {
    console.error('❌ [AGENT-TOGGLE] Error toggling agent status:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to toggle agent status',
      error: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
    });
  }
});

// MANUAL: Trigger immediate status update for testing (optional)
router.post('/campaigns/:id/trigger-status-update', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    // Trigger immediate status update for this campaign
    await triggerManualStatusUpdate(campaign._id);
    
    res.json({
      success: true,
      message: 'Manual status update triggered successfully',
      data: { campaignId: campaign._id, timestamp: new Date() }
    });

  } catch (error) {
    console.error('Error triggering manual status update:', error);
    res.status(500).json({ success: false, error: 'Failed to trigger status update' });
  }
});

// FORCE: Save current run to CampaignHistory immediately
router.post('/campaigns/:id/force-save-history', extractClientId, async (req, res) => {
  try {
    const { id } = req.params;
    const { runId: providedRunId, startTime, endTime } = req.body || {};

    const campaign = await Campaign.findOne({ _id: id, clientId: req.clientId });
    if (!campaign) {
      return res.status(404).json({ success: false, error: 'Campaign not found' });
    }

    const details = Array.isArray(campaign.details) ? campaign.details.filter(Boolean) : [];
    if (details.length === 0) {
      return res.status(400).json({ success: false, error: 'No call details to save' });
    }

    // Infer runId if not provided: pick the most frequent runId in details
    let runId = providedRunId;
    if (!runId) {
      const freq = new Map();
      for (const d of details) {
        if (d && d.runId) freq.set(d.runId, (freq.get(d.runId) || 0) + 1);
      }
      runId = [...freq.entries()].sort((a, b) => b[1] - a[1])[0]?.[0];
    }
    if (!runId) {
      return res.status(400).json({ success: false, error: 'runId not found in details and not provided' });
    }

    // Filter to this run's details
    const runDetails = details.filter(d => d && d.runId === runId);
    if (runDetails.length === 0) {
      return res.status(400).json({ success: false, error: 'No details found for given runId' });
    }

    // Build contacts array for history
    const contactsById = new Map((campaign.contacts || []).map(c => [String(c._id || ''), c]));
    const contacts = runDetails.map(d => {
      const contact = d.contactId ? contactsById.get(String(d.contactId)) : undefined;
      return {
        documentId: d.uniqueId,
        number: contact?.phone || contact?.number || '',
        name: contact?.name || '',
        leadStatus: (d.leadStatus || (d.status === 'completed' ? 'connected' : 'not_connected')),
        contactId: String(d.contactId || ''),
        time: (d.time instanceof Date ? d.time.toISOString() : (d.time || new Date()).toString()),
        status: d.status || 'completed',
        duration: typeof d.callDuration === 'number' ? d.callDuration : 0,
        transcriptCount: 0,
        whatsappMessageSent: false,
        whatsappRequested: false
      };
    });

    // Compute stats
    const totalContacts = contacts.length;
    const successfulCalls = contacts.filter(c => String(c.leadStatus || '').toLowerCase() === 'connected').length;
    const failedCalls = totalContacts - successfulCalls;
    const totalCallDuration = contacts.reduce((s, c) => s + (c.duration || 0), 0);
    const averageCallDuration = totalContacts > 0 ? Math.round(totalCallDuration / totalContacts) : 0;

    const CampaignHistory = require('../models/CampaignHistory');
    const existing = await CampaignHistory.findOne({ runId }).lean();
    let instanceNumber = existing?.instanceNumber;
    if (typeof instanceNumber !== 'number') {
      const count = await CampaignHistory.countDocuments({ campaignId: id });
      instanceNumber = count + 1;
    }

    const end = endTime ? new Date(endTime) : new Date();
    let start = startTime ? new Date(startTime) : (runDetails[0]?.time ? new Date(runDetails[0].time) : new Date(end.getTime() - 1000));
    if (!(start instanceof Date) || isNaN(start)) start = new Date(end.getTime() - 1000);
    const elapsedSec = Math.max(0, Math.floor((end - start) / 1000));
    const toHms = (secs) => ({ hours: Math.floor(secs / 3600), minutes: Math.floor((secs % 3600) / 60), seconds: secs % 60 });

    const doc = await CampaignHistory.findOneAndUpdate(
      { runId },
      {
        $setOnInsert: {
          campaignId: campaign._id,
          runId,
          instanceNumber,
          startTime: start.toISOString(),
        },
        $set: {
          endTime: end.toISOString(),
          runTime: toHms(elapsedSec),
          status: 'completed',
          contacts,
          stats: { totalContacts, successfulCalls, failedCalls, totalCallDuration, averageCallDuration },
          batchInfo: { isIntermediate: false }
        }
      },
      { upsert: true, new: true }
    );

    // Also ensure campaign is not running anymore
    if (campaign.isRunning) {
      campaign.isRunning = false;
      await campaign.save();
    }

    return res.json({ success: true, message: 'Campaign history saved', data: { runId: doc.runId, instanceNumber: doc.instanceNumber, totals: doc.stats } });
  } catch (error) {
    console.error('❌ [FORCE-SAVE-HISTORY] Error:', error);
    return res.status(500).json({ success: false, error: error.message });
  }
});

// DEBUG: Check call status for debugging
router.get('/debug/call-status/:uniqueId', extractClientId, async (req, res) => {
  try {
    const { uniqueId } = req.params;
    
    // Debug the call status
    const debugInfo = await debugCallStatus(uniqueId);
    
    if (debugInfo) {
      res.json({
        success: true,
        message: 'Call status debug information',
        data: debugInfo
      });
    } else {
      res.status(404).json({
        success: false,
        error: 'Call not found or debug failed'
      });
    }

  } catch (error) {
    console.error('Error debugging call status:', error);
    res.status(500).json({ success: false, error: 'Failed to debug call status' });
  }
});

// MIGRATION: Manually trigger migration from 'missed' to 'completed'
router.post('/migrate/missed-to-completed', extractClientId, async (req, res) => {
  try {
    // Run the migration
    await migrateMissedToCompleted();
    
    res.json({
      success: true,
      message: 'Migration from missed to completed completed successfully'
    });

  } catch (error) {
    console.error('Error running migration:', error);
    res.status(500).json({ success: false, error: 'Failed to run migration' });
  }
});

// MANUAL: Trigger immediate status update for all campaigns
router.post('/trigger-status-update', extractClientId, async (req, res) => {
  try {
    console.log('🔧 MANUAL: Triggering immediate status update for all campaigns...');
    
    // Trigger manual status update
    await triggerManualStatusUpdate();
    
    res.json({
      success: true,
      message: 'Manual status update triggered successfully'
    });

  } catch (error) {
    console.error('Error triggering manual status update:', error);
    res.status(500).json({ success: false, error: 'Failed to trigger status update' });
  }
});

// DEBUG: Check campaigns with active calls
router.get('/debug/active-campaigns', extractClientId, async (req, res) => {
  try {
    const Campaign = require('../models/Campaign');
    
    // Find all campaigns with ringing or ongoing calls
    const campaigns = await Campaign.find({
      'details.status': { $in: ['ringing', 'ongoing'] }
    }).lean();
    
    const activeCalls = [];
    
    for (const campaign of campaigns) {
      const calls = campaign.details.filter(d => d.status === 'ringing' || d.status === 'ongoing');
      calls.forEach(call => {
        activeCalls.push({
          campaignId: campaign._id,
          campaignName: campaign.name,
          uniqueId: call.uniqueId,
          status: call.status,
          timeSinceInitiation: Math.floor((new Date() - call.time) / 1000)
        });
      });
    }
    
    res.json({
      success: true,
      message: `Found ${activeCalls.length} active calls in ${campaigns.length} campaigns`,
      data: {
        totalCampaigns: campaigns.length,
        totalActiveCalls: activeCalls.length,
        activeCalls
      }
    });

  } catch (error) {
    console.error('Error checking active campaigns:', error);
    res.status(500).json({ success: false, error: 'Failed to check active campaigns' });
  }
});

// DEBUG: Test Cashfree configuration
router.get('/debug/cashfree-config', async (req, res) => {
  try {
    const config = {
      env: CashfreeConfig.ENV,
      baseUrl: CashfreeConfig.BASE_URL,
      hasClientId: !!CashfreeConfig.CLIENT_ID,
      hasClientSecret: !!CashfreeConfig.CLIENT_SECRET,
      returnUrl: CashfreeConfig.RETURN_URL,
      clientIdLength: CashfreeConfig.CLIENT_ID ? CashfreeConfig.CLIENT_ID.length : 0,
      clientSecretLength: CashfreeConfig.CLIENT_SECRET ? CashfreeConfig.CLIENT_SECRET.length : 0
    };
    
    // Test Cashfree API connectivity
    let apiTest = { success: false, error: null };
    if (CashfreeConfig.CLIENT_ID && CashfreeConfig.CLIENT_SECRET) {
      try {
        const axios = require('axios');
        const headers = {
          'x-client-id': CashfreeConfig.CLIENT_ID,
          'x-client-secret': CashfreeConfig.CLIENT_SECRET,
          'x-api-version': '2022-09-01'
        };
        
        // Test with a simple API call (get order details for a test order)
        const testOrderId = 'TEST_ORDER_' + Date.now();
        const response = await axios.get(`${CashfreeConfig.BASE_URL}/pg/orders/${testOrderId}`, { headers });
        apiTest = { success: true, status: response.status };
      } catch (apiError) {
        apiTest = { 
          success: false, 
          error: apiError.message,
          status: apiError.response?.status,
          data: apiError.response?.data
        };
      }
    }
    
    res.json({
      success: true,
      message: 'Cashfree configuration status',
      data: { ...config, apiTest }
    });
  } catch (error) {
    console.error('Error checking Cashfree config:', error);
    res.status(500).json({ success: false, error: 'Failed to check Cashfree config' });
  }
});

// DEBUG: Test session ID cleaning
router.get('/debug/session-id-test', async (req, res) => {
  try {
    const testSessionIds = [
      'A2Qd_bjinlFffRyQv_VrCpBBq9BsHphuHkiJk32Y1-f8j5aS76SVxj1f5drbKHa4Y4ZqS_yfThywMs9iVS29u-S-b6mQ-JIzKeUtu1Gi0wdOR8WinFE8AaOWedYpayment',
      'session_A2Qd_bjinlFffRyQv_VrCpBBq9BsHphuHkiJk32Y1-f8j5aS76SVxj1f5drbKHa4Y4ZqS_yfThywMs9iVS29u-S-b6mQ-JIzKeUtu1Gi0wdOR8WinFE8AaOWedYpayment',
      'A2Qd_bjinlFffRyQv_VrCpBBq9BsHphuHkiJk32Y1-f8j5aS76SVxj1f5drbKHa4Y4ZqS_yfThywMs9iVS29u-S-b6mQ-JIzKeUtu1Gi0wdOR8WinFE8AaOWedYpaymentpayment',
      'session_Hwy4YTM7uaVyBVyocz3_qjmXYqd3K--eUXcP6PkojjRtU95L6eQD6hYp95qGyifvAb7U7jZy0kSrp2ZWukiH36kwZNKI5NKXLfQhtdEds2J5zgva2vqfJy1aKLopayment',
      'session_BNLJhn4eS7Y1FSsVlRxUOaINuUy7xJJEw_nyXuIgHMHa_4A7v7c-9maJ5XQ7gh-5sX6rgO5i5zLwWY9L1D4NFZF1mddRrcTd4Cn492yrIFxqzNqpqmZ2Xptndmopayment',
      'normal_session_id_12345',
      'session_with_underscores_and_dashes-123'
    ];
    
    const results = testSessionIds.map(originalId => {
      let sessionId = String(originalId);
      
      // Clean the session ID - remove any invalid characters and ensure it's properly formatted
      sessionId = sessionId.replace(/[^a-zA-Z0-9_-]/g, '');
      
      // Remove the problematic 'payment' suffix that Cashfree sometimes adds
      sessionId = sessionId.replace(/payment$/i, '');
      sessionId = sessionId.replace(/paymentpayment$/i, '');
      
      // Remove any other problematic suffixes that might cause issues
      sessionId = sessionId.replace(/session$/i, '');
      sessionId = sessionId.replace(/order$/i, '');
      
      // Clean up any double underscores or dashes that might have been created
      sessionId = sessionId.replace(/_{2,}/g, '_');
      sessionId = sessionId.replace(/-{2,}/g, '-');
      
      // Remove leading/trailing underscores and dashes
      sessionId = sessionId.replace(/^[_-]+/, '').replace(/[_-]+$/, '');
      
      return {
        original: originalId,
        cleaned: sessionId,
        isValid: sessionId.length >= 10 && /^[a-zA-Z0-9_-]+$/.test(sessionId),
        length: sessionId.length
      };
    });
    
    res.json({
      success: true,
      message: 'Session ID cleaning test results',
      data: results
    });
  } catch (error) {
    console.error('Error testing session ID cleaning:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to test session ID cleaning',
      error: error.message
    });
  }
});

// DEBUG: Test payment link API endpoints
router.get('/debug/test-payment-link-endpoints', async (req, res) => {
  try {
    const orderId = 'AITOTA_1756232842240'; // The order that failed
    
    if (!CashfreeConfig.CLIENT_ID || !CashfreeConfig.CLIENT_SECRET) {
      return res.status(500).json({
        success: false,
        message: 'Cashfree configuration missing'
      });
    }

    const axios = require('axios');
    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2023-08-01',
      'Content-Type': 'application/json'
    };
    
    const results = [];
    
    // Test 1: Get order details
    try {
      console.log('Test 1: Getting order details');
      const orderResp = await axios.get(`${CashfreeConfig.BASE_URL}/pg/orders/${orderId}`, { headers });
      results.push({
        test: 'Get Order Details',
        success: true,
        data: orderResp.data
      });
    } catch (error) {
      results.push({
        test: 'Get Order Details',
        success: false,
        error: error.message,
        response: error.response?.data
      });
    }
    
    // Test 2: Create payment link using /payments endpoint
    try {
      console.log('Test 2: Creating payment link using /payments endpoint');
      const paymentResp = await axios.post(
        `${CashfreeConfig.BASE_URL}/pg/orders/${orderId}/payments`,
        {
          payment_method: {
            upi: { enabled: true },
            card: { enabled: true },
            netbanking: { enabled: true },
            app: { enabled: true }
          },
          order_meta: {
            return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
          }
        },
        { headers }
      );
      results.push({
        test: 'Create Payment Link (/payments)',
        success: true,
        data: paymentResp.data
      });
    } catch (error) {
      results.push({
        test: 'Create Payment Link (/payments)',
        success: false,
        error: error.message,
        response: error.response?.data
      });
    }
    
    // Test 3: Create payment link using /payment-links endpoint
    try {
      console.log('Test 3: Creating payment link using /payment-links endpoint');
      const linkResp = await axios.post(
        `${CashfreeConfig.BASE_URL}/pg/payment-links`,
        {
          order_id: orderId,
          payment_method: {
            upi: { enabled: true },
            card: { enabled: true },
            netbanking: { enabled: true },
            app: { enabled: true }
          },
          order_meta: {
            return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
          }
        },
        { headers }
      );
      results.push({
        test: 'Create Payment Link (/payment-links)',
        success: true,
        data: linkResp.data
      });
    } catch (error) {
      results.push({
        test: 'Create Payment Link (/payment-links)',
        success: false,
        error: error.message,
        response: error.response?.data
      });
    }
    
    res.json({
      success: true,
      message: 'Payment link endpoint tests completed',
      data: results
    });
  } catch (error) {
    console.error('Error testing payment link endpoints:', error.message);
    res.status(500).json({
      success: false,
      message: 'Failed to test payment link endpoints',
      error: error.message
    });
  }
});

// DEBUG: Test payment link creation for current order
router.get('/debug/test-current-payment-link', async (req, res) => {
  try {
    const orderId = req.query.orderId || 'AITOTA_1756233688485'; // Use the current order ID
    
    if (!CashfreeConfig.CLIENT_ID || !CashfreeConfig.CLIENT_SECRET) {
      return res.status(500).json({
        success: false,
        message: 'Cashfree configuration missing'
      });
    }

    const axios = require('axios');
    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2023-08-01',
      'Content-Type': 'application/json'
    };
    
    console.log('Testing payment link creation for order:', orderId);
    
    const paymentLinkPayload = {
      order_id: orderId,
      payment_method: {
        upi: { enabled: true },
        card: { enabled: true },
        netbanking: { enabled: true },
        app: { enabled: true },
        paylater: { enabled: true }
      },
      order_meta: {
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`,
        notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
      }
    };
    
    console.log('Payment link payload:', JSON.stringify(paymentLinkPayload, null, 2));
    
    const response = await axios.post(
      `${CashfreeConfig.BASE_URL}/pg/payment-links`,
      paymentLinkPayload,
      { 
        headers,
        timeout: 30000
      }
    );
    
    const data = response.data || {};
    console.log('Payment link response:', JSON.stringify(data, null, 2));
    
    res.json({
      success: true,
      message: 'Payment link test completed',
      data: {
        orderId: orderId,
        response: data,
        hasPaymentLink: !!data.payment_link,
        paymentLink: data.payment_link
      }
    });
  } catch (error) {
    console.error('Payment link test failed:', error.message);
    res.status(500).json({
      success: false,
      message: 'Failed to test payment link creation',
      error: error.message,
      response: error.response?.data
    });
  }
});

// DEBUG: Test specific failed order
router.get('/debug/test-failed-order', async (req, res) => {
  try {
    const orderId = 'AITOTA_1756232359917'; // The order that failed
    
    if (!CashfreeConfig.CLIENT_ID || !CashfreeConfig.CLIENT_SECRET) {
      return res.status(500).json({
        success: false,
        message: 'Cashfree configuration missing'
      });
    }

    const axios = require('axios');
    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2023-08-01', // Use latest API version
      'Content-Type': 'application/json'
    };
    
    // Try to create payment link for the failed order
    console.log('Testing payment link creation for failed order:', orderId);
    
    const paymentLinkPayload = {
      payment_method: {
        upi: { enabled: true },
        card: { enabled: true },
        netbanking: { enabled: true },
        app: { enabled: true },
        paylater: { enabled: true }
      },
      order_meta: {
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`,
        notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
      }
    };
    
    console.log('Payment link payload:', JSON.stringify(paymentLinkPayload, null, 2));
    
    const response = await axios.post(
      `${CashfreeConfig.BASE_URL}/pg/orders/${orderId}/payments`,
      paymentLinkPayload,
      { headers }
    );
    
    const data = response.data || {};
    console.log('Payment link response:', JSON.stringify(data, null, 2));
    
    res.json({
      success: true,
      message: 'Test completed for failed order',
      data: {
        orderId: orderId,
        response: data,
        hasPaymentLink: !!data.payment_link,
        paymentLink: data.payment_link
      }
    });
  } catch (error) {
    console.error('Error testing failed order:', error.message);
    res.status(500).json({
      success: false,
      message: 'Failed to test failed order',
      error: error.message,
      response: error.response?.data
    });
  }
});
// DEBUG: Test payment link creation with latest API
router.get('/debug/cashfree-payment-link-test', async (req, res) => {
  try {
    if (!CashfreeConfig.CLIENT_ID || !CashfreeConfig.CLIENT_SECRET) {
      return res.status(500).json({
        success: false,
        message: 'Cashfree configuration missing'
      });
    }

    const axios = require('axios');
    const orderId = `TEST_LINK_${Date.now()}`;
    
    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2023-08-01', // Use latest API version
      'Content-Type': 'application/json'
    };
    
    // Step 1: Create order
    const orderPayload = {
      order_id: orderId,
      order_amount: 1.00,
      order_currency: 'INR',
      customer_details: {
        customer_id: 'test_customer',
        customer_email: 'test@example.com',
        customer_phone: '9999999999',
        customer_name: 'Test Customer'
      },
      order_meta: {
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`,
        notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
      }
    };

    console.log('Creating test order with payload:', JSON.stringify(orderPayload, null, 2));
    
    const orderResponse = await axios.post(`${CashfreeConfig.BASE_URL}/pg/orders`, orderPayload, { headers });
    const orderData = orderResponse.data || {};
    
    console.log('Order created successfully:', JSON.stringify(orderData, null, 2));
    
          // Step 2: Create payment link
      if (orderData.order_id) {
        const paymentLinkPayload = {
          payment_method: {
            upi: { enabled: true },
            card: { enabled: true },
            netbanking: { enabled: true },
            app: { enabled: true },
            paylater: { enabled: true }
          },
          order_meta: {
            return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`,
            notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
          }
        };
      
      console.log('Creating payment link with payload:', JSON.stringify(paymentLinkPayload, null, 2));
      
      const linkResponse = await axios.post(
        `${CashfreeConfig.BASE_URL}/pg/orders/${orderId}/payments`,
        paymentLinkPayload,
        { headers }
      );
      
      const linkData = linkResponse.data || {};
      console.log('Payment link created successfully:', JSON.stringify(linkData, null, 2));
      
      res.json({
        success: true,
        message: 'Payment link test completed successfully',
        data: {
          orderId: orderId,
          orderResponse: orderData,
          paymentLinkResponse: linkData,
          hasPaymentLink: !!linkData.payment_link,
          paymentLink: linkData.payment_link
        }
      });
    } else {
      res.status(500).json({
        success: false,
        message: 'Failed to create order for payment link test',
        data: orderData
      });
    }
  } catch (error) {
    console.error('Error in payment link test:', error.message);
    res.status(500).json({
      success: false,
      message: 'Failed to test payment link creation',
      error: error.message,
      response: error.response?.data
    });
  }
});

// DEBUG: Test Cashfree order creation
router.get('/debug/cashfree-test-order', async (req, res) => {
  try {
    if (!CashfreeConfig.CLIENT_ID || !CashfreeConfig.CLIENT_SECRET) {
      return res.status(500).json({
        success: false,
        message: 'Cashfree configuration missing'
      });
    }

    const axios = require('axios');
    const orderId = `TEST_${Date.now()}`;
    
    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2022-09-01',
      'Content-Type': 'application/json'
    };
    
    const payload = {
      order_id: orderId,
      order_amount: 1.00,
      order_currency: 'INR',
      customer_details: {
        customer_id: 'test_customer',
        customer_email: 'test@example.com',
        customer_phone: '9999999999',
        customer_name: 'Test Customer'
      },
      order_meta: {
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
      }
    };

    console.log('Creating test Cashfree order with payload:', JSON.stringify(payload, null, 2));
    
    const response = await axios.post(`${CashfreeConfig.BASE_URL}/pg/orders`, payload, { headers });
    const data = response.data || {};
    
    console.log('Test order created successfully:', JSON.stringify(data, null, 2));
    
    res.json({
      success: true,
      message: 'Test order created successfully',
      data: {
        orderId: orderId,
        cashfreeResponse: data,
        hasPaymentLink: !!data.payment_link,
        hasPaymentSessionId: !!data.payment_session_id
      }
    });
  } catch (error) {
    console.error('Error creating test Cashfree order:', error.message);
    res.status(500).json({
      success: false,
      message: 'Failed to create test order',
      error: error.message,
      response: error.response?.data
    });
  }
});

// Plan and Credit Routes for Clients
router.get('/plans',  async (req, res) => {
  try {
    const Plan = require('../models/Plan');
    const plans = await Plan.getActivePlans();
    
    res.json({
      success: true,
      data: plans
    });
  } catch (error) {
    console.error('Error fetching plans:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch plans'
    });
  }
});

router.get('/plans/popular',  async (req, res) => {
  try {
    const Plan = require('../models/Plan');
    const plans = await Plan.getPopularPlans();
    
    res.json({
      success: true,
      data: plans
    });
  } catch (error) {
    console.error('Error fetching popular plans:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch popular plans'
    });
  }
});

router.get('/credits/balance', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { getClientBalance } = require('../controllers/creditController');
    req.params.clientId = req.clientId;
    return getClientBalance(req, res);
  } catch (error) {
    console.error('Error fetching credit balance:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch credit balance'
    });
  }
});

router.get('/credits/history', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { getCreditHistoryOptimized } = require('../controllers/creditController');
    req.params.clientId = req.clientId;
    return getCreditHistoryOptimized(req, res);
  } catch (error) {
    console.error('Error fetching credit history:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch credit history'
    });
  }
});

// New dedicated payment history API
router.get('/credits/payment-history', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { getPaymentHistory } = require('../controllers/creditController');
    req.params.clientId = req.clientId;
    return getPaymentHistory(req, res);
  } catch (error) {
    console.error('Error fetching payment history:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to fetch payment history'
    });
  }
});

// GET /api/v1/client/call-logs/transcript/:uniqueId
router.get('/call-logs/transcript/:uniqueId', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const CallLog = require('../models/CallLog');
    const { uniqueId } = req.params;
    const clientId = req.clientId;
    
    if (!uniqueId) {
      return res.status(400).json({
        success: false,
        message: 'Unique ID is required'
      });
    }
    
    // Find call log by uniqueId and clientId
    const callLog = await CallLog.findOne({ 
      'metadata.uniqueid': uniqueId,
      clientId: clientId 
    }).lean();
    
    if (!callLog) {
      return res.status(404).json({
        success: false,
        message: 'Call log not found'
      });
    }
    
    return res.json({
      success: true,
      data: {
        transcript: callLog.transcript || '',
        duration: callLog.duration || 0,
        mobile: callLog.mobile || '',
        callDirection: callLog.metadata?.callDirection || 'unknown',
        timestamp: callLog.time || callLog.createdAt
      }
    });
  } catch (error) {
    console.error('Error fetching call transcript:', error);
    return res.status(500).json({
      success: false,
      message: 'Failed to fetch call transcript',
      error: error.message
    });
  }
});

router.post('/plans/purchase', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { planId, billingCycle, couponCode, autoRenew } = req.body;
    const clientId = req.clientId;
    
    if (!planId || !billingCycle) {
      return res.status(400).json({
        success: false,
        message: 'Plan ID and billing cycle are required'
      });
    }
    
    const Plan = require('../models/Plan');
    const Credit = require('../models/Credit');
    const Coupon = require('../models/Coupon');
    
    // Get plan
    const plan = await Plan.findById(planId);
    if (!plan || !plan.isActive) {
      return res.status(404).json({
        success: false,
        message: 'Plan not found or inactive'
      });
    }
    
    // Get or create credit record
    let creditRecord = await Credit.getOrCreateCreditRecord(clientId);
    
    // Calculate price
    let finalPrice = plan.price;
    let discountApplied = 0;
    let couponUsed = null;
    
    // Apply billing cycle discount
    const cycleDiscount = plan.discounts[`${billingCycle}Discount`] || 0;
    if (cycleDiscount > 0) {
      discountApplied = (finalPrice * cycleDiscount) / 100;
      finalPrice -= discountApplied;
    }
    
    // Apply coupon if provided
    if (couponCode) {
      const coupon = await Coupon.findValidCoupon(couponCode);
      if (coupon && coupon.appliesToPlan(planId, plan.category)) {
        const couponDiscount = coupon.calculateDiscount(finalPrice);
        finalPrice -= couponDiscount;
        discountApplied += couponDiscount;
        couponUsed = coupon.code;
      }
    }
    
    // Calculate credits to add
    const creditsToAdd = plan.creditsIncluded + plan.bonusCredits;
    
    // Add credits to client account
    await creditRecord.addCredits(
      creditsToAdd,
      'purchase',
      `Plan purchase: ${plan.name} (${billingCycle})`,
      planId,
      `TXN_${Date.now()}`
    );
    
    // Update current plan information
    const startDate = new Date();
    let endDate = new Date();
    
    switch (billingCycle) {
      case 'monthly':
        endDate.setMonth(endDate.getMonth() + 1);
        break;
      case 'quarterly':
        endDate.setMonth(endDate.getMonth() + 3);
        break;
      case 'yearly':
        endDate.setFullYear(endDate.getFullYear() + 1);
        break;
    }
    
    creditRecord.currentPlan = {
      planId: planId,
      startDate: startDate,
      endDate: endDate,
      billingCycle: billingCycle,
      autoRenew: autoRenew || false
    };
    
    await creditRecord.save();
    
    // Apply coupon usage if used
    if (couponUsed) {
      const coupon = await Coupon.findValidCoupon(couponCode);
      if (coupon) {
        await coupon.applyCoupon(clientId, planId, plan.price);
      }
    }
    
    res.json({
      success: true,
      message: 'Plan purchased successfully',
      data: {
        plan: plan.name,
        creditsAdded: creditsToAdd,
        price: plan.price,
        discountApplied: discountApplied,
        finalPrice: finalPrice,
        billingCycle: billingCycle,
        startDate: startDate,
        endDate: endDate,
        couponUsed: couponUsed,
        newBalance: creditRecord.currentBalance
      }
    });
  } catch (error) {
    console.error('Error purchasing plan:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to purchase plan'
    });
  }
});

router.post('/coupons/validate',  async (req, res) => {
  try {
    const { couponCode, planId } = req.body;
    const clientId = req.clientId;
    
    if (!couponCode || !planId) {
      return res.status(400).json({
        success: false,
        message: 'Coupon code and plan ID are required'
      });
    }
    
    const Coupon = require('../models/Coupon');
    const Plan = require('../models/Plan');
    
    const coupon = await Coupon.findValidCoupon(couponCode);
    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: 'Invalid or expired coupon'
      });
    }
    
    const plan = await Plan.findById(planId);
    if (!plan) {
      return res.status(404).json({
        success: false,
        message: 'Plan not found'
      });
    }
    
    // Check if coupon applies to plan
    if (!coupon.appliesToPlan(planId, plan.category)) {
      return res.status(400).json({
        success: false,
        message: 'Coupon not applicable to this plan'
      });
    }
    
    // Check if user can use coupon
    const canUse = await coupon.canBeUsedBy(clientId);
    if (!canUse.valid) {
      return res.status(400).json({
        success: false,
        message: canUse.reason
      });
    }
    
    const discount = coupon.calculateDiscount(plan.price);
    
    res.json({
      success: true,
      message: 'Coupon is valid',
      data: {
        coupon: {
          code: coupon.code,
          name: coupon.name,
          description: coupon.description,
          discountType: coupon.discountType,
          discountValue: coupon.discountValue
        },
        discount: discount,
        finalPrice: plan.price - discount
      }
    });
  } catch (error) {
    console.error('Error validating coupon:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to validate coupon'
    });
  }
});

router.put('/credits/settings', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { lowBalanceAlert, autoPurchase } = req.body;
    const clientId = req.clientId;
    
    const Credit = require('../models/Credit');
    const creditRecord = await Credit.findOne({ clientId });
    
    if (!creditRecord) {
      return res.status(404).json({
        success: false,
        message: 'Credit record not found'
      });
    }
    
    if (lowBalanceAlert) {
      creditRecord.settings.lowBalanceAlert = {
        ...creditRecord.settings.lowBalanceAlert,
        ...lowBalanceAlert
      };
    }
    
    if (autoPurchase) {
      creditRecord.settings.autoPurchase = {
        ...creditRecord.settings.autoPurchase,
        ...autoPurchase
      };
    }
    
    await creditRecord.save();
    
    res.json({
      success: true,
      message: 'Credit settings updated successfully',
      data: creditRecord.settings
    });
  } catch (error) {
    console.error('Error updating credit settings:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to update credit settings'
    });
  }
});

// Confirm Paytm payment and credit static plan
router.post('/credits/paytm/confirm', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { orderId, planKey } = req.body;
    const clientId = req.clientId;

    if (!orderId || !planKey) {
      return res.status(400).json({ success: false, message: 'orderId and planKey are required' });
    }

    const Credit = require('../models/Credit');
    const creditRecord = await Credit.getOrCreateCreditRecord(clientId);

    // Idempotency: don't apply if this orderId already in history
    const alreadyApplied = (creditRecord.history || []).some(h => h.transactionId === String(orderId));
    if (alreadyApplied) {
      return res.json({ success: true, message: 'Payment already applied', data: { balance: creditRecord.currentBalance } });
    }

    const mapping = {
      basic: { credits: 1000, bonus: 0, price: 1000 },
      professional: { credits: 5000, bonus: 500, price: 5000 },
      enterprise: { credits: 10000, bonus: 1000, price: 10000 },
    };

    const key = String(planKey).toLowerCase();
    const plan = mapping[key];
    if (!plan) {
      return res.status(400).json({ success: false, message: 'Invalid planKey' });
    }

    const totalCredits = plan.credits + (plan.bonus || 0);

    await creditRecord.addCredits(totalCredits, 'purchase', `Paytm order ${orderId} • ${key} plan`, null, String(orderId));

    return res.json({ success: true, message: 'Credits added', data: { balance: creditRecord.currentBalance, added: totalCredits } });
  } catch (error) {
    console.error('Error confirming Paytm payment:', error);
    res.status(500).json({ success: false, message: 'Failed to confirm payment' });
  }
});

// Initiate Paytm payment from backend and handle redirect server-side
router.post('/payments/initiate', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { amount, customerEmail, customerPhone, customerName, planKey } = req.body || {};
    if (!amount) {
      return res.status(400).json({ success: false, message: 'amount is required' });
    }

    // Fallback billing data from client profile if not provided
    let email = customerEmail;
    let phone = customerPhone;
    let name = customerName;
    try {
      const Client = require('../models/Client');
      const client = await Client.findById(req.clientId);
      if (client) {
        if (!email) email = client.email;
        if (!phone) phone = client.mobileNo;
        if (!name) name = client.name;
      }
    } catch {}

    // Final fallbacks
    if (!email) email = 'client@example.com';
    if (!phone) phone = '9999999999';
    if (!name) name = 'Client';

    // Call external Paytm gateway API
    const axios = require('axios');
    const gatewayBase = 'https://paytm-gateway-n0py.onrender.com';
    const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
    const payload = {
      amount,
      customerEmail: email,
      customerPhone: phone,
      customerName: name,
      projectId: 'aitota-pricing',
      redirectUrl: `${FRONTEND_URL}/auth/dashboard`
    };
    const gwResp = await axios.post(`${gatewayBase}/api/paytm/initiate`, payload, { timeout: 15000 });
    const data = gwResp.data || {};

    if (!data.success) {
      return res.status(500).json({ success: false, message: data.message || 'Payment initiation failed' });
    }

    // Prefer gateway redirectUrl if present
    if (data.redirectUrl) {
      return res.redirect(302, data.redirectUrl);
    }

    // Otherwise render an HTML form auto-submitting to Paytm
    const paytmUrl = data.paytmUrl;
    const params = data.paytmParams || {};
    if (!paytmUrl) {
      return res.status(500).json({ success: false, message: 'Missing paytmUrl from gateway' });
    }
    const inputs = Object.entries(params)
      .map(([k, v]) => `<input type="hidden" name="${k}" value="${String(v)}"/>`)
      .join('');
    const html = `<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>Redirecting…</title></head><body>
      <form id=\"paytmForm\" method=\"POST\" action=\"${paytmUrl}\">${inputs}</form>
      <script>document.getElementById('paytmForm').submit();</script>
    </body></html>`;
    res.status(200).send(html);
  } catch (error) {
    console.error('Error initiating payment:', error.message);
    res.status(500).json({ success: false, message: 'Failed to initiate payment' });
  }
});
// GET variant for browser redirects without Authorization header. Token passed as query param 't'.
// 
// Cashfree Direct Payment Link Flow:
// 1. Create order using POST /pg/orders
// 2. Create payment link using POST /pg/payment-links
// 3. Extract payment_link from response
// 4. Redirect to direct payment link (https://payments.cashfree.com/links/...)
// 5. This generates the production-style direct payment link as requested
//
router.get('/payments/initiate/direct', async (req, res) => {
  try {
    const jwt = require('jsonwebtoken');
    const { t, amount, planKey } = req.query || {};
    try { console.log('[INITIATE/DIRECT] query:', req.query); } catch {}
    if (!t) return res.status(401).send('Missing token');
    if (!amount) return res.status(400).send('Missing amount');
    const planKeyNorm = (typeof planKey === 'string' ? planKey : String(planKey || '')).toLowerCase();
    if (!planKeyNorm) return res.status(400).send('Missing planKey');

    let clientId;
    try {
      const decoded = jwt.verify(t, process.env.JWT_SECRET);
      if (decoded?.userType !== 'client') return res.status(401).send('Invalid token');
      clientId = decoded.id;
    } catch (e) {
      return res.status(401).send('Invalid token');
    }

    // Billing info from client profile
    const Client = require('../models/Client');
    const client = await Client.findById(clientId);
    let email = client?.email || 'client@example.com';
    let phone = client?.mobileNo || '9999999999';
    // Normalize phone to 10 digits as Cashfree expects
    try {
      const digits = String(phone || '').replace(/\D/g, '');
      if (digits.length >= 10) {
        phone = digits.slice(-10);
      }
    } catch {}
    let name = client?.name || 'Client';

    console.log('Creating Cashfree direct payment link');
    
    // Generate unique order ID
    const cashfreeOrderId = `AITOTA_${Date.now()}`;
    const axios = require('axios');
    
    // Validate Cashfree configuration
    if (!CashfreeConfig.CLIENT_ID || !CashfreeConfig.CLIENT_SECRET) {
      console.error('Cashfree configuration missing:', {
        hasClientId: !!CashfreeConfig.CLIENT_ID,
        hasClientSecret: !!CashfreeConfig.CLIENT_SECRET,
        env: CashfreeConfig.ENV
      });
      return res.status(500).json({ 
        success: false, 
        message: 'Payment gateway configuration error' 
      });
    }
    
    console.log('Cashfree configuration:', {
      env: CashfreeConfig.ENV,
      baseUrl: CashfreeConfig.BASE_URL,
      hasClientId: !!CashfreeConfig.CLIENT_ID,
      hasClientSecret: !!CashfreeConfig.CLIENT_SECRET,
      clientIdLength: CashfreeConfig.CLIENT_ID ? CashfreeConfig.CLIENT_ID.length : 0,
      clientIdPrefix: CashfreeConfig.CLIENT_ID ? CashfreeConfig.CLIENT_ID.substring(0, 4) : 'N/A'
    });
    
    // Check if we're using the right environment
    if (CashfreeConfig.ENV === 'prod' && CashfreeConfig.CLIENT_ID && CashfreeConfig.CLIENT_ID.startsWith('TEST')) {
      console.error('WARNING: Using TEST credentials in PROD environment!');
    }
    if (CashfreeConfig.ENV === 'sandbox' && CashfreeConfig.CLIENT_ID && !CashfreeConfig.CLIENT_ID.startsWith('TEST')) {
      console.error('WARNING: Using PROD credentials in SANDBOX environment!');
    }
    
    // Persist INITIATED payment
    try {
      const Payment = require('../models/Payment');
      await Payment.create({ clientId, orderId: cashfreeOrderId, planKey: planKeyNorm, amount: Number(amount), email, phone, status: 'INITIATED', gateway: 'cashfree' });
    } catch (e) { console.error('Payment INITIATED save failed:', e.message); }

    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2023-08-01', // Use latest API version for payment links
      'Content-Type': 'application/json'
    };

    // Step 1: Create order
    const orderPayload = {
      order_id: cashfreeOrderId,
      order_amount: Number(amount),
      order_currency: 'INR',
      customer_details: {
        customer_id: String(clientId),
        customer_email: email,
        customer_phone: phone,
        customer_name: name
      },
      order_meta: {
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${cashfreeOrderId}`,
        notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${cashfreeOrderId}`
      },
      order_note: `Payment for ${planKeyNorm} plan`,
      order_tags: {
        plan: planKeyNorm,
        client_id: String(clientId)
      }
    };

    let orderResponse;
    try {
      console.log('Step 1: Creating Cashfree order with payload:', JSON.stringify(orderPayload, null, 2));
      console.log('Using Cashfree API URL:', `${CashfreeConfig.BASE_URL}/pg/orders`);
      console.log('Using API version:', headers['x-api-version']);
      
      const orderResp = await axios.post(`${CashfreeConfig.BASE_URL}/pg/orders`, orderPayload, { headers });
      orderResponse = orderResp.data || {};
      
      console.log('Cashfree order created successfully:', JSON.stringify(orderResponse, null, 2));
      console.log('Order ID:', orderResponse.order_id);
      console.log('CF Order ID:', orderResponse.cf_order_id);
      console.log('Order Status:', orderResponse.order_status);
      
    } catch (e) {
      const status = e.response?.status;
      const data = e.response?.data;
      console.error('Cashfree create order failed:', status, data || e.message);
      console.error('Request payload was:', JSON.stringify(orderPayload, null, 2));
      return res.status(502).json({ success: false, message: 'Cashfree create order failed', status, data });
    }

    // Step 2: Create payment link using Cashfree's payment link API
    // Based on Cashfree documentation, we need to create a payment link through their API
    console.log('Step 2: Creating payment link through Cashfree API');
    
    let paymentLinkResponse = {};
    
    // Clean the session ID if it has the 'payment' suffix
    let sessionId = String(orderResponse.payment_session_id);
    if (sessionId.endsWith('payment')) {
      sessionId = sessionId.replace(/payment$/, '');
      console.log('Cleaned session ID:', sessionId);
    }
    
    // Try to create payment link using Cashfree's payment link API
    const paymentLinkPayload = {
      link_id: `link_${orderResponse.order_id}`,
      link_amount: orderResponse.order_amount,
      link_currency: orderResponse.order_currency,
      link_purpose: `Payment for ${planKeyNorm} plan`,
      customer_details: {
        customer_id: orderResponse.customer_details.customer_id,
        customer_name: orderResponse.customer_details.customer_name,
        customer_email: orderResponse.customer_details.customer_email,
        customer_phone: orderResponse.customer_details.customer_phone
      },
      link_auto_reminders: true,
      link_notify: {
        send_sms: true,
        send_email: true
      },
      link_expiry_time: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(), // 24 hours
      link_meta: {
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderResponse.order_id}`,
        notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderResponse.order_id}`
      }
    };
    
    try {
      console.log('Creating payment link with payload:', JSON.stringify(paymentLinkPayload, null, 2));
      console.log('Using Cashfree API URL:', `${CashfreeConfig.BASE_URL}/pg/links`);
      
      const linkResp = await axios.post(`${CashfreeConfig.BASE_URL}/pg/links`, paymentLinkPayload, { headers });
      paymentLinkResponse = linkResp.data || {};
      
      console.log('Payment link created successfully:', JSON.stringify(paymentLinkResponse, null, 2));
      console.log('Has Payment Link:', !!paymentLinkResponse.link_url);
      console.log('Payment Link:', paymentLinkResponse.link_url);
      
    } catch (e) {
      console.error('Payment link creation failed:', e.response?.data || e.message);
      
      // Fallback: Use the session ID to create a hosted checkout URL
      console.log('Fallback: Using hosted checkout with session ID');
      const isProduction = CashfreeConfig.BASE_URL.includes('api.cashfree.com');
      
      // Use the original session ID (don't clean it) for hosted checkout
      const originalSessionId = orderResponse.payment_session_id;
      console.log('Original session ID:', originalSessionId);
      console.log('Cleaned session ID:', sessionId);
      
      // Prefer payments domain with query param; ensure URL-encoded session id
      const encodedOriginal = encodeURIComponent(originalSessionId);
      const encodedCleaned = encodeURIComponent(sessionId);
      let hostedCheckoutUrl;
      if (isProduction) {
        // Use cleaned session id (without trailing 'payment')
        hostedCheckoutUrl = `https://payments.cashfree.com/order?session_id=${encodedCleaned}`;
        console.log('Trying production payments URL with cleaned session ID:', hostedCheckoutUrl);
      } else {
        hostedCheckoutUrl = `https://payments-test.cashfree.com/order?session_id=${encodedCleaned}`;
        console.log('Trying sandbox payments URL with cleaned session ID:', hostedCheckoutUrl);
      }
      
      paymentLinkResponse.link_url = hostedCheckoutUrl;
      console.log('Final URL:', hostedCheckoutUrl);
    }

    // Check if we got a payment link
    if (paymentLinkResponse.link_url) {
      console.log('✅ Using Cashfree payment link:', paymentLinkResponse.link_url);
      
      // Update payment record with link details
      try {
        const Payment = require('../models/Payment');
        await Payment.findOneAndUpdate(
          { orderId: cashfreeOrderId },
          { 
            paymentLink: paymentLinkResponse.link_url,
            sessionId: sessionId,
            rawResponse: { order: orderResponse, paymentLink: paymentLinkResponse }
          }
        );
      } catch (e) { console.error('Payment link update failed:', e.message); }
      
      return res.redirect(302, paymentLinkResponse.link_url);
    }



    // If we reach here, something is wrong with the payment link creation
    console.error('Failed to create payment link:', paymentLinkResponse);
    
    return res.status(500).json({ 
      success: false, 
      message: 'Failed to create payment link. Please try again.',
      data: {
        orderId: orderResponse.order_id,
        cfOrderId: orderResponse.cf_order_id,
        orderStatus: orderResponse.order_status,
        hasPaymentSessionId: !!orderResponse.payment_session_id,
        hasPaymentLink: !!paymentLinkResponse.link_url,
        fullOrderResponse: orderResponse,
        fullPaymentLinkResponse: paymentLinkResponse
      }
    });
  } catch (error) {
    console.error('Error in direct payment link creation:', error.message || error);
    const msg = error?.message || 'Failed to create payment link';
    res.status(500).json({ success: false, message: msg });
  }
});

// Test endpoint to try different Cashfree URL formats
router.get('/payments/cashfree/test-urls', (req, res) => {
  try {
    const { sessionId, orderId } = req.query;
    
    if (!sessionId) {
      return res.status(400).json({ 
        success: false, 
        message: 'sessionId parameter is required' 
      });
    }
    
    const isProduction = CashfreeConfig.BASE_URL.includes('api.cashfree.com');
    
    // Clean session ID if it ends with 'payment'
    let cleanedSessionId = sessionId;
    if (sessionId.endsWith('payment')) {
      cleanedSessionId = sessionId.replace(/payment$/, '');
    }
    
    // Generate different URL formats to test
    const urlFormats = {
      'original_session': `https://${isProduction ? 'cashfree.com' : 'sandbox.cashfree.com'}/pg/orders/${sessionId}`,
      'cleaned_session': `https://${isProduction ? 'cashfree.com' : 'sandbox.cashfree.com'}/pg/orders/${cleanedSessionId}`,
      'payments_links': orderId ? `https://${isProduction ? 'payments.cashfree.com' : 'payments-test.cashfree.com'}/links/${orderId}` : 'N/A (no orderId)',
      'payments_order': `https://${isProduction ? 'payments.cashfree.com' : 'payments-test.cashfree.com'}/order/${cleanedSessionId}`,
      'payments_query': `https://${isProduction ? 'payments.cashfree.com' : 'payments-test.cashfree.com'}/order?session_id=${cleanedSessionId}`,
      'checkout_path': `https://${isProduction ? 'cashfree.com' : 'sandbox.cashfree.com'}/checkout/${cleanedSessionId}`
    };
    
    console.log('Testing Cashfree URL formats:', urlFormats);
    
    return res.json({ 
      success: true, 
      message: 'URL formats generated for testing',
      environment: isProduction ? 'production' : 'sandbox',
      sessionId: {
        original: sessionId,
        cleaned: cleanedSessionId
      },
      urlFormats
    });
    
  } catch (error) {
    console.error('Test URLs endpoint error:', error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// Initiate payment using Cashfree SDK flow (matches provided example)
router.post('/payments/initiate/sdk', async (req, res) => {
  try {
    const jwt = require('jsonwebtoken');
    const { t, planKey, amount } = req.body || {};
    if (!t) return res.status(401).json({ success: false, message: 'Missing token' });
    if (!amount) return res.status(400).json({ success: false, message: 'Missing amount' });

    let decoded;
    try { decoded = jwt.verify(t, process.env.JWT_SECRET); } catch { return res.status(401).json({ success: false, message: 'Invalid token' }); }
    if (decoded?.userType !== 'client' || !decoded?.id) return res.status(401).json({ success: false, message: 'Invalid token' });

    const clientId = decoded.id;
    const Client = require('../models/Client');
    const Payment = require('../models/Payment');
    const client = await Client.findById(clientId);
    if (!client) return res.status(404).json({ success: false, message: 'Client not found' });

    const axios = require('axios');
    const orderId = `AITOTA_${Date.now()}`;
    const headers = {
      'x-client-id': CashfreeConfig.CLIENT_ID,
      'x-client-secret': CashfreeConfig.CLIENT_SECRET,
      'x-api-version': '2023-08-01',
      'Content-Type': 'application/json'
    };
    const orderRequest = {
      order_id: orderId,
      order_amount: Number(amount),
      order_currency: 'INR',
      customer_details: {
        customer_id: String(clientId),
        customer_name: client?.name || 'Client',
        customer_email: client?.email || 'client@example.com',
        customer_phone: (String(client?.mobileNo || '9999999999').replace(/\D/g, '').slice(-10)) || '9999999999'
      },
      order_meta: {
        notify_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`,
        return_url: `${CashfreeConfig.RETURN_URL}?order_id=${orderId}`
      }
    };
    const apiUrl = `${CashfreeConfig.BASE_URL}/pg/orders`;
    let data = {};
    try {
      const resp = await axios.post(apiUrl, orderRequest, { headers });
      data = resp?.data || {};
    } catch (e) {
      const status = e.response?.status;
      const errData = e.response?.data || e.message;
      console.error('Cashfree REST create order failed:', status, errData);
      return res.status(502).json({ success: false, message: 'Cashfree create order failed', status, error: errData });
    }

    // Persist INITIATED payment
    await Payment.create({
      clientId,
      orderId,
      planKey: (planKey || '').toString().toLowerCase(),
      amount: Number(amount),
      email: client?.email,
      phone: orderRequest.customer_details.customer_phone,
      gateway: 'cashfree',
      status: 'INITIATED',
      rawResponse: { sdk: data }
    });

    // Return session to frontend so it can launch checkout via payments domain
    return res.json({
      success: true,
      environment: CashfreeConfig.ENVIRONMENT,
      order_id: data.order_id,
      cf_order_id: data.cf_order_id,
      payment_session_id: data.payment_session_id
    });
  } catch (error) {
    const status = error?.response?.status;
    const errData = error?.response?.data || error?.message;
    console.error('Cashfree SDK initiate error:', status, errData);
    return res.status(500).json({ success: false, message: 'Failed to initiate payment', status, error: errData });
  }
});

// Comprehensive Cashfree diagnostic endpoint
router.get('/debug/cashfree-diagnostics', async (req, res) => {
  try {
    const diagnostics = {
      timestamp: new Date().toISOString(),
      environment: {
        NODE_ENV: process.env.NODE_ENV,
        CASHFREE_ENV: process.env.CASHFREE_ENV,
        CASHFREE_CLIENT_ID: process.env.CASHFREE_CLIENT_ID ? `${process.env.CASHFREE_CLIENT_ID.substring(0, 4)}...` : 'NOT_SET',
        CASHFREE_SECRET_KEY: process.env.CASHFREE_SECRET_KEY ? `${process.env.CASHFREE_SECRET_KEY.substring(0, 8)}...` : 'NOT_SET',
        CASHFREE_SECRET_KEY_TEST: process.env.CASHFREE_SECRET_KEY_TEST ? `${process.env.CASHFREE_SECRET_KEY.substring(0, 8)}...` : 'NOT_SET'
      },
      config: {
        ENV: CashfreeConfig.ENV,
        BASE_URL: CashfreeConfig.BASE_URL,
        CLIENT_ID: CashfreeConfig.CLIENT_ID ? `${CashfreeConfig.CLIENT_ID.substring(0, 4)}...` : 'NOT_SET',
        CLIENT_SECRET: CashfreeConfig.CLIENT_SECRET ? `${CashfreeConfig.CLIENT_SECRET.substring(0, 8)}...` : 'NOT_SET',
        RETURN_URL: CashfreeConfig.RETURN_URL
      },
      urlFormats: {
        production: {
          path: 'https://payments.cashfree.com/order/{sessionId}',
          query_session_id: 'https://payments.cashfree.com/order?session_id={sessionId}',
          query_session: 'https://payments.cashfree.com/order?session={sessionId}',
          hash: 'https://payments.cashfree.com/order/#{sessionId}',
          checkout_path: 'https://payments.cashfree.com/checkout/{sessionId}'
        },
        sandbox: {
          path: 'https://sandbox.cashfree.com/pg/orders/{sessionId}',
          query_session_id: 'https://sandbox.cashfree.com/pg/orders?session_id={sessionId}',
          query_session: 'https://sandbox.cashfree.com/pg/orders?session={sessionId}',
          hash: 'https://sandbox.cashfree.com/pg/orders/#{sessionId}',
          checkout_path: 'https://sandbox.cashfree.com/checkout/{sessionId}'
        }
      },
      testSessionIds: [
        {
          original: 'session_hN9kFZIn9f71raO3G41brM9lrbHUTiyKMJ1R_CB8t_Fe86dg-A2uzFUH43NcuW-G-p97plFrIW44RU8aqrDs12N37BYpxQVs5dj4l8fJLge0-h1ngYqHrcVvMfQpayment',
          cleaned: 'session_hN9kFZIn9f71raO3G41brM9lrbHUTiyKMJ1R_CB8t_Fe86dg-A2uzFUH43NcuW-G-p97plFrIW44RU8aqrDs12N37BYpxQVs5dj4l8fJLge0-h1ngYqHrcVvMfQ',
          hasPaymentSuffix: true,
          length: 131
        }
      ],
      recommendations: [
        'All URL formats are failing - this suggests a fundamental issue with the session ID or configuration',
        'Check if Cashfree credentials are correct for the environment being used',
        'Verify the session ID format in Cashfree documentation',
        'Consider testing with a fresh order creation',
        'Check Cashfree support for session ID format requirements'
      ]
    };
    
    res.json({
      success: true,
      message: 'Cashfree comprehensive diagnostics',
      data: diagnostics
    });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// Debug endpoint to test session ID cleaning
router.get('/debug/session-id-clean', (req, res) => {
  try {
    const testSessionIds = [
      'session_98FwWBjqO8Wzu2NWNqdY93Y0Aehqyjegr_sBbMXTDcNfzpsr12KzXyS18ypvvBsYWeUK1ox83YfMebEKCtkrbErsxULYYnX25UQvUGZK2DnJ_QvBCiug3WPM93cpayment',
      'session_98FwWBjqO8Wzu2NWNqdY93Y0Aehqyjegr_sBbMXTDcNfzpsr12KzXyS18ypvvBsYWeUK1ox83YfMebEKCtkrbErsxULYYnX25UQvBCiug3WPM93c',
      'session_normal_id',
      'invalid_id_payment',
      'session_GEqmGFcAuKJIXDdQjRj1XinL_zIw_JSvEMNQKkU_zBb22ROs6f65FRIW5ES18s01cb09LMU8qxjdplIOaRgiZ7t1qipSxemUUv0W7mxy8OjquhooHUnqtNaSC5spayment',
      'session_hN9kFZIn9f71raO3G41brM9lrbHUTiyKMJ1R_CB8t_Fe86dg-A2uzFUH43NcuW-G-p97plFrIW44RU8aqrDs12N37BYpxQVs5dj4l8fJLge0-h1ngYqHrcVvMfQpayment'
    ];
    
    const results = testSessionIds.map(originalId => {
      let cleanedId = originalId;
      if (originalId.endsWith('payment')) {
        cleanedId = originalId.replace(/payment$/, '');
      }
      
      const isValid = cleanedId.startsWith('session_') && cleanedId.length >= 10;
      const hasValidFormat = /^session_[a-zA-Z0-9_-]+$/.test(cleanedId);
      
      // Generate different URL formats for testing
      const urlFormats = {
        pathFormat: `https://payments.cashfree.com/order/${cleanedId}`,
        querySessionIdFormat: `https://payments.cashfree.com/order?session_id=${cleanedId}`,
        querySessionFormat: `https://payments.cashfree.com/order?session=${cleanedId}`,
        hashFormat: `https://payments.cashfree.com/order/#${cleanedId}`,
        checkoutPathFormat: `https://payments.cashfree.com/checkout/${cleanedId}`
      };
      
      return {
        original: originalId,
        cleaned: cleanedId,
        isValid,
        hasValidFormat,
        endsWithPayment: originalId.endsWith('payment'),
        length: cleanedId.length,
        urlFormats
      };
    });
    
    res.json({
      success: true,
      message: 'Session ID cleaning test results with URL formats',
      data: results
    });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// POST /api/v1/client/credits/paytm/confirm
// Accepts { orderId, planKey } from frontend after redirect
router.post('/credits/paytm/confirm', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { orderId, planKey } = req.body || {};
    if (!orderId) {
      return res.status(400).json({ success: false, message: 'orderId missing' });
    }
    const plan = (planKey || '').toLowerCase();
    let creditsToAdd = 0;
    if (plan === 'basic') creditsToAdd = 1000;
    else if (plan === 'professional') creditsToAdd = 5500; // includes 500 bonus
    else if (plan === 'enterprise') creditsToAdd = 11000; // includes 1000 bonus

    if (!creditsToAdd) {
      return res.status(400).json({ success: false, message: 'Unknown planKey' });
    }

    const Credit = require('../models/Credit');
    const credit = await Credit.getOrCreateCreditRecord(req.clientId);
    await credit.addCredits(creditsToAdd, 'purchase', `Paytm order ${orderId}`, {
      gateway: 'paytm',
      orderId,
      planKey: plan,
    });
    return res.json({ success: true, message: 'Credits applied', data: { balance: credit.currentBalance } });
  } catch (e) {
    console.error('Paytm confirm error:', e.message);
    return res.status(500).json({ success: false, message: 'Failed to apply credits' });
  }
});



// Payment status check endpoint
router.get('/payments/status/:orderId', verifyClientOrAdminAndExtractClientId, async (req, res) => {
  try {
    const { orderId } = req.params;
    
    const Payment = require('../models/Payment');
    const payment = await Payment.findOne({ orderId, clientId: req.clientId });
    
    if (!payment) {
      return res.status(404).json({
        success: false,
        message: 'Payment not found'
      });
    }

    res.json({
      success: true,
      data: {
        orderId: payment.orderId,
        status: payment.status,
        amount: payment.amount,
        planKey: payment.planKey,
        gateway: payment.gateway,
        credited: payment.credited,
        creditsAdded: payment.creditsAdded,
        createdAt: payment.createdAt,
        transactionId: payment.transactionId,
        sessionId: payment.sessionId,
        paymentLink: payment.paymentLink
      }
    });

  } catch (error) {
    console.error('❌ Payment status check failed:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to check payment status',
      error: error.message
    });
  }
});

// Get agent by agentKey
router.get('/agent-by-key/:agentKey', verifyClientToken, async (req, res) => {
  try {
    const { agentKey } = req.params;
    
    if (!agentKey) {
      return res.status(400).json({
        success: false,
        message: 'AgentKey is required'
      });
    }
    
    const result = await Agent.findByAgentKey(agentKey);
    
    if (result.success) {
      res.json({
        success: true,
        data: {
          agentId: result.agentId,
          agentName: result.agent.agentName,
          agentKey: result.agent.agentKey,
          // Include other fields you might need
          category: result.agent.category,
          personality: result.agent.personality,
          isActive: result.agent.isActive
        }
      });
    } else {
      res.status(404).json({
        success: false,
        message: result.message
      });
    }
    
  } catch (error) {
    console.error('❌ Get agent by key error:', error);
    res.status(500).json({
      success: false,
      message: 'Error finding agent: ' + error.message
    });
  }
});

// System Health and Monitoring Endpoints
router.get('/system/health', extractClientId, async (req, res) => {
  try {
    const health = getSystemHealth();
    res.json({
      success: true,
      data: health
    });
  } catch (error) {
    console.error('Error getting system health:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get system health'
    });
  }
});

router.get('/system/limits', extractClientId, async (req, res) => {
  try {
    const limits = getSafeLimits();
    res.json({
      success: true,
      data: limits
    });
  } catch (error) {
    console.error('Error getting safe limits:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to get safe limits'
    });
  }
});

router.post('/system/reset-circuit-breakers', extractClientId, async (req, res) => {
  try {
    resetCircuitBreakers();
    res.json({
      success: true,
      message: 'Circuit breakers reset successfully'
    });
  } catch (error) {
    console.error('Error resetting circuit breakers:', error);
    res.status(500).json({
      success: false,
      error: 'Failed to reset circuit breakers'
    });
  }
});
// Call validation endpoint
router.post('/calls/validate', authMiddleware, async (req, res) => {
  try {
    const { agentId, phoneNumber } = req.body;
    const clientId = req.user?.clientId || req.user?._id;

    if (!agentId || !phoneNumber) {
      return res.status(400).json({
        success: false,
        error: "Missing required parameters. Please provide agent ID and phone number."
      });
    }

    // Get agent details
    const agent = await Agent.findOne({ _id: agentId, clientId });
    if (!agent) {
      return res.status(404).json({
        success: false,
        error: "Agent not found. Please select a valid agent."
      });
    }

    // Check if agent is active
    if (!agent.isActive) {
      return res.status(400).json({
        success: false,
        error: "Agent is inactive. Please activate the agent first."
      });
    }

    // Check phone number format
    const phoneDigits = String(phoneNumber || "").replace(/[^\d]/g, "");
    if (!phoneDigits || phoneDigits.length < 10) {
      return res.status(400).json({
        success: false,
        error: "Invalid phone number. Please enter a valid phone number with at least 10 digits."
      });
    }

    // Check service provider
    const provider = String(agent?.serviceProvider || "").toLowerCase();
    if (!provider) {
      return res.status(400).json({
        success: false,
        error: "No telephony provider configured. Please configure a service provider (SANPBX, C-Zentrix, etc.) for this agent."
      });
    }

    // Provider-specific validation
    if (provider === "snapbx" || provider === "sanpbx") {
      // SANPBX validation
      if (!agent.accessToken) {
        return res.status(400).json({
          success: false,
          error: "SANPBX Access Token not configured. Please set the Access Token for this agent."
        });
      }
      if (!agent.accessKey) {
        return res.status(400).json({
          success: false,
          error: "SANPBX Access Key not configured. Please set the Access Key for this agent."
        });
      }
      if (!agent.callerId) {
        return res.status(400).json({
          success: false,
          error: "SANPBX Caller ID not configured. Please set a Caller ID for this agent."
        });
      }

      // Test SANPBX connectivity
      try {
        const tokenResponse = await fetch(
          "https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/gentoken",
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Accesstoken: agent.accessToken,
            },
            body: JSON.stringify({
              access_key: agent.accessKey,
            }),
          }
        );

        if (!tokenResponse.ok) {
          return res.status(500).json({
            success: false,
            error: "SANPBX service is not responding. Please check SANPBX service status or contact support."
          });
        }

        const tokenData = await tokenResponse.json();
        if (!tokenData.Apitoken) {
          return res.status(500).json({
            success: false,
            error: "SANPBX authentication failed. Please check SANPBX credentials or contact SANPBX support."
          });
        }

      } catch (error) {
        return res.status(500).json({
          success: false,
          error: "SANPBX connection failed. Please check your network connection or contact support."
        });
      }

    } else if (provider === "c-zentrix" || provider === "c-zentrax") {
      // C-Zentrix validation
      if (!agent.X_API_KEY) {
        return res.status(400).json({
          success: false,
          error: "C-Zentrix API Key not configured. Please set the X API Key for this agent."
        });
      }
      if (!agent.accountSid) {
        return res.status(400).json({
          success: false,
          error: "C-Zentrix Account SID not configured. Please set the Account SID for this agent."
        });
      }
      if (!agent.callerId) {
        return res.status(400).json({
          success: false,
          error: "C-Zentrix Caller ID not configured. Please set a Caller ID for this agent."
        });
      }
    }

    // All validations passed
    return res.json({
      success: true,
      message: "Configuration validated successfully. Ready to make call."
    });

  } catch (error) {
    console.error("Call validation error:", error);
    return res.status(500).json({
      success: false,
      error: "System error occurred during validation. Please try again or contact support."
    });
  }
});

module.exports = router;
