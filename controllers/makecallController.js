const crypto = require("crypto");
const ClientApiKey = require("../models/ClientApiKey");
const Client = require("../models/Client");
const Agent = require("../models/Agent");
const jwt = require("jsonwebtoken");
const { makeSingleCall: makeSingleCallService } = require("../services/campaignCallingService");

// Helper: generate unique ID for tracking single calls
function generateUniqueId() {
  const timePart = Date.now();
  const randomPart = Math.random().toString(36).slice(2, 10);
  return `aidial-${timePart}-${randomPart}`;
}

// Function to verify API key
const verifyApiKey = async (apiKey) => {
  try {
    if (!apiKey || !apiKey.startsWith("ait_")) {
      return null;
    }

    // Hash the provided API key
    const secret = process.env.ENCRYPTION_SECRET || "default-secret-key-change-in-production";
    const providedKeyHash = crypto.createHmac("sha256", secret).update(apiKey).digest("hex");

    // Find matching API key in database
    const apiKeyRecord = await ClientApiKey.findOne({
      keyHash: providedKeyHash,
      isActive: true
    });

    if (!apiKeyRecord) {
      return null;
    }

    return apiKeyRecord;
  } catch (error) {
    console.error("Error verifying API key:", error);
    return null;
  }
};

// Login endpoint for makecall
exports.makecallLogin = async (req, res) => {
  try {
    const { name, email, number, apiKey } = req.body;

    // Validate required fields
    if (!name || !email || !number || !apiKey) {
      return res.status(400).json({
        success: false,
        message: "All fields are required: name, email, number, and apiKey"
      });
    }

    // Validate email format
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({
        success: false,
        message: "Invalid email format"
      });
    }

    // Validate phone number (basic validation)
    const phoneRegex = /^[0-9]{10,15}$/;
    const cleanNumber = number.replace(/\D/g, '');
    if (!phoneRegex.test(cleanNumber)) {
      return res.status(400).json({
        success: false,
        message: "Invalid phone number format"
      });
    }

    // Verify API key
    
    const apiKeyRecord = await verifyApiKey(apiKey);
    if (!apiKeyRecord) {
      // Log failed login attempt due to invalid API key (only if not already exists)
      try {
        const MakecallLoginLog = require('../models/MakecallLoginLog');
        
        // Check if failed login already exists for this email
        const existingFailedLogin = await MakecallLoginLog.findOne({
          email: email,
          success: false,
          errorMessage: 'Invalid or inactive API key'
        });
        
        if (!existingFailedLogin) {
          await MakecallLoginLog.create({
            clientId: null,
            apiKeyId: null,
            email: email,
            name: name,
            phone: cleanNumber,
            success: false,
            errorMessage: 'Invalid or inactive API key',
            loginAt: new Date()
          });
          console.log(`New failed login logged for email: ${email} - Invalid API key`);
        } else {
          console.log(`Failed login already exists for email: ${email}, skipping duplicate log`);
        }
      } catch (logError) {
        console.error('Failed to log invalid API key attempt:', logError);
      }
      
      return res.status(401).json({
        success: false,
        message: "Invalid or inactive API key"
      });
    }

    // Get the client associated with this API key
    const client = await Client.findById(apiKeyRecord.clientId);
    if (!client) {
      // Log failed login attempt due to client not found (only if not already exists)
      try {
        const MakecallLoginLog = require('../models/MakecallLoginLog');
        
        // Check if failed login already exists for this email with this error
        const existingFailedLogin = await MakecallLoginLog.findOne({
          email: email,
          success: false,
          errorMessage: 'Client not found for this API key'
        });
        
        if (!existingFailedLogin) {
          await MakecallLoginLog.create({
            clientId: apiKeyRecord.clientId,
            apiKeyId: apiKeyRecord._id,
            email: email,
            name: name,
            phone: cleanNumber,
            success: false,
            errorMessage: 'Client not found for this API key',
            loginAt: new Date()
          });
          console.log(`New failed login logged for email: ${email} - Client not found`);
        } else {
          console.log(`Failed login already exists for email: ${email}, skipping duplicate log`);
        }
      } catch (logError) {
        console.error('Failed to log client not found attempt:', logError);
      }
      
      return res.status(404).json({
        success: false,
        message: "Client not found for this API key"
      });
    }

    // Log the login attempt (only if not already exists for this user)
    let loginLogId = null;
    try {
      const MakecallLoginLog = require('../models/MakecallLoginLog');
      
      // Check if login already exists for this user (email + client combination)
      const existingLogin = await MakecallLoginLog.findOne({
        clientId: client._id,
        email: email,
        success: true
      });
      
      if (existingLogin) {
        loginLogId = existingLogin._id;
        console.log(`Login already exists for user: ${email}, using existing log ${String(loginLogId)}`);
      } else {
        const created = await MakecallLoginLog.create({
          clientId: client._id,
          apiKeyId: apiKeyRecord._id,
          email: email,
          name: name,
          phone: cleanNumber,
          success: true,
          loginAt: new Date()
        });
        loginLogId = created._id;
        console.log(`New login logged for user: ${email} (${String(loginLogId)})`);
      }
    } catch (logError) {
      console.error('Failed to log makecall login:', logError);
      // Don't fail the login if logging fails
    }

    // Generate JWT token for makecall session
    const token = jwt.sign(
      {
        id: loginLogId,
        email: client.email,
        name: client.name || name,
        phone: client.phone || cleanNumber,
        apiKey: apiKeyRecord._id,
        clientId: client._id,
        clientUserId: client._id,
        type: 'makecall'
      },
      process.env.JWT_SECRET,
      { expiresIn: '24h' }
    );

    // Return success response
    res.json({
      success: true,
      message: "Login successful",
      data: {
        token,
        loginLogId,
        client: {
          id: client._id,
          name: client.name || name,
          email: client.email,
          phone: client.phone || cleanNumber
        },
        apiKey: {
          id: apiKeyRecord._id,
          preview: apiKeyRecord.keyPreview
        }
      }
    });

  } catch (error) {
    console.error("Makecall login error:", error);
    res.status(500).json({
      success: false,
      message: "Internal server error",
      error: error.message
    });
  }
};

exports.makeSingleCall = async (req, res) => {
  try {
    const clientUserId = req.clientUserId;
    const { phone, name, agentId, custom_field } = req.body || {};

    // Security: Validate required fields
    if (!phone) {
      return res.status(400).json({
        success: false,
        error: 'Missing phone',
        message: 'phone is required in request body'
      });
    }

    if (!name) {
      return res.status(400).json({
        success: false,
        error: 'Missing name',
        message: 'name is required in request body'
      });
    }

    if (!agentId) {
      return res.status(400).json({
        success: false,
        error: 'Missing agentId',
        message: 'agentId is required in request body'
      });
    }

    // Security: Validate phone number format
    const phoneRegex = /^[0-9]{10,15}$/;
    const cleanNumber = phone.replace(/\D/g, '');
    if (!phoneRegex.test(cleanNumber)) {
      return res.status(400).json({
        success: false,
        error: 'Invalid phone format',
        message: 'Phone number must be 10-15 digits'
      });
    }

    // Security: Validate name (basic sanitization)
    if (typeof name !== 'string' || name.trim().length < 2 || name.trim().length > 100) {
      return res.status(400).json({
        success: false,
        error: 'Invalid name',
        message: 'Name must be 2-100 characters long'
      });
    }

    // Security: Sanitize custom_field if provided
    let sanitizedCustomField = null;
    if (custom_field) {
      if (typeof custom_field !== 'string' || custom_field.length > 500) {
        return res.status(400).json({
          success: false,
          error: 'Invalid custom field',
          message: 'Custom field must be a string with maximum 500 characters'
        });
      }
      // Basic sanitization - remove potentially harmful characters
      sanitizedCustomField = custom_field.replace(/[<>\"'&]/g, '').trim();
    }

    // Credit check (same as start-calling)
    try {
      const Credit = require('../models/Credit');
      const creditRecord = await Credit.getOrCreateCreditRecord(req.clientId);
      const currentBalance = Number(creditRecord?.currentBalance || 0);
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

    // Security: Handle agentId validation and client ownership check
    let actualAgentId = agentId;
    let agent = null;

    // Check if agentId looks like an agentKey (8 characters, alphanumeric)
    if (agentId && agentId.length === 8 && /^[a-f0-9]+$/i.test(agentId)) {
      // It's an agentKey, find the agent by agentKey
      const agentResult = await Agent.findByAgentKey(agentId);
      if (agentResult.success) {
        actualAgentId = agentResult.agentId;
        agent = agentResult.agent;
        console.log(`🔑 Found agent by agentKey: ${agentId} -> ${actualAgentId}`);
      } else {
        return res.status(404).json({ 
          success: false, 
          error: 'Agent not found', 
          message: `No agent found with agentKey: ${agentId}` 
        });
      }
    } else {
      // It's a regular document ID, find agent by _id
      agent = await Agent.findById(agentId).lean();
      if (!agent) {
        return res.status(404).json({ 
          success: false, 
          error: 'Agent not found', 
          message: `No agent found with ID: ${agentId}` 
        });
      }
      actualAgentId = agentId;
    }

    // Ensure we have the agent object
    if (!agent) {
      agent = await Agent.findById(actualAgentId).lean();
      if (!agent) {
        return res.status(404).json({ success: false, error: 'Agent not found' });
      }
    }

    // Security: Check if agent belongs to the same client
    if (agent.clientId && agent.clientId.toString() !== req.clientId.toString()) {
      return res.status(403).json({
        success: false,
        error: 'Agent access denied',
        message: 'Agent does not belong to your client account'
      });
    }

    // Security: Check if agent is active
    if (!agent.isActive) {
      return res.status(403).json({
        success: false,
        error: 'Agent inactive',
        message: 'Agent is not active. Please activate the agent before making calls.'
      });
    }

    // Security: Check if agent has required configuration
    if (!agent.serviceProvider) {
      return res.status(400).json({
        success: false,
        error: 'Agent misconfigured',
        message: 'Agent service provider is not configured'
      });
    }

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
    const normalizedDigits = normalizePhone(phone);
    if (!normalizedDigits) {
      return res.status(400).json({ success: false, error: 'Invalid phone' });
    }

    // Branch: SANPBX provider flow
    if (String(agent.serviceProvider).toLowerCase() === 'snapbx' || String(agent.serviceProvider).toLowerCase() === 'sanpbx') {
      try {
        const axios = require('axios');
        const accessToken = agent.accessToken;
        const accessKey = agent.accessKey;
        const callerId = agent.callerId;
        
        // Security: Validate SANPBX credentials
        if (!accessToken || !accessKey || !callerId) {
          return res.status(400).json({ 
            success: false, 
            error: 'SANPBX_MISSING_FIELDS', 
            message: 'accessToken, accessKey and callerId are required on agent for SANPBX' 
          });
        }

        // Ensure leading '0' if provider expects local dialing
        const callTo = normalizedDigits.startsWith('0') ? normalizedDigits : `0${normalizedDigits}`;
        const uniqueId = generateUniqueId();

        // 1) Generate API token (send access token in header)
        const tokenUrl = `https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/gentoken`;
        const tokenResp = await axios.post(
          tokenUrl,
          { access_key: accessKey },
          { headers: { Accesstoken: accessToken } }
        );
        const sanToken = tokenResp?.data?.Apitoken;
        if (!sanToken) {
          return res.status(502).json({ success: false, error: 'SANPBX_TOKEN_FAILED', data: tokenResp?.data || null });
        }

        // 2) Dial call (send API token in header)
        const dialUrl = `https://clouduat28.sansoftwares.com/pbxadmin/sanpbxapi/dialcall`;
        const dialBody = {
          appid: 2,
          call_to: callTo,
          caller_id: callerId,
          custom_field: {
            name: name,
            uniqueid: uniqueId,
            clientUserId: clientUserId
          }
        };
        const dialResp = await axios.post(
          dialUrl,
          dialBody,
          { headers: { Apitoken: sanToken } }
        );
        // Log to MakecallLoginLog with initial ringing status
        try {
          const MakecallLoginLog = require('../models/MakecallLoginLog');
          if (req.loginLogId && uniqueId) {
            await MakecallLoginLog.findByIdAndUpdate(
              req.loginLogId,
              {
                $push: {
                  logs: {
                    uniqueId,
                    name,
                    number: normalizedDigits,
                    status: 'ringing',
                    time: new Date()
                  }
                }
              }
            );
          }
        } catch (e) {
          console.warn('Failed to append SANPBX call to MakecallLoginLog:', e?.message);
        }

        return res.status(200).json({ success: true, provider: 'sanpbx', uniqueId, data: dialResp?.data || {} });
      } catch (e) {
        const status = e?.response?.status;
        const data = e?.response?.data;
        return res.status(502).json({ success: false, error: 'SANPBX_DIAL_FAILED', status, data, message: e.message });
      }
    }

    // Default: fall back to existing C-Zentrax flow via makeSingleCall
    // Get API key from agent document
    let resolvedApiKey = agent.X_API_KEY || '';
    let apiKeySource = 'agent';
    
    if (!resolvedApiKey) {
      // fallback to client-level key if agent key missing
      const { getClientApiKey } = require('../services/campaignCallingService');
      resolvedApiKey = await getClientApiKey(req.clientId);
      apiKeySource = 'client';
    }
    
    if (!resolvedApiKey) {
      return res.status(400).json({ 
        success: false, 
        error: 'No API key found', 
        message: 'No API key found for agent or client' 
      });
    }
    
    console.log(`🔑 Using API key from ${apiKeySource}: ${resolvedApiKey.substring(0, 10)}...`);

    const result = await makeSingleCallService(
      {
        name: name,
        phone: normalizedDigits,
        clientUserId: clientUserId
      },
      actualAgentId,
      resolvedApiKey,
      null,
      req.clientId
    );

    // Log to MakecallLoginLog with initial ringing status (if we have uniqueId)
    try {
      const MakecallLoginLog = require('../models/MakecallLoginLog');
      if (req.loginLogId && result?.uniqueId) {
        await MakecallLoginLog.findByIdAndUpdate(
          req.loginLogId,
          {
            $push: {
              logs: {
                uniqueId: result.uniqueId,
                name,
                number: normalizedDigits,
                status: 'ringing',
                time: new Date()
              }
            }
          }
        );
      }
    } catch (e) {
      console.warn('Failed to append CZ call to MakecallLoginLog:', e?.message);
    }

    return res.status(200).json({ success: true, data: result });
  } catch (error) {
    console.error('Single call error:', error);
    return res.status(500).json({ success: false, error: 'Failed to initiate single call' });
  }
};

// Get makecall dashboard data
exports.getMakecallDashboard = async (req, res) => {
  try {
    const clientId = req.clientId;
    
    if (!clientId) {
      return res.status(401).json({
        success: false,
        message: "Unauthorized"
      });
    }

    // Get client details
    const client = await Client.findById(clientId);
    if (!client) {
      return res.status(404).json({
        success: false,
        message: "Client not found"
      });
    }

    // Get active API key
    const apiKey = await ClientApiKey.findOne({
      clientId,
      isActive: true
    }).select("keyPreview createdAt");

      // Prefer recent logs from MakecallLoginLog.logs for this session
    const MakecallLoginLog = require('../models/MakecallLoginLog');
    let recentCalls = [];
    if (req.loginLogId) {
      const loginDoc = await MakecallLoginLog.findById(req.loginLogId).select('logs').lean();
      const logs = Array.isArray(loginDoc?.logs) ? loginDoc.logs : [];
      // Take last 10 entries, newest first
      recentCalls = logs.slice(-10).reverse().map((l) => ({
        _id: l._id,
        mobile: l.number,
        name: l.name,
        contactName: l.name,
        status: l.status,
        disposition: l.disposition || '',
        uniqueId: l.uniqueId,
        createdAt: l.time || new Date()
      }));
    } else {
      // Fallback to CallLog if session log not available
      const CallLog = require("../models/CallLog");
      const callLogs = await CallLog.find({
        clientId,
        'metadata.customParams.clientUserId': req.clientUserId
      })
        .sort({ createdAt: -1 })
        .select("mobile leadStatus createdAt metadata.isActive metadata.customParams.uniqueid metadata.customParams.clientUserId metadata.customParams.name metadata.customParams.contact_name")
        .lean();
      recentCalls = callLogs;
    }

    // Get credit balance (if you have a Credit model)
    let creditBalance = 0;
    try {
      const Credit = require("../models/Credit");
      const creditRecord = await Credit.findOne({ clientId });
      if (creditRecord) {
        creditBalance = creditRecord.currentBalance || 0;
      }
    } catch (error) {
      console.log("Credit model not available or error fetching credits:", error.message);
    }

    res.json({
      success: true,
      data: {
        client: {
          id: client._id,
          name: client.name,
          email: client.email,
          phone: client.phone
        },
        apiKey: apiKey ? {
          preview: apiKey.keyPreview,
          createdAt: apiKey.createdAt
        } : null,
        recentCalls: recentCalls.map(call => {
          // If coming from MakecallLoginLog, fields already normalized
          if (!call.metadata) {
            return ({
              id: call._id,
              mobile: call.mobile,
              name: call.name,
              contactName: call.contactName,
              status: call.status,
              disposition: call.disposition || '',
              uniqueId: call.uniqueId,
              createdAt: call.createdAt
            });
          }
          // Else, compute from CallLog
          const leadStatus = String(call.leadStatus || '').toLowerCase();
          const isActive = call?.metadata?.isActive;
          const ageSec = Math.floor((Date.now() - new Date(call.createdAt).getTime()) / 1000);
          let normalizedStatus = 'ringing';
          if (isActive === true) normalizedStatus = 'ongoing';
          else if (isActive === false) normalizedStatus = 'completed';
          else if (ageSec >= 40) normalizedStatus = 'missed';
          else normalizedStatus = 'ringing';
          return ({
            id: call._id,
            mobile: call.mobile,
            name: call.metadata?.customParams?.name,
            contactName: call.metadata?.customParams?.contact_name,
            status: normalizedStatus,
            disposition: call.leadStatus || '',
            uniqueId: call.metadata?.customParams?.uniqueid,
            createdAt: call.createdAt
          });
        }),
        stats: {
          totalCalls: recentCalls.length,
          // Add more stats as needed
        }
      }
    });

  } catch (error) {
    console.error("Get makecall dashboard error:", error);
    res.status(500).json({
      success: false,
      message: "Internal server error",
      error: error.message
    });
  }
};

// Make single call disposition
exports.makeSingleCallDisposition = async (req, res) => {
  try {
    const { uniqueId, clientUserId } = req.body || {};

    if (!uniqueId) {
      return res.status(400).json({ success: false, error: 'Missing uniqueId', message: 'uniqueId is required' });
    }

    // Use IDs from token for security; allow body clientUserId only for extra validation
    const tokenClientId = req.clientId;
    const tokenClientUserId = req.clientUserId;

    const CallLog = require('../models/CallLog');

    // Find the most recent log for this client and uniqueId
    const log = await CallLog.findOne(
      {
        clientId: tokenClientId,
        'metadata.customParams.uniqueid': uniqueId
      },
      {},
      { sort: { createdAt: -1 } }
    ).lean();

    if (!log) {
      return res.status(404).json({ success: false, error: 'NOT_FOUND', message: 'No call log found for uniqueId' });
    }

    // Validate clientUserId if present in log metadata
    const logClientUserId = log?.metadata?.customParams?.clientUserId || log?.metadata?.clientUserId;
    if (logClientUserId && String(logClientUserId) !== String(tokenClientUserId)) {
      return res.status(403).json({ success: false, error: 'FORBIDDEN', message: 'clientUserId does not match' });
    }
    // Optional extra check if caller provided clientUserId in body
    if (clientUserId && String(clientUserId) !== String(tokenClientUserId)) {
      return res.status(403).json({ success: false, error: 'FORBIDDEN', message: 'clientUserId mismatch' });
    }

    // Disposition typically stored in leadStatus; include helpful metadata
    return res.status(200).json({
      success: true,
      data: {
        disposition: log.disposition || "",
        subDisposition: log.subDisposition || "",
        uniqueId,
        clientUserId
      }
    });
  } catch (error) {
    console.error('Get single call disposition error:', error);
    return res.status(500).json({ success: false, error: 'INTERNAL_ERROR', message: error.message });
  }
}

// Live status API: check call status by uniqueId using CallLog and update MakecallLoginLog history
exports.getLiveCallStatus = async (req, res) => {
  try {
    const { uniqueId } = req.body || {};
    if (!uniqueId) {
      return res.status(400).json({ success: false, error: 'Missing uniqueId' });
    }

    const CallLog = require('../models/CallLog');
    const MakecallLoginLog = require('../models/MakecallLoginLog');

    // Find most recent log for this uniqueId & client
    const log = await CallLog.findOne(
      {
        clientId: req.clientId,
        'metadata.customParams.uniqueid': uniqueId
      },
      {},
      { sort: { createdAt: -1 } }
    ).lean();

    let normalizedStatus = 'ringing';
    let disposition = '';
    let isActive = undefined;
    let createdAt = undefined;

    if (!log) {
      // No log found: compute age from the first entry in MakecallLoginLog if exists
      const parent = await MakecallLoginLog.findById(req.loginLogId).lean();
      const history = parent?.logs?.find(l => l.uniqueId === uniqueId);
      const baseTime = history?.time ? new Date(history.time).getTime() : null;
      if (baseTime && (Date.now() - baseTime) / 1000 >= 40) {
        normalizedStatus = 'missed';
      } else {
        normalizedStatus = 'ringing';
      }
      createdAt = history?.time || null;
    } else {
      disposition = log.leadStatus || '';
      isActive = log?.metadata?.isActive;
      createdAt = log.createdAt;
      if (isActive === true) normalizedStatus = 'ongoing';
      else if (isActive === false) normalizedStatus = 'completed';
      else if ((Date.now() - new Date(createdAt).getTime()) / 1000 >= 40) normalizedStatus = 'missed';
      else normalizedStatus = 'ringing';
    }

    // Update the most recent existing log entry for this uniqueId (do not create new one)
    try {
      if (req.loginLogId) {
        const parent = await MakecallLoginLog.findById(req.loginLogId).lean();
        const candidates = (parent?.logs || []).filter(l => String(l.uniqueId) === String(uniqueId));
        const last = candidates.length ? candidates[candidates.length - 1] : null;
        if (last && last._id) {
          await MakecallLoginLog.updateOne(
            { _id: req.loginLogId, 'logs._id': last._id },
            {
              $set: {
                'logs.$.status': normalizedStatus,
                'logs.$.disposition': disposition,
                'logs.$.time': new Date()
              }
            }
          );
        }
      }
    } catch (e) {
      console.warn('Failed to update live status in MakecallLoginLog:', e?.message);
    }

    return res.status(200).json({
      success: true,
      data: {
        uniqueId,
        status: normalizedStatus,
        disposition,
        isActive: isActive ?? null,
        createdAt
      }
    });
  } catch (error) {
    console.error('Live call status error:', error);
    return res.status(500).json({ success: false, error: 'INTERNAL_ERROR', message: error.message });
  }
}

// Middleware to verify makecall token
exports.verifyMakecallToken = (req, res, next) => {
  try {
    const token = req.headers.authorization?.replace('Bearer ', '');
    
    if (!token) {
      return res.status(401).json({
        success: false,
        message: "Authorization token required"
      });
    }

    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    // Accept either makecall token or standard client token
    if (decoded && decoded.type === 'makecall') {
      // Makecall session token
      req.loginLogId = decoded.id; // MakecallLoginLog document id
      req.clientId = decoded.clientId;
      req.apiKeyId = decoded.apiKey;
      req.clientUserId = decoded.clientUserId || decoded.clientId; // stored explicitly at login
      return next();
    }

    if (decoded && decoded.userType === 'client') {
      // Standard client auth token
      req.loginLogId = undefined;
      req.apiKeyId = undefined;
      req.clientId = decoded.id;
      req.clientUserId = decoded.id;
      return next();
    }

    return res.status(401).json({
      success: false,
      message: "Invalid token type"
    });
  } catch (error) {
    console.error("Token verification error:", error);
    res.status(401).json({
      success: false,
      message: "Invalid or expired token"
    });
  }
};
