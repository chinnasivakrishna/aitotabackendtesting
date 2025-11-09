const axios = require('axios');
const telephony = require('../config/telephony');

// Token cache per agent (keyed by agentId + accessKey to handle different agents)
const sanpbxTokenCache = new Map();

exports.callCzentrix = async ({ phone, agent, contact, campaignId, uniqueid }) => {
  // Use agent's X_API_KEY if available, otherwise fall back to config
  const apiKey = agent.X_API_KEY || telephony.czentrix.apiKey;
  const client = telephony.czentrix.client;
  
  if (!apiKey) {
    throw new Error('C-Zentrix API key not found. Please set X_API_KEY on agent or in config.');
  }
  
  if (!agent.callerId) {
    throw new Error('Agent missing callerId for C-Zentrix call');
  }
  
  const body = {
    transaction_id: "CTI_BOT_DIAL",
    phone_num: phone,
    uniqueid,
    callerid: agent.callerId,
    uuid: campaignId || campaignId.toString(),
    custom_param: {
      name: contact.name || '',
      uniqueid
    },
    resFormat: 3
  };
  
  const headers = {
    "X-CLIENT": client,
    "X-API-KEY": apiKey,
    "Content-Type": "application/json"
  };
  
  console.log(`📞 [TELEPHONY] C-Zentrix call request:`, { phone, uniqueid, callerid: agent.callerId, uuid: campaignId });
  const resp = await axios.post(telephony.czentrix.url, body, { headers });
  console.log(`📞 [TELEPHONY] C-Zentrix response:`, resp.data);
  return resp.data;
};

exports.getSanpbxToken = async (agent) => {
  // Use agent-specific credentials
  const accessToken = agent.accessToken;
  const accessKey = agent.accessKey || 'mob'; // Default to 'mob' if not set
  
  if (!accessToken) {
    throw new Error('SANPBX accessToken not found on agent');
  }
  
  // Create cache key based on agent ID and accessKey
  const cacheKey = `${agent._id || agent.agentId}_${accessKey}`;
  
  // Check cache first
  const cached = sanpbxTokenCache.get(cacheKey);
  if (cached && cached.expiry > Date.now()) {
    console.log(`📞 [TELEPHONY] Using cached SANPBX token for agent ${agent._id || agent.agentId}`);
    return cached.token;
  }
  
  // Get new token
  console.log(`📞 [TELEPHONY] Getting new SANPBX token for agent ${agent._id || agent.agentId}`);
  const resp = await axios.post(
    telephony.sanpbx.gentokenUrl, 
    { access_key: accessKey }, 
    {
      headers: { Accesstoken: accessToken },
      timeout: 10000
    }
  );
  
  const apiToken = resp.data.Apittoken || resp.data.Apitoken;
  if (!apiToken) {
    throw new Error(`Failed to get SANPBX token: ${JSON.stringify(resp.data)}`);
  }
  
  // Cache the token (expiry is typically 1 hour, cache for 50 minutes to be safe)
  const expiryTime = resp.data.expiry_time 
    ? new Date(resp.data.expiry_time).getTime() - 600000 // 10 minutes before expiry
    : Date.now() + (50 * 60 * 1000); // Default 50 minutes
  
  sanpbxTokenCache.set(cacheKey, {
    token: apiToken,
    expiry: expiryTime
  });
  
  console.log(`✅ [TELEPHONY] SANPBX token obtained and cached for agent ${agent._id || agent.agentId}`);
  return apiToken;
};

exports.callSanpbx = async ({ phone, agent, contact, uniqueid }) => {
  // Validate required fields
  if (!agent.accessToken) {
    throw new Error('Agent missing accessToken for SANPBX call');
  }
  
  if (!agent.callerId) {
    throw new Error('Agent missing callerId for SANPBX call');
  }
  
  // Get agent-specific token
  const apiToken = await exports.getSanpbxToken(agent);
  
  // Use agent's appId if available, otherwise default to 2
  const appId = agent.appId ? parseInt(agent.appId) : 2;
  console.log(`📞 [TELEPHONY] App ID: ${appId}`);
  
  // Format phone number for SANPBX:
  // 1. Remove +91 if present
  // 2. Add 0 before the number if it doesn't start with 0
  const originalPhone = String(phone).trim();
  let formattedPhone = originalPhone;
  
  // Remove +91 prefix if present
  formattedPhone = formattedPhone.replace(/^\+91/, '');
  
  // Remove any remaining + sign
  formattedPhone = formattedPhone.replace(/^\+/, '');
  
  // Remove any spaces or dashes
  formattedPhone = formattedPhone.replace(/[\s\-]/g, '');
  
  // Add 0 prefix if number doesn't start with 0
  if (!formattedPhone.startsWith('0')) {
    formattedPhone = '0' + formattedPhone;
  }
  
  console.log(`📞 [TELEPHONY] Phone number formatted: ${originalPhone} → ${formattedPhone}`);
  
  const body = {
    appid: appId,
    call_to: formattedPhone,
    custom_field: {
      uniqueid,
      name: contact.name || ''
    },
    caller_id: agent.callerId
  };
  
  const headers = { Apitoken: apiToken };
  
  console.log(`📞 [TELEPHONY] SANPBX dial call request:`, { 
    phone: formattedPhone, 
    uniqueid, 
    caller_id: agent.callerId,
    appid: appId 
  });
  
  const resp = await axios.post(telephony.sanpbx.dialcallUrl, body, { 
    headers,
    timeout: 10000
  });
  
  console.log(`📞 [TELEPHONY] SANPBX dial call response:`, resp.data);
  return resp.data;
};
