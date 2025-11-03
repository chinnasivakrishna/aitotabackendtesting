const Campaign = require('../models/Campaign');
const CallLog = require('../models/CallLog');
const Agent = require('../models/Agent');
const telephonyService = require('./telephonyService');
const kafkaService = require('./kafkaService');
const wsServer = require('../sockets/wsServer');
const { v4: uuidv4 } = require('uuid');

function updateCampaignStatus(campaign, status) {
  campaign.status = status;
  campaign.isRunning = ['start', 'resume'].includes(status);
  campaign.updatedAt = new Date();
  return campaign.save();
}

function broadcastCampaign(campaign, event) {
  wsServer.broadcastCampaignEvent(campaign._id, event, campaign);
  kafkaService.send('campaign-status', { campaignId: campaign._id, status: event });
}

exports.startCampaign = async (campaignId, { n, g, r, mode }) => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  await updateCampaignStatus(campaign, 'start');
  broadcastCampaign(campaign, 'start');
  // Resolve agent reference: campaign.agent is an array of strings per schema
  const agentRef = Array.isArray(campaign.agent) ? (campaign.agent[0] || null) : campaign.agent;
  let agent = null;
  if (agentRef) {
    // Try by Mongo _id first, then by custom agentId field
    agent = await Agent.findById(agentRef).lean();
    if (!agent) {
      agent = await Agent.findOne({ agentId: agentRef }).lean();
    }
  }
  if (!agent) {
    throw new Error('Agent not found for campaign. Please ensure campaign.agent contains a valid Agent ID or agentId');
  }

  // Batch contacts processing for N-G-R
  const contacts = campaign.contacts;
  function* batch(arr, size) { for (let i = 0; i < arr.length; i += size) yield arr.slice(i, i + size); }
  const contactBatches = [...batch(contacts, n)];
  for (const batch of contactBatches) {
    if (mode === 'series') {
      for (const contact of batch)
        await handleCall(campaign, contact, agent);
    } else {
      await Promise.all(batch.map(contact => handleCall(campaign, contact, agent)));
    }
  }
  await updateCampaignStatus(campaign, 'stop');
  broadcastCampaign(campaign, 'stop');
};

async function handleCall(campaign, contact, agent) {
  const uniqueid = uuidv4();
  let res;
  if (agent.serviceProvider === "c-zentrix") {
    res = await telephonyService.callCzentrix({
      phone: contact.phone, agent, contact, campaignId: campaign._id, uniqueid
    });
  } else if (agent.serviceProvider === "sanpbx") {
    res = await telephonyService.callSanpbx({
      phone: contact.phone, agent, contact, uniqueid
    });
  }
  let log = null, waited = 0;
  while (waited < 40000) {
    log = await CallLog.findOne({ "metadata.customParams.uniqueid": uniqueid });
    if (log) break;
    await new Promise(res => setTimeout(res, 2000));
    waited += 2000;
  }
  let status = "ringing";
  if (log && log.isActive) status = "ongoing";
  else if (log && !log.isActive) status = "completed";
  // Persist under the defined `details` array in Campaign schema
  const detail = {
    uniqueId: uniqueid,
    contactId: contact._id || contact.id,
    time: new Date(),
    status,
    runId: campaign._id.toString()
  };
  if (log && log.leadStatus) {
    detail.leadStatus = log.leadStatus;
  }
  await Campaign.findByIdAndUpdate(
    campaign._id,
    {
      $push: { details: detail },
      $set: { updatedAt: new Date() }
    },
    { new: false }
  );
  kafkaService.send('call-status', { campaignId: campaign._id, uniqueid, status });
  wsServer.broadcastCallEvent(campaign._id, uniqueid, status, log);
}

exports.stopCampaign = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  await updateCampaignStatus(campaign, 'stop');
  broadcastCampaign(campaign, 'stop');
};

exports.pauseCampaign = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  await updateCampaignStatus(campaign, 'pause');
  broadcastCampaign(campaign, 'pause');
};
exports.resumeCampaign = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  await updateCampaignStatus(campaign, 'resume');
  broadcastCampaign(campaign, 'resume');
};
exports.getStatus = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  return { status: campaign.status, isRunning: campaign.isRunning };
};
