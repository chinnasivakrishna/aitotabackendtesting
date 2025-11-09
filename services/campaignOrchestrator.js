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
  
  // Don't set isRunning here - let the Kafka consumer handle it atomically
  // This prevents race conditions where the consumer sees it as already running
  // broadcastCampaign(campaign, 'start'); // Don't broadcast until consumer confirms
  
  // Publish campaign start command to Kafka instead of processing directly
  const campaignCommand = {
    action: 'start',
    campaignId: campaignId.toString(),
    n: n || 1,
    g: g || 5,
    r: r || 30,
    mode: mode || 'parallel',
    timestamp: new Date().toISOString()
  };
  
  console.log(`📨 [KAFKA] Publishing campaign start command for campaign ${campaignId}`);
  await kafkaService.send('campaign-commands', campaignCommand);
  console.log(`✅ [KAFKA] Campaign start command published successfully`);
  
  // The Kafka consumer will handle setting isRunning and broadcasting
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
  
  // Publish stop command to Kafka
  const campaignCommand = {
    action: 'stop',
    campaignId: campaignId.toString(),
    timestamp: new Date().toISOString()
  };
  
  console.log(`📨 [KAFKA] Publishing campaign stop command for campaign ${campaignId}`);
  await kafkaService.send('campaign-commands', campaignCommand);
  
  await updateCampaignStatus(campaign, 'stop');
  broadcastCampaign(campaign, 'stop');
};

exports.pauseCampaign = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  
  // Publish pause command to Kafka
  const campaignCommand = {
    action: 'pause',
    campaignId: campaignId.toString(),
    timestamp: new Date().toISOString()
  };
  
  console.log(`📨 [KAFKA] Publishing campaign pause command for campaign ${campaignId}`);
  await kafkaService.send('campaign-commands', campaignCommand);
  
  await updateCampaignStatus(campaign, 'pause');
  broadcastCampaign(campaign, 'pause');
};

exports.resumeCampaign = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  
  // Publish resume command to Kafka
  const campaignCommand = {
    action: 'resume',
    campaignId: campaignId.toString(),
    timestamp: new Date().toISOString()
  };
  
  console.log(`📨 [KAFKA] Publishing campaign resume command for campaign ${campaignId}`);
  await kafkaService.send('campaign-commands', campaignCommand);
  
  await updateCampaignStatus(campaign, 'resume');
  broadcastCampaign(campaign, 'resume');
};
exports.getStatus = async campaignId => {
  const campaign = await Campaign.findById(campaignId);
  if (!campaign) throw new Error('Not found');
  return { status: campaign.status, isRunning: campaign.isRunning };
};
