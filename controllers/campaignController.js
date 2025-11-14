const orchestrator = require('../services/campaignOrchestrator');
const wsServer = require('../sockets/wsServer');
const Campaign = require('../models/Campaign');
const mongoose = require('mongoose');

// Helper function to validate campaign ID
function validateCampaignId(id) {
  if (!id) {
    throw new Error('Campaign ID is required');
  }
  if (!mongoose.Types.ObjectId.isValid(id)) {
    throw new Error('Invalid campaign ID format');
  }
  return id;
}

// Get campaign details
exports.getCampaign = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    const campaign = await Campaign.findById(campaignId)
      .populate('agent', 'agentId name serviceProvider')
      .populate('groupIds', 'name description')
      .select('-details -uniqueIds -contacts'); // Exclude heavy fields
    
    if (!campaign) {
      return res.status(404).json({ success: false, message: 'Campaign not found' });
    }
    
    res.json({ success: true, data: campaign });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
};

// Start campaign
exports.start = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    
    // Validate N-G-R parameters if provided
    const { n, g, r, mode } = req.body;
    if (n !== undefined && (n < 1 || n > 100)) {
      return res.status(400).json({ success: false, message: 'N (batch size) must be between 1 and 100' });
    }
    if (g !== undefined && (g < 0 || g > 300)) {
      return res.status(400).json({ success: false, message: 'G (gap) must be between 0 and 300 seconds' });
    }
    if (r !== undefined && (r < 0 || r > 300)) {
      return res.status(400).json({ success: false, message: 'R (rest) must be between 0 and 300 seconds' });
    }
    if (mode && !['parallel', 'series'].includes(mode)) {
      return res.status(400).json({ success: false, message: 'Mode must be either "parallel" or "series"' });
    }
    
    // Fire-and-forget to avoid gateway timeout; orchestration runs in background
    orchestrator.startCampaign(campaignId, req.body).catch(err => {
      console.error('Campaign start failed:', err?.message || err);
    });
    res.status(202).json({ success: true, started: true, message: 'Campaign start command received' });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    next(err);
  }
};

// Stop campaign
exports.stop = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    await orchestrator.stopCampaign(campaignId);
    res.json({ success: true, message: 'Campaign stopped successfully' });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Not found') {
      return res.status(404).json({ success: false, message: 'Campaign not found' });
    }
    next(err);
  }
};

// Pause campaign
exports.pause = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    await orchestrator.pauseCampaign(campaignId);
    res.json({ success: true, message: 'Campaign paused successfully' });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Not found') {
      return res.status(404).json({ success: false, message: 'Campaign not found' });
    }
    next(err);
  }
};

// Resume campaign
exports.resume = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    await orchestrator.resumeCampaign(campaignId);
    res.json({ success: true, message: 'Campaign resumed successfully' });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Not found') {
      return res.status(404).json({ success: false, message: 'Campaign not found' });
    }
    next(err);
  }
};

// Get campaign status
exports.getStatus = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    const status = await orchestrator.getStatus(campaignId);
    res.json({ success: true, data: status });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Not found') {
      return res.status(404).json({ success: false, message: 'Campaign not found' });
    }
    next(err);
  }
};

// Get campaign transcripts
exports.getTranscripts = async (req, res, next) => {
  try {
    const campaignId = validateCampaignId(req.params.id);
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : undefined;
    
    if (limit !== undefined && (isNaN(limit) || limit < 1 || limit > 1000)) {
      return res.status(400).json({ success: false, message: 'Limit must be between 1 and 1000' });
    }
    
    const snapshot = await wsServer.buildCampaignTranscriptSnapshot(campaignId, {
      limit: Number.isFinite(limit) && limit > 0 ? limit : undefined,
    });
    res.json({ success: true, data: snapshot });
  } catch (err) {
    if (err?.message === 'Campaign ID is required' || err?.message === 'Invalid campaign ID format') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Invalid campaignId' || err?.message === 'campaignId is required') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Campaign not found') {
      return res.status(404).json({ success: false, message: err.message });
    }
    next(err);
  }
};
