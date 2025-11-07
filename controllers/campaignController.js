const orchestrator = require('../services/campaignOrchestrator');
const wsServer = require('../sockets/wsServer');
exports.start = async (req, res, next) => {
  try {
    // Fire-and-forget to avoid gateway timeout; orchestration runs in background
    orchestrator.startCampaign(req.params.id, req.body).catch(err => {
      console.error('Campaign start failed:', err?.message || err);
    });
    res.status(202).json({ success: true, started: true });
  } catch (err) { next(err); }
};
exports.stop = async (req, res, next) => {
  try { await orchestrator.stopCampaign(req.params.id); res.json({ success: true }); }
  catch (err) { next(err); }
};
exports.pause = async (req, res, next) => {
  try { await orchestrator.pauseCampaign(req.params.id); res.json({ success: true }); }
  catch (err) { next(err); }
};
exports.resume = async (req, res, next) => {
  try { await orchestrator.resumeCampaign(req.params.id); res.json({ success: true }); }
  catch (err) { next(err); }
};
exports.getStatus = async (req, res, next) => {
  try { res.json(await orchestrator.getStatus(req.params.id)); }
  catch (err) { next(err); }
};
exports.getTranscripts = async (req, res, next) => {
  try {
    const limit = req.query.limit ? parseInt(req.query.limit, 10) : undefined;
    const snapshot = await wsServer.buildCampaignTranscriptSnapshot(req.params.id, {
      limit: Number.isFinite(limit) && limit > 0 ? limit : undefined,
    });
    res.json({ success: true, data: snapshot });
  } catch (err) {
    if (err?.message === 'Invalid campaignId' || err?.message === 'campaignId is required') {
      return res.status(400).json({ success: false, message: err.message });
    }
    if (err?.message === 'Campaign not found') {
      return res.status(404).json({ success: false, message: err.message });
    }
    next(err);
  }
};
