const orchestrator = require('../services/campaignOrchestrator');
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
