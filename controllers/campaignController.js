const orchestrator = require('../services/campaignOrchestrator');
exports.start = async (req, res, next) => {
  try { await orchestrator.startCampaign(req.params.id, req.body); res.json({ success: true }); }
  catch (err) { next(err); }
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
