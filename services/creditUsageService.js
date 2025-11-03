const Credit = require('../models/Credit');
const CallLog = require('../models/CallLog');
const Campaign = require('../models/Campaign');

// Default rate: 2 credits per minute
const DEFAULT_CREDITS_PER_MINUTE = 2;

/**
 * Calculate billable minutes from a call log.
 * Falls back to campaign details duration if present.
 */
async function getCallDurationSecondsByUniqueId(uniqueId) {
  const log = await CallLog.findOne({ 'metadata.customParams.uniqueid': uniqueId }).lean();
  if (log?.metadata?.callEndTime && log?.createdAt) {
    return Math.max(0, Math.floor((new Date(log.metadata.callEndTime) - new Date(log.createdAt)) / 1000));
  }
  // Fallback: look up campaign details
  const campaign = await Campaign.findOne({ 'details.uniqueId': uniqueId }, { details: 1 }).lean();
  const detail = campaign?.details?.find(d => d.uniqueId === uniqueId);
  if (detail?.callDuration) return detail.callDuration;
  if (detail?.time) return Math.max(0, Math.floor((Date.now() - new Date(detail.time)) / 1000));
  return 0;
}

/**
 * Round up seconds to full minutes (minimum 1 minute if any duration)
 */
function toBillableMinutes(seconds) {
  if (!seconds || seconds <= 0) return 0;
  const minutes = Math.ceil(seconds / 60);
  return Math.max(1, minutes);
}

/**
 * Deduct credits for a completed call given clientId and uniqueId.
 * Applies inbound or outbound the same rate as requested.
 */
async function deductCreditsForCall({ clientId, uniqueId, ratePerMinute = DEFAULT_CREDITS_PER_MINUTE }) {
  const log = await CallLog.findOne({ 'metadata.customParams.uniqueid': uniqueId }).lean();
  const durationSec = await getCallDurationSecondsByUniqueId(uniqueId);
  const billableMinutes = toBillableMinutes(durationSec);
  const creditsToDeduct = billableMinutes * ratePerMinute;

  if (creditsToDeduct <= 0) {
    return { success: true, deducted: 0, minutes: 0 };
  }

  const number = log?.mobile || log?.phoneNumber || log?.metadata?.callerId || '';
  const direction = log?.metadata?.callDirection || (log?.callType === 'outbound' ? 'outbound' : 'inbound');
  const campaignId = log?.campaignId || null;
  const agentId = log?.agentId || null;

  const credit = await Credit.getOrCreateCreditRecord(clientId);
  await credit.useCredits(creditsToDeduct, 'call', `Call ${direction} ${number} • ${billableMinutes} min`, {
    duration: billableMinutes,
    seconds: durationSec,
    number,
    direction,
    uniqueId,
    campaignId: campaignId ? String(campaignId) : undefined,
    agentId: agentId ? String(agentId) : undefined,
  });

  return { success: true, deducted: creditsToDeduct, minutes: billableMinutes };
}

module.exports = {
  deductCreditsForCall,
  DEFAULT_CREDITS_PER_MINUTE,
};


