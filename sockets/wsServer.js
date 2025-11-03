const socketio = require('socket.io');
let io = null;
function init(server) {
  io = socketio(server, { cors: { origin: '*' } });
  io.on('connection', socket => {
    socket.on('join-campaign', campaignId => { socket.join('campaign-' + campaignId); });
    socket.on('leave-campaign', campaignId => { socket.leave('campaign-' + campaignId); });
  });
}
function broadcastCampaignEvent(campaignId, event, payload) {
  io && io.to('campaign-' + campaignId).emit('campaign-status', { event, ...payload });
}
function broadcastCallEvent(campaignId, uniqueId, status, callLog) {
  io && io.to('campaign-' + campaignId).emit('call-status', { uniqueId, status, callLog });
}
module.exports = { init, broadcastCampaignEvent, broadcastCallEvent };
