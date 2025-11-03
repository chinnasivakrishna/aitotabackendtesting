const mongoose = require('mongoose');

const waMessageSchema = new mongoose.Schema({
  messageId: { type: String, index: true },
  direction: { type: String, enum: ['sent', 'received'], required: true },
  text: { type: String, default: '' },
  status: { type: String, default: '' },
  type: { type: String, default: 'text' },
  timestamp: { type: Date, required: true },
}, { _id: false });

const waChatSchema = new mongoose.Schema({
  clientId: { type: String, index: true, required: true },
  phoneNumber: { type: String, index: true, required: true },
  contactName: { type: String, default: '' },
  messages: { type: [waMessageSchema], default: [] },
  lastSyncedAt: { type: Date, default: Date.now },
}, { timestamps: true });

waChatSchema.index({ clientId: 1, phoneNumber: 1 }, { unique: true });

module.exports = mongoose.model('WaChat', waChatSchema);



