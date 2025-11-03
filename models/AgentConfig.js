const mongoose = require('mongoose');

const agentConfigItemSchema = new mongoose.Schema({
  n: { type: Number, default: 1 },
  g: { type: Number, required: true },
  rSec: { type: Number, required: true },
  isDefault: { type: Boolean, default: false }
}, { _id: true });

const agentConfigSchema = new mongoose.Schema({
  agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent', required: true, index: true },
  agentName: { type: String },
  didNumber: { type: String },
  mode: { type: String, enum: ['parallel', 'serial'], default: 'serial' },
  pMode: { type: String, enum: ['slow', 'fast', 'effective'], default: 'effective' },
  items: { type: [agentConfigItemSchema], default: [] },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

agentConfigSchema.pre('save', function(next) {
  this.updatedAt = new Date();
  next();
});

module.exports = mongoose.model('AgentConfig', agentConfigSchema);


