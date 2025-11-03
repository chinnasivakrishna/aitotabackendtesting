const mongoose = require('mongoose')

const agentAccessRequestSchema = new mongoose.Schema({
  clientId: { type: String, required: true, index: true },
  agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent', required: true, index: true },
  platform: { type: String, enum: ['whatsapp', 'telegram', 'email', 'sms'], required: true, index: true },
  templateName: { type: String },
  status: { type: String, enum: ['pending', 'approved', 'rejected'], default: 'pending', index: true },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
})

agentAccessRequestSchema.pre('save', function(next) {
  this.updatedAt = new Date()
  next()
})

module.exports = mongoose.model('AgentAccessRequest', agentAccessRequestSchema)


