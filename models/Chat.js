const mongoose = require('mongoose')

const chatMessageSchema = new mongoose.Schema({
  role: { type: String, enum: ['system', 'user', 'assistant', 'transcript'], required: true },
  type: { type: String, enum: ['text', 'transcript', 'event'], default: 'text' },
  text: { type: String, default: '' },
  meta: { type: Object, default: {} },
  createdAt: { type: Date, default: Date.now }
}, { _id: false })

const chatSchema = new mongoose.Schema({
  clientId: { type: String, index: true },
  agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent', index: true },
  sessionId: { type: String, index: true },
  connectionId: { type: String },
  startedAt: { type: Date, default: Date.now },
  endedAt: { type: Date },
  lastActivityAt: { type: Date, default: Date.now },
  messages: { type: [chatMessageSchema], default: [] }
})

chatSchema.index({ clientId: 1, agentId: 1, startedAt: -1 })

module.exports = mongoose.model('Chat', chatSchema)


