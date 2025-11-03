const mongoose = require('mongoose')

const templateSchema = new mongoose.Schema({
  platform: {
    type: String,
    enum: ['whatsapp', 'telegram', 'email', 'sms'],
    required: true,
    index: true
  },
  name: { type: String, required: true },
  url: { type: String, required: true },
  imageUrl: { type: String },
  description: { type: String },
  createdBy: { type: String },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
})

templateSchema.pre('save', function(next) {
  this.updatedAt = new Date()
  next()
})

module.exports = mongoose.model('Template', templateSchema)


