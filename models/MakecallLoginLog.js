const mongoose = require('mongoose');

const makecallLoginLogSchema = new mongoose.Schema({
  clientId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Client',
    required: false // Can be null for failed logins
  },
  apiKeyId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'ClientApiKey',
    required: false // Can be null for failed logins
  },
  email: {
    type: String,
    required: false,
    trim: true
  },
  name: {
    type: String,
    required: false,
    trim: true
  },
  phone: {
    type: String,
    required: false,
    trim: true
  },
  success: {
    type: Boolean,
    required: true,
    default: false
  },
  logs: [
    {
        uniqueId:{
            type: String,
            required: true
        },
        name: {
            type: String,
            required: false,
            trim: true
        },
        number: {
            type: String,
            required: true,
            trim: true
        },
        status: {
            type: String,
            required: false,
            trim: true
        },
        disposition: {
            type: String,
            required: false,
        },
        subDisposition: {
            type: String,
            required: false,
        },
        time: {
            type: Date,
            required: false,
            default: Date.now
        },
    },
  ],
  loginAt: {
    type: Date,
    required: true,
    default: Date.now
  }
}, {
  timestamps: true
});

// Index for better query performance
makecallLoginLogSchema.index({ clientId: 1, loginAt: -1 });
makecallLoginLogSchema.index({ success: 1, loginAt: -1 });
makecallLoginLogSchema.index({ email: 1, success: 1 }); // For duplicate check optimization

module.exports = mongoose.model('MakecallLoginLog', makecallLoginLogSchema);
