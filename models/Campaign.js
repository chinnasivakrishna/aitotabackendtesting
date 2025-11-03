const mongoose = require('mongoose');

const campaignSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    trim: true
  },
  description: {
    type: String,
    trim: true
  },
  category: {
    type: String,
    trim: true
  },
  isRunning: {
    type: Boolean,
    default: false
  },
  agent: [{
    type: String,
    trim: true
  }],
  groupIds: [{
    type: mongoose.Schema.Types.ObjectId,
    ref: 'Group'
  }],
  clientId: {
    type: String,
    required: true,
    index: true
  },
  // Array to store detailed call information for campaign calls
  details: [{
    uniqueId: { 
      type: String, 
      required: true,
      index: true 
    },
    contactId: { 
      type: mongoose.Schema.Types.ObjectId, 
      ref: 'Contact'
      // Removed required: true to make it optional
    },
    time: { 
      type: Date, 
      default: Date.now 
    },
    status: { 
      type: String, 
      enum: ['ringing', 'ongoing', 'completed'], 
      default: 'ringing' 
    },
    runId:{
      type: String,
      required: false
    },
    leadStatus: {
      type: String,
      enum: [
        // Connected - Interested
        'vvi',                    // very very interested
        'maybe',                  // maybe
        'enrolled',               // enrolled
        
        // Connected - Not Interested
        'junk_lead',              // junk lead
        'not_required',           // not required
        'enrolled_other',         // enrolled other
        'decline',                // decline
        'not_eligible',           // not eligible
        'wrong_number',           // wrong number
        
        // Connected - Followup
        'hot_followup',           // hot followup
        'cold_followup',          // cold followup
        'schedule',               // schedule
        
        // Not Connected
        'not_connected'           // not connected
      ],
      default: 'not_connected'
    },
  }],
  // Array to store campaign contacts (copied from groups but can be manipulated independently)
  contacts: [{
    _id: {type: mongoose.Schema.Types.ObjectId},
    name: { type: String, required: false, default: '' },
    phone: { type: String, required: true },
    status: { type: String, enum: ['default', 'interested', 'maybe', 'not interested'], default: 'default' },
    email: { type: String, default: "" },
    bookmarked: { type: Boolean, default: false },
    addedAt: { type: Date, default: Date.now }
  }],
  // Persist per-group contact selections (range or explicit indices) for visibility across the app
  groupSelections: [{
    groupId: { type: mongoose.Schema.Types.ObjectId, ref: 'Group', required: true },
    groupName: { type: String, default: '' },
    // Either a range [startIndex, endIndex) or explicit indices; both can be stored
    startIndex: { type: Number, default: 0 },
    endIndex: { type: Number, default: 0 },
    selectedIndices: [{ type: Number }],
    count: { type: Number, default: 0 },
    replace: { type: Boolean, default: false },
    addedAt: { type: Date, default: Date.now }
  }],
  createdAt: {
    type: Date,
    default: Date.now
  },
  updatedAt: {
    type: Date,
    default: Date.now
  }
});

// Index for better performance on status queries
campaignSchema.index({ 'details.status': 1 });

// Ensure virtual fields are included when converting to JSON
campaignSchema.set('toJSON', { virtuals: true });
campaignSchema.set('toObject', { virtuals: true });

module.exports = mongoose.model('Campaign', campaignSchema); 