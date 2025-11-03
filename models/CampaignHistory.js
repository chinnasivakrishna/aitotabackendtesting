const mongoose = require('mongoose');

const CampaignHistorySchema = new mongoose.Schema({
    campaignId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: 'Campaign',
        required: true
    },
    runId: {
        type: String,
        required: true,
        unique: true
    },
    instanceNumber: {
        type: Number,
        required: true
    },
    startTime: {
        type: String,
        required: true
    },
    endTime: {
        type: String,
        required: true
    },
    runTime: {
        hours: { type: Number, default: 0 },
        minutes: { type: Number, default: 0 },
        seconds: { type: Number, default: 0 }
    },
    status: {
        type: String,
        enum: ['running', 'completed', 'failed', 'paused'],
        default: 'running'
    },
    contacts: [{
        documentId: String,
        number: String,
        name: String,
        leadStatus: String,
        contactId: String,
        time: String,
        status: String,
        duration: Number,
        transcriptCount: Number,
        whatsappMessageSent: Boolean,
        whatsappRequested: Boolean
    }],
    stats: {
        totalContacts: { type: Number, default: 0 },
        successfulCalls: { type: Number, default: 0 },
        failedCalls: { type: Number, default: 0 },
        totalCallDuration: { type: Number, default: 0 },
        averageCallDuration: { type: Number, default: 0 }
    },
    batchInfo: {
        batchNumber: { type: Number },
        totalBatches: { type: Number },
        isIntermediate: { type: Boolean, default: false }
    },
    createdAt: {
        type: Date,
        default: Date.now
    },
    updatedAt: {
        type: Date,
        default: Date.now
    }
});

// Update the updatedAt field before saving
CampaignHistorySchema.pre('save', function(next) {
    this.updatedAt = new Date();
    next();
});

module.exports = mongoose.model('CampaignHistory', CampaignHistorySchema);