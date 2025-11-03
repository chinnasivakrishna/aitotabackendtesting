const mongoose = require('mongoose');

const campaignSchema = new mongoose.Schema({
    name: { type: String, required: true, trim: true },
    description: { type: String, trim: true },
    category: { type: String, trim: true },
    
    // Campaign execution state
    isRunning: { type: Boolean, default: false, index: true },
    status: { 
        type: String, 
        enum: ['pending', 'running', 'paused', 'stopped', 'completed', 'failed'], 
        default: 'pending',
        index: true
    },
    pauseFlag: { type: Boolean, default: false },
    currentBatch: { type: Number, default: 0 },
    currentContactIndex: { type: Number, default: 0 },
    
    // NGR Configuration (N-G-R formula for parallel mode)
    mode: { type: String, enum: ['parallel', 'serial'], default: 'serial' },
    n: { type: Number, default: 1 }, // Number of calls in batch
    g: { type: Number, default: 5 }, // Gap between calls in seconds
    r: { type: Number, default: 10 }, // Rest after batch in seconds
    
    // Provider selection
    provider: { type: String, enum: ['sanpbx', 'czentrix', 'c-zentrix'], default: 'sanpbx' },
    
    // Assignments
    agent: [{ type: String, trim: true }],
    agentId: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent' },
    groupIds: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Group' }],
    clientId: { type: String, required: true, index: true },
    
    // Campaign contacts (copied from groups)
    contacts: [{
        _id: { type: mongoose.Schema.Types.ObjectId },
        name: { type: String, default: '' },
        phone: { type: String, required: true },
        status: { type: String, enum: ['pending', 'calling', 'completed', 'failed'], default: 'pending' },
        email: { type: String, default: '' },
        bookmarked: { type: Boolean, default: false },
        addedAt: { type: Date, default: Date.now }
    }],
    
    // Group selections (for tracking which contacts from which groups)
    groupSelections: [{
        groupId: { type: mongoose.Schema.Types.ObjectId, ref: 'Group', required: true },
        groupName: { type: String, default: '' },
        startIndex: { type: Number, default: 0 },
        endIndex: { type: Number, default: 0 },
        selectedIndices: [{ type: Number }],
        count: { type: Number, default: 0 },
        replace: { type: Boolean, default: false },
        addedAt: { type: Date, default: Date.now }
    }],
    
    // Execution metadata
    startedAt: { type: Date },
    completedAt: { type: Date },
    pausedAt: { type: Date },
    resumedAt: { type: Date },
    totalCalls: { type: Number, default: 0 },
    successfulCalls: { type: Number, default: 0 },
    failedCalls: { type: Number, default: 0 },
    
    createdAt: { type: Date, default: Date.now },
    updatedAt: { type: Date, default: Date.now }
});

// Indexes for performance
campaignSchema.index({ clientId: 1, status: 1 });
campaignSchema.index({ isRunning: 1, status: 1 });

campaignSchema.pre('save', function(next) {
    this.updatedAt = Date.now();
    next();
});

module.exports = mongoose.model('Campaigns', campaignSchema);
