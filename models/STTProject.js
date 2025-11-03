const mongoose = require('mongoose');

const STTItemSchema = new mongoose.Schema(
  {
    s3Key: { type: String, required: true },
    originalFilename: { type: String },
    contentType: { type: String },
    transcriptKey: { type: String },
    qaKey: { type: String },
    status: {
      type: String,
      enum: ['uploaded', 'processing', 'completed', 'failed'],
      default: 'uploaded',
    },
    error: { type: String },
    logs: [
      {
        at: { type: Date, default: Date.now },
        level: { type: String, enum: ['info', 'warn', 'error'], default: 'info' },
        message: { type: String },
        meta: { type: Object },
      },
    ],
  },
  { timestamps: true }
);

const STTProjectSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    description: { type: String },
    category: { type: String },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'Admin' },
    items: [STTItemSchema],
  },
  { timestamps: true }
);

module.exports = mongoose.model('STTProject', STTProjectSchema);


