const mongoose = require('mongoose');

const didNumberSchema = new mongoose.Schema(
  {
    did: { type: String, required: true, unique: true, index: true },
    provider: { type: String, required: true, enum: ['snapbx', 'sanpbx', 'c-zentrix', 'tata', 'other'], index: true },
    callerId: { type: String },
    status: { type: String, enum: ['available', 'assigned'], default: 'available', index: true },
    assignedAgent: { type: mongoose.Schema.Types.ObjectId, ref: 'Agent', default: null },
    assignedClient: { type: String, default: null }, // stores Client _id as string (matches Agent.clientId type)
    notes: { type: String },
  },
  { timestamps: true }
);

// Helper: derive callerId from DID (last 7 digits)
didNumberSchema.statics.deriveCallerIdFromDid = function(did) {
  const numeric = String(did || '').replace(/\D/g, '');
  if (!numeric) return '';
  return numeric.slice(-7);
};

module.exports = mongoose.model('DidNumber', didNumberSchema);


