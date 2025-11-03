const mongoose = require("mongoose")
const clientApiKeySchema = new mongoose.Schema({
  clientId: { type: String, required: true, index: true },
  encryptedKey: { type: String, required: true },
  keyHash: { type: String, required: true },
  keyPreview: { type: String, required: true },
  isActive: { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
})

clientApiKeySchema.index({ clientId: 1, isActive: 1 })

clientApiKeySchema.pre("save", function (next) {
  this.updatedAt = Date.now()
  next()
})

module.exports = mongoose.model("ClientApiKey", clientApiKeySchema)


