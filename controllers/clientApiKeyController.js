const crypto = require("crypto")
const ClientApiKey = require("../models/ClientApiKey")

const generateRawKey = () => {
  // 32 bytes -> 64 hex chars, prefix with ait_
  return "ait_" + crypto.randomBytes(24).toString("hex")
}

const hashKey = (key) => {
  // Store only a hash (HMAC with server secret) to allow verification later if needed
  const secret = process.env.ENCRYPTION_SECRET || "default-secret-key-change-in-production"
  return crypto.createHmac("sha256", secret).update(key).digest("hex")
}

exports.generateClientApiKey = async (req, res) => {
  try {
    const clientId = req.clientId
    if (!clientId) {
      return res.status(401).json({ success: false, error: "Unauthorized" })
    }

    const rawKey = generateRawKey()
    const keyHash = hashKey(rawKey)
    const keyPreview = rawKey.substring(0, 8) + "..." + rawKey.slice(-4)
    // Encrypt raw key with server secret
    const algorithm = "aes-256-cbc"
    const secretKey = crypto
      .createHash("sha256")
      .update(process.env.ENCRYPTION_SECRET || "default-secret-key-change-in-production")
      .digest()
    const iv = crypto.randomBytes(16)
    const cipher = crypto.createCipheriv(algorithm, secretKey, iv)
    let encrypted = cipher.update(rawKey, "utf8", "hex")
    encrypted += cipher.final("hex")
    const encryptedKey = iv.toString("hex") + ":" + encrypted

    // Deactivate previous keys
    await ClientApiKey.updateMany({ clientId, isActive: true }, { isActive: false })

    // Save new key record
    const record = await ClientApiKey.create({
      clientId,
      encryptedKey,
      keyHash,
      keyPreview,
      isActive: true,
    })

    return res.json({
      success: true,
      data: {
        key: rawKey,
        keyPreview: record.keyPreview,
        createdAt: record.createdAt,
      },
    })
  } catch (error) {
    console.error("[API-KEY] generateClientApiKey error:", error)
    return res.status(500).json({ success: false, error: error.message })
  }
}

exports.copyActiveClientApiKey = async (req, res) => {
  try {
    const clientId = req.clientId
    if (!clientId) return res.status(401).json({ success: false, error: "Unauthorized" })

    const record = await ClientApiKey.findOne({ clientId, isActive: true })
    if (!record) return res.status(404).json({ success: false, error: "No API key found" })

    const algorithm = "aes-256-cbc"
    const secretKey = crypto
      .createHash("sha256")
      .update(process.env.ENCRYPTION_SECRET || "default-secret-key-change-in-production")
      .digest()
    const parts = String(record.encryptedKey).split(":")
    const iv = Buffer.from(parts[0], "hex")
    const encrypted = parts[1]
    const decipher = crypto.createDecipheriv(algorithm, secretKey, iv)
    let decrypted = decipher.update(encrypted, "hex", "utf8")
    decrypted += decipher.final("utf8")

    return res.json({ success: true, data: { key: decrypted, keyPreview: record.keyPreview } })
  } catch (e) {
    console.error("[API-KEY] copyActiveClientApiKey error:", e)
    return res.status(500).json({ success: false, error: e.message })
  }
}

exports.getActiveClientApiKey = async (req, res) => {
  try {
    const clientId = req.clientId
    if (!clientId) {
      return res.status(401).json({ success: false, error: "Unauthorized" })
    }

    const record = await ClientApiKey.findOne({ clientId, isActive: true })
      .select("_id keyPreview createdAt updatedAt")
      .lean()

    return res.json({
      success: true,
      data: record || null,
    })
  } catch (error) {
    console.error("[API-KEY] getActiveClientApiKey error:", error)
    return res.status(500).json({ success: false, error: error.message })
  }
}


