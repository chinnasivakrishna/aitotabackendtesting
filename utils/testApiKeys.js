// Utility script to test API key encryption/decryption
const crypto = require("crypto")

const testEncryption = () => {
  console.log("Testing API Key Encryption/Decryption...")

  const algorithm = "aes-256-cbc"
  const secretKey = crypto.createHash("sha256").update("test-secret-key").digest()
  const testKey = "sk-test123456789abcdef"

  // Encrypt
  const iv = crypto.randomBytes(16)
  const cipher = crypto.createCipheriv(algorithm, secretKey, iv)
  let encrypted = cipher.update(testKey, "utf8", "hex")
  encrypted += cipher.final("hex")
  const encryptedKey = iv.toString("hex") + ":" + encrypted

  console.log("Original Key:", testKey)
  console.log("Encrypted Key:", encryptedKey)

  // Decrypt
  const parts = encryptedKey.split(":")
  const ivDecrypt = Buffer.from(parts[0], "hex")
  const encryptedData = parts[1]

  const decipher = crypto.createDecipheriv(algorithm, secretKey, ivDecrypt)
  let decrypted = decipher.update(encryptedData, "hex", "utf8")
  decrypted += decipher.final("utf8")

  console.log("Decrypted Key:", decrypted)
  console.log("Match:", testKey === decrypted ? "✅ SUCCESS" : "❌ FAILED")
}

testEncryption()
