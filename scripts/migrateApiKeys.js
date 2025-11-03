// Migration script to move existing API keys to tenant-based system
const mongoose = require("mongoose")
const ApiKey = require("../models/ApiKey")
const Client = require("../models/Client")
require("dotenv").config()

const migrateApiKeys = async () => {
  try {
    await mongoose.connect(process.env.MONGODB_URI || "mongodb://localhost:27017/ai-agents")
    console.log("Connected to MongoDB for migration")

    // Create default client if not exists
    let defaultClient = await Client.findOne({ clientId: "default-client" })
    if (!defaultClient) {
      defaultClient = new Client({
        clientId: "default-client",
        clientName: "Default Client",
        email: "admin@example.com",
        status: "active",
      })
      await defaultClient.save()
      console.log("✅ Created default client")
    }

    // Migrate environment variables to database
    const envApiKeys = [
      { provider: "openai", key: process.env.OPENAI_API_KEY },
      { provider: "deepgram", key: process.env.DEEPGRAM_API_KEY },
      { provider: "sarvam", key: process.env.SARVAM_API_KEY },
      { provider: "elevenlabs", key: process.env.ELEVENLABS_API_KEY },
    ]

    for (const { provider, key } of envApiKeys) {
      if (key && key.trim()) {
        const existingKey = await ApiKey.findOne({
          clientId: "default-client",
          provider: provider,
        })

        if (!existingKey) {
          const encryptedKey = ApiKey.encryptKey(key)
          const keyPreview = key.substring(0, 8) + "..." + key.slice(-4)

          const apiKey = new ApiKey({
            clientId: "default-client",
            provider: provider,
            keyName: `${provider.charAt(0).toUpperCase() + provider.slice(1)} API Key`,
            encryptedKey: encryptedKey,
            keyPreview: keyPreview,
            isActive: true,
            metadata: {
              description: `Migrated from environment variables`,
              environment: "production",
            },
          })

          await apiKey.save()
          console.log(`✅ Migrated ${provider} API key`)
        } else {
          console.log(`⚠️  ${provider} API key already exists, skipping`)
        }
      }
    }

    console.log("🎉 Migration completed successfully!")
    process.exit(0)
  } catch (error) {
    console.error("❌ Migration failed:", error)
    process.exit(1)
  }
}

migrateApiKeys()
