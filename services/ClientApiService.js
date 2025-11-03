const ApiKey = require("../models/ApiKey")
const Client = require("../models/Client")

class ClientApiService {
  constructor() {
    this.providerConfigs = {
      openai: {
        name: "OpenAI",
        description: "Large Language Model for conversational AI",
        keyFormat: "sk-...",
        testEndpoint: "https://api.openai.com/v1/models",
        defaultModel: "gpt-4o-mini",
      },
      deepgram: {
        name: "Deepgram",
        description: "Speech-to-Text transcription service",
        keyFormat: "Token ...",
        testEndpoint: "https://api.deepgram.com/v1/projects",
        defaultModel: "nova-2",
      },
      sarvam: {
        name: "Sarvam AI",
        description: "Text-to-Speech synthesis service",
        keyFormat: "API-Subscription-Key",
        testEndpoint: "https://api.sarvam.ai/text-to-speech",
        defaultVoice: "abhilash",
      },
      elevenlabs: {
        name: "ElevenLabs",
        description: "Advanced Text-to-Speech service",
        keyFormat: "xi-api-key",
        testEndpoint: "https://api.elevenlabs.io/v1/voices",
      },
      google_cloud: {
        name: "Google Cloud",
        description: "Google Cloud Speech and Text services",
        keyFormat: "API Key",
        testEndpoint: "https://speech.googleapis.com/v1/speech:recognize",
      },
      azure_speech: {
        name: "Azure Speech",
        description: "Microsoft Azure Speech services",
        keyFormat: "Subscription Key",
        testEndpoint: "https://api.cognitive.microsoft.com/sts/v1.0/issuetoken",
      },
      lmnt: {
        name: "LMNT",
        description: "LMNT Text-to-Speech service",
        keyFormat: "API Key",
        testEndpoint: "https://api.lmnt.com/v1/voices",
        defaultVoice: "default",
      },
    }
  }

  // Get all API keys for a client
  async getClientApiKeys(clientId) {
    try {
      const apiKeys = await ApiKey.find({ clientId, isActive: true })
        .select("-encryptedKey") // Don't return encrypted keys
        .sort({ provider: 1, createdAt: -1 })

      return {
        success: true,
        data: apiKeys,
        count: apiKeys.length,
      }
    } catch (error) {
      console.error(`[CLIENT_API] Error fetching API keys for client ${clientId}:`, error)
      return {
        success: false,
        error: error.message,
      }
    }
  }

  // Add or update API key for a client
  async setApiKey(clientId, provider, keyData) {
    try {
      const { key, keyName, description, configuration = {} } = keyData

      if (!key || !key.trim()) {
        throw new Error("API key is required")
      }

      if (!this.providerConfigs[provider]) {
        throw new Error(`Unsupported provider: ${provider}`)
      }

      // Test the API key before saving
      const testResult = await this.testApiKey(provider, key, configuration)
      if (!testResult.success) {
        throw new Error(`API key validation failed: ${testResult.error}`)
      }

      // Encrypt the key
      const encryptedKey = ApiKey.encryptKey(key)
      const keyPreview = key.substring(0, 8) + "..." + key.slice(-4)

      // Update or create API key
      const apiKeyData = {
        clientId,
        provider,
        keyName: keyName || this.providerConfigs[provider].name,
        encryptedKey,
        keyPreview,
        isActive: true,
        configuration: {
          ...this.providerConfigs[provider],
          ...configuration,
        },
        metadata: {
          description: description || `${this.providerConfigs[provider].name} API key`,
          environment: process.env.NODE_ENV || "production",
        },
      }

      const existingKey = await ApiKey.findOne({ clientId, provider })
      let savedKey

      if (existingKey) {
        // Update existing key
        Object.assign(existingKey, apiKeyData)
        savedKey = await existingKey.save()
        console.log(`[CLIENT_API] Updated ${provider} API key for client ${clientId}`)
      } else {
        // Create new key
        savedKey = new ApiKey(apiKeyData)
        await savedKey.save()
        console.log(`[CLIENT_API] Added new ${provider} API key for client ${clientId}`)
      }

      // Return without encrypted key
      const result = savedKey.toObject()
      delete result.encryptedKey
      return result
    } catch (error) {
      console.error(`[CLIENT_API] Error setting API key for client:`, error)
      return {
        success: false,
        error: error.message,
      }
    }
  }

  // Get decrypted API key for internal use
  async getDecryptedApiKey(clientId, provider) {
    try {
      const apiKey = await ApiKey.findOne({
        clientId,
        provider,
        isActive: true,
      })

      if (!apiKey) {
        return {
          success: false,
          error: `No active ${provider} API key found for client ${clientId}`,
        }
      }

      const decryptedKey = apiKey.getDecryptedKey()

      // Update usage statistics
      apiKey.usage.totalRequests += 1
      apiKey.usage.lastUsed = new Date()
      await apiKey.save()

      return {
        success: true,
        key: decryptedKey,
        configuration: apiKey.configuration,
      }
    } catch (error) {
      console.error(`[CLIENT_API] Error getting decrypted key for ${provider}:`, error)
      return {
        success: false,
        error: error.message,
      }
    }
  }

  // Test API key validity
  async testApiKey(provider, key, configuration = {}) {
    try {
      const config = this.providerConfigs[provider]
      if (!config) {
        throw new Error(`Unsupported provider: ${provider}`)
      }

      let testResult = { success: false, error: "Test not implemented" }

      switch (provider) {
        case "openai":
          testResult = await this.testOpenAIKey(key)
          break
        case "deepgram":
          testResult = await this.testDeepgramKey(key)
          break
        case "sarvam":
          testResult = await this.testSarvamKey(key)
          break
        case "elevenlabs":
          testResult = await this.testElevenLabsKey(key)
          break
        case "lmnt":
          testResult = await this.testLMNTKey(key)
          break
        default:
          testResult = { success: true, message: "Test not available for this provider" }
      }

      return testResult
    } catch (error) {
      return {
        success: false,
        error: error.message,
      }
    }
  }

  // Provider-specific test methods
  async testOpenAIKey(key) {
    try {
      const response = await fetch("https://api.openai.com/v1/models", {
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
        },
      })

      if (response.ok) {
        const data = await response.json()
        return {
          success: true,
          message: `OpenAI API key valid. Found ${data.data?.length || 0} models.`,
        }
      } else {
        const error = await response.text()
        return {
          success: false,
          error: `OpenAI API key invalid: ${response.status} - ${error}`,
        }
      }
    } catch (error) {
      return {
        success: false,
        error: `OpenAI API test failed: ${error.message}`,
      }
    }
  }

  async testDeepgramKey(key) {
    try {
      const response = await fetch("https://api.deepgram.com/v1/projects", {
        headers: {
          Authorization: `Token ${key}`,
          "Content-Type": "application/json",
        },
      })

      if (response.ok) {
        return {
          success: true,
          message: "Deepgram API key is valid",
        }
      } else {
        const error = await response.text()
        return {
          success: false,
          error: `Deepgram API key invalid: ${response.status} - ${error}`,
        }
      }
    } catch (error) {
      return {
        success: false,
        error: `Deepgram API test failed: ${error.message}`,
      }
    }
  }

  async testSarvamKey(key) {
    try {
      // Test with a simple TTS request
      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": key,
        },
        body: JSON.stringify({
          inputs: ["Test"],
          target_language_code: "en-IN",
          speaker: "abhilash",
          model: "bulbul:v2",
        }),
      })

      if (response.ok || response.status === 400) {
        // 400 might be expected for test data
        return {
          success: true,
          message: "Sarvam AI API key is valid",
        }
      } else {
        const error = await response.text()
        return {
          success: false,
          error: `Sarvam API key invalid: ${response.status} - ${error}`,
        }
      }
    } catch (error) {
      return {
        success: false,
        error: `Sarvam API test failed: ${error.message}`,
      }
    }
  }

  async testElevenLabsKey(key) {
    try {
      const response = await fetch("https://api.elevenlabs.io/v1/voices", {
        headers: {
          "xi-api-key": key,
          "Content-Type": "application/json",
        },
      })

      if (response.ok) {
        const data = await response.json()
        return {
          success: true,
          message: `ElevenLabs API key valid. Found ${data.voices?.length || 0} voices.`,
        }
      } else {
        const error = await response.text()
        return {
          success: false,
          error: `ElevenLabs API key invalid: ${response.status} - ${error}`,
        }
      }
    } catch (error) {
      return {
        success: false,
        error: `ElevenLabs API test failed: ${error.message}`,
      }
    }
  }

  // Add LMNT test method
  async testLMNTKey(key) {
    try {
      const response = await fetch("https://api.lmnt.com/v1/voices", {
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
        },
      })
      if (response.ok) {
        const data = await response.json()
        return {
          success: true,
          message: `LMNT API key valid. Found ${data.voices?.length || 0} voices.`,
        }
      } else {
        const error = await response.text()
        return {
          success: false,
          error: `LMNT API key invalid: ${response.status} - ${error}`,
        }
      }
    } catch (error) {
      return {
        success: false,
        error: `LMNT API test failed: ${error.message}`,
      }
    }
  }

  // Delete API key
  async deleteApiKey(clientId, provider) {
    try {
      const result = await ApiKey.findOneAndUpdate({ clientId, provider }, { isActive: false }, { new: true })

      if (!result) {
        return {
          success: false,
          error: `No ${provider} API key found for client ${clientId}`,
        }
      }

      return {
        success: true,
        message: `${provider} API key deactivated successfully`,
      }
    } catch (error) {
      return {
        success: false,
        error: error.message,
      }
    }
  }

  // Get provider configurations
  getProviderConfigs() {
    return this.providerConfigs
  }
}

module.exports = ClientApiService
