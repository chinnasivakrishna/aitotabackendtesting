// Test script to verify base64 string storage
const mongoose = require('mongoose')
const Agent = require('../models/Agent')
require('dotenv').config()

const testBase64Storage = async () => {
  try {
    await mongoose.connect(process.env.MONGODB_URI || "mongodb://localhost:27017/ai-agents")
    console.log("✅ Connected to MongoDB for base64 storage test")

    // Create test audio data
    const testAudioText = "Hello World Audio Test"
    const testBuffer = Buffer.from(testAudioText, 'utf8')
    const base64String = testBuffer.toString('base64')
    
    console.log(`📊 Test data: ${testAudioText}`)
    console.log(`📊 Buffer size: ${testBuffer.length} bytes`)
    console.log(`📊 Base64 string length: ${base64String.length} chars`)
    console.log(`📊 Base64 string: ${base64String.substring(0, 50)}...`)

    // Create test agent with base64 audio
    const testAgent = new Agent({
      tenantId: "test-tenant",
      agentName: "Test Agent Base64",
      description: "Test agent for base64 audio storage",
      firstMessage: "Hello",
      systemPrompt: "You are a test agent",
      audioBytes: base64String,
      audioMetadata: {
        format: "mp3",
        sampleRate: 22050,
        channels: 1,
        size: testBuffer.length,
        generatedAt: new Date(),
        language: "en",
        speaker: "test",
        provider: "test"
      }
    })

    // Save the agent
    const savedAgent = await testAgent.save()
    console.log(`✅ Agent saved with ID: ${savedAgent._id}`)
    console.log(`✅ Audio bytes type: ${typeof savedAgent.audioBytes}`)
    console.log(`✅ Audio bytes length: ${savedAgent.audioBytes.length} chars`)

    // Retrieve the agent
    const retrievedAgent = await Agent.findById(savedAgent._id)
    console.log(`✅ Agent retrieved successfully`)
    console.log(`✅ Retrieved audio bytes type: ${typeof retrievedAgent.audioBytes}`)
    console.log(`✅ Retrieved audio bytes length: ${retrievedAgent.audioBytes.length} chars`)

    // Test conversion back to Buffer
    const convertedBuffer = Buffer.from(retrievedAgent.audioBytes, 'base64')
    const convertedText = convertedBuffer.toString('utf8')
    console.log(`✅ Converted back to text: "${convertedText}"`)
    console.log(`✅ Conversion successful: ${convertedText === testAudioText ? 'YES' : 'NO'}`)

    // Clean up
    await Agent.findByIdAndDelete(savedAgent._id)
    console.log(`✅ Test agent cleaned up`)

    console.log("🎉 Base64 storage test completed successfully!")
    process.exit(0)
  } catch (error) {
    console.error("❌ Base64 storage test failed:", error)
    process.exit(1)
  }
}

testBase64Storage() 