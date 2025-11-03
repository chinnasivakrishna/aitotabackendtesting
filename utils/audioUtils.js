/**
 * Audio utility functions for handling Buffer operations in MongoDB
 */

class AudioUtils {
    /**
     * Validate if data is a proper Buffer
     */
    static isValidBuffer(data) {
      return data && Buffer.isBuffer(data) && data.length > 0
    }

    /**
     * Ensure data is converted to a proper Buffer
     */
    static ensureBuffer(data) {
      if (!data) {
        throw new Error("No data provided")
      }

      if (Buffer.isBuffer(data)) {
        return data
      }

      if (typeof data === 'string') {
        // Try to convert from base64
        try {
          return Buffer.from(data, 'base64')
        } catch (error) {
          throw new Error(`Failed to convert string to Buffer: ${error.message}`)
        }
      }

      if (Array.isArray(data)) {
        try {
          return Buffer.from(data)
        } catch (error) {
          throw new Error(`Failed to convert array to Buffer: ${error.message}`)
        }
      }

      if (data instanceof Uint8Array) {
        return Buffer.from(data)
      }

      throw new Error(`Unsupported data type for Buffer conversion: ${typeof data}`)
    }
  
    /**
     * Convert base64 string to Buffer
     */
    static base64ToBuffer(base64String) {
      if (!base64String || typeof base64String !== "string") {
        throw new Error("Invalid base64 string provided")
      }
  
      try {
        return Buffer.from(base64String, "base64")
      } catch (error) {
        throw new Error(`Failed to convert base64 to buffer: ${error.message}`)
      }
    }
  
    /**
     * Convert Buffer to base64 string
     */
    static bufferToBase64(buffer) {
      if (!this.isValidBuffer(buffer)) {
        throw new Error("Invalid buffer provided")
      }
  
      return buffer.toString("base64")
    }
  
    /**
     * Get audio content type from format
     */
    static getContentType(format) {
      const contentTypes = {
        mp3: "audio/mpeg",
        wav: "audio/wav",
        ogg: "audio/ogg",
        aac: "audio/aac",
        flac: "audio/flac",
      }
  
      return contentTypes[format?.toLowerCase()] || "audio/mpeg"
    }
  
    /**
     * Validate audio metadata
     */
    static validateAudioMetadata(metadata) {
      const required = ["format", "size"]
      const missing = required.filter((field) => !metadata[field])
  
      if (missing.length > 0) {
        throw new Error(`Missing required metadata fields: ${missing.join(", ")}`)
      }
  
      if (metadata.size <= 0) {
        throw new Error("Audio size must be greater than 0")
      }
  
      return true
    }
  
    /**
     * Create audio metadata object
     */
    static createAudioMetadata(audioBuffer, options = {}) {
      if (!this.isValidBuffer(audioBuffer)) {
        throw new Error("Invalid audio buffer provided")
      }
  
      return {
        format: options.format || "mp3",
        sampleRate: options.sampleRate || 22050,
        channels: options.channels || 1,
        size: audioBuffer.length,
        generatedAt: new Date(),
        language: options.language || "en",
        speaker: options.speaker,
        provider: options.provider || "sarvam",
      }
    }
  
    /**
     * Compress audio buffer (basic implementation)
     */
    static compressAudio(buffer) {
      // This is a placeholder - in production you might want to use
      // actual audio compression libraries like ffmpeg
      if (!this.isValidBuffer(buffer)) {
        throw new Error("Invalid buffer provided for compression")
      }
  
      // For now, just return the original buffer
      // In production, implement actual compression
      return buffer
    }
  
    /**
     * Validate audio file size
     */
    static validateAudioSize(buffer, maxSizeMB = 10) {
      if (!this.isValidBuffer(buffer)) {
        throw new Error("Invalid buffer provided")
      }
  
      const maxSizeBytes = maxSizeMB * 1024 * 1024
      if (buffer.length > maxSizeBytes) {
        throw new Error(`Audio file too large. Maximum size is ${maxSizeMB}MB`)
      }
  
      return true
    }
  
    /**
     * Create WAV header for raw audio data
     */
    static createWAVHeader(audioBuffer, sampleRate = 22050, channels = 1, bitsPerSample = 16) {
      const byteRate = (sampleRate * channels * bitsPerSample) / 8
      const blockAlign = (channels * bitsPerSample) / 8
      const dataSize = audioBuffer.length
      const fileSize = 36 + dataSize
  
      const header = Buffer.alloc(44)
      let offset = 0
  
      // RIFF header
      header.write("RIFF", offset)
      offset += 4
      header.writeUInt32LE(fileSize, offset)
      offset += 4
      header.write("WAVE", offset)
      offset += 4
  
      // fmt chunk
      header.write("fmt ", offset)
      offset += 4
      header.writeUInt32LE(16, offset)
      offset += 4
      header.writeUInt16LE(1, offset)
      offset += 2
      header.writeUInt16LE(channels, offset)
      offset += 2
      header.writeUInt32LE(sampleRate, offset)
      offset += 4
      header.writeUInt32LE(byteRate, offset)
      offset += 4
      header.writeUInt16LE(blockAlign, offset)
      offset += 2
      header.writeUInt16LE(bitsPerSample, offset)
      offset += 2
  
      // data chunk
      header.write("data", offset)
      offset += 4
      header.writeUInt32LE(dataSize, offset)
  
      return Buffer.concat([header, audioBuffer])
    }
  }
  
  module.exports = AudioUtils
  