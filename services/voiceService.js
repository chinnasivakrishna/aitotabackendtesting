const WebSocket = require("ws")
const { SarvamAIClient } = require("sarvamai")

class VoiceService {
  constructor() {
    this.sarvamApiKey = process.env.SARVAM_API_KEY
    this.elevenlabsApiKey = process.env.ELEVENLABS_API_KEY
    this.openaiApiKey = process.env.OPENAI_API_KEY
    this.deepgramApiKey = process.env.DEEPGRAM_API_KEY
  }

  // Voice mappings for different services
  getVoiceMappings() {
    return {
      sarvam: {
        anushka: { name: "Anushka", id: "anushka" },
        abhilash: { name: "Abhilash", id: "abhilash" },
        manisha: { name: "Manisha", id: "manisha" },
        vidya: { name: "Vidya", id: "vidya" },
        arya: { name: "Arya", id: "arya" },
        karun: { name: "Karun", id: "karun" },
        hitesh: { name: "Hitesh", id: "hitesh" },
        aditya: { name: "Aditya", id: "aditya" },
        isha: { name: "Isha", id: "isha" },
        ritu: { name: "Ritu", id: "ritu" },
        chirag: { name: "Chirag", id: "chirag" },
        harsh: { name: "Harsh", id: "harsh" },
        sakshi: { name: "Sakshi", id: "sakshi" },
        priya: { name: "Priya", id: "priya" },
        neha: { name: "Neha", id: "neha" },
        rahul: { name: "Rahul", id: "rahul" },
        pooja: { name: "Pooja", id: "pooja" },
        rohan: { name: "Rohan", id: "rohan" },
        simran: { name: "Simran", id: "simran" },
        kavya: { name: "Kavya", id: "kavya" },
        anjali: { name: "Anjali", id: "anjali" },
        sneha: { name: "Sneha", id: "sneha" },
        kiran: { name: "Kiran", id: "kiran" },
        vikram: { name: "Vikram", id: "vikram" },
        rajesh: { name: "Rajesh", id: "rajesh" },
        sunita: { name: "Sunita", id: "sunita" },
        tara: { name: "Tara", id: "tara" },
        anirudh: { name: "Anirudh", id: "anirudh" },
        kriti: { name: "Kriti", id: "kriti" },
        ishaan: { name: "Ishaan", id: "ishaan" },
      },
      elevenlabs: {
        kumaran: { name: "Kumaran", id: "rgltZvTfiMmgWweZhh7n" },
        monika: { name: "Monika", id: "NaKPQmdr7mMxXuXrNeFC" },
        aahir: { name: "Aahir", id: "RKshBIkZ7DwU6YNPq5Jd" },
        kanika: { name: "Kanika", id: "xccfcojYYGnqTTxwZEDU" },
      },
      smallest: {
        leon: { name: "Leon", id: "leon" },
        alice: { name: "Alice", id: "alice" },
        katie: { name: "Katie", id: "katie" },
        natalie: { name: "Natalie", id: "natalie" },
        meera: { name: "Meera", id: "meera" },
        ishika: { name: "Ishika", id: "ishika" },
        christine: { name: "Christine", id: "christine" },
        aarushi: { name: "Aarushi", id: "aarushi" },
        john: { name: "John", id: "john" },
        bruce: { name: "Bruce", id: "bruce" },
        ashley: { name: "Ashley", id: "ashley" },
        eleanor: { name: "Eleanor", id: "eleanor" },
        madison: { name: "Madison", id: "madison" },
        tasha: { name: "Tasha", id: "tasha" },
        chloe: { name: "Chloe", id: "chloe" },
        ryan: { name: "Ryan", id: "ryan" },
        alistair: { name: "Alistair", id: "alistair" },
        walter: { name: "Walter", id: "walter" },
        julian: { name: "Julian", id: "julian" },
        shivangi: { name: "Shivangi", id: "shivangi" },
        ronald: { name: "Ronald", id: "ronald" },
        gerard: { name: "Gerard", id: "gerard" },
        isabel: { name: "Isabel", id: "isabel" },
        enzo: { name: "Enzo", id: "enzo" },
        hamees: { name: "Hamees", id: "hamees" },
        yasmin: { name: "Yasmin", id: "yasmin" },
        biswa: { name: "Biswa", id: "biswa" },
        adele: { name: "Adele", id: "adele" },
        maya: { name: "Maya", id: "maya" },
        anika: { name: "Anika", id: "anika" },
        rishika: { name: "Rishika", id: "rishika" },
        wasim: { name: "Wasim", id: "wasim" },
        ganya: { name: "Ganya", id: "ganya" },
        khushi: { name: "Khushi", id: "khushi" },
        priya: { name: "Priya", id: "priya" },
        aahan: { name: "Aahan", id: "aahan" },
        fatema: { name: "Fatema", id: "fatema" },
        ariba: { name: "Ariba", id: "ariba" },
        varun: { name: "Varun", id: "varun" },
        isha: { name: "Isha", id: "isha" },
        neha: { name: "Neha", id: "neha" },
        ayaan: { name: "Ayaan", id: "ayaan" },
        felix: { name: "Felix", id: "felix" },
        tanvi: { name: "Tanvi", id: "tanvi" },
        sanjay: { name: "Sanjay", id: "sanjay" },
        amit: { name: "Amit", id: "amit" },
        disha: { name: "Disha", id: "disha" },
        advait: { name: "Advait", id: "advait" },
        nirupma: { name: "Nirupma", id: "nirupma" },
        anita: { name: "Anita", id: "anita" },
        rohit: { name: "Rohit", id: "rohit" },
        sangeeta: { name: "Sangeeta", id: "sangeeta" },
        ram: { name: "Ram", id: "ram" },
        kiara: { name: "Kiara", id: "kiara" },
        aditya: { name: "Aditya", id: "aditya" },
        saad: { name: "Saad", id: "saad" },
        kabir: { name: "Kabir", id: "kabir" },
        rohan: { name: "Rohan", id: "rohan" },
        vikram: { name: "Vikram", id: "vikram" },
        claire: { name: "Claire", id: "claire" },
        vihaan: { name: "Vihaan", id: "vihaan" },
        blofeld: { name: "Blofeld", id: "blofeld" },
        chirag: { name: "Chirag", id: "chirag" },
        luther: { name: "Luther", id: "luther" },
        julia: { name: "Julia", id: "julia" },
        erica: { name: "Erica", id: "erica" },
        nyah: { name: "Nyah", id: "nyah" },
        william: { name: "William", id: "william" },
        aditi: { name: "Aditi", id: "aditi" },
        angela: { name: "Angela", id: "angela" },
        radhika: { name: "Radhika", id: "radhika" },
        zorin: { name: "Zorin", id: "zorin" },
        alec: { name: "Alec", id: "alec" },
        solomon: { name: "Solomon", id: "solomon" },
        yash: { name: "Yash", id: "yash" },
        chinmay: { name: "Chinmay", id: "chinmay" },
        karan: { name: "Karan", id: "karan" },
        ilsa: { name: "Ilsa", id: "ilsa" },
        bellatrix: { name: "Bellatrix", id: "bellatrix" },
        nikita: { name: "Nikita", id: "nikita" },
        roma: { name: "Roma", id: "roma" },
        kartik: { name: "Kartik", id: "kartik" },
        gargi: { name: "Gargi", id: "gargi" },
        albus: { name: "Albus", id: "albus" },
        lakshya: { name: "Lakshya", id: "lakshya" },
        irisha: { name: "Irisha", id: "irisha" },
        lukas: { name: "Lukas", id: "lukas" },
        dhruv: { name: "Dhruv", id: "dhruv" },
        bogambo: { name: "Bogambo", id: "bogambo" },
        natasha: { name: "Natasha", id: "natasha" },
        malcolm: { name: "Malcolm", id: "malcolm" },
        andrea: { name: "Andrea", id: "andrea" },
        adriana: { name: "Adriana", id: "adriana" },
        vijay: { name: "Vijay", id: "vijay" },
        vidya: { name: "Vidya", id: "vidya" },
        dmitry: { name: "Dmitry", id: "dmitry" },
        maria: { name: "Maria", id: "maria" },
        emmanuel: { name: "Emmanuel", id: "emmanuel" },
      }
    }
  }

  // Basic allowlist of supported Sarvam voices for bulbul:v2
  // Extend as needed based on provider docs
  getSupportedSpeakers() {
    return new Set([
      "anushka",
      "abhilash",
      "manisha",
      "vidya",
      "arya",
      "karun",
      "hitesh",
      "aditya",
      "isha",
      "ritu",
      "chirag",
      "harsh",
      "sakshi",
      "priya",
      "neha",
      "rahul",
      "pooja",
      "rohan",
      "simran",
      "kavya",
      "anjali",
      "sneha",
      "kiran",
      "vikram",
      "rajesh",
      "sunita",
      "tara",
      "anirudh",
      "kriti",
      "ishaan",
    ])
  }

  getDefaultSpeakerForLanguage(language) {
    const lang = (language || "en").toLowerCase()
    return lang === "hi" ? "anushka" : "abhilash"
  }

  sanitizeSpeaker(language, requestedSpeaker) {
    const supported = this.getSupportedSpeakers()
    if (!requestedSpeaker || typeof requestedSpeaker !== "string") {
      return this.getDefaultSpeakerForLanguage(language)
    }
    const speakerLower = requestedSpeaker.toLowerCase()
    if (!supported.has(speakerLower)) {
      console.warn(
        `[VOICE_SERVICE] Unsupported speaker '${requestedSpeaker}'. Falling back to default for language '${language}'.`
      )
      return this.getDefaultSpeakerForLanguage(language)
    }
    return speakerLower
  }

  // Convert text to speech using Sarvam AI or ElevenLabs
  async textToSpeech(text, language = "en", speaker = null, serviceProvider = "sarvam") {
    if (!text.trim()) {
      throw new Error("Empty text provided")
    }

    if (serviceProvider === "elevenlabs") {
      return await this.textToSpeechElevenLabs(text, language, speaker)
    } else {
      return await this.textToSpeechSarvam(text, language, speaker)
    }
  }

  // Convert text to speech using Sarvam AI (matching your unifiedVoiceServer)
  async textToSpeechSarvam(text, language = "en", speaker = null) {
    if (!this.sarvamApiKey) {
      throw new Error("Sarvam API key not configured")
    }

    try {
      // Build request body matching your unifiedVoiceServer implementation
      const targetLanguage = language === "hi" ? "hi-IN" : "en-IN"
      const safeSpeaker = this.sanitizeSpeaker(language, speaker)
      const requestBody = {
        inputs: [text],
        target_language_code: targetLanguage,
        speaker: safeSpeaker,
        pitch: 0,
        pace: 1.0,
        loudness: 1.0,
        speech_sample_rate: 22050,
        enable_preprocessing: true,
        model: "bulbul:v2",
      }

      console.log("[VOICE_SERVICE] TTS Request:", requestBody)

      const response = await fetch("https://api.sarvam.ai/text-to-speech", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "API-Subscription-Key": this.sarvamApiKey,
        },
        body: JSON.stringify(requestBody),
      })

      if (!response.ok) {
        const errorText = await response.text()
        let errorData
        try {
          errorData = JSON.parse(errorText)
        } catch {
          errorData = { error: errorText }
        }
        const extractedMessage =
          (typeof errorData.error === "string" && errorData.error) ||
          (typeof errorData.message === "string" && errorData.message) ||
          (errorData.error && errorData.error.message) ||
          JSON.stringify(errorData)
        console.error("[VOICE_SERVICE] API Error:", {
          status: response.status,
          error: extractedMessage,
          requestBody,
        })
        // Retry once with minimal payload and default speaker on 400
        if (response.status === 400) {
          const fallbackBody = {
            inputs: [text],
            target_language_code: targetLanguage,
            speaker: this.getDefaultSpeakerForLanguage(language),
            model: "bulbul:v2",
          }
          console.warn("[VOICE_SERVICE] Retrying with minimal payload:", fallbackBody)
          const retryRes = await fetch("https://api.sarvam.ai/text-to-speech", {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "API-Subscription-Key": this.sarvamApiKey,
            },
            body: JSON.stringify(fallbackBody),
          })
          if (retryRes.ok) {
            const retryData = await retryRes.json()
            if (!retryData.audios || retryData.audios.length === 0) {
              throw new Error("No audio data received from Sarvam AI (fallback)")
            }
            const audioBase64 = retryData.audios[0]
            const audioBuffer = Buffer.from(audioBase64, "base64")
            console.log(`[VOICE_SERVICE] Fallback audio generated: ${audioBuffer.length} bytes`)
            return {
              audioBuffer,
              audioBase64,
              sampleRate: 22050,
              channels: 1,
              format: "mp3",
              usedSpeaker: this.getDefaultSpeakerForLanguage(language),
              targetLanguage,
            }
          } else {
            const retryErrText = await retryRes.text()
            let retryErr
            try { retryErr = JSON.parse(retryErrText) } catch { retryErr = { error: retryErrText } }
            const retryMsg = retryErr.error || retryErr.message || JSON.stringify(retryErr)
            throw new Error(`Sarvam AI API error: 400 (retry failed) - ${retryMsg}`)
          }
        }
        throw new Error(`Sarvam AI API error: ${response.status} - ${extractedMessage}`)
      }

      const responseData = await response.json()
      if (!responseData.audios || responseData.audios.length === 0) {
        throw new Error("No audio data received from Sarvam AI")
      }

      // Convert base64 audio to buffer (bytes format for database storage)
      const audioBase64 = responseData.audios[0]
      const audioBuffer = Buffer.from(audioBase64, "base64")

      console.log(`[VOICE_SERVICE] Audio generated successfully: ${audioBuffer.length} bytes`)

      return {
        audioBuffer: audioBuffer,
        audioBase64: audioBase64,
        sampleRate: 22050,
        channels: 1,
        format: "mp3",
        usedSpeaker: safeSpeaker,
        targetLanguage: targetLanguage,
      }
    } catch (error) {
      console.error(`[VOICE_SERVICE] TTS error: ${error.message}`)
      throw error
    }
  }

  // Convert text to speech using ElevenLabs
  async textToSpeechElevenLabs(text, language = "en", speaker = null) {
    if (!this.elevenlabsApiKey) {
      throw new Error("ElevenLabs API key not configured")
    }

    try {
      const voiceMappings = this.getVoiceMappings()
      const voiceId = voiceMappings.elevenlabs[speaker]?.id || voiceMappings.elevenlabs.kumaran.id
      
      const requestBody = {
        text: text,
        model_id: "eleven_multilingual_v2",
        voice_settings: {
          stability: 0.5,
          similarity_boost: 0.5
        }
      }

      console.log("[VOICE_SERVICE] ElevenLabs TTS Request:", { voiceId, textLength: text.length })

      const response = await fetch(`https://api.elevenlabs.io/v1/text-to-speech/${voiceId}`, {
        method: "POST",
        headers: {
          "Accept": "audio/mpeg",
          "Content-Type": "application/json",
          "xi-api-key": this.elevenlabsApiKey,
        },
        body: JSON.stringify(requestBody),
      })

      if (!response.ok) {
        const errorText = await response.text()
        let errorData
        try {
          errorData = JSON.parse(errorText)
        } catch {
          errorData = { error: errorText }
        }
        const extractedMessage =
          (typeof errorData.error === "string" && errorData.error) ||
          (typeof errorData.message === "string" && errorData.message) ||
          (errorData.error && errorData.error.message) ||
          JSON.stringify(errorData)
        console.error("[VOICE_SERVICE] ElevenLabs API Error:", {
          status: response.status,
          error: extractedMessage,
          voiceId,
        })
        throw new Error(`ElevenLabs API error: ${response.status} - ${extractedMessage}`)
      }

      const audioBuffer = await response.arrayBuffer()
      const audioBase64 = Buffer.from(audioBuffer).toString('base64')

      console.log(`[VOICE_SERVICE] ElevenLabs audio generated successfully: ${audioBuffer.byteLength} bytes`)

      return {
        audioBuffer: Buffer.from(audioBuffer),
        audioBase64: audioBase64,
        sampleRate: 22050,
        channels: 1,
        format: "mp3",
        usedSpeaker: speaker,
        targetLanguage: language,
        serviceProvider: "elevenlabs",
      }
    } catch (error) {
      console.error(`[VOICE_SERVICE] ElevenLabs TTS error: ${error.message}`)
      throw error
    }
  }

  // Convert buffer to Python bytes string format (matching your unifiedVoiceServer)
  bufferToPythonBytesString(buffer) {
    let result = "b'"
    for (let i = 0; i < buffer.length; i++) {
      const byte = buffer[i]
      if (byte >= 32 && byte <= 126 && byte !== 92 && byte !== 39) {
        result += String.fromCharCode(byte)
      } else {
        result += "\\x" + byte.toString(16).padStart(2, "0")
      }
    }
    result += "'"
    return result
  }

  // Create WAV header for audio processing
  createWAVHeader(audioBuffer, sampleRate = 22050, channels = 1, bitsPerSample = 16) {
    const byteRate = (sampleRate * channels * bitsPerSample) / 8
    const blockAlign = (channels * bitsPerSample) / 8
    const dataSize = audioBuffer.length
    const fileSize = 36 + dataSize

    const header = Buffer.alloc(44)
    let offset = 0

    header.write("RIFF", offset)
    offset += 4
    header.writeUInt32LE(fileSize, offset)
    offset += 4
    header.write("WAVE", offset)
    offset += 4
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
    header.write("data", offset)
    offset += 4
    header.writeUInt32LE(dataSize, offset)

    return Buffer.concat([header, audioBuffer])
  }

  // Test API connectivity
  async testConnection() {
    try {
      const testText = "Hello, this is a test message."
      const result = await this.textToSpeech(testText, "en")
      return {
        success: true,
        message: "Voice service connection successful",
        audioSize: result.size,
      }
    } catch (error) {
      return {
        success: false,
        message: `Voice service connection failed: ${error.message}`,
      }
    }
  }
}

module.exports = VoiceService
