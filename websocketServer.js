const WebSocket = require('ws');
const http = require('http');
const Agent = require('./models/Agent');
const User = require('./models/User');
const Chat = require('./models/Chat');
const SystemPrompt = require('./models/SystemPrompt');
const VoiceService = require('./services/voiceService');
const axios = require('axios');

class VoiceChatWebSocketServer {
  constructor(server) {
    this.wss = new WebSocket.Server({ server });
    this.connections = new Map(); // Map to store active connections
    this.voiceService = new VoiceService();
    
    // API Keys
    this.openaiApiKey = process.env.OPENAI_API_KEY;
    this.deepgramApiKey = process.env.DEEPGRAM_API_KEY;
    
    this.setupWebSocketServer();
  }

  setupWebSocketServer() {
    this.wss.on('connection', (ws, req) => {
      console.log(`[WEBSOCKET] New connection from ${req.socket.remoteAddress}`);
      
      const connectionId = `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const connectionData = {
        id: connectionId,
        ws: ws,
        agent: null,
        user: null,
        sessionId: null,
        clientId: null,
        chatId: null,
        // Deepgram streaming state
        deepgramWs: null,
        deepgramReady: false,
        deepgramQueue: [],
        interimBuffer: '',
        lastFinalText: '',
        // PCM aggregation for Deepgram (collect ~100ms before sending)
        pcmChunkQueue: [],
        pcmQueuedBytes: 0,
        forwardedBytes: 0,
        // Processing state
        isProcessing: false,
        lastActivity: Date.now()
      };
      
      this.connections.set(connectionId, connectionData);
      
      // Send connection confirmation
      ws.send(JSON.stringify({
        event: 'connected',
        connectionId: connectionId,
        timestamp: new Date().toISOString()
      }));
      
      ws.on('message', async (message) => {
        try {
          const data = JSON.parse(message);
          await this.handleMessage(connectionId, data);
        } catch (error) {
          console.error(`[WEBSOCKET] Error handling message:`, error);
          ws.send(JSON.stringify({
            event: 'error',
            message: 'Invalid message format',
            timestamp: new Date().toISOString()
          }));
        }
      });
      
      ws.on('close', () => {
        console.log(`[WEBSOCKET] Connection ${connectionId} closed`);
        const conn = this.connections.get(connectionId);
        // Mark chat ended if exists
        if (conn?.chatId) {
          Chat.findByIdAndUpdate(
            conn.chatId,
            { $set: { endedAt: new Date(), lastActivityAt: new Date() }, $push: { messages: { role: 'system', type: 'event', text: 'Connection closed' } } },
            { new: false }
          ).catch(e => console.error('[CHAT] Failed to mark chat ended on close:', e.message))
        }
        if (conn?.deepgramWs && conn.deepgramWs.readyState === WebSocket.OPEN) {
          try { conn.deepgramWs.close(); } catch {}
        }
        this.connections.delete(connectionId);
      });
      
      ws.on('error', (error) => {
        console.error(`[WEBSOCKET] Connection ${connectionId} error:`, error);
        const conn = this.connections.get(connectionId);
        if (conn?.chatId) {
          Chat.findByIdAndUpdate(
            conn.chatId,
            { $push: { messages: { role: 'system', type: 'event', text: `Connection error: ${error?.message || 'unknown'}` } }, $set: { lastActivityAt: new Date() } },
            { new: false }
          ).catch(e => console.error('[CHAT] Failed to append error to chat:', e.message))
        }
        if (conn?.deepgramWs && conn.deepgramWs.readyState === WebSocket.OPEN) {
          try { conn.deepgramWs.close(); } catch {}
        }
        this.connections.delete(connectionId);
      });
    });
  }

  async handleMessage(connectionId, data) {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      console.error(`[WEBSOCKET] Connection ${connectionId} not found`);
      return;
    }

    connection.lastActivity = Date.now();

    switch (data.event) {
      case 'start':
        await this.handleStart(connectionId, data);
        break;
      case 'media':
        await this.handleMedia(connectionId, data);
        break;
      case 'user_message':
        await this.handleUserMessage(connectionId, data);
        break;
      case 'stop':
        await this.handleStop(connectionId, data);
        break;
      default:
        console.warn(`[WEBSOCKET] Unknown event: ${data.event}`);
    }
  }

  sendLog(connectionId, level, message, meta = {}) {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.ws || connection.ws.readyState !== 1) return;
    try {
      connection.ws.send(JSON.stringify({
        event: 'log',
        level,
        message,
        meta,
        timestamp: new Date().toISOString()
      }));
    } catch (e) {
      console.error('[WEBSOCKET][LOG-SEND] Failed to send log:', e.message);
    }
  }

  async handleStart(connectionId, data) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    try {
      console.log(`[WEBSOCKET] Starting session for connection ${connectionId}`);
      this.sendLog(connectionId, 'info', 'Session starting', { connectionId });
      
      const startData = data.start;
      const extraData = JSON.parse(Buffer.from(startData.extraData, 'base64').toString());
      
      // Get agent details
      const agent = await Agent.findById(extraData.agentId);
      if (!agent) {
        throw new Error('Agent not found');
      }
      
      connection.agent = agent;
      connection.clientId = extraData.clientId;
      
      // Generate a unique session ID for this connection
      const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      connection.sessionId = sessionId;
      
      // Disable DB user creation (no mobile/registration saving)
      connection.user = {
        _id: null,
        getConversationHistory: () => [],
        addMessage: async () => {},
        shouldPromptForRegistration: () => false,
        isRegistered: false,
        updateRegistration: async () => {},
        incrementRegistrationAttempts: async () => {}
      };

      // Create Chat document for this session
      try {
        const chat = await Chat.create({
          clientId: connection.clientId,
          agentId: agent._id,
          sessionId: connection.sessionId,
          connectionId: connection.id,
          messages: [{ role: 'system', type: 'event', text: 'Session started' }]
        });
        connection.chatId = chat._id;
      } catch (e) {
        console.error('[CHAT] Failed to create chat session:', e.message);
      }
      
      // Send start confirmation
      connection.ws.send(JSON.stringify({
        event: 'start',
        streamSid: data.streamSid,
        sessionId: connection.sessionId,
        timestamp: new Date().toISOString()
      }));
      this.sendLog(connectionId, 'success', 'Session started', { sessionId, agentName: agent.agentName });
      
      // Connect to Deepgram WS
      const initialLanguage = (agent.language && typeof agent.language === 'string') ? agent.language : 'en';
      await this.connectToDeepgram(connectionId, initialLanguage);

      // Send initial greeting (TTS)
      await this.sendInitialGreeting(connectionId);
      
      console.log(`[WEBSOCKET] Session started for agent: ${agent.agentName}, sessionId: ${sessionId}`);
      
    } catch (error) {
      console.error(`[WEBSOCKET] Error in handleStart:`, error);
      this.sendLog(connectionId, 'error', 'Error in handleStart', { error: error.message });
      connection.ws.send(JSON.stringify({
        event: 'error',
        message: error.message,
        timestamp: new Date().toISOString()
      }));
    }
  }

  async connectToDeepgram(connectionId, language = 'en') {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    // Guard for missing API key
    if (!this.deepgramApiKey) {
      console.error('[DEEPGRAM] Missing DEEPGRAM_API_KEY');
      this.sendLog(connectionId, 'error', 'Deepgram WS not configured (missing API key)');
      return;
    }

    try {
      const deepgramUrl = new URL('wss://api.deepgram.com/v1/listen');
      // We send 16-bit PCM at 8kHz mono to Deepgram
      deepgramUrl.searchParams.append('encoding', 'linear16');
      deepgramUrl.searchParams.append('sample_rate', '8000');
      deepgramUrl.searchParams.append('channels', '1');
      deepgramUrl.searchParams.append('model', 'nova-2');
      deepgramUrl.searchParams.append('language', "hi");
      deepgramUrl.searchParams.append('interim_results', 'true');
      deepgramUrl.searchParams.append('smart_format', 'true');
      deepgramUrl.searchParams.append('endpointing', '300');

      console.log('[DEEPGRAM][WS] Connecting →', deepgramUrl.toString());
      this.sendLog(connectionId, 'info', 'Deepgram WS connecting', {
        url: deepgramUrl.toString(),
        encoding: 'linear16', sampleRate: 8000, channels: 1, model: 'nova-2', language
      });

      const dg = new WebSocket(deepgramUrl.toString(), {
        headers: { Authorization: `Token ${this.deepgramApiKey}` }
      });

      connection.deepgramWs = dg;
      connection.deepgramReady = false;
      connection.deepgramQueue = [];
      connection.interimBuffer = '';
      connection.lastFinalText = '';

      dg.onopen = () => {
        connection.deepgramReady = true;
        console.log('[DEEPGRAM][WS] Connected');
        this.sendLog(connectionId, 'success', 'Deepgram WS connected', { encoding: 'linear16', sampleRate: 8000, channels: 1 });
        if (connection.deepgramQueue.length > 0) {
          console.log('[DEEPGRAM][WS] Flushing queued audio:', connection.deepgramQueue.length);
          this.sendLog(connectionId, 'info', 'Flushing queued audio to Deepgram WS', { queued: connection.deepgramQueue.length });
        }
        for (const buf of connection.deepgramQueue) {
          try { dg.send(buf); } catch (e) {
            console.error('[DEEPGRAM][WS] Flush send error:', e.message);
            this.sendLog(connectionId, 'error', 'Flush send error', { error: e.message });
          }
        }
        connection.deepgramQueue = [];
      };

      dg.onmessage = async (event) => {
        try {
          const payload = JSON.parse(event.data);
          // Log Deepgram message types for debugging
          if (payload.type) {
            this.sendLog(connectionId, 'info', 'Deepgram WS message', { type: payload.type, is_final: payload.is_final });
          }
          await this.handleDeepgramMessage(connectionId, payload);
        } catch (e) {
          console.error('[DEEPGRAM][WS] Message parse error:', e.message);
          this.sendLog(connectionId, 'error', 'Deepgram WS message parse error', { error: e.message });
        }
      };

      dg.onerror = (err) => {
        connection.deepgramReady = false;
        console.error('[DEEPGRAM][WS] Error:', err.message);
        this.sendLog(connectionId, 'error', 'Deepgram WS error', { error: err.message });
      };

      dg.onclose = (ev) => {
        connection.deepgramReady = false;
        console.warn('[DEEPGRAM][WS] Closed:', ev?.code, ev?.reason);
        this.sendLog(connectionId, 'warning', 'Deepgram WS closed', { code: ev?.code, reason: ev?.reason });
      };
    } catch (error) {
      console.error('[DEEPGRAM][WS] Connect exception:', error.message);
      this.sendLog(connectionId, 'error', 'Failed to connect to Deepgram WS', { error: error.message });
    }
  }

  async handleDeepgramMessage(connectionId, data) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    if (data.type === 'Results') {
      const alt = data.channel?.alternatives?.[0];
      const transcript = alt?.transcript || '';
      const isFinal = !!data.is_final;
      this.sendLog(connectionId, 'info', 'Deepgram Results received', { transcriptPreview: transcript.slice(0, 80), isFinal });
      if (!transcript) return;

      // Send live transcript to client
      connection.ws.send(JSON.stringify({
        event: 'transcript',
        text: transcript,
        final: isFinal,
        timestamp: new Date().toISOString()
      }));

      // Persist transcript snippets
      if (connection.chatId) {
        try {
          await Chat.findByIdAndUpdate(
            connection.chatId,
            {
              $push: { messages: { role: 'transcript', type: 'transcript', text: transcript } },
              $set: { lastActivityAt: new Date() }
            },
            { new: false }
          )
        } catch (e) {
          console.error('[CHAT] Failed to append transcript:', e.message)
        }
      }

      // Accumulate and process final utterances
      if (isFinal) {
        const text = transcript.trim();
        if (text && text !== connection.lastFinalText) {
          connection.lastFinalText = text;
          // Inform UI as conversation message
          connection.ws.send(JSON.stringify({
            event: 'conversation',
            userMessage: text,
            timestamp: new Date().toISOString()
          }));

          // Persist user message
          if (connection.chatId) {
            try {
              await Chat.findByIdAndUpdate(
                connection.chatId,
                {
                  $push: { messages: { role: 'user', type: 'text', text } },
                  $set: { lastActivityAt: new Date() }
                },
                { new: false }
              )
            } catch (e) {
              console.error('[CHAT] Failed to append user message:', e.message)
            }
          }

          // Process with LLM and TTS
          await this.processUserUtterance(connectionId, text);
        }
      } else if (data.speech_final) {
        // Some Deepgram payloads may include speech_final flag
        this.sendLog(connectionId, 'info', 'Deepgram speech_final received');
      }
    } else if (data.type === 'UtteranceEnd') {
      // Optionally handle utterance end events if needed
      this.sendLog(connectionId, 'info', 'Deepgram UtteranceEnd');
    }
  }

  async processUserUtterance(connectionId, text) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;
    if (!text || connection.isProcessing) return;

    try {
      connection.isProcessing = true;

      // Registration prompt and AI reply handled inside generateAIResponse
      const aiResponse = await this.generateAIResponse(connectionId, text);
      if (aiResponse && aiResponse.text) {
        // Send AI text to client
        connection.ws.send(JSON.stringify({
          event: 'conversation',
          aiResponse: aiResponse.text,
          timestamp: new Date().toISOString()
        }));

        // Persist assistant message
        if (connection.chatId) {
          try {
            await Chat.findByIdAndUpdate(
              connection.chatId,
              {
                $push: { messages: { role: 'assistant', type: 'text', text: aiResponse.text } },
                $set: { lastActivityAt: new Date() }
              },
              { new: false }
            )
          } catch (e) {
            console.error('[CHAT] Failed to append assistant message:', e.message)
          }
        }

        // TTS and stream back
        const audioResponse = await this.textToSpeech(aiResponse.text, connection.agent.voiceSelection, connectionId);
        if (audioResponse) {
          connection.ws.send(JSON.stringify({
            event: 'media',
            streamSid: connection.sessionId,
            media: { payload: audioResponse },
            timestamp: new Date().toISOString()
          }));
        }
      }
    } catch (error) {
      this.sendLog(connectionId, 'error', 'Error processing utterance', { error: error.message });
    } finally {
      connection.isProcessing = false;
    }
  }

  async handleMedia(connectionId, data) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    try {
      if (data.media && data.media.payload) {
        const payloadLength = data.media.payload.length;
        this.sendLog(connectionId, 'info', 'Audio chunk received', { bytesBase64: payloadLength });
        const audioBuffer = Buffer.from(data.media.payload, 'base64');

        // Aggregate small PCM chunks to ~100ms (at 8kHz mono, 16-bit = 16000 bytes per second → 1600 bytes per 100ms)
        connection.pcmChunkQueue.push(audioBuffer);
        connection.pcmQueuedBytes += audioBuffer.length;

        // Flush condition: >= 1600 bytes or Deepgram not ready → keep queueing
        const FLUSH_BYTES = 1600; // 100ms of 16-bit PCM @ 8kHz mono
        if (connection.pcmQueuedBytes >= FLUSH_BYTES) {
          const combined = Buffer.concat(connection.pcmChunkQueue);
          connection.pcmChunkQueue = [];
          connection.pcmQueuedBytes = 0;

          if (connection.deepgramWs && connection.deepgramReady && connection.deepgramWs.readyState === WebSocket.OPEN) {
            try {
              connection.deepgramWs.send(combined);
              connection.forwardedBytes += combined.length;
              this.sendLog(connectionId, 'info', 'Forwarded PCM to Deepgram', { bytes: combined.length, totalForwarded: connection.forwardedBytes });
            } catch (e) {
              console.error('[DEEPGRAM][WS] Send error:', e.message);
              this.sendLog(connectionId, 'error', 'Deepgram WS send error', { error: e.message });
              // Re-queue on failure
              connection.pcmChunkQueue.push(combined);
              connection.pcmQueuedBytes += combined.length;
            }
          } else {
            // Deepgram not ready → buffer in deepgramQueue to flush on open
            connection.deepgramQueue.push(combined);
            this.sendLog(connectionId, 'warning', 'Deepgram WS not ready, queueing aggregated audio', { queued: connection.deepgramQueue.length, queuedBytes: combined.length });
          }
        }
      }
    } catch (error) {
      console.error(`[WEBSOCKET] Error in handleMedia:`, error);
      this.sendLog(connectionId, 'error', 'Error in handleMedia', { error: error.message });
    }
  }

  async handleUserMessage(connectionId, data) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    try {
      const text = (data?.text || '').toString().trim();
      if (!text) return;

      // Mirror to UI as conversation update
      connection.ws.send(JSON.stringify({
        event: 'conversation',
        userMessage: text,
        timestamp: new Date().toISOString()
      }));

      // Persist user text message
      if (connection.chatId) {
        try {
          await Chat.findByIdAndUpdate(
            connection.chatId,
            { $push: { messages: { role: 'user', type: 'text', text } }, $set: { lastActivityAt: new Date() } },
            { new: false }
          );
        } catch (e) {
          console.error('[CHAT] Failed to append user text message:', e.message);
        }
      }

      // Process with LLM + TTS
      await this.processUserUtterance(connectionId, text);
    } catch (error) {
      console.error('[WEBSOCKET] handleUserMessage error:', error.message);
      this.sendLog(connectionId, 'error', 'handleUserMessage error', { error: error.message });
    }
  }

  async handleStop(connectionId, data) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    console.log(`[WEBSOCKET] Stopping session for connection ${connectionId}`);
    
    // Close Deepgram WS if open
    if (connection.deepgramWs && connection.deepgramWs.readyState === WebSocket.OPEN) {
      try { connection.deepgramWs.close(); } catch {}
    }

    // Clean up connection
    // Mark chat ended
    if (connection.chatId) {
      try {
        await Chat.findByIdAndUpdate(
          connection.chatId,
          { $set: { endedAt: new Date(), lastActivityAt: new Date() }, $push: { messages: { role: 'system', type: 'event', text: 'Session stopped' } } },
          { new: false }
        )
      } catch (e) {
        console.error('[CHAT] Failed to mark chat ended on stop:', e.message)
      }
    }

    this.connections.delete(connectionId);
    
    connection.ws.send(JSON.stringify({
      event: 'stop',
      timestamp: new Date().toISOString()
    }));
  }

  async sendInitialGreeting(connectionId) {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.agent) return;

    try {
      const greeting = connection.agent.firstMessage || "Hello! How can I help you today?";
      this.sendLog(connectionId, 'info', 'Sending initial greeting', { greeting });
      
      // Conversation persistence disabled
      
      // Convert greeting to speech
      const audioGreeting = await this.textToSpeech(greeting, connection.agent.voiceSelection, connectionId);
      
      if (audioGreeting) {
        const connection2 = this.connections.get(connectionId);
        connection2.ws.send(JSON.stringify({
          event: 'media',
          streamSid: connection2.sessionId,
          media: { payload: audioGreeting },
          timestamp: new Date().toISOString()
        }));
      }
      
    } catch (error) {
      console.error(`[WEBSOCKET] Error sending initial greeting:`, error);
      this.sendLog(connectionId, 'error', 'Error sending initial greeting', { error: error.message });
    }
  }

  async promptForRegistration(connectionId) {
    const connection = this.connections.get(connectionId);
    if (!connection) return;

    try {
      const prompt = "I'd like to get to know you better. Could you please tell me your name and mobile number?";
      this.sendLog(connectionId, 'info', 'Prompting for registration', { prompt });
      
      // Add system message
      await connection.user.addMessage(
        connection.agent._id,
        connection.agent.agentName,
        'system',
        prompt,
        null
      );
      
      // Convert to speech
      const audioPrompt = await this.textToSpeech(prompt, connection.agent.voiceSelection, connectionId);
      
      if (audioPrompt) {
        const connection2 = this.connections.get(connectionId);
        connection2.ws.send(JSON.stringify({
          event: 'media',
          streamSid: connection2.sessionId,
          media: { payload: audioPrompt },
          timestamp: new Date().toISOString()
        }));
      }
      
      // Increment registration attempts
      await connection.user.incrementRegistrationAttempts();
      
    } catch (error) {
      console.error(`[WEBSOCKET] Error prompting for registration:`, error);
      this.sendLog(connectionId, 'error', 'Error prompting for registration', { error: error.message });
    }
  }

  async transcribeAudio(audioBase64, connectionId) {
    // Deprecated with Deepgram WS; kept for fallback/testing
    try {
      const audioBuffer = Buffer.from(audioBase64, 'base64');
      this.sendLog(connectionId, 'info', 'Deepgram HTTP request', { bytes: audioBuffer.length, encoding: 'linear16', sampleRate: 8000 });
      const url = 'https://api.deepgram.com/v1/listen?model=nova-2&language=en-US&smart_format=true&encoding=linear16&sample_rate=8000&channels=1';
      const response = await axios.post(
        url,
        audioBuffer,
        {
          headers: {
            'Authorization': `Token ${this.deepgramApiKey}`,
            'Content-Type': 'application/octet-stream'
          },
          timeout: 10000
        }
      );
      if (response.data && response.data.results && response.data.results.channels) {
        const transcript = response.data.results.channels[0].alternatives?.[0]?.transcript || '';
        this.sendLog(connectionId, 'success', 'Deepgram HTTP transcript received', { transcript });
        return transcript;
      }
      this.sendLog(connectionId, 'warning', 'Deepgram HTTP returned no transcript');
      return null;
    } catch (error) {
      const status = error.response?.status;
      const errMsg = error.response?.data || error.message;
      console.error(`[DEEPGRAM] Transcription error:`, errMsg);
      this.sendLog(connectionId, 'error', 'Deepgram HTTP transcription error', { status, errMsg });
      return null;
    }
  }

  async generateAIResponse(connectionId, userMessage) {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.agent || !connection.user) return null;

    try {
      // Build combined system prompt: default SystemPrompt (if any) + agent.systemPrompt
      let combinedSystemPrompt = connection.agent.systemPrompt || '';
      try {
        const defaultPrompt = await SystemPrompt.findOne({ isDefault: true });
        if (defaultPrompt && defaultPrompt.promptText) {
          combinedSystemPrompt = `${defaultPrompt.promptText}\n\n${combinedSystemPrompt}`.trim();
        }
      } catch (_) {}

      // Prepare request body for external RAG endpoint
      const queryId = `query_${Date.now()}`;
      const sessionId = connection.sessionId || `session_${Date.now()}`;
      const body = {
        query_id: queryId,
        session_id: sessionId,
        query: userMessage,
        book_name: connection.agent && connection.agent._id ? String(connection.agent._id) : 'unknown',
        chapter_name: '',
        client_id: connection.clientId ? String(connection.clientId) : 'unknown',
        system_prompt: combinedSystemPrompt || 'you are a helpful assistant',
        llm: 'openai',
        top_k: 5
      };
      console.log(body);
      this.sendLog(connectionId, 'info', 'Calling RAG endpoint', { url: 'https://vectrize.ailisher.com/api/v1/rag/query', query_id: body.query_id });
      const resp = await axios.post(
        'https://vectrize.ailisher.com/api/v1/rag/query',
        body,
        { headers: { 'Content-Type': 'application/json' }, timeout: 20000 }
      );

      const llmResponse = resp?.data?.data?.llm_response;
      if (llmResponse && typeof llmResponse === 'string') {
        this.sendLog(connectionId, 'success', 'RAG responded', { textPreview: llmResponse.slice(0, 120) });
        return { text: llmResponse };
      }
      console.log(llmResponse);

      this.sendLog(connectionId, 'warning', 'RAG returned no llm_response');
      return { text: "I'm sorry, I couldn't find an answer to that." };
    } catch (error) {
      console.error('[RAG] Error generating response:', error.message);
      this.sendLog(connectionId, 'error', 'RAG error', { error: error.response?.data || error.message });
      return { text: "I'm sorry, I'm having trouble processing your request right now." };
    }
  }

  extractUserDetails(userMessage) {
    const details = {
      name: null,
      mobileNumber: null
    };
    
    // Extract name patterns
    const namePatterns = [
      /(?:my name is|i'm|i am|call me|this is)\s+([a-zA-Z\s]+)/i,
      /(?:name|naam)\s+(?:is\s+)?([a-zA-Z\s]+)/i,
      /([a-zA-Z]+\s+[a-zA-Z]+)/i  // Generic name pattern
    ];
    
    for (const pattern of namePatterns) {
      const match = userMessage.match(pattern);
      if (match && match[1]) {
        const name = match[1].trim();
        if (name.length > 2 && name.length < 50) {
          details.name = name;
          break;
        }
      }
    }
    
    // Extract mobile number patterns
    const mobilePatterns = [
      /(\d{10})/,  // 10 digits
      /(\d{11})/,  // 11 digits
      /(\d{12})/,  // 12 digits
      /(?:mobile|phone|number|no)\s*(?:is\s+)?(\d{10,12})/i
    ];
    
    for (const pattern of mobilePatterns) {
      const match = userMessage.match(pattern);
      if (match && match[1]) {
        const mobile = match[1].trim();
        if (mobile.length >= 10 && mobile.length <= 12) {
          details.mobileNumber = mobile;
          break;
        }
      }
    }
    
    console.log(`[REGISTRATION] Extracted details from "${userMessage}":`, details);
    return details;
  }

  async textToSpeech(text, voiceSelection = 'abhilash', connectionId) {
    try {
      const connection = this.connections.get(connectionId);
      const language = (connection?.agent?.language && typeof connection.agent.language === 'string')
        ? connection.agent.language
        : 'en';
      const startedAt = Date.now();
      const result = await this.voiceService.textToSpeech(text, language, voiceSelection);
      const durationMs = Date.now() - startedAt;
      const bytesBase64 = result?.audioBase64 ? result.audioBase64.length : 0;
      this.sendLog(connectionId, 'success', 'TTS synthesized', { durationMs, bytesBase64, language, speaker: voiceSelection });
      return result?.audioBase64 || null;
    } catch (error) {
      this.sendLog(connectionId, 'error', 'TTS synthesis error', { error: error.message });
      return null;
    }
  }

  // Utility method to get connection info
  getConnectionInfo() {
    return {
      totalConnections: this.connections.size,
      connections: Array.from(this.connections.values()).map(c => ({
        id: c.id,
        deepgramReady: !!c.deepgramReady,
        queuedAudio: c.deepgramQueue?.length || 0,
        agent: c.agent?.agentName || null,
        sessionId: c.sessionId
      }))
    };
  }
}

module.exports = VoiceChatWebSocketServer;
