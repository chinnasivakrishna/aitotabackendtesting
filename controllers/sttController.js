const STTProject = require('../models/STTProject');
const axios = require('axios');
const { putobject, getobject, s3Client } = require('../utils/s3');

// Create project
exports.createProject = async (req, res) => {
  try {
    const { name, description, category } = req.body || {};
    if (!name) return res.status(400).json({ success: false, message: 'Project name is required' });
    const project = await STTProject.create({ name, description, category, createdBy: req.user?.id });
    res.json({ success: true, data: project });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// List projects
exports.listProjects = async (req, res) => {
  try {
    const projects = await STTProject.find({}).sort({ createdAt: -1 }).lean();
    res.json({ success: true, data: projects });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// Get project by id
exports.getProject = async (req, res) => {
  try {
    const { id } = req.params;
    const project = await STTProject.findById(id).lean();
    if (!project) return res.status(404).json({ success: false, message: 'Project not found' });
    res.json({ success: true, data: project });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// Presign upload for an audio file to S3
exports.presignAudioUpload = async (req, res) => {
  try {
    const { id } = req.params; // project id
    const { filename, contentType } = req.body || {};
    if (!filename || !contentType) return res.status(400).json({ success: false, message: 'filename and contentType required' });
    const safeName = String(filename).replace(/[^a-zA-Z0-9_.-]/g, '_');
    const key = `stt/${id}/${Date.now()}-${Math.random().toString(36).slice(2, 8)}-${safeName}`;
    const url = await putobject(key, contentType);
    res.json({ success: true, data: { key, url } });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// Register an uploaded audio item and kick off processing
exports.addItem = async (req, res) => {
  try {
    const { id } = req.params; // project id
    const { s3Key, filename, contentType } = req.body || {};
    if (!s3Key) return res.status(400).json({ success: false, message: 's3Key required' });
    const project = await STTProject.findById(id);
    if (!project) return res.status(404).json({ success: false, message: 'Project not found' });
    const item = {
      s3Key,
      originalFilename: filename || '',
      contentType: contentType || 'audio/wav',
      status: 'processing',
    };
    project.items.push(item);
    await project.save();
    const createdItem = project.items[project.items.length - 1];
    // Fire and forget processing
    processItem(project._id, createdItem._id).catch(() => {});
    res.json({ success: true, data: project });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// Public URLs for downloads
exports.getTranscriptUrl = async (req, res) => {
  try {
    const { itemId } = req.params;
    const { project, item } = await findItem(itemId);
    if (!item?.transcriptKey) return res.status(404).json({ success: false, message: 'Transcript not ready' });
    const url = await getobject(item.transcriptKey);
    res.json({ success: true, url });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

exports.getQAUrl = async (req, res) => {
  try {
    const { itemId } = req.params;
    const { project, item } = await findItem(itemId);
    if (!item?.qaKey) return res.status(404).json({ success: false, message: 'Q&A not ready' });
    const url = await getobject(item.qaKey);
    res.json({ success: true, url });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// Fetch logs for an item
exports.getItemLogs = async (req, res) => {
  try {
    const { itemId } = req.params;
    const { item } = await findItem(itemId);
    return res.json({ success: true, data: (item.logs || []).sort((a,b)=> new Date(a.at) - new Date(b.at)) });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

// Trigger processing explicitly (optional)
exports.processItem = async (req, res) => {
  try {
    const { itemId } = req.params;
    await processItem(null, itemId);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
};

async function findItem(itemId) {
  const project = await STTProject.findOne({ 'items._id': itemId });
  if (!project) throw new Error('Item not found');
  const item = project.items.id(itemId);
  return { project, item };
}

async function processItem(projectIdOrNull, itemId) {
  const { project, item } = await findItem(itemId);
  try {
    // 1) Get a temporary URL for audio
    const audioUrl = await getobject(item.s3Key);
    if (!Array.isArray(item.logs)) item.logs = [];
    item.logs.push({ level: 'info', message: 'Generated signed URL for audio' });
    // 2) Transcribe with Deepgram
    const dgKey = process.env.DEEPGRAM_API_KEY;
    if (!dgKey) throw new Error('DEEPGRAM_API_KEY not set');
    const base = 'https://api.deepgram.com/v1/listen';
    const endpoint = `${base}?model=nova-2&smart_format=true&punctuate=true&detect_language=true&paragraphs=true&numerals=true`;
    let transcriptText = '';
    let detectedLang = '';
    let confidence = undefined;
    try {
      const dgResp = await axios.post(
        endpoint,
        { url: audioUrl },
        {
          headers: {
            Authorization: `Token ${dgKey}`,
            'Content-Type': 'application/json',
          },
        }
      );
      const alt = dgResp.data?.results?.channels?.[0]?.alternatives?.[0];
      transcriptText = alt?.transcript || '';
      confidence = alt?.confidence;
      detectedLang = alt?.language || dgResp.data?.metadata?.detected_language || '';
      item.logs.push({ level: 'info', message: 'Deepgram response received', meta: { words: alt?.words?.length || 0, confidence, detectedLang } });
    } catch (dgErr) {
      item.logs.push({ level: 'warn', message: 'Deepgram primary attempt failed', meta: { error: dgErr?.response?.data || dgErr.message } });
      // Fallback: attempt without smart_format
      const fallbackResp = await axios.post(
        base,
        { url: audioUrl },
        { headers: { Authorization: `Token ${dgKey}`, 'Content-Type': 'application/json' } }
      );
      const alt = fallbackResp.data?.results?.channels?.[0]?.alternatives?.[0];
      transcriptText = alt?.transcript || '';
      confidence = alt?.confidence;
      detectedLang = alt?.language || fallbackResp.data?.metadata?.detected_language || '';
      item.logs.push({ level: 'info', message: 'Deepgram fallback used', meta: { confidence, detectedLang } });
    }

    // If language detected and not forced yet, or confidence is low, try a language-forced pass
    if ((!transcriptText || (typeof confidence === 'number' && confidence < 0.75)) && detectedLang) {
      try {
        const forced = `${base}?model=nova-2&language=${encodeURIComponent(detectedLang)}&smart_format=true&punctuate=true&paragraphs=true&numerals=true`;
        const forcedResp = await axios.post(
          forced,
          { url: audioUrl },
          { headers: { Authorization: `Token ${dgKey}`, 'Content-Type': 'application/json' } }
        );
        const alt2 = forcedResp.data?.results?.channels?.[0]?.alternatives?.[0];
        const text2 = alt2?.transcript || '';
        const conf2 = alt2?.confidence;
        if (text2 && (!transcriptText || (typeof conf2 === 'number' && conf2 > (confidence || 0)))) {
          transcriptText = text2;
          confidence = conf2;
          item.logs.push({ level: 'info', message: 'Deepgram language-forced pass improved result', meta: { detectedLang, confidence } });
        } else {
          item.logs.push({ level: 'info', message: 'Language-forced pass did not improve' });
        }
      } catch (langErr) {
        item.logs.push({ level: 'warn', message: 'Language-forced pass failed', meta: { error: langErr?.response?.data || langErr.message, detectedLang } });
      }
    }
    if (!transcriptText) throw new Error('No transcript returned');
    item.logs.push({ level: 'info', message: 'Transcript generated', meta: { length: transcriptText.length } });
    // 3) Save transcript to S3 as txt
    const transcriptKey = `stt/${project._id}/${item._id}-transcript.txt`;
    await uploadTextToS3(transcriptKey, transcriptText);
    item.logs.push({ level: 'info', message: 'Transcript uploaded to S3', meta: { key: transcriptKey } });
    // 4) Generate Q&A with OpenAI
    const qaText = await generateQA(transcriptText, detectedLang);
    const qaKey = `stt/${project._id}/${item._id}-qa.txt`;
    await uploadTextToS3(qaKey, qaText);
    item.logs.push({ level: 'info', message: 'Q&A uploaded to S3', meta: { key: qaKey } });
    // 5) Update item
    item.transcriptKey = transcriptKey;
    item.qaKey = qaKey;
    item.status = 'completed';
    await project.save();
  } catch (e) {
    item.status = 'failed';
    item.error = e.message;
    if (!Array.isArray(item.logs)) item.logs = [];
    item.logs.push({ level: 'error', message: 'Processing failed', meta: { error: e.message } });
    await project.save();
    throw e;
  }
}

async function uploadTextToS3(key, content) {
  // Prepend UTF-8 BOM to help Windows editors detect encoding
  const bom = '\uFEFF';
  const body = `${bom}${content}`;
  const url = await putobject(key, 'text/plain; charset=utf-8');
  await axios.put(url, body, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
}

async function generateQA(transcript, languageCode) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) return 'OpenAI not configured. Provide OPENAI_API_KEY to enable Q&A.';

  // Split very long transcripts into manageable chunks for the model
  const chunks = [];
  const maxChunkSize = 6000; // characters per chunk (approximate for token safety)
  for (let i = 0; i < transcript.length; i += maxChunkSize) {
    chunks.push(transcript.slice(i, i + maxChunkSize));
  }

  const headers = { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' };
  const model = 'gpt-4o-mini';
  const systemPrompt = [
    'You are an expert call analyst and training data creator.',
    'Your job is to carefully study the given transcript of a human caller talking with a customer.',
    '- Use plain text only. Output format strictly as multiple lines of "Q: ..." then "A: ..." pairs.',
    '',
    'Objective:',
    '- Convert the transcript into *high-quality conversational Q&A pairs* for training an AI calling agent.',
    '- Focus on extracting the essence of the dialogue: customer queries, objections, hesitations, interests, and caller’s responses.',
    '- General chit-chat (hello, how are you, etc.) should be skipped unless it shows useful patterns for building rapport.',
    '',
    'Rules:',
    '1. Cover all important intents/topics discussed (product details, pricing, objections, interest signals, closing, follow-ups).',
    '2. Rewrite questions in a clear and generalised form (not word-for-word copy).',
    '3. Rewrite answers in short, professional, polite, and convincing sentences (1–3 sentences max).',
    '4. Ensure that every Q&A is useful for *training a sales/marketing AI agent*.',
    '5. Do NOT invent new topics — only use what is present in the transcript, but you may *rephrase/standardise* for clarity.',
    '6. Output format strictly as multiple lines of:',
    '   Q: [customer style question/objection]',
    '   A: [caller style helpful response]',
    '',
    languageCode ? `- IMPORTANT: Write all questions and answers in this language: ${languageCode}.` : '',
  ].join('\n');

  const results = [];
  for (let idx = 0; idx < chunks.length; idx++) {
    const section = chunks[idx];
    try {
      const body = {
        model,
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: `From the following transcript section ${idx + 1}/${chunks.length}, extract as many Q&A pairs as possible. If the transcript language is not English, write Q&A in the same language (${languageCode || 'unknown'}).\n\nTranscript Section:\n${section}` },
        ],
        temperature: 0.2,
        max_tokens: 2000,
      };
      const resp = await axios.post('https://api.openai.com/v1/chat/completions', body, { headers });
      const text = resp.data?.choices?.[0]?.message?.content?.trim();
      if (text) {
        const header = chunks.length > 1 ? `Section ${idx + 1} Q&A\n` : '';
        results.push(`${header}${text}`);
      }
    } catch (e) {
      results.push(`Q&A generation failed for section ${idx + 1}: ${e.message}`);
    }
  }

  const combined = results.join('\n\n');
  return combined || 'No Q&A generated.';
}


