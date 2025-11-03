const express = require('express');
const router = express.Router();
const stt = require('../controllers/sttController');

// Projects
router.post('/projects', stt.createProject);
router.get('/projects', stt.listProjects);
router.get('/projects/:id', stt.getProject);

// Upload & items
router.post('/projects/:id/presign', stt.presignAudioUpload);
router.post('/projects/:id/items', stt.addItem);

// Results
router.get('/items/:itemId/transcript-url', stt.getTranscriptUrl);
router.get('/items/:itemId/qa-url', stt.getQAUrl);
router.post('/items/:itemId/process', stt.processItem);
router.get('/items/:itemId/logs', stt.getItemLogs);

module.exports = router;


