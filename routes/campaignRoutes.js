const express = require('express');
const controller = require('../controllers/campaignController');
const router = express.Router();

// Campaign control endpoints
// IMPORTANT: More specific routes must come before generic /:id route
router.get('/:id/status', controller.getStatus);          // GET /api/campaigns/:id/status - Get campaign status
router.get('/:id/transcripts', controller.getTranscripts); // GET /api/campaigns/:id/transcripts - Get campaign transcripts
router.post('/:id/start', controller.start);             // POST /api/campaigns/:id/start - Start campaign
router.post('/:id/stop', controller.stop);               // POST /api/campaigns/:id/stop - Stop campaign
router.post('/:id/pause', controller.pause);             // POST /api/campaigns/:id/pause - Pause campaign
router.post('/:id/resume', controller.resume);           // POST /api/campaigns/:id/resume - Resume campaign
router.get('/:id', controller.getCampaign);              // GET /api/campaigns/:id - Get campaign details (must be last)

module.exports = router;
