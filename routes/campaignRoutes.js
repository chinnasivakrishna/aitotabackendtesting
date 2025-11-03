const express = require('express');
const controller = require('../controllers/campaignController');
const router = express.Router();
router.post('/:id/start', controller.start);
router.post('/:id/stop', controller.stop);
router.post('/:id/pause', controller.pause);
router.post('/:id/resume', controller.resume);
router.get('/:id/status', controller.getStatus);
module.exports = router;
