const express = require('express');
const router = express.Router();
const { makecallLogin, getMakecallDashboard, verifyMakecallToken, makeSingleCall , makeSingleCallDisposition, getLiveCallStatus} = require('../controllers/makecallController');

// Public login route - no authentication required
router.post('/login', makecallLogin);

// Protected dashboard route - requires makecall token
router.get('/dashboard', verifyMakecallToken, getMakecallDashboard);

// Protected single call route - requires makecall token
router.post('/calls/single', verifyMakecallToken, makeSingleCall);

//Protected single call disposition 
router.post('/calls/single/disposition', verifyMakecallToken, makeSingleCallDisposition);

// Live status check (support both GET with query and POST with body)
router.get('/calls/single/live-status', verifyMakecallToken, getLiveCallStatus);
router.post('/calls/single/live-status', verifyMakecallToken, getLiveCallStatus);

// Health check route
router.get('/health', (req, res) => {
  res.json({
    success: true,
    message: "Makecall API is running",
    timestamp: new Date().toISOString()
  });
});

module.exports = router;
