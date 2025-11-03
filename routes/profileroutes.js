const express = require('express');
const router = express.Router();
const profileController = require('../controllers/profilecontroller');
const { verifyClientToken, verifyClientOrHumanAgentToken } = require('../middlewares/authmiddleware');

// Create profile for authenticated client
router.post('/', verifyClientToken, profileController.createProfile);

// Get profile by profile ID (for authenticated client)
router.get('/:profileId', verifyClientOrHumanAgentToken, profileController.getProfile);

// Get profile by client ID (for backward compatibility)
router.get('/client/:clientId', verifyClientOrHumanAgentToken, profileController.getProfileByClientId);

// Update profile by profile ID (for authenticated client)
router.put('/:profileId', verifyClientToken, profileController.updateProfile);

// Delete profile by profile ID (for authenticated client)
router.delete('/:profileId', verifyClientToken, profileController.deleteProfile);

// Get all profiles with pagination and search (for admin purposes)
router.get('/', verifyClientOrHumanAgentToken, profileController.getAllProfiles);

module.exports = router; 