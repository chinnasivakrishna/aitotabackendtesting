const crypto = require('crypto');

// Generate a short, unique hash for business URLs
const generateBusinessHash = () => {
  // Generate 8-character hash using crypto
  return crypto.randomBytes(4).toString('hex');
};

// Validate hash format (8 characters, hex)
const isValidHash = (hash) => {
  return hash && /^[a-f0-9]{8}$/.test(hash);
};

module.exports = {
  generateBusinessHash,
  isValidHash
};

