// config/cashfree.js
const dotenv = require('dotenv');
dotenv.config();

const isProduction = process.env.NODE_ENV === 'prod' || process.env.NODE_ENV === 'production' || process.env.CASHFREE_ENV === 'production';

const config = {
  CLIENT_ID: isProduction 
    ? process.env.CASHFREE_CLIENT_ID 
    : process.env.CASHFREE_CLIENT_ID_TEST,
  CLIENT_SECRET: isProduction 
    ? process.env.CASHFREE_SECRET_KEY 
    : process.env.CASHFREE_SECRET_KEY_TEST,
  BASE_URL: isProduction 
    ? 'https://api.cashfree.com' 
    : 'https://sandbox.cashfree.com',
  ENVIRONMENT: isProduction ? 'production' : 'sandbox',
  RETURN_URL: process.env.BACKEND_URL || 'https://app.aitota.com',
  ENV: isProduction ? 'prod' : 'test'
};
console.log(`🏦 Cashfree initialized in ${config.ENVIRONMENT} mode`);
console.log(`🏦 Environment variables:`, {
  NODE_ENV: process.env.NODE_ENV,
  CASHFREE_ENV: process.env.CASHFREE_ENV,
  isProduction: isProduction,
  CASHFREE_CLIENT_ID: process.env.CASHFREE_CLIENT_ID ? process.env.CASHFREE_CLIENT_ID.substring(0, 10) + '...' : 'NOT_SET',
  CASHFREE_CLIENT_ID_TEST: process.env.CASHFREE_CLIENT_ID_TEST ? process.env.CASHFREE_CLIENT_ID_TEST.substring(0, 10) + '...' : 'NOT_SET'
});
console.log(`🏦 Cashfree config:`, {
  ENV: config.ENV,
  BASE_URL: config.BASE_URL,
  RETURN_URL: config.RETURN_URL,
  hasClientId: !!config.CLIENT_ID,
  hasClientSecret: !!config.CLIENT_SECRET,
  clientIdPrefix: config.CLIENT_ID ? config.CLIENT_ID.substring(0, 4) : 'N/A'
});

module.exports = config;