const dotenv = require('dotenv');
dotenv.config();

const PaytmConfig = {
  MID: process.env.PAYTM_MID,
  WEBSITE: process.env.PAYTM_WEBSITE || 'WEBSTAGING',
  CHANNEL_ID: process.env.PAYTM_CHANNEL_ID || 'WEB',
  INDUSTRY_TYPE_ID: process.env.PAYTM_INDUSTRY_TYPE || 'Retail',
  MERCHANT_KEY: process.env.PAYTM_KEY,
  CALLBACK_URL: process.env.PAYTM_CALLBACK_URL || 'http://localhost:4000/api/v1/paytm/callback',
  PAYTM_URL: process.env.PAYTM_URL || 'https://securegw.paytm.in/order/process',
  STATUS_URL: process.env.PAYTM_STATUS_URL || 'https://securegw.paytm.in/order/status',
  FRONTEND_URL: process.env.FRONTEND_URL || 'http://localhost:5173',
};

module.exports = PaytmConfig;


