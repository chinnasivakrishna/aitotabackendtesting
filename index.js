const express = require('express');
const dotenv = require('dotenv');
const cors = require('cors');
const connectDB = require('./config/db');
const axios = require('axios');
const http = require('http');
const { Kafka } = require('kafkajs');
const VoiceChatWebSocketServer = require('./websocketServer');
const superadminRoutes = require('./routes/superadminroutes')
const adminRoutes = require('./routes/adminroutes');
const clientRoutes = require('./routes/clientroutes')
const profileRoutes = require('./routes/profileroutes')
const chatRoutes = require('./routes/chatroutes')
const templateRoutes = require('./routes/templateroutes')
const seriesCampaignRoutes = require('./routes/seriesCampaign')
const agentAccessRoutes = require('./routes/agentAccessRoutes')
const makecallRoutes = require('./routes/makecallRoutes')
const sttRoutes = require('./routes/sttRoutes')
const dispositionRoutes = require('./routes/dispositionRoutes')
const Business = require('./models/MyBussiness');
// const humanAgentRoutes = require('./routes/humanAgentRoutes');
const { CLIENT_ID, CLIENT_SECRET, BASE_URL } = require('./config/cashfree');
const jwt = require('jsonwebtoken');
const app = express();
const Client = require("./models/Client");
const Payment = require("./models/Payment");
const server = http.createServer(app);
const campaignRoutes = require('./routes/campaignRoutes');
const errorHandler = require('./utils/errorHandler');
// Cashfree callback (return_url handler)

// Import required modules (add these to your existing imports)

// Cashfree payment initiation endpoint - add this to your clientRoutes or main app
app.post('/api/v1/client/payments/initiate/cashfree', async (req, res) => {
  try {
    const { amount, planKey } = req.body;
    
    // Extract token from Authorization header
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return res.status(401).json({
        success: false,
        message: 'Authorization token required'
      });
    }

    // Verify JWT token
    const jwt = require('jsonwebtoken');
    let clientId;
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      clientId = decoded.id || decoded.clientId;
    } catch (e) {
      return res.status(401).json({
        success: false,
        message: 'Invalid authorization token'
      });
    }

    if (!amount || !planKey) {
      return res.status(400).json({
        success: false,
        message: 'Amount and planKey are required'
      });
    }

    // Validate plan
    const validPlans = ['basic', 'professional', 'enterprise'];
    if (!validPlans.includes(planKey.toLowerCase())) {
      return res.status(400).json({
        success: false,
        message: 'Invalid plan selected'
      });
    }

    // Generate unique order ID
    const orderId = `CF_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    // Get client info
    const Client = require('./models/Client'); // Adjust path as needed
    const client = await Client.findById(clientId);
    
    if (!client) {
      return res.status(404).json({
        success: false,
        message: 'Client not found'
      });
    }

    // Cashfree order creation payload
    const orderPayload = {
      order_id: orderId,
      order_amount: parseFloat(amount),
      order_currency: 'INR',
      customer_details: {
        customer_id: clientId.toString(),
        customer_name: client.name || client.username || client.email || 'Customer',
        customer_email: client.email || `${clientId}@aitota.com`,
        customer_phone: client.phone || client.mobile || '9999999999'
      },
      order_meta: {
        return_url: `${process.env.BACKEND_URL || 'https://app.aitota.com'}/api/v1/cashfree/callback?order_id=${orderId}`,
        notify_url: `${process.env.BACKEND_URL || 'https://app.aitota.com'}/api/v1/cashfree/webhook`,
        payment_methods: 'cc,dc,nb,upi,paylater,emi,cardlessemi,wallet'
      },
      order_tags: {
        plan: planKey,
        client_id: clientId.toString()
      }
    };

    // Create order with Cashfree
    const headers = {
      'x-client-id': CLIENT_ID,
      'x-client-secret': CLIENT_SECRET,
      'x-api-version': '2022-09-01',
      'Content-Type': 'application/json'
    };

    console.log('🏦 Creating Cashfree order:', orderId);
    
    const cashfreeResponse = await axios.post(
      `${BASE_URL}/pg/orders`,
      orderPayload,
      { headers }
    );

    const { payment_session_id, order_status } = cashfreeResponse.data;

    if (!payment_session_id) {
      throw new Error('Failed to create payment session with Cashfree');
    }

    // Save payment record to database
    const Payment = require('./models/Payment');
    const paymentRecord = new Payment({
      orderId,
      clientId,
      amount: parseFloat(amount),
      planKey: planKey.toLowerCase(),
      gateway: 'cashfree',
      status: 'INITIATED',
      sessionId: payment_session_id,
      orderStatus: order_status,
      createdAt: new Date(),
      rawResponse: cashfreeResponse.data
    });

    await paymentRecord.save();

    // Return session details for frontend
    res.json({
      success: true,
      data: {
        orderId,
        sessionId: payment_session_id,
        amount: parseFloat(amount),
        planKey,
        environment: BASE_URL.includes('sandbox') ? 'sandbox' : 'production'
      }
    });

  } catch (error) {
    console.error('❌ Cashfree payment initiation failed:', error.response?.data || error.message);
    
    res.status(500).json({
      success: false,
      message: 'Payment initiation failed',
      error: error.response?.data?.message || error.message
    });
  }
});

// Cashfree direct payment initiation (similar to your Paytm route)
app.get('/api/v1/client/payments/initiate/cashfree-direct', async (req, res) => {
  try {
    const { t: token, amount, planKey } = req.query;
    
    if (!token || !amount || !planKey) {
      return res.status(400).json({
        success: false,
        message: 'Token, amount and planKey are required'
      });
    }

    // Decode and verify token
    const jwt = require('jsonwebtoken');
    let clientId;
    
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      clientId = decoded.id || decoded.clientId;
    } catch (e) {
      return res.status(401).json({
        success: false,
        message: 'Invalid token'
      });
    }

    // Get client info
    const Client = require('./models/Client');
    const client = await Client.findById(clientId);
    
    if (!client) {
      return res.status(404).json({
        success: false,
        message: 'Client not found'
      });
    }

    // Generate unique order ID
    const orderId = `CF_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

    const orderPayload = {
      order_id: orderId,
      order_amount: parseFloat(amount),
      order_currency: 'INR',
      customer_details: {
        customer_id: clientId.toString(),
        customer_name: client.name || client.username || client.email || 'Customer',
        customer_email: client.email || `${clientId}@aitota.com`,
        customer_phone: client.phone || client.mobile || '9999999999'
      },
      order_meta: {
        return_url: `${process.env.BACKEND_URL || 'https://app.aitota.com'}/api/v1/cashfree/callback?order_id=${orderId}`,
        notify_url: `${process.env.BACKEND_URL || 'https://app.aitota.com'}/api/v1/cashfree/webhook`,
        payment_methods: 'cc,dc,nb,upi,paylater,emi,cardlessemi,wallet'
      },
      order_tags: {
        plan: planKey,
        client_id: clientId.toString()
      }
    };

    const headers = {
      'x-client-id': CLIENT_ID,
      'x-client-secret': CLIENT_SECRET,
      'x-api-version': '2022-09-01',
      'Content-Type': 'application/json'
    };

    const cashfreeResponse = await axios.post(
      `${BASE_URL}/pg/orders`,
      orderPayload,
      { headers }
    );

    const { payment_session_id } = cashfreeResponse.data;

    // Save payment record
    const Payment = require('./models/Payment');
    const paymentRecord = new Payment({
      orderId,
      clientId,
      amount: parseFloat(amount),
      planKey: planKey.toLowerCase(),
      gateway: 'cashfree',
      status: 'INITIATED',
      sessionId: payment_session_id,
      createdAt: new Date(),
      rawResponse: cashfreeResponse.data
    });

    await paymentRecord.save();

    // Redirect to Cashfree payment page
    const paymentUrl = `https://payments${BASE_URL.includes('sandbox') ? '-test' : ''}.cashfree.com/pay/${orderId}/${payment_session_id}`;
    res.redirect(paymentUrl);

  } catch (error) {
    console.error('❌ Cashfree direct payment failed:', error.response?.data || error.message);
    const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
    const SUCCESS_PATH = process.env.PAYMENT_SUCCESS_PATH || '/auth/dashboard';
    res.redirect(`${FRONTEND_URL}${SUCCESS_PATH}?status=FAILED&error=${encodeURIComponent(error.message)}`);
  }
});

// Cashfree webhook handler (optional but recommended for better reliability)
app.post('/api/v1/cashfree/webhook', express.json(), async (req, res) => {
  try {
    const webhookData = req.body;
    console.log('🔔 Cashfree webhook received:', webhookData);

    if (!webhookData || typeof webhookData !== 'object') {
      console.warn('Webhook body missing or not JSON');
      return res.status(200).json({ success: true });
    }

    // Verify webhook signature (implement based on Cashfree docs)
    // const signature = req.headers['x-webhook-signature'];
    // if (!verifyWebhookSignature(webhookData, signature)) {
    //   return res.status(401).json({ success: false, message: 'Invalid signature' });
    // }

    // Normalize payload across possible Cashfree webhook shapes
    const normalized = {
      orderId: webhookData.order_id || webhookData.data?.order?.order_id || webhookData.data?.order_id,
      orderStatus: webhookData.order_status || webhookData.data?.order?.order_status || webhookData.data?.order_status,
      paymentStatus: webhookData.payment_status || webhookData.data?.payment?.payment_status || webhookData.data?.payment_status,
      paymentId: webhookData.cf_payment_id || webhookData.data?.payment?.cf_payment_id,
      amount: webhookData.order_amount || webhookData.data?.order?.order_amount,
      customerId: webhookData.customer_id || webhookData.data?.customer_details?.customer_id,
      planTag: webhookData.order_tags?.plan || webhookData.data?.order?.order_tags?.plan,
      type: webhookData.type
    };

    if (!normalized.orderId) {
      console.warn('Webhook missing orderId, skipping');
      return res.json({ success: true });
    }

    const isSuccess = (normalized.paymentStatus === 'SUCCESS') || (normalized.orderStatus === 'PAID') || (normalized.type === 'PAYMENT_SUCCESS_WEBHOOK');

    const Payment = require('./models/Payment');
    let payment = await Payment.findOne({ orderId: normalized.orderId });

    if (!payment) {
      // Fallback: create a minimal Payment record so we can credit
      try {
        payment = await Payment.create({
          orderId: normalized.orderId,
          clientId: normalized.customerId,
          amount: normalized.amount,
          planKey: (normalized.planTag || '').toLowerCase(),
          gateway: 'cashfree',
          status: isSuccess ? 'SUCCESS' : (normalized.orderStatus || normalized.paymentStatus || 'PENDING'),
          transactionId: normalized.paymentId,
          rawCallback: webhookData
        });
      } catch (e) {
        console.error('Failed to upsert Payment from webhook:', e.message);
      }
    } else {
      payment.status = isSuccess ? 'SUCCESS' : (normalized.orderStatus || normalized.paymentStatus || payment.status || 'PENDING');
      payment.transactionId = payment.transactionId || normalized.paymentId;
      payment.rawCallback = webhookData;
      await payment.save();
    }

    // Auto-credit on success
    if (payment && isSuccess && !payment.credited) {
      try {
        const Credit = require('./models/Credit');
        const mapping = { basic: 1000, professional: 5500, enterprise: 11000 };
        // Prefer plan on payment, else from webhook tag
        const key = (payment.planKey || normalized.planTag || '').toLowerCase();
        const creditsToAdd = mapping[key] || 0;
        if (creditsToAdd > 0 && payment.clientId) {
          const creditRecord = await Credit.getOrCreateCreditRecord(payment.clientId);
          const txn = payment.transactionId || normalized.paymentId;
          await creditRecord.addCredits(creditsToAdd, 'purchase', `Cashfree ${key} • order ${normalized.orderId} • tx ${txn}`, null, txn);
          payment.credited = true;
          payment.creditsAdded = creditsToAdd;
          await payment.save();
        } else if (!payment.clientId && normalized.customerId) {
          // If payment lacks clientId, but webhook has customerId, still attempt
          const creditRecord = await Credit.getOrCreateCreditRecord(normalized.customerId);
          if (creditsToAdd > 0) {
            await creditRecord.addCredits(creditsToAdd, 'purchase', `Cashfree ${key} • order ${normalized.orderId} • tx ${normalized.paymentId}`, null, normalized.paymentId);
          }
        }
      } catch (e) {
        console.error('Auto-credit from webhook failed:', e.message);
      }
    }

    res.json({ success: true });
  } catch (error) {
    console.error('❌ Cashfree webhook error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Allow CORS preflight for create-order explicitly
app.options('/api/v1/payments/cashfree/create-order', cors({
  origin: [process.env.FRONTEND_URL || 'http://localhost:5173', 'https://www.aitota.com'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  methods: ['POST', 'OPTIONS']
}));

app.post('/api/v1/payments/cashfree/create-order', cors({
  origin: [process.env.FRONTEND_URL || 'http://localhost:5173', 'https://www.aitota.com'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  methods: ['POST', 'OPTIONS']
}), express.json(), async (req, res) => {
  try {
    const { amount, planKey } = req.body || {};
    console.log('🧾 [CREATE-ORDER] headers:', req.headers);
    console.log('🧾 [CREATE-ORDER] body:', req.body);

    // Require auth header
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return res.status(401).json({ success: false, message: 'Authorization token required' });
    }
    // Verify JWT
    const jwt = require('jsonwebtoken');
    let clientId;
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      clientId = decoded.id || decoded.clientId;
    } catch (e) {
      return res.status(401).json({ success: false, message: 'Invalid authorization token' });
    }

    // Resolve amount
    let orderAmount = parseFloat(amount);
    if ((amount === undefined || amount === null || amount === '') || isNaN(orderAmount) || orderAmount <= 0) {
      // Try plan mapping fallback
      const mappingBase = { basic: 1, professional: 5000, enterprise: 10000 };
      const key = typeof planKey === 'string' ? planKey.toLowerCase() : '';
      const base = mappingBase[key];
      if (base) {
        const gst = Math.round(base * 0.18 * 100) / 100;
        orderAmount = Math.round((base + gst) * 100) / 100;
        console.log('🧮 [CREATE-ORDER] computed amount from planKey:', { planKey: key, base, orderAmount });
      } else {
        return res.status(400).json({ success: false, message: 'Missing required field: amount', details: { received: req.body } });
      }
    }

    // Get client details from DB
    const Client = require('./models/Client');
    const client = await Client.findById(clientId);
    if (!client) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }

    let customerName = client.name || client.username || client.email || 'Customer';
    let customerEmail = client.email || `${clientId}@aitota.com`;
    let customerPhone = client.phone || client.mobile || client.mobileNo || '9999999999';
    try {
      const digits = String(customerPhone).replace(/\D/g, '');
      if (digits.length >= 10) customerPhone = digits.slice(-10);
    } catch {}

    const orderId = 'order_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    const customerId = String(clientId);

    const getBaseUrl = () => {
      const host = req.get('host') || '';
      if (host.includes('.onrender.com') || host.includes('.vercel.app') || host.includes('.herokuapp.com') || process.env.NODE_ENV === 'production') {
        return `https://${host}`;
      }
      const proto = (req.headers['x-forwarded-proto'] || req.protocol || 'http');
      return `${proto}://${host}`;
    };

    const baseUrl = getBaseUrl();

    const { BASE_URL, CLIENT_ID, CLIENT_SECRET } = require('./config/cashfree');

    if (!CLIENT_ID || !CLIENT_SECRET) {
      return res.status(500).json({ success: false, message: 'Cashfree credentials not configured' });
    }

    const orderData = {
      order_id: orderId,
      order_amount: orderAmount,
      order_currency: 'INR',
      customer_details: {
        customer_id: customerId,
        customer_name: customerName,
        customer_email: customerEmail,
        customer_phone: customerPhone
      },
      order_meta: {
        return_url: `${baseUrl}/api/v1/cashfree/callback?order_id=${orderId}`,
        notify_url: `${baseUrl}/api/v1/cashfree/webhook`
      },
      order_tags: planKey ? { plan: String(planKey).toLowerCase(), client_id: customerId } : { client_id: customerId }
    };

    const headers = {
      'x-client-id': CLIENT_ID,
      'x-client-secret': CLIENT_SECRET,
      'x-api-version': '2023-08-01',
      'Accept': 'application/json',
      'Content-Type': 'application/json'
    };

    const cfResp = await axios.post(`${BASE_URL}/pg/orders`, orderData, { headers });
    const result = cfResp.data || {};

    if (!result.payment_session_id) {
      return res.status(400).json({ success: false, message: result.message || 'Failed to create order', details: result });
    }

    // Save payment record
    try {
      const Payment = require('./models/Payment');
      await Payment.create({
        clientId,
        orderId,
        planKey: String(planKey || '').toLowerCase(),
        amount: orderAmount,
        email: customerEmail,
        phone: customerPhone,
        status: 'INITIATED',
        gateway: 'cashfree',
        sessionId: result.payment_session_id,
        orderStatus: result.order_status,
        rawResponse: result
      });
    } catch (e) {
      console.error('Payment save failed (non-fatal):', e.message);
    }

    return res.json({
      success: true,
      order_id: result.order_id,
      payment_session_id: result.payment_session_id,
      order_status: result.order_status
    });
  } catch (error) {
    console.error('Create-order error:', error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({ success: false, message: error.response?.data?.message || error.message });
  }
});

// New: Cashfree verify-payment API (mirrors tested backend behavior)
app.post('/api/v1/payments/cashfree/verify-payment', async (req, res) => {
  try {
    const { order_id } = req.body || {};
    if (!order_id) {
      return res.status(400).json({ success: false, message: 'Order ID is required' });
    }

    const { BASE_URL, CLIENT_ID, CLIENT_SECRET } = require('./config/cashfree');
    const headers = {
      'x-client-id': CLIENT_ID,
      'x-client-secret': CLIENT_SECRET,
      'x-api-version': '2023-08-01',
      'Accept': 'application/json'
    };

    const resp = await axios.get(`${BASE_URL}/pg/orders/${order_id}`, { headers });
    const data = resp.data || {};

    return res.json({
      success: true,
      order_status: data.order_status,
      payment_status: data.order_status === 'PAID' ? 'SUCCESS' : data.order_status,
      order_id: data.order_id,
      order_amount: data.order_amount
    });
  } catch (error) {
    console.error('Verify-payment error:', error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({ success: false, message: error.response?.data?.message || error.message });
  }
});

// Verify and credit: verifies payment and adds credits to the client account
app.options('/api/v1/payments/cashfree/verify-and-credit', cors({
  origin: [process.env.FRONTEND_URL || 'http://localhost:5173', 'https://www.aitota.com'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  methods: ['POST', 'OPTIONS']
}));
app.post('/api/v1/payments/cashfree/verify-and-credit', cors({
  origin: [process.env.FRONTEND_URL || 'http://localhost:5173', 'https://www.aitota.com'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  methods: ['POST', 'OPTIONS']
}), express.json(), async (req, res) => {
  try {
    const { order_id } = req.body || {};
    if (!order_id) return res.status(400).json({ success: false, message: 'Order ID is required' });

    // Auth
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return res.status(401).json({ success: false, message: 'Authorization token required' });
    const jwt = require('jsonwebtoken');
    let clientId;
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      clientId = decoded.id || decoded.clientId;
    } catch (e) {
      return res.status(401).json({ success: false, message: 'Invalid authorization token' });
    }

    // Verify with Cashfree
    const { BASE_URL, CLIENT_ID, CLIENT_SECRET } = require('./config/cashfree');
    const headers = {
      'x-client-id': CLIENT_ID,
      'x-client-secret': CLIENT_SECRET,
      'x-api-version': '2023-08-01',
      'Accept': 'application/json'
    };
    const resp = await axios.get(`${BASE_URL}/pg/orders/${order_id}`, { headers });
    const data = resp.data || {};
    const isPaid = data.order_status === 'PAID';

    // Update payment doc
    const Payment = require('./models/Payment');
    const paymentDoc = await Payment.findOneAndUpdate(
      { orderId: order_id, clientId },
      { status: isPaid ? 'SUCCESS' : (data.order_status || 'FAILED'), transactionId: data.cf_payment_id || data.reference_id, rawVerification: data },
      { new: true }
    );

    if (!isPaid) {
      return res.json({ success: true, order_status: data.order_status, credited: false });
    }

    // Add credits if not already credited
    if (paymentDoc && !paymentDoc.credited) {
      try {
        const Credit = require('./models/Credit');
        const mapping = { basic: 1000, professional: 5500, enterprise: 11000 };
        const key = (paymentDoc.planKey || '').toLowerCase();
        const creditsToAdd = mapping[key] || 0;
        if (creditsToAdd > 0) {
          const creditRecord = await Credit.getOrCreateCreditRecord(clientId);
          const txn = data.cf_payment_id || data.reference_id;
          await creditRecord.addCredits(creditsToAdd, 'purchase', `Cashfree ${key} • order ${order_id} • tx ${txn}`, null, txn);
          await Payment.findOneAndUpdate({ orderId: order_id }, { credited: true, creditsAdded: creditsToAdd });
        }
      } catch (e) {
        console.error('Credit add failed:', e.message);
      }
    }

    // Return latest balance
    try {
      const Credit = require('./models/Credit');
      const creditRecord = await Credit.getOrCreateCreditRecord(clientId);
      return res.json({ success: true, order_status: 'PAID', credited: true, currentBalance: creditRecord.currentBalance });
    } catch {
      return res.json({ success: true, order_status: 'PAID', credited: true });
    }
  } catch (error) {
    console.error('Verify-and-credit error:', error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({ success: false, message: error.response?.data?.message || error.message });
  }
});

// Enhanced Cashfree callback handler with better error handling
app.get('/api/v1/cashfree/callback', async (req, res) => {
  try {
    const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
    const SUCCESS_PATH = process.env.PAYMENT_SUCCESS_PATH || '/auth/dashboard';
    const { order_id, order_token, payment_status, cf_payment_id } = req.query || {};
    
    if (!order_id) {
      console.error('❌ Callback missing order_id');
      return res.redirect(`${FRONTEND_URL}${SUCCESS_PATH}?status=FAILED&error=Missing order ID`);
    }

    console.log('🔔 Cashfree callback received:', { order_id, payment_status, cf_payment_id });

    // Verify payment status with Cashfree API
    let status = 'FAILED';
    let transactionId = cf_payment_id;
    
    try {
      const { BASE_URL, CLIENT_ID, CLIENT_SECRET } = require('./config/cashfree');
      const axios = require('axios');
      const headers = {
        'x-client-id': CLIENT_ID,
        'x-client-secret': CLIENT_SECRET,
        'x-api-version': '2022-09-01'
      };
      
      const resp = await axios.get(`${BASE_URL}/pg/orders/${order_id}`, { headers });
      const data = resp.data || {};
      
      // Determine status based on Cashfree response
      if (data.order_status === 'PAID' || data.payment_status === 'SUCCESS') {
        status = 'SUCCESS';
      } else if (data.order_status === 'ACTIVE' || data.payment_status === 'PENDING') {
        status = 'PENDING';
      } else {
        status = data.order_status || data.payment_status || 'FAILED';
      }
      
      transactionId = data.cf_payment_id || data.reference_id || cf_payment_id;
      
      console.log('✅ Payment status verified:', { status, transactionId, orderStatus: data.order_status });
      
    } catch (e) {
      console.error('❌ Cashfree status fetch failed:', e.message);
      // Fallback to query params if API call fails
      if (payment_status === 'SUCCESS') {
        status = 'SUCCESS';
      }
    }

    // Update payment record
    let paymentDoc = null;
    try {
      const Payment = require('./models/Payment');
      paymentDoc = await Payment.findOneAndUpdate(
        { orderId: order_id },
        { 
          status, 
          transactionId, 
          rawCallback: req.query,
          updatedAt: new Date()
        },
        { new: true }
      );
      
      if (paymentDoc) {
        console.log('✅ Payment record updated:', { orderId: order_id, status, transactionId });
      } else {
        console.error('❌ Payment record not found for order:', order_id);
      }
      
    } catch (e) {
      console.error('❌ Payment update failed:', e.message);
    }

    // Auto-credit if success
    if (paymentDoc && status === 'SUCCESS' && !paymentDoc.credited) {
      try {
        const Credit = require('./models/Credit');
        const mapping = { basic: 1000, professional: 5500, enterprise: 11000 };
        const key = (paymentDoc.planKey || '').toLowerCase();
        const creditsToAdd = mapping[key] || 0;
        
        if (creditsToAdd > 0 && paymentDoc.clientId) {
          const creditRecord = await Credit.getOrCreateCreditRecord(paymentDoc.clientId);
          await creditRecord.addCredits(creditsToAdd, 'purchase', `Cashfree ${key} • order ${order_id} • tx ${transactionId}`, null, transactionId);
          
          // Mark payment as credited
          await Payment.findOneAndUpdate(
            { orderId: order_id }, 
            { credited: true, creditsAdded: creditsToAdd }
          );
          
          console.log('✅ Credits added successfully:', { clientId: paymentDoc.clientId, creditsAdded: creditsToAdd });
        }
      } catch (e) {
        console.error('❌ Auto-credit failed:', e.message);
      }
    }

    // Redirect to frontend with status
    const redirectUrl = `${FRONTEND_URL}${SUCCESS_PATH}?orderId=${encodeURIComponent(order_id)}&status=${encodeURIComponent(status)}&transactionId=${encodeURIComponent(transactionId || '')}`;
    console.log('🔄 Redirecting to frontend:', redirectUrl);
    
    return res.redirect(redirectUrl);
    
  } catch (e) {
    console.error('❌ Cashfree callback error:', e.message);
    const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
    const SUCCESS_PATH = process.env.PAYMENT_SUCCESS_PATH || '/auth/dashboard';
    res.redirect(`${FRONTEND_URL}${SUCCESS_PATH}?status=FAILED&error=${encodeURIComponent(e.message)}`);
  }
});

// Payment status check endpoint
app.get('/api/v1/client/payments/status/:orderId', async (req, res) => {
  try {
    const { orderId } = req.params;
    const token = req.headers.authorization?.replace('Bearer ', '');
    
    if (!token) {
      return res.status(401).json({
        success: false,
        message: 'Authorization token required'
      });
    }

    // Verify token and get clientId
    const jwt = require('jsonwebtoken');
    let clientId;
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      clientId = decoded.id || decoded.clientId;
    } catch (e) {
      return res.status(401).json({
        success: false,
        message: 'Invalid authorization token'
      });
    }

    const Payment = require('./models/Payment');
    const payment = await Payment.findOne({ orderId, clientId });
    
    if (!payment) {
      return res.status(404).json({
        success: false,
        message: 'Payment not found'
      });
    }

    res.json({
      success: true,
      data: {
        orderId: payment.orderId,
        status: payment.status,
        amount: payment.amount,
        planKey: payment.planKey,
        gateway: payment.gateway,
        credited: payment.credited,
        creditsAdded: payment.creditsAdded,
        createdAt: payment.createdAt,
        transactionId: payment.transactionId
      }
    });

  } catch (error) {
    console.error('❌ Payment status check failed:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to check payment status',
      error: error.message
    });
  }
});
app.get('/api/v1/cashfree/callback', async (req, res) => {
  try {
    const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
    const SUCCESS_PATH = process.env.PAYMENT_SUCCESS_PATH || '/auth/dashboard';
    const { order_id, order_token } = req.query || {};
    if (!order_id) return res.redirect(`${FRONTEND_URL}${SUCCESS_PATH}?status=FAILED`);

    // Verify payment status with Cashfree
    const { BASE_URL, CLIENT_ID, CLIENT_SECRET } = require('./config/cashfree');
    const axios = require('axios');
    const headers = {
      'x-client-id': CLIENT_ID,
      'x-client-secret': CLIENT_SECRET,
      'x-api-version': '2022-09-01'
    };
    let status = 'FAILED';
    let transactionId = undefined;
    try {
      const resp = await axios.get(`${BASE_URL}/pg/orders/${order_id}`, { headers });
      const data = resp.data || {};
      status = (data.order_status === 'PAID') ? 'SUCCESS' : data.order_status || 'FAILED';
      transactionId = data.cf_payment_id || data.reference_id;
    } catch (e) {
      console.error('Cashfree status fetch failed:', e.message);
    }

    // Update payment record
    let paymentDoc = null;
    try {
      const Payment = require('./models/Payment');
      paymentDoc = await Payment.findOneAndUpdate(
        { orderId: order_id },
        { status, transactionId, rawCallback: req.query },
        { new: true }
      );
    } catch (e) {
      console.error('Payment update failed:', e.message);
    }

    // Auto-credit if success
    if (paymentDoc && status === 'SUCCESS' && !paymentDoc.credited) {
      try {
        const Credit = require('./models/Credit');
        const mapping = { basic: 1000, professional: 5500, enterprise: 11000 };
        const key = (paymentDoc.planKey || '').toLowerCase();
        const creditsToAdd = mapping[key] || 0;
        if (creditsToAdd > 0 && paymentDoc.clientId) {
          const creditRecord = await Credit.getOrCreateCreditRecord(paymentDoc.clientId);
          await creditRecord.addCredits(creditsToAdd, 'purchase', `Cashfree ${key} • order ${order_id} • tx ${transactionId}`, null, transactionId);
          const Payment = require('./models/Payment');
          await Payment.findOneAndUpdate({ orderId: order_id }, { credited: true, creditsAdded: creditsToAdd });
        }
      } catch (e) {
        console.error('Auto-credit on Cashfree failed:', e.message);
      }
    }

    return res.redirect(`${FRONTEND_URL}${SUCCESS_PATH}?orderId=${encodeURIComponent(order_id)}&status=${encodeURIComponent(status)}`);
  } catch (e) {
    console.error('Cashfree callback error:', e.message);
    res.status(200).send('OK');
  }
});

dotenv.config();

// Increase payload size limit to handle audio data
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

app.use(cors());

// Initialize WebSocket server
const wsServer = new VoiceChatWebSocketServer(server);

app.get('/', (req,res)=>{
    res.send("hello world")
})

// WebSocket server status endpoint
app.get('/ws/status', (req, res) => {
    const status = wsServer.getConnectionInfo();
    res.json({
        success: true,
        data: status
    });
});

// Paytm callback handler - redirects to frontend with orderId/status
app.post('/api/v1/paytm/callback', async (req, res) => {
  try {
    const FRONTEND_URL = process.env.FRONTEND_URL || 'http://localhost:5173';
    const SUCCESS_PATH = process.env.PAYMENT_SUCCESS_PATH || '/auth/dashboard';
    const body = req.body || {};
    const orderId = body.ORDERID || body.orderId || '';
    const status = body.STATUS || body.status || 'SUCCESS';
    let paymentDoc = null;
    try {
      const Payment = require('./models/Payment');
      paymentDoc = await Payment.findOneAndUpdate(
        { orderId },
        {
          status: (status === 'TXN_SUCCESS' || status === 'SUCCESS') ? 'SUCCESS' : (status || 'FAILED'),
          transactionId: body.TXNID || body.transactionId,
          responseCode: body.RESPCODE || body.responseCode,
          responseMsg: body.RESPMSG || body.responseMsg,
          rawCallback: body,
        },
        { new: true }
      );
    } catch (e) {
      console.error('Failed to upsert Payment from callback:', e.message);
    }

    // Auto-credit on SUCCESS
    if (paymentDoc && (paymentDoc.status === 'TXN_SUCCESS' || paymentDoc.status === 'SUCCESS' || status === 'TXN_SUCCESS' || status === 'SUCCESS')) {
      try {
        if (!paymentDoc.credited) {
          const Credit = require('./models/Credit');
          const mapping = {
            basic: 1000,
            professional: 5500,
            enterprise: 11000,
          };
          const key = (paymentDoc.planKey || '').toLowerCase();
          const creditsToAdd = mapping[key] || 0;
          if (creditsToAdd > 0 && paymentDoc.clientId) {
            const creditRecord = await Credit.getOrCreateCreditRecord(paymentDoc.clientId);
            // Idempotent by using orderId as transactionId in history if your addCredits supports metadata
            await creditRecord.addCredits(creditsToAdd, 'purchase', `Paytm order ${orderId} • ${key} plan`, {
              gateway: 'paytm',
              orderId,
              transactionId: body.TXNID || paymentDoc.transactionId,
            });
            const Payment = require('./models/Payment');
            await Payment.findOneAndUpdate(
              { orderId },
              { credited: true, creditsAdded: creditsToAdd }
            );
          }
        }
      } catch (e) {
        console.error('Auto-credit after callback failed:', e.message);
      }
    }
    const redirect = `${FRONTEND_URL}${SUCCESS_PATH}?orderId=${encodeURIComponent(orderId)}&status=${encodeURIComponent(status)}`;
    return res.redirect(302, redirect);
  } catch (e) {
    console.error('Paytm callback error:', e.message);
    res.status(200).send('OK');
  }
});

// Call Logs APIs
app.get("/api/v1/logs", async (req, res) => {
  try {
    const {
      clientId,
      limit = 50,
      page = 1,
      leadStatus,
      isActive,
      sortBy = 'createdAt',
      sortOrder = 'desc',
      customField,
      customValue,
      customContains,
      uniqueid,
      mobile,
      agentId,
    } = req.query;

    const filters = {};
    if (clientId) filters.clientId = clientId;
    if (leadStatus) filters.leadStatus = leadStatus;
    if (typeof isActive !== 'undefined') filters['metadata.isActive'] = isActive === 'true';
    if (mobile) filters.mobile = mobile;
    if (agentId) filters.agentId = agentId;
    if (uniqueid) filters['metadata.customParams.uniqueid'] = uniqueid;

    if (customField && (customValue || customContains)) {
      const path = `metadata.customParams.${customField}`;
      if (customContains) {
        filters[path] = { $regex: customContains, $options: 'i' };
      } else {
        filters[path] = customValue;
      }
    }

    const sort = {};
    sort[sortBy] = sortOrder === 'desc' ? -1 : 1;
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const CallLog = require("./models/CallLog");

    const [logs, totalCount, activeCount, clientIds] = await Promise.all([
      CallLog.find(filters).sort(sort).limit(parseInt(limit)).skip(skip).lean().exec(),
      CallLog.countDocuments(filters),
      CallLog.countDocuments({ ...filters, 'metadata.isActive': true }),
      CallLog.distinct('clientId', {})
    ]);

    const response = {
      logs,
      pagination: {
        total: totalCount,
        page: parseInt(page),
        limit: parseInt(limit),
        pages: Math.ceil(totalCount / parseInt(limit)),
      },
      stats: {
        total: totalCount,
        active: activeCount,
        clients: clientIds.length,
        timestamp: new Date().toISOString(),
      },
      filters: {
        clientId,
        leadStatus,
        isActive,
        customField,
        customValue,
        customContains,
        uniqueid,
        mobile,
        agentId,
        availableClients: clientIds.sort(),
      }
    };

    res.json(response);
  } catch (error) {
    console.error("❌ [LOGS-API] Error fetching logs:", error.message);
    res.status(500).json({
      error: "Failed to fetch logs",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Get most recent active call log for quick polling
app.get('/api/v1/logs/active', async (req, res) => {
  try {
    const { clientId, mobile, agentId, limit = 1 } = req.query;
    const CallLog = require('./models/CallLog');

    const filters = { 'metadata.isActive': true };
    if (clientId) filters.clientId = clientId;
    if (mobile) filters.mobile = mobile;
    if (agentId) filters.agentId = agentId;

    const logs = await CallLog.find(filters)
      .sort({ 'metadata.lastUpdated': -1 })
      .limit(parseInt(limit))
      .lean()
      .exec();

    res.json({ logs, timestamp: new Date().toISOString() });
  } catch (error) {
    console.error('❌ [LOGS-ACTIVE] Error fetching active logs:', error.message);
    res.status(500).json({ error: 'Failed to fetch active logs', message: error.message });
  }
});

// Get specific call log by ID
app.get("/api/v1/logs/:id", async (req, res) => {
  try {
    const { id } = req.params;
    const CallLog = require("./models/CallLog");
    const log = await CallLog.findById(id).lean();
    if (!log) {
      return res.status(404).json({
        error: "Call log not found",
        id: id,
        timestamp: new Date().toISOString(),
      });
    }
    res.json({ log, timestamp: new Date().toISOString() });
  } catch (error) {
    console.error("❌ [LOGS-API] Error fetching log:", error.message);
    res.status(500).json({
      error: "Failed to fetch log",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Get live statistic
app.get("/api/v1/logs/stats", async (req, res) => {
  try {
    const CallLog = require("./models/CallLog");
    const [totalCalls, activeCalls, todaysCalls, statusBreakdown, clientBreakdown] = await Promise.all([
      CallLog.countDocuments(),
      CallLog.countDocuments({ 'metadata.isActive': true }),
      CallLog.countDocuments({ createdAt: { $gte: new Date(new Date().setHours(0, 0, 0, 0)) } }),
      CallLog.aggregate([{ $group: { _id: "$leadStatus", count: { $sum: 1 } } }]),
      CallLog.aggregate([
        { $group: { _id: "$clientId", count: { $sum: 1 }, activeCalls: { $sum: { $cond: ["$metadata.isActive", 1, 0] } } } },
        { $sort: { count: -1 } },
        { $limit: 10 }
      ])
    ]);

    const wsStatus = wsServer.getConnectionInfo ? wsServer.getConnectionInfo() : {};

    const stats = {
      overview: {
        total: totalCalls,
        active: activeCalls,
        today: todaysCalls,
        timestamp: new Date().toISOString(),
      },
      statusBreakdown: statusBreakdown.reduce((acc, item) => {
        acc[item._id] = item.count;
        return acc;
      }, {}),
      topClients: clientBreakdown,
      server: wsStatus,
    };

    res.json(stats);
  } catch (error) {
    console.error("❌ [LOGS-STATS] Error generating stats:", error.message);
    res.status(500).json({
      error: "Failed to generate statistics",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Clean up stale active calls (utility endpoint)
app.post("/api/v1/logs/cleanup", async (req, res) => {
  try {
    const CallLog = require("./models/CallLog");
    const result = await CallLog.cleanupStaleActiveCalls();
    res.json({ message: "Cleanup completed", modifiedCount: result.modifiedCount, timestamp: new Date().toISOString() });
  } catch (error) {
    console.error("❌ [LOGS-CLEANUP] Error during cleanup:", error.message);
    res.status(500).json({
      error: "Failed to cleanup stale calls",
      message: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

app.post('/api/v1/client/proxy/clicktobot', async (req, res) => {
    try {
      const { apiKey, payload } = req.body;
      console.log(req.body)
      
      const response = await axios.post(
        'https://3neysomt18.execute-api.us-east-1.amazonaws.com/dev/clicktobot',
        payload,
        {
          headers: {
            'X-CLIENT': 'czobd',
            'X-API-KEY': apiKey,
            'Content-Type': 'application/json',
          },
        }
      );
  
      res.json({
        success: true,
        data: response.data
      });
    } catch (error) {
      console.error('Proxy error:', error.response?.data || error.message);
      res.status(500).json({
        success: false,
        error: error.response?.data || error.message
      });
    }
  });

app.use('/api/campaigns', campaignRoutes);
app.use('/api/v1/superadmin',superadminRoutes);
app.use('/api/v1/admin',adminRoutes);
app.use('/api/v1/client',clientRoutes);
// app.use('/api/v1/human-agent', humanAgentRoutes);
app.use('/api/v1/auth/client/profile', profileRoutes);
app.use('/api/v1/chat', chatRoutes);
app.use('/api/v1/templates', templateRoutes);
app.use('/api/v1/agent-access', agentAccessRoutes);
app.use('/makecall', makecallRoutes);
app.use('/api/v1/stt', sttRoutes);
app.use('/api/v1/dispositions', dispositionRoutes);
app.use('/api/v1/client/series-campaign', seriesCampaignRoutes);

// Public API endpoint for business details (no authentication required)
app.get('/api/v1/public/business/:identifier', async (req, res) => {
  try {
    const { identifier } = req.params;
    
    // Check if identifier is a hash (8 characters) or ObjectId (24 characters)
    const isHash = /^[a-f0-9]{8}$/.test(identifier);
    const isObjectId = /^[a-f0-9]{24}$/.test(identifier);
    
    if (!isHash && !isObjectId) {
      return res.status(400).json({
        success: false,
        message: 'Invalid business identifier format'
      });
    }

    // Find business by hash or ObjectId
    let business;
    if (isHash) {
      business = await Business.findOne({ hash: identifier });
    } else {
      business = await Business.findById(identifier);
    }
    
    if (!business) {
      return res.status(404).json({
        success: false,
        message: 'Business not found'
      });
    }

    // Generate fresh URLs for images using getobject
    const { getobject } = require('./utils/s3');
    let imageWithUrl = business.image;
    let documentsWithUrl = business.documents;

    try {
      // Generate fresh URL for image
      if (business.image && business.image.key) {
        const imageUrl = await getobject(business.image.key);
        imageWithUrl = { ...business.image, url: imageUrl };
      }
      
      // Generate fresh URL for documents if provided
      if (business.documents && business.documents.key) {
        const documentsUrl = await getobject(business.documents.key);
        documentsWithUrl = { ...business.documents, url: documentsUrl };
      }
    } catch (s3Error) {
      console.error('Error generating S3 URLs:', s3Error);
      // Keep original URLs if S3 fails
      imageWithUrl = business.image;
      documentsWithUrl = business.documents;
    }

    // Return business details (excluding sensitive information)
    res.json({
      success: true,
      data: {
        _id: business._id,
        title: business.title,
        category: business.category,
        type: business.type,
        image: imageWithUrl,
        documents: documentsWithUrl,
        videoLink: business.videoLink,
        link: business.link,
        description: business.description,
        mrp: business.mrp,
        offerPrice: business.offerPrice,
        createdAt: business.createdAt,
        updatedAt: business.updatedAt
      }
    });
  } catch (error) {
    console.error('Error fetching public business details:', error);
    res.status(500).json({
      success: false,
      message: 'Internal server error'
    });
  }
});

// Manual fix stuck calls endpoint (for debugging)
app.post("/api/v1/debug/fix-stuck-calls", async (req, res) => {
  try {
    const { fixStuckCalls } = require('./services/campaignCallingService');
    await fixStuckCalls();
    res.json({ success: true, message: 'Stuck calls fixed successfully' });
  } catch (error) {
    console.error('❌ Error fixing stuck calls:', error);
    res.status(500).json({ error: 'Failed to fix stuck calls', message: error.message });
  }
});

// Manual fix specific stuck call endpoint (for debugging)
app.post("/api/v1/debug/fix-specific-call", async (req, res) => {
  try {
    const { uniqueId } = req.body;
    if (!uniqueId) {
      return res.status(400).json({ error: 'uniqueId is required' });
    }
    
    const CallLog = require('./models/CallLog');
    const Campaign = require('./models/Campaign');
    
    console.log(`🔧 MANUAL: Fixing specific stuck call: ${uniqueId}`);
    
    // Find the CallLog
    const callLog = await CallLog.findOne({
      'metadata.customParams.uniqueid': uniqueId
    });
    
    if (!callLog) {
      return res.status(404).json({ error: 'CallLog not found' });
    }
    
    // Update CallLog to mark as inactive
    await CallLog.findByIdAndUpdate(callLog._id, {
      'metadata.isActive': false,
      'metadata.callEndTime': new Date(),
      leadStatus: 'not_connected'
    });
    // Deduct credits for completed call if possible
    try {
      const { deductCreditsForCall } = require('./services/creditUsageService');
      const uniqueId = callLog?.metadata?.customParams?.uniqueid;
      const clientId = callLog?.clientId;
      if (clientId && uniqueId) {
        await deductCreditsForCall({ clientId, uniqueId });
      }
    } catch (e) {
      console.error('Credit deduction failed:', e.message);
    }
    
    // Find and update campaign details
    const campaigns = await Campaign.find({
      'details.uniqueId': uniqueId
    });
    
    let updatedCampaigns = 0;
    for (const campaign of campaigns) {
      const callDetail = campaign.details.find(d => d.uniqueId === uniqueId);
      if (callDetail && callDetail.status !== 'completed') {
        callDetail.status = 'completed';
        callDetail.lastStatusUpdate = new Date();
        callDetail.callDuration = Math.floor((new Date() - callDetail.time) / 1000);
        await campaign.save();
        updatedCampaigns++;
      }
    }
    
    res.json({ 
      success: true, 
      message: `Fixed stuck call ${uniqueId}`,
      updatedCampaigns,
      callLogId: callLog._id
    });
    
  } catch (error) {
    console.error('❌ Error fixing specific stuck call:', error);
    res.status(500).json({ error: 'Failed to fix stuck call', message: error.message });
  }
});

// Agent configuration CRUD (admin)
app.get('/api/v1/admin/agent-config', async (req, res) => {
  try {
    const AgentConfig = require('./models/AgentConfig');
    const { agentId } = req.query;
    const filter = agentId ? { agentId } : {};
    const rows = await AgentConfig.find(filter).lean();
    res.json({ success: true, data: rows });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// Get agent configuration mode for campaigns (client access)
app.get('/api/v1/client/agent-config/:agentId', async (req, res) => {
  try {
    const AgentConfig = require('./models/AgentConfig');
    const { agentId } = req.params;
    
    console.log(`🔧 BACKEND: Fetching agent config for agentId: ${agentId}`);
    
    if (!agentId) {
      return res.status(400).json({ success: false, message: 'Agent ID is required' });
    }

    const config = await AgentConfig.findOne({ agentId }).lean();
    console.log(`🔧 BACKEND: Found config:`, config);
    
    if (!config) {
      // Return default serial mode if no config exists
      console.log(`🔧 BACKEND: No config found, returning default serial mode`);
      return res.json({ 
        success: true, 
        data: { 
          agentId, 
          mode: 'serial', 
          items: [],
          isDefault: true 
        } 
      });
    }

    console.log(`🔧 BACKEND: Returning config with mode: ${config.mode}`);
    res.json({ success: true, data: config });
  } catch (e) {
    console.error(`🔧 BACKEND: Error fetching agent config:`, e);
    res.status(500).json({ success: false, message: e.message });
  }
});

app.post('/api/v1/admin/agent-config', async (req, res) => {
  try {
    const AgentConfig = require('./models/AgentConfig');
    const Agent = require('./models/Agent');
    const { agentId, items, mode } = req.body || {};
    if (!agentId) return res.status(400).json({ success: false, message: 'agentId is required' });
    const agent = await Agent.findById(agentId).lean();
    // Enforce single default
    const normalizedItems = Array.isArray(items) ? items.map((it, idx) => ({ n: 1, g: Number(it.g)||1, rSec: Number(it.rSec)||5, isDefault: !!it.isDefault })) : [];
    if (normalizedItems.length) {
      let foundDefault = false;
      for (let i = 0; i < normalizedItems.length; i++) {
        if (normalizedItems[i].isDefault) {
          if (!foundDefault) {
            foundDefault = true;
          } else {
            normalizedItems[i].isDefault = false;
          }
        }
      }
      if (!foundDefault) normalizedItems[0].isDefault = true;
    }
    const payload = {
      agentId,
      agentName: agent?.agentName || '',
      didNumber: agent?.didNumber || '',
      mode: (mode === 'serial' ? 'serial' : 'parallel'),
      items: normalizedItems
    };
    const doc = await AgentConfig.findOneAndUpdate({ agentId }, payload, { upsert: true, new: true, setDefaultsOnInsert: true });
    res.json({ success: true, data: doc });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.put('/api/v1/admin/agent-config/:id', async (req, res) => {
  try {
    const AgentConfig = require('./models/AgentConfig');
    const { id } = req.params;
    const { items, mode } = req.body || {};
    const normalizedItems = Array.isArray(items) ? items.map((it) => ({ n: 1, g: Number(it.g)||1, rSec: Number(it.rSec)||5, isDefault: !!it.isDefault })) : [];
    if (normalizedItems.length) {
      let foundDefault = false;
      for (let i = 0; i < normalizedItems.length; i++) {
        if (normalizedItems[i].isDefault) {
          if (!foundDefault) {
            foundDefault = true;
          } else {
            normalizedItems[i].isDefault = false;
          }
        }
      }
      if (!foundDefault) normalizedItems[0].isDefault = true;
    }
    const doc = await AgentConfig.findByIdAndUpdate(id, { items: normalizedItems, ...(mode ? { mode: (mode === 'serial' ? 'serial' : 'parallel') } : {}) }, { new: true });
    if (!doc) return res.status(404).json({ success: false, message: 'Not found' });
    res.json({ success: true, data: doc });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

app.delete('/api/v1/admin/agent-config/:id', async (req, res) => {
  try {
    const AgentConfig = require('./models/AgentConfig');
    const { id } = req.params;
    const r = await AgentConfig.findByIdAndDelete(id);
    res.json({ success: true, deleted: !!r });
  } catch (e) {
    res.status(500).json({ success: false, message: e.message });
  }
});

// Single API for complete payment flow - frontend only sends amount and bearer token
app.post("/api/v1/payments/process", express.json(), async (req, res) => {
  try {
    const { amount, planKey } = req.body || {};

    // ✅ Validate amount
    const orderAmount = parseFloat(amount);
    if (!amount || isNaN(orderAmount) || orderAmount <= 0) {
      return res.status(400).json({
        success: false,
        message: "Valid amount is required",
      });
    }

    // ✅ Auth validation
    const token = req.headers.authorization?.replace("Bearer ", "");
    if (!token) {
      return res.status(401).json({
        success: false,
        message: "Authorization token required",
      });
    }

    // ✅ Verify JWT
    const jwt = require('jsonwebtoken');
    let clientId;
    try {
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      clientId = decoded.id || decoded.clientId;
      console.log(clientId)
    } catch {
      return res.status(401).json({
        success: false,
        message: "Invalid or expired authorization token",
      });
    }


    // ✅ Get client details
    const client = await Client.findById(clientId);
    if (!client) {
      return res.status(404).json({
        success: false,
        message: "Client not found",
      });
    }

    // ✅ Prepare customer details
    let customerName = client.name || client.username || client.email || "Customer";
    let customerEmail = client.email || `${clientId}@aitota.com`;
    let customerPhone = client.phone || client.mobile || client.mobileNo || "9999999999";
    try {
      const digits = String(customerPhone).replace(/\D/g, "");
      if (digits.length >= 10) customerPhone = digits.slice(-10);
    } catch {
      customerPhone = "9999999999";
    }

    // ✅ Generate order ID
    const orderId = `order_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const customerId = String(clientId);

    // ✅ Determine base URL
    const getBaseUrl = () => {
      const host = req.get("host") || "";
      if (
        host.includes(".onrender.com") ||
        host.includes(".vercel.app") ||
        host.includes(".herokuapp.com") ||
        process.env.NODE_ENV === "production"
      ) {
        return `https://${host}`;
      }
      const proto = req.headers["x-forwarded-proto"] || req.protocol || "http";
      return `${proto}://${host}`;
    };
    const baseUrl = getBaseUrl();

    // ✅ Check Cashfree credentials
    if (!CLIENT_ID || !CLIENT_SECRET || !BASE_URL) {
      return res.status(500).json({
        success: false,
        message: "Payment gateway not configured",
      });
    }
    console.log(CLIENT_ID)
    console.log(CLIENT_SECRET)

    // ✅ Create Cashfree order payload
    const orderData = {
      order_id: orderId,
      order_amount: orderAmount,
      order_currency: "INR",
      customer_details: {
        customer_id: customerId,
        customer_name: customerName,
        customer_email: customerEmail,
        customer_phone: customerPhone,
      },
      order_meta: {
        return_url: `${baseUrl}/api/v1/cashfree/callback?order_id=${orderId}`,
        notify_url: `${baseUrl}/api/v1/cashfree/webhook`,
      },
      order_tags: { client_id: customerId },
    };

    // ✅ Create order with Cashfree
    const headers = {
      "x-client-id": CLIENT_ID,
      "x-client-secret": CLIENT_SECRET,
      "x-api-version": "2023-08-01",
      Accept: "application/json",
      "Content-Type": "application/json",
    };

    const cfResp = await axios.post(`${BASE_URL}/pg/orders`, orderData, { headers });
    const result = cfResp.data || {};

    if (!result.payment_session_id) {
      return res.status(400).json({
        success: false,
        message: result.message || "Failed to create payment order",
      });
    }

    // ✅ Save payment record
    await Payment.create({
      clientId,
      orderId,
      planKey: planKey || "basic",
      amount: orderAmount,
      email: customerEmail,
      phone: customerPhone,
      status: "INITIATED",
      gateway: "cashfree",
      sessionId: result.payment_session_id,
      orderStatus: result.order_status,
      rawResponse: result,
    });
    console.log(result.payment_session_id)
    const cleanSessionId = result.payment_session_id.replace(/paymentpayment$/, "");

    // ✅ Return session ID & order details
    return res.json({
      success: true,
      message: "Order created successfully",
      orderId,
      orderToken: result.payment_session_id,
      orderAmount,
      customerName,
      customerEmail,
      customerPhone,
    });
  } catch (error) {
    console.error("💥 [PAYMENT-PROCESS] Error:", error.response?.data || error.message);
    return res.status(error.response?.status || 500).json({
      success: false,
      message: error.response?.data?.message || "Payment processing failed",
    });
  }
});

app.post('/api/v1/cashfree/webhook', express.json(), async (req, res) => {
  try {
    const event = req.body || {};
    console.log("📩 [CASHFREE-WEBHOOK]", JSON.stringify(event, null, 2));

    const { order_id, cf_payment_id, payment_status, order_amount } = event || {};

    if (!order_id || !payment_status) {
      return res.status(400).json({ success: false, message: "Invalid webhook data" });
    }

    // Find the payment record
    const Payment = require('./models/Payment');
    const payment = await Payment.findOne({ orderId: order_id });
    if (!payment) {
      return res.status(404).json({ success: false, message: "Payment record not found" });
    }

    // Update payment status
    payment.status = payment_status;
    payment.transactionId = cf_payment_id;
    payment.rawVerification = event;
    await payment.save();

    if (payment_status === "SUCCESS" && !payment.credited) {
      // Add credits (1 INR = 1 credit)
      const Credit = require('./models/Credit');
      const creditRecord = await Credit.getOrCreateCreditRecord(payment.clientId);

      await creditRecord.addCredits(
        Math.floor(order_amount),
        "purchase",
        `Payment • ${order_amount} credits • order ${order_id} • tx ${cf_payment_id}`,
        null,
        cf_payment_id
      );

      payment.credited = true;
      payment.creditsAdded = Math.floor(order_amount);
      await payment.save();

      console.log("✅ Credits added for order", order_id);
    }

    res.json({ success: true });

  } catch (err) {
    console.error("❌ Webhook error:", err.message);
    res.status(500).json({ success: false, message: "Server error" });
  }
});

const PORT = process.env.PORT || 4000;

connectDB().then(async () => {
    // Always run maintenance on startup
    try {
        // Optional: quick MSK IAM connectivity test (non-blocking)
        try {
            const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim()).filter(Boolean) : [];
            console.log('🧩 Kafka config:', { clientId: process.env.KAFKA_CLIENT_ID || 'demo-cluster-1', brokers });
          if (brokers.length) {
                console.log('🔎 Attempting Kafka TLS connect test (no SASL/IAM)...');
                const kafka = new Kafka({
                  clientId: process.env.KAFKA_CLIENT_ID || 'demo-cluster-1',
                  brokers,
                    ssl: true
              });
              const testProducer = kafka.producer();
              await testProducer.connect();
                console.log('✅ Kafka TLS: Connected successfully ✅');
      
              // ✅ Try sending a test message
              await testProducer.send({
                  topic: 'test-topic',
                  messages: [{ key: 'test', value: 'Kafka IAM connection successful!' }],
              });
                console.log('📨 Kafka test message sent successfully!');
      
              await testProducer.disconnect();
                console.log('🔌 Kafka TLS: Disconnected (test complete)');
          } else {
              console.warn('⚠️ Kafka test skipped: KAFKA_BROKER env not set');
          }
        } catch (e) {
            console.warn('⚠️ Kafka TLS connectivity test failed:', e && (e.stack || e.message || e));
        }
      
        const { fixStuckCalls, cleanupStaleActiveCalls, cleanupStuckCampaignsOnRestart } = require('./services/campaignCallingService');
        await fixStuckCalls();
        console.log('✅ SERVER RESTART: Stuck calls check completed');
        await cleanupStaleActiveCalls();
        console.log('✅ SERVER RESTART: Stale calls cleanup completed');
        await cleanupStuckCampaignsOnRestart();
        console.log('✅ SERVER RESTART: Stuck campaigns cleanup completed');
    } catch (error) {
        console.error('❌ SERVER RESTART: Error during stuck call check:', error);
    }
    
    // Start Kafka consumer for campaign commands
    try {
        const { startConsumer } = require('./services/campaignConsumer');
        await startConsumer();
        console.log('✅ Kafka consumer started for campaign processing');
    } catch (error) {
        console.error('❌ Failed to start Kafka consumer:', error);
        // Don't exit - server can still run without consumer
    }
    
    server.listen(PORT, () => {
        console.log(`🚀 Server is running on http://localhost:${PORT}`);
        console.log(`🔌 WebSocket server is ready on ws://localhost:${PORT}`);
        console.log(`📊 WebSocket status: http://localhost:${PORT}/ws/status`);
    });
}).catch(err => {
    console.error('❌ Database connection failed:', err);
    process.exit(1);
});

