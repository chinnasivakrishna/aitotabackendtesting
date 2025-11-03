const express=require('express');
const router=express.Router();

const TelegramServiceController=require('../controllers/telegrambotcontroller');

const telegramService = new TelegramServiceController();

// Test Telegram bot connection
router.post('/test-connection', async (req, res) => {
    try {
      const result = await telegramService.testConnection();
      if (result.success) {
        res.json({ success: true, message: result.message });
      } else {
        res.status(500).json({ success: false, error: result.error });
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  });



// Send only text message
router.post('/send-text', async (req, res) => {
    try {
      const { text } = req.body;
      
      if (!text) {
        return res.status(400).json({ 
          success: false, 
          error: 'Text message is required' 
        });
      }
  
      const result = await telegramService.sendTextMessage(text);
      
      if (result.success) {
        res.json({ success: true, message: result.message });
      } else {
        res.status(500).json({ success: false, error: result.error });
      }
    } catch (error) {
      res.status(500).json({ success: false, error: error.message });
    }
  });


  module.exports=router;