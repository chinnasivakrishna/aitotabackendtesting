const { Telegraf } = require('telegraf');
require('dotenv').config();

class TelegramServiceController {
  constructor() {
    this.bot = new Telegraf(process.env.TELEGRAMBOT_API_KEY);
    this.chatId = process.env.CHATID;
  }


  // Compress video if it's too large
  

  // Send text message to Telegram
  async sendTextMessage(text) {
    try {
      if (!this.chatId) {
        throw new Error('Chat ID not configured');
      }
      
      await this.bot.telegram.sendMessage(this.chatId, text, { parse_mode: 'HTML' });
      return { success: true, message: 'Text sent to Telegram successfully' };
    } catch (error) {
      console.error('Error sending text to Telegram:', error);
      return { success: false, error: error.message };
    }
  }


  // Test connection
  async testConnection() {
    try {
      if (!this.chatId) {
        throw new Error('Chat ID not configured');
      }
      
      await this.bot.telegram.sendMessage(this.chatId, '🤖 Bot connection test successful!');
      return { success: true, message: 'Telegram bot connection test successful' };
    } catch (error) {
      console.error('Telegram bot connection test failed:', error);
      return { success: false, error: error.message };
    }
  }
}

module.exports = TelegramServiceController;
