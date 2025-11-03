const mongoose = require('mongoose');
const Campaign = require('../models/Campaign');

// Connect to MongoDB
mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/aitota', {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

async function fixCampaignValidation() {
  try {
    console.log('🔧 Starting campaign validation fix...');
    
    // Find all campaigns with details that might have validation issues
    const campaigns = await Campaign.find({
      'details.0': { $exists: true }
    });
    
    console.log(`📋 Found ${campaigns.length} campaigns with details`);
    
    let fixedCount = 0;
    
    for (const campaign of campaigns) {
      let needsUpdate = false;
      
      if (campaign.details && Array.isArray(campaign.details)) {
        for (let i = 0; i < campaign.details.length; i++) {
          const detail = campaign.details[i];
          
          // Fix missing fields
          if (!detail.lastStatusUpdate) {
            detail.lastStatusUpdate = detail.time || new Date();
            needsUpdate = true;
          }
          
          if (typeof detail.callDuration === 'undefined') {
            detail.callDuration = 0;
            needsUpdate = true;
          }
          
          // Ensure contactId is properly set (can be null)
          if (detail.contactId === undefined) {
            detail.contactId = null;
            needsUpdate = true;
          }
        }
      }
      
      if (needsUpdate) {
        await campaign.save();
        fixedCount++;
        console.log(`✅ Fixed campaign ${campaign._id}`);
      }
    }
    
    console.log(`✅ Migration completed! Fixed ${fixedCount} campaigns`);
    
  } catch (error) {
    console.error('❌ Error fixing campaign validation:', error);
  } finally {
    await mongoose.connection.close();
    console.log('🔌 Database connection closed');
  }
}

// Run the migration
fixCampaignValidation();
