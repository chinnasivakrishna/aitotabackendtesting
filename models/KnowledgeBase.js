const mongoose = require("mongoose");

const knowledgeBaseSchema = new mongoose.Schema({
  // Reference to the agent
  agentId: { 
    type: mongoose.Schema.Types.ObjectId, 
    ref: 'Agent', 
    required: true,
    index: true 
  },
  
  // Reference to the client who owns this knowledge
  clientId: { 
    type: String, 
    required: true,
    index: true 
  },
  
  // Content type
  type: {
    type: String,
    enum: ['pdf', 'text', 'image', 'youtube', 'link', 'website'],
    required: true
  },
  
  // Title/name for the knowledge item
  title: { 
    type: String, 
    required: true,
    trim: true
  },
  
  // Description/notes about this knowledge item
  description: { 
    type: String,
    trim: true
  },
  
  // Content based on type
  content: {
    // For PDF files - S3 key
    s3Key: { type: String },
    
    // For text content
    text: { type: String },
    
    // For images - S3 key
    imageKey: { type: String },
    
    // For YouTube videos
    youtubeId: { type: String },
    youtubeUrl: { type: String },
    
    // For external links
    url: { type: String },
    linkText: { type: String }
  },
  
  // File metadata (for PDFs and images)
  fileMetadata: {
    originalName: { type: String },
    fileSize: { type: Number },
    mimeType: { type: String },
    uploadedAt: { type: Date, default: Date.now }
  },
  
  // Tags for categorization
  tags: [{ type: String, trim: true }],
  
  // Status
  isActive: { type: Boolean, default: true },
  // Embedding status
  isEmbedded: { type: Boolean, default: false, index: true },
  embeddedAt: { type: Date },
  embedMeta: {
    message: { type: String },
    processedChunks: { type: Number },
    totalBatches: { type: Number },
    totalLatency: { type: String },
    chunkingLatency: { type: String },
    embeddingLatency: { type: String }
  },
  
  // Timestamps
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

// Indexes for better query performance
knowledgeBaseSchema.index({ agentId: 1, type: 1 });
knowledgeBaseSchema.index({ clientId: 1, type: 1 });
knowledgeBaseSchema.index({ isActive: 1 });
knowledgeBaseSchema.index({ isEmbedded: 1 });

// Update the updatedAt field before saving
knowledgeBaseSchema.pre("save", function (next) {
  this.updatedAt = Date.now();
  next();
});

// Method to get the appropriate URL based on content type
knowledgeBaseSchema.methods.getContentUrl = async function() {
  const { getobject } = require('../utils/s3');
  
  switch (this.type) {
    case 'pdf':
    case 'image':
      if (this.content.s3Key) {
        try {
          return await getobject(this.content.s3Key);
        } catch (error) {
          console.error('Error generating S3 URL:', error);
          return null;
        }
      }
      return null;
      
    case 'youtube':
      return this.content.youtubeUrl || `https://www.youtube.com/watch?v=${this.content.youtubeId}`;
      
    case 'link':
      return this.content.url;
      
    default:
      return null;
  }
};

// Method to get display content based on type
knowledgeBaseSchema.methods.getDisplayContent = function() {
  switch (this.type) {
    case 'text':
      return this.content.text;
    case 'youtube':
      return this.content.youtubeUrl || `https://www.youtube.com/watch?v=${this.content.youtubeId}`;
    case 'link':
      return this.content.linkText || this.content.url;
    default:
      return this.title;
  }
};

// Static method to get knowledge by agent
knowledgeBaseSchema.statics.getByAgent = function(agentId, clientId) {
  return this.find({ 
    agentId, 
    clientId, 
    isActive: true 
  }).sort({ createdAt: -1 });
};

// Static method to get knowledge by type
knowledgeBaseSchema.statics.getByType = function(agentId, clientId, type) {
  return this.find({ 
    agentId, 
    clientId, 
    type, 
    isActive: true 
  }).sort({ createdAt: -1 });
};

module.exports = mongoose.model("KnowledgeBase", knowledgeBaseSchema);
