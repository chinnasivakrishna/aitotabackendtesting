const express = require('express');
const router = express.Router();
const Agent = require('../models/Agent');

// Get agent dispositions by agentId
router.get('/agent/:agentId/dispositions', async (req, res) => {
  try {
    const { agentId } = req.params;
    
    console.log(`🔧 BACKEND: Fetching dispositions for agentId: ${agentId}`);
    
    if (!agentId) {
      return res.status(400).json({ success: false, message: 'Agent ID is required' });
    }

    const agent = await Agent.findById(agentId).select('depositions agentName').lean();
    
    if (!agent) {
      return res.status(404).json({ success: false, message: 'Agent not found' });
    }

    const dispositions = agent.depositions || [];
    
    console.log(`🔧 BACKEND: Found ${dispositions.length} dispositions for agent ${agent.agentName}`);
    
    res.json({ 
      success: true, 
      data: {
        agentId,
        agentName: agent.agentName,
        dispositions,
        hasDispositions: dispositions.length > 0
      }
    });
  } catch (error) {
    console.error(`🔧 BACKEND: Error fetching agent dispositions:`, error);
    res.status(500).json({ success: false, message: error.message });
  }
});

// Get all agents with dispositions for a client (admin access)
router.get('/client/:clientId/agents/dispositions', async (req, res) => {
  try {
    const { clientId } = req.params;
    
    console.log(`🔧 BACKEND: Fetching all agents with dispositions for clientId: ${clientId}`);
    
    if (!clientId) {
      return res.status(400).json({ success: false, message: 'Client ID is required' });
    }

    const agents = await Agent.find({ 
      clientId,
      isActive: true,
      'depositions.0': { $exists: true } // Only agents that have dispositions
    }).select('_id agentName depositions').lean();
    
    const agentsWithDispositions = agents.map(agent => ({
      agentId: agent._id,
      agentName: agent.agentName,
      dispositions: agent.depositions || [],
      dispositionCount: agent.depositions?.length || 0
    }));
    
    console.log(`🔧 BACKEND: Found ${agentsWithDispositions.length} agents with dispositions for client ${clientId}`);
    
    res.json({ 
      success: true, 
      data: {
        clientId,
        agents: agentsWithDispositions,
        totalAgentsWithDispositions: agentsWithDispositions.length
      }
    });
  } catch (error) {
    console.error(`🔧 BACKEND: Error fetching client agents with dispositions:`, error);
    res.status(500).json({ success: false, message: error.message });
  }
});

module.exports = router;









