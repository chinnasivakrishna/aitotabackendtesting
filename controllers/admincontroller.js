const Admin = require("../models/Admin");
const SystemPrompt = require("../models/SystemPrompt");
const bcrypt=require("bcrypt");
const jwt = require('jsonwebtoken');
const Client = require("../models/Client");
const Agent = require("../models/Agent");
const { getobject } = require("../utils/s3");
const DidNumber = require("../models/DidNumber");
const Campaign = require("../models/Campaign");


// Generate JWT Token for admin
const generateAdminToken = (id) => {
  return jwt.sign(
    { 
      id,
      userType: 'admin' // Explicitly set userType
    }, 
    process.env.JWT_SECRET, 
    {
      expiresIn: '7d'
    }
  );
};

const loginAdmin = async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({
        success: false,
        message: "Email and password are required"
      });
    }

    // Check if admin exists
    const admin = await Admin.findOne({ email });
    if (!admin) {
      return res.status(401).json({
        success: false,
        message: "Invalid email or password"
      });
    }

    // Check if password matches
    const isPasswordValid = await bcrypt.compare(password, admin.password);
    if (!isPasswordValid) {
      return res.status(401).json({
        success: false,
        message: "Invalid email or password"
      });
    }

    // Generate token with userType
    const token = jwt.sign(
      { 
        id: admin._id,
        userType: 'admin'
      },
      process.env.JWT_SECRET,
      { expiresIn: "7d" }
    );

    res.status(200).json({
      success: true,
      token,
      admin: {
        _id: admin._id,
        name: admin.name,
        email: admin.email
      }
    });
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({
      success: false,
      message: error.message || "An error occurred during login"
    });
  }
};

const registerAdmin = async (req, res) => {
    try {
        const { name, email, password, admincode } = req.body;

        if(admincode!= process.env.ADMIN_REGISTRATION_CODE){
            console.log(admincode,process.env.ADMIN_REGISTRATION_CODE)
            return res.status(401).json({ message: 'Invalid admin code' });
        }

        const existingadmin=await Admin.findOne({email});
        if(existingadmin){
            return res.status(400).json({ message: 'Admin already exists' });
        }

         // Hash password before saving
         const salt = await bcrypt.genSalt(10);
         const hashedPassword = await bcrypt.hash(password, salt);
 
        const admin = await Admin.create({ name, email, password:hashedPassword });
        const token=generateAdminToken(admin._id);

        res.status(201).json({
            success: true,
            token,
            user: {
                _id: admin._id,
                name: admin.name,
                email: admin.email,
                password:hashedPassword,
                admincode:admin.admincode
            }
        }); 
       } 
    catch (error) {
        res.status(500).json({ message: error.message });
    }
    
}
const getClients = async (req, res) => {
    try {
      const minimal = String(req.query.minimal || '').toLowerCase() === '1' || String(req.query.minimal || '').toLowerCase() === 'true';
      const clients = await Client.find()
        .select(minimal
          ? '_id name businessName websiteUrl isApproved businessLogoUrl businessLogoKey'
          : '-password')
        .sort({createdAt:-1})
        .lean();

      if (minimal) {
        // Best-effort: ensure businessLogoUrl is present if businessLogoKey exists
        const withLogos = await Promise.all(
          clients.map(async (c) => {
            try {
              if (!c.businessLogoUrl && c.businessLogoKey) {
                c.businessLogoUrl = await getobject(c.businessLogoKey);
              }
            } catch (_) {}
            // Do not expose key in minimal response
            delete c.businessLogoKey;
            return c;
          })
        );
        return res.status(200).json({ success: true, count: withLogos.length, data: withLogos });
      }

      const clientsWithLogos = await Promise.all(
        clients.map(async (clientObj) => {
          try {
            if (clientObj.businessLogoKey) {
              clientObj.businessLogoUrl = await getobject(clientObj.businessLogoKey);
            }
          } catch (e) {
            clientObj.businessLogoUrl = clientObj.businessLogoUrl || null;
          }
          return clientObj;
        })
      );

      res.status(200).json({
        success: true,
        count: clientsWithLogos.length,
        data: clientsWithLogos
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: error.message
      });
    }
  };
  
  // Get client profile by ID
  const getClientById = async (req, res) => {
    try {
      const client = await Client.findById(req.params.id).select('-password');
      
      if (!client) {
        return res.status(404).json({
          success: false,
          message: "Client not found"
        });
      }
      
      const clientObj = client.toObject();
      try {
        if (clientObj.businessLogoKey) {
          clientObj.businessLogoUrl = await getobject(clientObj.businessLogoKey);
        }
      } catch (e) {
        clientObj.businessLogoUrl = clientObj.businessLogoUrl || null;
      }

      res.status(200).json({
        success: true,
        data: clientObj
      });
    } catch (error) {
      res.status(500).json({
        success: false,
        message: error.message
      });
    }
  };

  const registerclient = async (req, res) => {
    try {
      const {
        name,
        email,
        password,
        businessName,
        websiteUrl,
        city,
        pincode,
        gstNo,
        panNo,
        aadharNo
      } = req.body;
  
      // Check if client already exists
      const existingClient = await Client.findOne({ email });
      if (existingClient) {
        return res.status(400).json({
          success: false,
          message: "Client with this email already exists"
        });
      }
  
      // Hash password
      const hashedPassword = await bcrypt.hash(password, 10);
  
      // Create new client
      const client = await Client.create({
        name,
        email,
        password: hashedPassword,
        businessName,
        websiteUrl,
        city,
        pincode,
        gstNo,
        panNo,
        aadharNo
      });

      // Initialize default credits (100) for new client
      try {
        const Credit = require("../models/Credit");
        const creditRecord = await Credit.getOrCreateCreditRecord(client._id);
        if ((creditRecord?.currentBalance || 0) === 0) {
          await creditRecord.addCredits(100, 'bonus', 'Welcome bonus credits');
        }
      } catch (e) {
        console.error('Failed to initialize default credits for client:', e.message);
      }
  
      // Telegram alert: client created by admin panel
      try {
        const { sendTelegramAlert } = require('../utils/telegramAlert');
        const when = new Date().toLocaleString('en-IN', { hour12: false });
        await sendTelegramAlert(`Client "${client.name || client.businessName || client.email}" is joined on ${when}.`);
      } catch (_) {}

      // Remove password from response
      const clientResponse = client.toObject();
      delete clientResponse.password;
  
      res.status(201).json({
        success: true,
        message: "Client created successfully",
        data: clientResponse
      });
    } catch (error) {
      console.error('Error creating client:', error);
      res.status(500).json({
        success: false,
        message: "Failed to create client"
      });
    }
  };

  const updateClient = async (req, res) => {
    try {
      const { clientId } = req.params;
      if (!clientId) {
        return res.status(400).json({ success: false, message: 'Missing clientId' });
      }

      // Only set provided fields
      const {
        name,
        businessName,
        websiteUrl,
        city,
        pincode,
        gstNo,
        panNo,
        aadharNo,
        mobileNo,
        address,
        state,
      } = req.body || {};

      const update = {};
      if (name !== undefined) update.name = name;
      if (businessName !== undefined) update.businessName = businessName;
      if (websiteUrl !== undefined) update.websiteUrl = websiteUrl;
      if (city !== undefined) update.city = city;
      if (pincode !== undefined) update.pincode = pincode;
      if (gstNo !== undefined) update.gstNo = gstNo;
      if (panNo !== undefined) update.panNo = panNo;
      if (aadharNo !== undefined) update.aadharNo = aadharNo;
      if (mobileNo !== undefined) update.mobileNo = mobileNo;
      if (address !== undefined) update.address = address;
      if (state !== undefined) update.state = state;

      if (Object.keys(update).length === 0) {
        return res.status(400).json({ success: false, message: 'No fields to update' });
      }

      const client = await Client.findByIdAndUpdate(
        clientId,
        { $set: update },
        { new: true }
      );

      if (!client) {
        return res.status(404).json({ success: false, message: 'Client not found' });
      }
      res.status(200).json({ success: true, message: 'Client updated successfully', client });
    } catch (error) {
      console.error('Error updating client:', error);
      res.status(500).json({ success: false, message: 'Failed to update client', error: error.message });
    }
  };

  const deleteclient = async(req, res) => {
    try {
        const id = req.params.id;
        if (!id) {
            return res.status(400).json({
                success: false,
                message: "Client ID is required"
            });
        }
  
        const client = await Client.findByIdAndDelete(id);
        if (!client) {
            return res.status(404).json({
                success: false,
                message: "Client not found"
            });
        }
  
        res.status(200).json({
            success: true,
            message: "Client deleted successfully"
        });
    } catch (error) {
        console.error('Error deleting client:', error);
        res.status(500).json({
            success: false,
            message: "Failed to delete client"
        });
    }
  }

// Get client token for admin access
const getClientToken = async (req, res) => {
  try {
    const { clientId } = req.params;
    const adminId = req.user.id;    

    console.log('getClientToken called with:', {
      clientId,
      adminId,
      userType: req.user.userType
    });

    // Verify admin exists and is authenticated
    if (req.user.userType !== 'admin') {
      console.log('Invalid user type:', req.user.userType);
      return res.status(401).json({ message: 'Only admins can access client tokens' });
    }

    const admin = await Admin.findById(adminId);
    if (!admin) {
      console.log('Admin not found:', adminId);
      return res.status(401).json({ message: 'Admin not found' });
    }
    console.log('Admin verified:', admin.email);

    // Get client details
    const client = await Client.findById(clientId);
    if (!client) {
      console.log('Client not found:', clientId);
      return res.status(404).json({ message: 'Client not found' });
    }
    console.log('Client found:', client.email);

    // Generate token for client with admin access flag
    const token = jwt.sign(
      { 
        id: client._id,
        email: client.email,
        userType: 'client',
        adminAccess: true, // Flag to indicate this is admin-accessed client session
        adminId: adminId // Store admin ID for tracking
      },
      process.env.JWT_SECRET,
      { expiresIn: '1d' }
    );

    console.log('Generated client token for:', client.email);
    res.json({ token });
  } catch (error) {
    console.error('Error in getClientToken:', error);
    res.status(500).json({ message: 'Internal server error' });
  }
};

// DID Numbers CRUD and assignment
const listDidNumbers = async (req, res) => {
  try {
    const { provider } = req.query;
    const filter = {};
    if (provider) {
      filter.provider = provider;
    }
    const items = await DidNumber.find(filter).sort({ createdAt: -1 });
    res.json({ success: true, data: items });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

const createDidNumber = async (req, res) => {
  try {
    const { did, provider, callerId, notes } = req.body;
    if (!did || !provider) {
      return res.status(400).json({ success: false, message: 'did and provider are required' });
    }
    const exists = await DidNumber.findOne({ did });
    if (exists) {
      return res.status(409).json({ success: false, message: 'DID already exists' });
    }
    const finalCallerId = callerId && String(callerId).trim() !== '' ? callerId : DidNumber.deriveCallerIdFromDid(did);
    const created = await DidNumber.create({ did, provider, callerId: finalCallerId, status: 'available', assignedAgent: null, assignedClient: null, notes });
    res.status(201).json({ success: true, data: created });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

// New function for adding DID numbers with enhanced caller ID logic
const addDidNumber = async (req, res) => {
  try {
    const { did, provider, callerId, notes } = req.body;
    
    // Validate required fields
    if (!did || !provider) {
      return res.status(400).json({ 
        success: false, 
        message: 'DID number and provider are required' 
      });
    }

    // Check if DID already exists
    const exists = await DidNumber.findOne({ did });
    if (exists) {
      return res.status(409).json({ 
        success: false, 
        message: 'DID number already exists' 
      });
    }

    // Determine caller ID: use provided one or derive from DID (last 6 digits)
    let finalCallerId;
    if (callerId && String(callerId).trim() !== '') {
      // Use provided caller ID
      finalCallerId = String(callerId).trim();
    } else {
        // Auto-generate caller ID from last 7 digits of DID
      finalCallerId = DidNumber.deriveCallerIdFromDid(did);
    }

    // Create the DID number record
    const created = await DidNumber.create({ 
      did, 
      provider, 
      callerId: finalCallerId, 
      status: 'available', 
      assignedAgent: null, 
      assignedClient: null, 
      notes: notes || '' 
    });

    res.status(201).json({ 
      success: true, 
      message: 'DID number added successfully',
      data: {
        ...created.toObject(),
        callerIdSource: callerId && String(callerId).trim() !== '' ? 'provided' : 'auto-generated'
      }
    });
  } catch (error) {
    console.error('Error adding DID number:', error);
    res.status(500).json({ 
      success: false, 
      message: error.message || 'Failed to add DID number' 
    });
  }
};

const assignDidToAgent = async (req, res) => {
  try {
    const { did } = req.params;
    const { agentId } = req.body;
    if (!did || !agentId) {
      return res.status(400).json({ success: false, message: 'did and agentId are required' });
    }
    const didDoc = await DidNumber.findOne({ did });
    if (!didDoc) return res.status(404).json({ success: false, message: 'DID not found' });
    const agent = await Agent.findById(agentId);
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    // Lock checks: prevent changing a locked agent's DID, and prevent taking a DID from a locked agent
    const isAgentLocked = async (id) => !!(await Campaign.exists({ isRunning: true, agent: { $in: [String(id), id] } }));
    const targetLocked = await isAgentLocked(agent._id);

    // If target agent is locked and trying to change to a different DID, block
    if (targetLocked && agent.didNumber && String(agent.didNumber) !== String(didDoc.did)) {
      return res.status(403).json({ success: false, message: 'Agent is in a running campaign. DID cannot be changed now.' });
    }

    // If DID is assigned to another agent who is locked, block reassignment
    if (didDoc.assignedAgent && String(didDoc.assignedAgent) !== String(agent._id)) {
      const currentOwnerLocked = await isAgentLocked(didDoc.assignedAgent);
      if (currentOwnerLocked) {
        return res.status(403).json({ success: false, message: 'DID is in use by an agent with a running campaign.' });
      }
    }

    // Update agent telephony fields for SANPBX/SNAPBX
    const derivedCallerId = didDoc.callerId && didDoc.callerId.trim() !== ''
      ? didDoc.callerId
      : DidNumber.deriveCallerIdFromDid(didDoc.did);

    // If DID is already assigned to another agent, unassign it from them first
    if (didDoc.assignedAgent && String(didDoc.assignedAgent) !== String(agent._id)) {
      await Agent.findByIdAndUpdate(didDoc.assignedAgent, {
        $unset: { didNumber: '', callerId: '' },
      });
    }

    // Also ensure no other agents still carry this DID (data hygiene)
    await Agent.updateMany(
      { didNumber: didDoc.did, _id: { $ne: agent._id } },
      { $unset: { didNumber: '', callerId: '' } }
    );

    // Always align the agent's provider with the DID's provider on assignment
    agent.serviceProvider = didDoc.provider;
    agent.didNumber = didDoc.did;
    agent.callerId = derivedCallerId;
  
    // Set SANPBX credentials if provider is snapbx/sanpbx
    if (didDoc.provider === 'snapbx' || didDoc.provider === 'sanpbx') {
      agent.accessToken = agent.accessToken || 'e4b197411fd53012607649f23a6d28f9';
      agent.accessKey = agent.accessKey || 'mob';
      agent.appId = agent.appId || '3';
    }
    
    await agent.save();

    // Update DID assignment
    didDoc.status = 'assigned';
    didDoc.assignedAgent = agent._id;
    didDoc.assignedClient = agent.clientId || null;
    if (!didDoc.callerId) didDoc.callerId = derivedCallerId;
    await didDoc.save();

    res.json({ success: true, data: { did: didDoc, agent } });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

const unassignDid = async (req, res) => {
  try {
    const { did } = req.params;
    const didDoc = await DidNumber.findOne({ did });
    if (!didDoc) return res.status(404).json({ success: false, message: 'DID not found' });

    // If linked to an agent, clear that agent's did/callerId
    if (didDoc.assignedAgent) {
      await Agent.findByIdAndUpdate(didDoc.assignedAgent, {
        $unset: { didNumber: '', callerId: '' },
      });
    }

    didDoc.status = 'available';
    didDoc.assignedAgent = null;
    didDoc.assignedClient = null;
    await didDoc.save();

    res.json({ success: true, data: didDoc });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

// Assign C-Zentrix provider details to agent (from backend only)
const assignCzentrixToAgent = async (req, res) => {
  try {
    const { agentId, accountSid, callerId, xApiKey, didNumber } = req.body || {};
    if (!agentId) return res.status(400).json({ success: false, message: 'agentId is required' });
    if (!accountSid || !callerId || !xApiKey) {
      return res.status(400).json({ success: false, message: 'accountSid, callerId and xApiKey are required' });
    }

    const agent = await Agent.findById(agentId);
    if (!agent) return res.status(404).json({ success: false, message: 'Agent not found' });

    // Prevent provider change while locked by running campaign
    const isLocked = await Campaign.exists({ isRunning: true, agent: { $in: [String(agentId), agentId] } });
    if (isLocked) {
      return res.status(403).json({ success: false, message: 'Agent has a running campaign. Provider cannot be changed now.' });
    }

    // Find and clear C-Zentrix details from any previously assigned agents
    // This ensures only one agent can have C-Zentrix assignment at a time
    const previouslyAssignedAgents = await Agent.find({
      serviceProvider: { $in: ['c-zentrix', 'c-zentrax'] },
      _id: { $ne: agentId } // Exclude the current agent being assigned
    });

    if (previouslyAssignedAgents.length > 0) {
      console.log(`🔄 [C-ZENTRIX-REASSIGN] Clearing C-Zentrix details from ${previouslyAssignedAgents.length} previously assigned agents`);
      
      // Clear C-Zentrix details from all previously assigned agents
      await Agent.updateMany(
        { 
          serviceProvider: { $in: ['c-zentrix', 'c-zentrax'] },
          _id: { $ne: agentId }
        },
        {
          $unset: {
            serviceProvider: '',
            accountSid: '',
            callerId: '',
            X_API_KEY: '',
            didNumber: '',
            accessToken: '',
            accessKey: '',
            appId: ''
          },
          $set: {
            updatedAt: new Date()
          }
        }
      );

      console.log(`✅ [C-ZENTRIX-REASSIGN] Cleared C-Zentrix details from agents: ${previouslyAssignedAgents.map(a => a._id).join(', ')}`);
    }

    // Update DID if provided; otherwise keep existing or clear if previously SANPBX-only flow requires
    if (typeof didNumber !== 'undefined') {
      agent.didNumber = String(didNumber || '').trim() || undefined;
    }
    agent.accessToken = undefined;
    agent.accessKey = undefined;
    agent.appId = undefined;

    agent.serviceProvider = 'c-zentrix';
    agent.accountSid = String(accountSid);
    agent.callerId = String(callerId);
    agent.X_API_KEY = String(xApiKey);
    agent.updatedAt = new Date();
    await agent.save();

    console.log(`✅ [C-ZENTRIX-REASSIGN] Successfully assigned C-Zentrix to agent ${agentId}`);

    return res.json({ 
      success: true, 
      data: agent,
      message: previouslyAssignedAgents.length > 0 
        ? `C-Zentrix assigned successfully. Cleared details from ${previouslyAssignedAgents.length} previously assigned agents.`
        : 'C-Zentrix assigned successfully.'
    });
  } catch (error) {
    console.error('[assignCzentrixToAgent] error:', error);
    return res.status(500).json({ success: false, message: error.message || 'Failed to assign C-Zentrix' });
  }
};

// Approve client (set isApproved to true)
const approveClient = async (req, res) => {
  try {
    const { clientId } = req.params;
    const client = await Client.findById(clientId);
    if (!client) {
      return res.status(404).json({ success: false, message: 'Client not found' });
    }
    client.isApproved = true;
    await client.save();
    res.status(200).json({ success: true, message: 'Client approved successfully', client });
  } catch (error) {
    console.error('Error approving client:', error);
    res.status(500).json({ success: false, message: 'Failed to approve client' });
  }
};

// Get all agents from all clients
const getAllAgents = async (req, res) => {
  try {
    const projection = '_id agentName description category personality serviceProvider didNumber isActive clientId createdAt updatedAt callerId';
    const agents = await Agent.find({}, projection)
      .sort({ createdAt: -1 })
      .lean();

    res.status(200).json({
      success: true,
      data: agents
    });
  } catch (error) {
    console.error('Error fetching all agents:', error);
    res.status(500).json({
      success: false,
      message: error.message || 'Failed to fetch agents'
    });
  }
};

// Toggle agent status (enable/disable)
const toggleAgentStatus = async (req, res) => {
  try {
    const { agentId } = req.params;
    
    if (!agentId) {
      return res.status(400).json({
        success: false,
        message: 'Agent ID is required'
      });
    }

    
    // Find the agent first to get current status
    const currentAgent = await Agent.findById(agentId);
    if (!currentAgent) {
      return res.status(404).json({
        success: false,
        message: 'Agent not found'
      });
    }

    // Toggle the status
    const newStatus = !currentAgent.isActive;
    
    // Update the agent
    const agent = await Agent.findByIdAndUpdate(
      agentId,
      { isActive: newStatus },
      { new: true, runValidators: true }
    );

    res.status(200).json({
      success: true,
      message: `Agent ${newStatus ? 'enabled' : 'disabled'} successfully`,
      data: agent
    });
  } catch (error) {
    console.error('Error toggling agent status:', error);
    res.status(500).json({
      success: false,
      message: error.message || 'Failed to toggle agent status'
    });
  }
};

// Delete agent
const deleteAgent = async (req, res) => {
  try {
    const { agentId } = req.params;
    
    if (!agentId) {
      return res.status(400).json({
        success: false,
        message: 'Agent ID is required'
      });
    }

    
    // Find the agent first
    const agent = await Agent.findById(agentId);
    if (!agent) {
      return res.status(404).json({
        success: false,
        message: 'Agent not found'
      });
    }

    // Delete the agent
    await Agent.findByIdAndDelete(agentId);

    res.status(200).json({
      success: true,
      message: 'Agent deleted successfully'
    });
  } catch (error) {
    console.error('Error deleting agent:', error);
    res.status(500).json({
      success: false,
      message: error.message || 'Failed to delete agent'
    });
  }
};

// Copy agent to another client
const copyAgent = async (req, res) => {
  try {
    const { agentId, targetClientId } = req.body;
    
    if (!agentId || !targetClientId) {
      return res.status(400).json({
        success: false,
        message: 'Agent ID and target client ID are required'
      });
    }

    
    // Verify the target client exists and is approved
    const targetClient = await Client.findById(targetClientId);
    if (!targetClient) {
      return res.status(404).json({
        success: false,
        message: 'Target client not found'
      });
    }
    
    if (!targetClient.isApproved) {
      return res.status(400).json({
        success: false,
        message: 'Target client is not approved'
      });
    }

    // Get the source agent
    const sourceAgent = await Agent.findById(agentId);
    if (!sourceAgent) {
      return res.status(404).json({
        success: false,
        message: 'Source agent not found'
      });
    }

     // Helper to generate a unique 8-char hex agentKey
     const generateUniqueAgentKey = async () => {
      const hex = () => Math.random().toString(16).slice(2, 10).padEnd(8, '0').slice(0,8);
      let key;
      let exists = true;
      // Try a few times to avoid rare collisions
      for (let i = 0; i < 10; i++) {
        key = hex();
        const found = await Agent.findOne({ agentKey: key }).lean();
        if (!found) { exists = false; break; }
      }
      if (exists) {
        // Fallback: include time-based entropy
        key = (Date.now().toString(16) + Math.random().toString(16).slice(2)).slice(0,8);
      }
      return key;
    };


    // Create a copy of the agent with new client ID
    const agentCopy = {
      ...sourceAgent.toObject(),
      _id: undefined, // Remove the original ID
      clientId: targetClientId,
      agentName: `${sourceAgent.agentName}`,
      isActive: false, // Start as inactive by default
      createdAt: new Date(),
      updatedAt: new Date()
    };

    // Remove any fields that shouldn't be copied
    delete agentCopy._id;
    delete agentCopy.__v;

     // Ensure a new unique agentKey
     agentCopy.agentKey = await generateUniqueAgentKey();

    // Create the new agent
    const newAgent = await Agent.create(agentCopy);

    res.status(201).json({
      success: true,
      message: 'Agent copied successfully',
      data: newAgent
    });
  } catch (error) {
    console.error('Error copying agent:', error);
    res.status(500).json({
      success: false,
      message: error.message || 'Failed to copy agent'
    });
  }
};

// Update agent
const updateAgent = async (req, res) => {
  try {
    const { agentId } = req.params;
    const updateData = req.body;

    // Remove fields that shouldn't be updated
    const allowedFields = [
      'agentName',
      'description', 
      'category',
      'personality',
      'language',
      'firstMessage',
      'systemPrompt',
      'sttSelection',
      'ttsSelection',
      'llmSelection',
      'voiceSelection',
      'contextMemory',
      'brandInfo',
      'details',
      // Q&A
      'qa',
      'startingMessages',
      'knowledgeBase',
      'depositions',
      'whatsappEnabled',
      'telegramEnabled',
      'emailEnabled',
      'smsEnabled',
      'whatsapplink',
      'whatsapp',
      'telegram',
      'email',
      'sms'
      // NOTE: All telephony/provider fields are managed by backend flows (assignment/services)
    ];

    // Enforce lock logic: prevent changing DID for a locked agent
    const isRunningLocked = await Campaign.exists({ isRunning: true, agent: { $in: [String(agentId), agentId] } });
    if (isRunningLocked && Object.prototype.hasOwnProperty.call(updateData, 'didNumber')) {
      return res.status(403).json({ success: false, message: 'Agent has a running campaign. DID cannot be changed now.' });
    }

    const filteredUpdateData = {};
    Object.keys(updateData).forEach(key => {
      if (allowedFields.includes(key)) {
        filteredUpdateData[key] = updateData[key];
      }
    });

    // Enforce: Do not allow updating telephony/provider fields from generic update endpoint
    const forbiddenTelephonyFields = [
      'didNumber',
      'serviceProvider',
      'accountSid',
      'callingNumber',
      'callerId',
      'X_API_KEY',
      'accessToken',
      'accessKey',
      'appId'
    ];
    forbiddenTelephonyFields.forEach((key) => {
      if (Object.prototype.hasOwnProperty.call(filteredUpdateData, key)) {
        delete filteredUpdateData[key];
      }
    });

    // Add updatedAt timestamp
    filteredUpdateData.updatedAt = new Date();

    const updatedAgent = await Agent.findByIdAndUpdate(
      agentId,
      filteredUpdateData,
      { new: true, runValidators: true }
    );

    if (!updatedAgent) {
      return res.status(404).json({
        success: false,
        message: "Agent not found"
      });
    }

    res.status(200).json({
      success: true,
      message: "Agent updated successfully",
      data: updatedAgent
    });

  } catch (error) {
    console.error('Error updating agent:', error);
    res.status(500).json({
      success: false,
      message: error.message || "An error occurred while updating the agent"
    });
  }
};

module.exports = { loginAdmin, registerAdmin,getClients,getClientById,registerclient,deleteclient,getClientToken, approveClient, getAllAgents, toggleAgentStatus, copyAgent, deleteAgent, updateAgent,updateClient, listDidNumbers, createDidNumber, addDidNumber, assignDidToAgent, unassignDid, assignCzentrixToAgent };

// Return agents locked due to running campaigns
module.exports.getCampaignLocks = async (_req, res) => {
  try {
    const running = await Campaign.find({ isRunning: true }).lean();
    const lockedAgentIds = new Set();
    for (const camp of running) {
      const agentIds = Array.isArray(camp.agent) ? camp.agent : [];
      agentIds.forEach((a) => { if (a) lockedAgentIds.add(String(a)); });
    }

    const ids = Array.from(lockedAgentIds);
    const lockedAgents = ids.length
      ? await Agent.find({ _id: { $in: ids } }).select('_id agentName clientId didNumber serviceProvider').lean()
      : [];

    return res.json({ success: true, data: { lockedAgentIds: ids, lockedAgents } });
  } catch (error) {
    console.error('[getCampaignLocks] error:', error.message);
    return res.status(500).json({ success: false, message: error.message });
  }
};
// System Prompt Handlers
module.exports.createSystemPrompt = async (req, res) => {
  try {
    const { title, promptText, isDefault, tags } = req.body;
    if (!title || !promptText) {
      return res.status(400).json({ success: false, message: 'title and promptText are required' });
    }

    if (isDefault) {
      await SystemPrompt.updateMany({ isDefault: true }, { $set: { isDefault: false } });
    }

    const created = await SystemPrompt.create({
      title,
      promptText,
      isDefault: !!isDefault,
      tags: Array.isArray(tags) ? tags : [],
      createdBy: req.user?.id || undefined,
    });

    res.status(201).json({ success: true, data: created });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

module.exports.getSystemPrompts = async (_req, res) => {
  try {
    const items = await SystemPrompt.find().sort({ isDefault: -1, createdAt: -1 });
    res.json({ success: true, data: items });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

module.exports.setDefaultSystemPrompt = async (req, res) => {
  try {
    const { id } = req.params;
    if (!id) return res.status(400).json({ success: false, message: 'id is required' });

    await SystemPrompt.updateMany({ isDefault: true }, { $set: { isDefault: false } });
    const updated = await SystemPrompt.findByIdAndUpdate(id, { isDefault: true }, { new: true });
    if (!updated) return res.status(404).json({ success: false, message: 'SystemPrompt not found' });
    res.json({ success: true, data: updated });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

module.exports.deleteSystemPrompt = async (req, res) => {
  try {
    const { id } = req.params;
    const existing = await SystemPrompt.findById(id);
    if (!existing) return res.status(404).json({ success: false, message: 'SystemPrompt not found' });

    await SystemPrompt.findByIdAndDelete(id);
    // If it was default and others exist, set the newest as default automatically
    if (existing.isDefault) {
      const newest = await SystemPrompt.findOne().sort({ createdAt: -1 });
      if (newest) {
        await SystemPrompt.findByIdAndUpdate(newest._id, { isDefault: true });
      }
    }
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

module.exports.updateSystemPrompt = async (req, res) => {
  try {
    const { id } = req.params;
    const { title, promptText, isDefault, tags } = req.body;
    if (!id) return res.status(400).json({ success: false, message: 'id is required' });

    const update = {};
    if (typeof title === 'string') update.title = title;
    if (typeof promptText === 'string') update.promptText = promptText;
    if (Array.isArray(tags)) update.tags = tags;

    if (typeof isDefault === 'boolean') {
      if (isDefault) {
        await SystemPrompt.updateMany({ isDefault: true }, { $set: { isDefault: false } });
        update.isDefault = true;
      } else {
        update.isDefault = false;
      }
    }

    const updated = await SystemPrompt.findByIdAndUpdate(id, update, { new: true });
    if (!updated) return res.status(404).json({ success: false, message: 'SystemPrompt not found' });
    res.json({ success: true, data: updated });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};
