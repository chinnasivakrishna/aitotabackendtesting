const jwt = require('jsonwebtoken');
const Admin = require('../models/Admin');
const Client = require('../models/Client');
const User = require('../models/User');
const HumanAgent = require('../models/HumanAgent');

const authMiddleware = async (req, res, next) => {
  try {
    let token;

    if (
      req.headers.authorization &&
      req.headers.authorization.startsWith('Bearer')
    ) {
      token = req.headers.authorization.split(' ')[1];
    }

    if (!token) {
      return res.status(401).json({
        success: false,
        message: 'Not authorized to access this route',
      });
    }

    try {
      // Verify token
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      // Check if token has userType
      if (!decoded.userType) {
        return res.status(401).json({
          success: false,
          message: 'Invalid token format',
        });
      }

      // Find user based on userType
      let user;
      if (decoded.userType === 'client') {
        user = await Client.findById(decoded.id).select('-password');
      } else if (decoded.userType === 'admin') {
        user = await Admin.findById(decoded.id).select('-password');
      } else if (decoded.userType === 'humanAgent') {
        user = await HumanAgent.findById(decoded.id);
      }

      if (!user) {
        return res.status(401).json({
          success: false,
          message: 'User not found',
        });
      }

      // Add user info to request
      req.user = {
        id: user._id,
        userType: decoded.userType,
        email: user.email
      };

      next();
    } catch (error) {
      console.error('Token verification error:', error);
      return res.status(401).json({
        success: false,
        message: 'Not authorized to access this route',
      });
    }
  } catch (error) {
    console.error('Auth middleware error:', error);
    return res.status(500).json({
      success: false,
      message: 'Internal server error',
    });
  }
};

// Verify user token
const verifyClientToken = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'No token provided' });
    }
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Find user by id
    const client = await Client.findById(decoded.id).select('-password');
    if (!client) {
      return res.status(401).json({ success: false, message: 'Invalid token' });
    }
    
    // Add user to request object
    req.client = client;
    next();
  } catch (error) {
    console.error('Token verification error:', error);
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
};

// Verify admin token
const verifyAdminToken = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'No token provided' });
    }
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Find admin by id
    const admin = await Admin.findById(decoded.id).select('-password');
    if (!admin) {
      return res.status(401).json({ success: false, message: 'Invalid token' });
    }
    
    // Add admin to request object
    req.admin = admin;
    req.adminId = String(decoded.id); // Also set adminId for consistency
    req.user = {
      id: admin._id,
      userType: 'admin',
      email: admin.email
    };
    next();
  } catch (error) {
    console.error('Admin token verification error:', error);
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
};
// Verify admin token
const verifyAdminTokenOnlyForRegister = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Find admin by id
    const admin = await Admin.findById(decoded.id).select('-password');
    if (!admin) {
      return res.status(401).json({ success: false, message: 'Invalid token' });
    }
    
    // Add admin to request object
    req.admin = admin;
    req.adminId = String(decoded.id); // Also set adminId for consistency
    next();
  } catch (error) {
    next();
  }
};

// Verify admin token
const verifyUserToken = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'No token provided' });
    }
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Find admin by id
    const user = await User.findById(decoded.id).select('-password');
    if (!user) {
      return res.status(401).json({ success: false, message: 'Invalid token' });
    }
    
    // Add admin to request object
    req.user = user;
    next();
  } catch (error) {
    console.error('User token verification error:', error);
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
};

// Check client access middleware
const checkClientAccess = (allowedClients = []) => {
  return async (req, res, next) => {
    try {
      const clientId = req.params.clientId || req.clientId;
      
      console.log('Checking client access for:', clientId);
      console.log('Request URL:', req.originalUrl);
      
      if (!clientId) {
        return res.status(400).json({
          success: false,
          message: 'Client ID is required.',
          error: {
            code: 'MISSING_CLIENT_ID',
            details: 'Client ID must be provided in the URL path'
          }
        });
      }

      // Validate client exists and is active
      const client = await Client.findOne({
        userId: clientId,
      });

      if (!client) {
        return res.status(400).json({
          success: false,
          message: 'Invalid client ID or client is not active.',
          error: {
            code: 'INVALID_CLIENT',
            details: `Client with ID ${clientId} not found or is not active`
          }
        });
      }

      // Add client info to request
      req.clientId = clientId;
      req.clientInfo = {
        id: client._id,
        userId: client.userId,
        businessName: client.businessName,
      };

      console.log('Client access granted for:', client.businessName);
      next();
    } catch (error) {
      console.error('Client access check error:', error);
      res.status(500).json({
        success: false,
        message: 'Internal server error during client validation.',
        error: {
          code: 'CLIENT_CHECK_ERROR',
          details: error.message
        }
      });
    }
  };
};

// Middleware to ensure user belongs to the client (additional security)
const ensureUserBelongsToClient = async (req, res, next) => {
  try {
    const clientId = req.params.clientId || req.clientId;
    
    if (req.user.clientId !== clientId) {
      return res.status(403).json({
        success: false,
        message: 'Access denied. User does not belong to this client.',
        error: {
          code: 'CLIENT_USER_MISMATCH',
          details: `User belongs to client ${req.user.clientId} but trying to access ${clientId}`
        }
      });
    }

    next();
  } catch (error) {
    console.error('User-client verification error:', error);
    res.status(500).json({
      success: false,
      message: 'Internal server error during user-client verification.',
      error: {
        code: 'USER_CLIENT_CHECK_ERROR',
        details: error.message
      }
    });
  }
};
// Verify human agent token
const verifyHumanAgentToken = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'No token provided' });
    }
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Check if it's a human agent token
    if (decoded.userType !== 'humanAgent') {
      return res.status(401).json({ success: false, message: 'Invalid token type for human agent access' });
    }
    
    // Find human agent by id
    const humanAgent = await HumanAgent.findById(decoded.id);
    if (!humanAgent) {
      return res.status(401).json({ success: false, message: 'Human agent not found' });
    }
    
    // Check if human agent is approved
    if (!humanAgent.isApproved) {
      return res.status(401).json({ success: false, message: 'Human agent account is not approved' });
    }
    
    // Add human agent to request object
    req.humanAgent = humanAgent;
    req.user = {
      id: humanAgent._id,
      userType: 'humanAgent',
      email: humanAgent.email,
      clientId: humanAgent.clientId
    };
    next();
  } catch (error) {
    console.error('Human agent token verification error:', error);
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
};

// Verify client or human agent token (accepts both)
const verifyClientOrHumanAgentToken = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'No token provided' });
    }
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Check if it's a client token
    if (decoded.userType === 'client') {
      const client = await Client.findById(decoded.id).select('-password');
      if (!client) {
        return res.status(401).json({ success: false, message: 'Client not found' });
      }
      
      // Add client to request object
      req.client = client;
      req.user = {
        id: client._id,
        userType: 'client',
        email: client.email,
        clientId: client.userId
      };
      next();
      return;
    }
    
    // Check if it's a human agent token
    if (decoded.userType === 'humanAgent') {
      const humanAgent = await HumanAgent.findById(decoded.id);
      if (!humanAgent) {
        return res.status(401).json({ success: false, message: 'Human agent not found' });
      }
      
      // Check if human agent is approved
      if (!humanAgent.isApproved) {
        return res.status(401).json({ success: false, message: 'Human agent account is not approved' });
      }
      
      // Add human agent to request object
      req.humanAgent = humanAgent;
      req.user = {
        id: humanAgent._id,
        userType: 'humanAgent',
        email: humanAgent.email,
        clientId: humanAgent.clientId
      };
      next();
      return;
    }
    
    // If neither client nor human agent token
    return res.status(401).json({ 
      success: false, 
      message: 'Invalid token type. Only client or human agent tokens are allowed' 
    });
    
  } catch (error) {
    console.error('Client or Human Agent token verification error:', error);
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
};

// Verify agent or client token (accepts both)
const verifyAgentOrClientToken = async (req, res, next) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, message: 'No token provided' });
    }
    
    const token = authHeader.split(' ')[1];
    
    // Verify token
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    
    // Check if it's a client token
    if (decoded.userType === 'client') {
      const client = await Client.findById(decoded.id).select('-password');
      if (!client) {
        return res.status(401).json({ success: false, message: 'Client not found' });
      }
      
      // Add client to request object
      req.client = client;
      req.user = {
        id: client._id,
        userType: 'client',
        email: client.email,
        clientId: client.userId
      };
      next();
      return;
    }
    
    // Check if it's an agent token (from Agent model)
    if (decoded.userType === 'agent') {
      const Agent = require('../models/Agent');
      const agent = await Agent.findById(decoded.id);
      if (!agent) {
        return res.status(401).json({ success: false, message: 'Agent not found' });
      }
      
      // Check if agent is active
      if (!agent.isActive) {
        return res.status(401).json({ success: false, message: 'Agent account is not active' });
      }
      
      // Add agent to request object
      req.agent = agent;
      req.user = {
        id: agent._id,
        userType: 'agent',
        clientId: agent.clientId
      };
      next();
      return;
    }
    
    // If neither client nor agent token
    return res.status(401).json({ 
      success: false, 
      message: 'Invalid token type. Only client or agent tokens are allowed' 
    });
    
  } catch (error) {
    console.error('Agent or Client token verification error:', error);
    return res.status(401).json({ success: false, message: 'Invalid token' });
  }
};

module.exports = { 
  verifyClientToken, 
  verifyAdminToken, 
  verifyAdminTokenOnlyForRegister, 
  verifyUserToken, 
  authMiddleware, 
  checkClientAccess, 
  ensureUserBelongsToClient,
  verifyHumanAgentToken,
  verifyClientOrHumanAgentToken,
  verifyAgentOrClientToken
}; 

// Accepts either a client or admin token. If admin, requires a clientId in query/body/params.
const verifyClientOrAdminAndExtractClientId = async (req, res, next) => {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ success: false, error: 'No token provided' });
    }

    const token = authHeader.split(' ')[1];
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    if (decoded.userType === 'client') {
      const client = await Client.findById(decoded.id).select('-password');
      if (!client) {
        return res.status(401).json({ success: false, error: 'Client not found' });
      }
      req.clientId = String(client._id);
      req.clientUserId = String(client.userId); // Store the client's userId for reference
      req.user = { id: client._id, userType: 'client', email: client.email };
      return next();
    }

    if (decoded.userType === 'admin') {
      const candidateClientId = req.query.clientId || req.body.clientId || req.params.clientId;
      if (candidateClientId) {
        const client = await Client.findById(candidateClientId).select('-password');
        if (!client) {
          return res.status(404).json({ success: false, error: 'Client not found' });
        }
        req.clientId = String(client._id);
        req.clientUserId = String(client.userId); // Store the client's userId for reference
      } else {
        // Allow admin without client context - clientId is optional for admin tokens
        req.clientId = undefined;
        req.clientUserId = undefined;
      }
      req.adminId = String(decoded.id); // Store admin ID
      req.user = { id: decoded.id, userType: 'admin' };
      return next();
    }

    return res.status(401).json({ success: false, error: 'Invalid token type' });
  } catch (error) {
    console.error('verifyClientOrAdminAndExtractClientId error:', error);
    return res.status(401).json({ success: false, error: 'Invalid token' });
  }
};

// Export appended separately to avoid breaking existing export object reference
module.exports.verifyClientOrAdminAndExtractClientId = verifyClientOrAdminAndExtractClientId;