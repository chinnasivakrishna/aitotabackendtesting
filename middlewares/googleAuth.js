const { OAuth2Client } = require('google-auth-library');

// Initialize Google OAuth2 client
const googleClient = new OAuth2Client(process.env.GOOGLE_CLIENT_ID);
/**
 * Middleware to verify Google ID token
 * This middleware validates the Google ID token sent from the Flutter app
 */
const verifyGoogleToken = async (req, res, next) => {
  console.log(req);
  try {
    const { token } = req.body;
    console.log('Received Google token:', token);
    const audience = [ process.env.GOOGLE_CLIENT_ID, process.env.GOOGLE_ANDROID_CLIENT_ID].filter(Boolean);
    console.log('Audience for Google verification:', audience);
    if (!token) {
      return res.status(400).json({
        success: false,
        message: 'Google token is required'
      });
    }
    console.log(token)
    // Verify the Google ID token
    const ticket = await googleClient.verifyIdToken({
      idToken: token,
      audience: audience,
    });

    const payload = ticket.getPayload();
    
    // Add Google user info to request
    req.googleUser = {
      googleId: payload.sub,
      email: payload.email,
      name: payload.name,
      picture: payload.picture,
      emailVerified: payload.email_verified,
      googleToken: token
    };

    
    next();
  } catch (error) {
    console.error('Google token verification error:', error);
    console.log(error)
    return res.status(401).json({
      success: false,
      message: 'Invalid Google token'
    });
  }
};


module.exports = {
  verifyGoogleToken,
};