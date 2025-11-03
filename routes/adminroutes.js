const express = require("express");
const router = express.Router();
const { loginAdmin, registerAdmin, getClients, getClientById, deleteclient, getClientToken, approveClient, getAllAgents, toggleAgentStatus, copyAgent, deleteAgent,updateClient, updateAgent, createSystemPrompt, getSystemPrompts, setDefaultSystemPrompt, deleteSystemPrompt, updateSystemPrompt, assignCzentrixToAgent } = require("../controllers/admincontroller");
const adminCtrl = require("../controllers/admincontroller");
const { verifyAdminToken } = require("../middlewares/authmiddleware");
const planController = require("../controllers/planController");
const creditController = require("../controllers/creditController");
const couponController = require("../controllers/couponController");

// Public routes
router.post("/login", loginAdmin);
router.post("/register", registerAdmin);

// Protected routes
router.get("/getclients", verifyAdminToken, getClients);
router.get("/getclient/:id", verifyAdminToken, getClientById);
router.delete("/deleteclient/:id", verifyAdminToken, deleteclient);
router.get("/get-client-token/:clientId", verifyAdminToken, getClientToken);
router.post("/approve-client/:clientId", verifyAdminToken, approveClient);
router.get("/all-agents", verifyAdminToken, getAllAgents);
router.put("/toggle-agent-status/:agentId", verifyAdminToken, toggleAgentStatus);
router.post("/copy-agent", verifyAdminToken, copyAgent);
router.delete("/delete-agent/:agentId", verifyAdminToken, deleteAgent);
router.put("/update-agent/:agentId", verifyAdminToken, updateAgent);
router.put("/update-client/:clientId", verifyAdminToken, updateClient);

// System Prompts
router.post('/system-prompts', verifyAdminToken, createSystemPrompt);
router.get('/system-prompts', verifyAdminToken, getSystemPrompts);
router.put('/system-prompts/:id/default', verifyAdminToken, setDefaultSystemPrompt);
router.put('/system-prompts/:id', verifyAdminToken, updateSystemPrompt);
router.delete('/system-prompts/:id', verifyAdminToken, deleteSystemPrompt);

// Plan Management Routes
router.post('/plans', verifyAdminToken, planController.createPlan);
router.get('/plans', verifyAdminToken, planController.getAllPlans);
router.get('/plans/stats', verifyAdminToken, planController.getPlanStats);
router.get('/plans/:id', verifyAdminToken, planController.getPlanById);
router.put('/plans/:id', verifyAdminToken, planController.updatePlan);
router.delete('/plans/:id', verifyAdminToken, planController.deletePlan);
router.patch('/plans/:id/toggle', verifyAdminToken, planController.togglePlanStatus);
router.post('/plans/:id/duplicate', verifyAdminToken, planController.duplicatePlan);

// Credit Management Routes
router.get('/credits', verifyAdminToken, creditController.getAllCreditRecords);
router.get('/credits/stats', verifyAdminToken, creditController.getCreditStats);
router.get('/credits/client/:clientId', verifyAdminToken, creditController.getClientBalance);
router.get('/credits/client/:clientId/history', verifyAdminToken, creditController.getCreditHistory);
router.post('/credits/purchase', verifyAdminToken, creditController.purchasePlan);
router.post('/credits/add', verifyAdminToken, creditController.addCredits);
router.post('/credits/use', verifyAdminToken, creditController.useCredits);
router.put('/credits/client/:clientId/settings', verifyAdminToken, creditController.updateCreditSettings);
router.post('/credits/validate-coupon', verifyAdminToken, creditController.validateCoupon);

// Coupon Management Routes
router.post('/coupons', verifyAdminToken, couponController.createCoupon);
router.post('/coupons/bulk', verifyAdminToken, couponController.bulkCreateCoupons);
router.get('/coupons', verifyAdminToken, couponController.getAllCoupons);
router.get('/coupons/stats', verifyAdminToken, couponController.getCouponStats);
router.get('/coupons/:id', verifyAdminToken, couponController.getCouponById);
router.get('/coupons/:id/usage', verifyAdminToken, couponController.getCouponUsageHistory);
router.put('/coupons/:id', verifyAdminToken, couponController.updateCoupon);
router.delete('/coupons/:id', verifyAdminToken, couponController.deleteCoupon);
router.patch('/coupons/:id/toggle', verifyAdminToken, couponController.toggleCouponStatus);
router.post('/coupons/validate', verifyAdminToken, couponController.validateCouponCode);

// DID Numbers Management
router.get('/did-numbers', verifyAdminToken, adminCtrl.listDidNumbers);
router.post('/did-numbers', verifyAdminToken, adminCtrl.createDidNumber);
router.post('/did-numbers/add', verifyAdminToken, adminCtrl.addDidNumber);
router.post('/did-numbers/:did/assign', verifyAdminToken, adminCtrl.assignDidToAgent);
router.post('/did-numbers/:did/unassign', verifyAdminToken, adminCtrl.unassignDid);

// Assign C-Zentrix provider details (no DID) to agent
router.post('/assign-czentrix', verifyAdminToken, assignCzentrixToAgent);

// Campaign locks: which agents are locked due to running campaigns
router.get('/campaign-locks', verifyAdminToken, adminCtrl.getCampaignLocks);

module.exports = router;
