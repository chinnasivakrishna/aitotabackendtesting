const Plan = require("../models/Plan");
const Coupon = require("../models/Coupon");

// Create a new plan
const createPlan = async (req, res) => {
  try {
    const {
      name,
      description,
      category,
      price,
      currency,
      billingCycle,
      creditsIncluded,
      bonusCredits,
      usageRates,
      features,
      discounts,
      limits,
      isPopular,
      sortOrder
    } = req.body;

    // Validate required fields
    if (!name || !description || !price || !creditsIncluded || !usageRates) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields"
      });
    }

    // Create new plan
    const plan = new Plan({
      name,
      description,
      category: category || "basic",
      price,
      currency: currency || "USD",
      billingCycle: billingCycle || "monthly",
      creditsIncluded,
      bonusCredits: bonusCredits || 0,
      usageRates,
      features: features || {},
      discounts: discounts || {},
      limits: limits || {},
      isPopular: isPopular || false,
      sortOrder: sortOrder || 0,
      createdBy: req.user.id
    });

    await plan.save();

    res.status(201).json({
      success: true,
      message: "Plan created successfully",
      data: plan
    });
  } catch (error) {
    console.error("Error creating plan:", error);
    res.status(500).json({
      success: false,
      message: "Failed to create plan",
      error: error.message
    });
  }
};

// Get all plans
const getAllPlans = async (req, res) => {
  try {
    const { 
      category, 
      isActive, 
      isPopular, 
      sortBy = "sortOrder", 
      sortOrder = "asc",
      page = 1,
      limit = 10
    } = req.query;

    // Build filter
    const filter = {};
    if (category) filter.category = category;
    if (isActive !== undefined) filter.isActive = isActive === "true";
    if (isPopular !== undefined) filter.isPopular = isPopular === "true";

    // Build sort
    const sort = {};
    sort[sortBy] = sortOrder === "desc" ? -1 : 1;

    // Pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const [plans, total] = await Promise.all([
      Plan.find(filter)
        .populate("createdBy", "name email")
        .sort(sort)
        .skip(skip)
        .limit(parseInt(limit))
        .lean(),
      Plan.countDocuments(filter)
    ]);

    res.status(200).json({
      success: true,
      data: plans,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total,
        pages: Math.ceil(total / parseInt(limit))
      }
    });
  } catch (error) {
    console.error("Error fetching plans:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch plans",
      error: error.message
    });
  }
};

// Get plan by ID
const getPlanById = async (req, res) => {
  try {
    const plan = await Plan.findById(req.params.id)
      .populate("createdBy", "name email")
      .lean();

    if (!plan) {
      return res.status(404).json({
        success: false,
        message: "Plan not found"
      });
    }

    res.status(200).json({
      success: true,
      data: plan
    });
  } catch (error) {
    console.error("Error fetching plan:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch plan",
      error: error.message
    });
  }
};

// Update plan
const updatePlan = async (req, res) => {
  try {
    const {
      name,
      description,
      category,
      price,
      currency,
      billingCycle,
      creditsIncluded,
      bonusCredits,
      usageRates,
      features,
      discounts,
      limits,
      isActive,
      isPopular,
      sortOrder
    } = req.body;

    const plan = await Plan.findById(req.params.id);

    if (!plan) {
      return res.status(404).json({
        success: false,
        message: "Plan not found"
      });
    }

    // Update fields
    if (name !== undefined) plan.name = name;
    if (description !== undefined) plan.description = description;
    if (category !== undefined) plan.category = category;
    if (price !== undefined) plan.price = price;
    if (currency !== undefined) plan.currency = currency;
    if (billingCycle !== undefined) plan.billingCycle = billingCycle;
    if (creditsIncluded !== undefined) plan.creditsIncluded = creditsIncluded;
    if (bonusCredits !== undefined) plan.bonusCredits = bonusCredits;
    if (usageRates !== undefined) plan.usageRates = usageRates;
    if (features !== undefined) plan.features = features;
    if (discounts !== undefined) plan.discounts = discounts;
    if (limits !== undefined) plan.limits = limits;
    if (isActive !== undefined) plan.isActive = isActive;
    if (isPopular !== undefined) plan.isPopular = isPopular;
    if (sortOrder !== undefined) plan.sortOrder = sortOrder;

    await plan.save();

    res.status(200).json({
      success: true,
      message: "Plan updated successfully",
      data: plan
    });
  } catch (error) {
    console.error("Error updating plan:", error);
    res.status(500).json({
      success: false,
      message: "Failed to update plan",
      error: error.message
    });
  }
};

// Delete plan
const deletePlan = async (req, res) => {
  try {
    const plan = await Plan.findById(req.params.id);

    if (!plan) {
      return res.status(404).json({
        success: false,
        message: "Plan not found"
      });
    }

    // Check if plan is being used by any clients
    const Credit = require("../models/Credit");
    const activeSubscriptions = await Credit.countDocuments({
      "currentPlan.planId": plan._id
    });

    if (activeSubscriptions > 0) {
      return res.status(400).json({
        success: false,
        message: `Cannot delete plan. ${activeSubscriptions} active subscription(s) found.`
      });
    }

    await Plan.findByIdAndDelete(req.params.id);

    res.status(200).json({
      success: true,
      message: "Plan deleted successfully"
    });
  } catch (error) {
    console.error("Error deleting plan:", error);
    res.status(500).json({
      success: false,
      message: "Failed to delete plan",
      error: error.message
    });
  }
};

// Toggle plan status
const togglePlanStatus = async (req, res) => {
  try {
    const plan = await Plan.findById(req.params.id);

    if (!plan) {
      return res.status(404).json({
        success: false,
        message: "Plan not found"
      });
    }

    plan.isActive = !plan.isActive;
    await plan.save();

    res.status(200).json({
      success: true,
      message: `Plan ${plan.isActive ? "activated" : "deactivated"} successfully`,
      data: plan
    });
  } catch (error) {
    console.error("Error toggling plan status:", error);
    res.status(500).json({
      success: false,
      message: "Failed to toggle plan status",
      error: error.message
    });
  }
};

// Get plan statistics
const getPlanStats = async (req, res) => {
  try {
    const Credit = require("../models/Credit");

    const [
      totalPlans,
      activePlans,
      popularPlans,
      planUsage
    ] = await Promise.all([
      Plan.countDocuments(),
      Plan.countDocuments({ isActive: true }),
      Plan.countDocuments({ isActive: true, isPopular: true }),
      Credit.aggregate([
        {
          $group: {
            _id: "$currentPlan.planId",
            count: { $sum: 1 }
          }
        },
        {
          $lookup: {
            from: "plans",
            localField: "_id",
            foreignField: "_id",
            as: "plan"
          }
        },
        {
          $unwind: "$plan"
        },
        {
          $project: {
            planName: "$plan.name",
            subscribers: "$count"
          }
        },
        {
          $sort: { subscribers: -1 }
        }
      ])
    ]);

    res.status(200).json({
      success: true,
      data: {
        totalPlans,
        activePlans,
        popularPlans,
        planUsage
      }
    });
  } catch (error) {
    console.error("Error fetching plan stats:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch plan statistics",
      error: error.message
    });
  }
};

// Duplicate plan
const duplicatePlan = async (req, res) => {
  try {
    const originalPlan = await Plan.findById(req.params.id);

    if (!originalPlan) {
      return res.status(404).json({
        success: false,
        message: "Plan not found"
      });
    }

    // Create new plan with copied data
    const newPlan = new Plan({
      ...originalPlan.toObject(),
      _id: undefined,
      name: `${originalPlan.name} (Copy)`,
      isActive: false,
      isPopular: false,
      createdBy: req.user.id,
      createdAt: undefined,
      updatedAt: undefined
    });

    await newPlan.save();

    res.status(201).json({
      success: true,
      message: "Plan duplicated successfully",
      data: newPlan
    });
  } catch (error) {
    console.error("Error duplicating plan:", error);
    res.status(500).json({
      success: false,
      message: "Failed to duplicate plan",
      error: error.message
    });
  }
};

module.exports = {
  createPlan,
  getAllPlans,
  getPlanById,
  updatePlan,
  deletePlan,
  togglePlanStatus,
  getPlanStats,
  duplicatePlan
};
