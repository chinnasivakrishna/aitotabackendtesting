const Coupon = require("../models/Coupon");
const Plan = require("../models/Plan");

// Create a new coupon
const createCoupon = async (req, res) => {
  try {
    const {
      code,
      name,
      description,
      discountType,
      discountValue,
      maxDiscount,
      maxUses,
      maxUsesPerUser,
      validFrom,
      validUntil,
      applicablePlans,
      applicableCategories,
      minimumPurchase,
      restrictions,
      isActive
    } = req.body;

    // Validate required fields
    if (!code || !name || !discountType || !discountValue || !validFrom || !validUntil) {
      return res.status(400).json({
        success: false,
        message: "Missing required fields"
      });
    }

    // Check if coupon code already exists
    const existingCoupon = await Coupon.findOne({ code: code.toUpperCase() });
    if (existingCoupon) {
      return res.status(400).json({
        success: false,
        message: "Coupon code already exists"
      });
    }

    // Create new coupon
    const coupon = new Coupon({
      code: code.toUpperCase(),
      name,
      description,
      discountType,
      discountValue,
      maxDiscount,
      maxUses: maxUses || null,
      maxUsesPerUser: maxUsesPerUser || 1,
      validFrom: new Date(validFrom),
      validUntil: new Date(validUntil),
      applicablePlans: applicablePlans || [],
      applicableCategories: applicableCategories || [],
      minimumPurchase: minimumPurchase || 0,
      restrictions: restrictions || {},
      isActive: isActive !== undefined ? isActive : true,
      createdBy: req.user.id
    });

    await coupon.save();

    res.status(201).json({
      success: true,
      message: "Coupon created successfully",
      data: coupon
    });
  } catch (error) {
    console.error("Error creating coupon:", error);
    res.status(500).json({
      success: false,
      message: "Failed to create coupon",
      error: error.message
    });
  }
};

// Get all coupons
const getAllCoupons = async (req, res) => {
  try {
    const { 
      isActive, 
      discountType, 
      sortBy = "createdAt", 
      sortOrder = "desc",
      page = 1,
      limit = 10,
      search
    } = req.query;

    // Build filter
    const filter = {};
    if (isActive !== undefined) filter.isActive = isActive === "true";
    if (discountType) filter.discountType = discountType;
    if (search) {
      filter.$or = [
        { code: { $regex: search, $options: "i" } },
        { name: { $regex: search, $options: "i" } },
        { description: { $regex: search, $options: "i" } }
      ];
    }

    // Build sort
    const sort = {};
    sort[sortBy] = sortOrder === "desc" ? -1 : 1;

    // Pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);

    const [coupons, total] = await Promise.all([
      Coupon.find(filter)
        .populate("createdBy", "name email")
        .populate("applicablePlans", "name")
        .sort(sort)
        .skip(skip)
        .limit(parseInt(limit))
        .lean(),
      Coupon.countDocuments(filter)
    ]);

    res.status(200).json({
      success: true,
      data: coupons,
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total,
        pages: Math.ceil(total / parseInt(limit))
      }
    });
  } catch (error) {
    console.error("Error fetching coupons:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch coupons",
      error: error.message
    });
  }
};

// Get coupon by ID
const getCouponById = async (req, res) => {
  try {
    const coupon = await Coupon.findById(req.params.id)
      .populate("createdBy", "name email")
      .populate("applicablePlans", "name price")
      .lean();

    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: "Coupon not found"
      });
    }

    res.status(200).json({
      success: true,
      data: coupon
    });
  } catch (error) {
    console.error("Error fetching coupon:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch coupon",
      error: error.message
    });
  }
};

// Update coupon
const updateCoupon = async (req, res) => {
  try {
    const {
      code,
      name,
      description,
      discountType,
      discountValue,
      maxDiscount,
      maxUses,
      maxUsesPerUser,
      validFrom,
      validUntil,
      applicablePlans,
      applicableCategories,
      minimumPurchase,
      restrictions,
      isActive
    } = req.body;

    const coupon = await Coupon.findById(req.params.id);

    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: "Coupon not found"
      });
    }

    // Check if new code already exists (if code is being changed)
    if (code && code.toUpperCase() !== coupon.code) {
      const existingCoupon = await Coupon.findOne({ code: code.toUpperCase() });
      if (existingCoupon) {
        return res.status(400).json({
          success: false,
          message: "Coupon code already exists"
        });
      }
    }

    // Update fields
    if (code !== undefined) coupon.code = code.toUpperCase();
    if (name !== undefined) coupon.name = name;
    if (description !== undefined) coupon.description = description;
    if (discountType !== undefined) coupon.discountType = discountType;
    if (discountValue !== undefined) coupon.discountValue = discountValue;
    if (maxDiscount !== undefined) coupon.maxDiscount = maxDiscount;
    if (maxUses !== undefined) coupon.maxUses = maxUses;
    if (maxUsesPerUser !== undefined) coupon.maxUsesPerUser = maxUsesPerUser;
    if (validFrom !== undefined) coupon.validFrom = new Date(validFrom);
    if (validUntil !== undefined) coupon.validUntil = new Date(validUntil);
    if (applicablePlans !== undefined) coupon.applicablePlans = applicablePlans;
    if (applicableCategories !== undefined) coupon.applicableCategories = applicableCategories;
    if (minimumPurchase !== undefined) coupon.minimumPurchase = minimumPurchase;
    if (restrictions !== undefined) coupon.restrictions = restrictions;
    if (isActive !== undefined) coupon.isActive = isActive;

    await coupon.save();

    res.status(200).json({
      success: true,
      message: "Coupon updated successfully",
      data: coupon
    });
  } catch (error) {
    console.error("Error updating coupon:", error);
    res.status(500).json({
      success: false,
      message: "Failed to update coupon",
      error: error.message
    });
  }
};

// Delete coupon
const deleteCoupon = async (req, res) => {
  try {
    const coupon = await Coupon.findById(req.params.id);

    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: "Coupon not found"
      });
    }

    // Check if coupon has been used
    if (coupon.currentUses > 0) {
      return res.status(400).json({
        success: false,
        message: "Cannot delete coupon that has been used"
      });
    }

    await Coupon.findByIdAndDelete(req.params.id);

    res.status(200).json({
      success: true,
      message: "Coupon deleted successfully"
    });
  } catch (error) {
    console.error("Error deleting coupon:", error);
    res.status(500).json({
      success: false,
      message: "Failed to delete coupon",
      error: error.message
    });
  }
};

// Toggle coupon status
const toggleCouponStatus = async (req, res) => {
  try {
    const coupon = await Coupon.findById(req.params.id);

    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: "Coupon not found"
      });
    }

    coupon.isActive = !coupon.isActive;
    await coupon.save();

    res.status(200).json({
      success: true,
      message: `Coupon ${coupon.isActive ? "activated" : "deactivated"} successfully`,
      data: coupon
    });
  } catch (error) {
    console.error("Error toggling coupon status:", error);
    res.status(500).json({
      success: false,
      message: "Failed to toggle coupon status",
      error: error.message
    });
  }
};

// Get coupon statistics
const getCouponStats = async (req, res) => {
  try {
    const [
      totalCoupons,
      activeCoupons,
      expiredCoupons,
      totalUses,
      mostUsedCoupons
    ] = await Promise.all([
      Coupon.countDocuments(),
      Coupon.countDocuments({ 
        isActive: true,
        validFrom: { $lte: new Date() },
        validUntil: { $gte: new Date() }
      }),
      Coupon.countDocuments({ validUntil: { $lt: new Date() } }),
      Coupon.aggregate([
        { $group: { _id: null, total: { $sum: "$currentUses" } } }
      ]),
      Coupon.find({ currentUses: { $gt: 0 } })
        .sort({ currentUses: -1 })
        .limit(5)
        .select("code name currentUses")
        .lean()
    ]);

    res.status(200).json({
      success: true,
      data: {
        totalCoupons,
        activeCoupons,
        expiredCoupons,
        totalUses: totalUses[0]?.total || 0,
        mostUsedCoupons
      }
    });
  } catch (error) {
    console.error("Error fetching coupon stats:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch coupon statistics",
      error: error.message
    });
  }
};

// Validate coupon code
const validateCouponCode = async (req, res) => {
  try {
    const { code, planId, clientId } = req.body;

    if (!code) {
      return res.status(400).json({
        success: false,
        message: "Coupon code is required"
      });
    }

    const coupon = await Coupon.findValidCoupon(code);
    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: "Invalid or expired coupon"
      });
    }

    let validationResult = {
      valid: true,
      coupon: {
        code: coupon.code,
        name: coupon.name,
        description: coupon.description,
        discountType: coupon.discountType,
        discountValue: coupon.discountValue,
        maxDiscount: coupon.maxDiscount
      }
    };

    // Check plan applicability if planId is provided
    if (planId) {
      const plan = await Plan.findById(planId);
      if (plan && !coupon.appliesToPlan(planId, plan.category)) {
        validationResult.valid = false;
        validationResult.message = "Coupon not applicable to this plan";
      } else if (plan) {
        validationResult.discount = coupon.calculateDiscount(plan.price);
        validationResult.finalPrice = plan.price - validationResult.discount;
      }
    }

    // Check user eligibility if clientId is provided
    if (clientId && validationResult.valid) {
      const canUse = await coupon.canBeUsedBy(clientId);
      if (!canUse.valid) {
        validationResult.valid = false;
        validationResult.message = canUse.reason;
      }
    }

    res.status(200).json({
      success: true,
      data: validationResult
    });
  } catch (error) {
    console.error("Error validating coupon code:", error);
    res.status(500).json({
      success: false,
      message: "Failed to validate coupon code",
      error: error.message
    });
  }
};

// Get coupon usage history
const getCouponUsageHistory = async (req, res) => {
  try {
    const { couponId } = req.params;
    const { page = 1, limit = 20 } = req.query;

    const coupon = await Coupon.findById(couponId)
      .populate("usageHistory.userId", "name email")
      .populate("usageHistory.planId", "name")
      .lean();

    if (!coupon) {
      return res.status(404).json({
        success: false,
        message: "Coupon not found"
      });
    }

    const total = coupon.usageHistory.length;
    const skip = (parseInt(page) - 1) * parseInt(limit);
    const paginatedHistory = coupon.usageHistory
      .sort((a, b) => new Date(b.usedAt) - new Date(a.usedAt))
      .slice(skip, skip + parseInt(limit));

    res.status(200).json({
      success: true,
      data: {
        coupon: {
          code: coupon.code,
          name: coupon.name,
          currentUses: coupon.currentUses,
          maxUses: coupon.maxUses
        },
        usageHistory: paginatedHistory,
        pagination: {
          page: parseInt(page),
          limit: parseInt(limit),
          total,
          pages: Math.ceil(total / parseInt(limit))
        }
      }
    });
  } catch (error) {
    console.error("Error fetching coupon usage history:", error);
    res.status(500).json({
      success: false,
      message: "Failed to fetch coupon usage history",
      error: error.message
    });
  }
};

// Bulk create coupons
const bulkCreateCoupons = async (req, res) => {
  try {
    const { coupons } = req.body;

    if (!Array.isArray(coupons) || coupons.length === 0) {
      return res.status(400).json({
        success: false,
        message: "Coupons array is required"
      });
    }

    const createdCoupons = [];
    const errors = [];

    for (const couponData of coupons) {
      try {
        // Check if coupon code already exists
        const existingCoupon = await Coupon.findOne({ 
          code: couponData.code.toUpperCase() 
        });
        
        if (existingCoupon) {
          errors.push({
            code: couponData.code,
            error: "Coupon code already exists"
          });
          continue;
        }

        const coupon = new Coupon({
          ...couponData,
          code: couponData.code.toUpperCase(),
          validFrom: new Date(couponData.validFrom),
          validUntil: new Date(couponData.validUntil),
          createdBy: req.user.id
        });

        await coupon.save();
        createdCoupons.push(coupon);
      } catch (error) {
        errors.push({
          code: couponData.code,
          error: error.message
        });
      }
    }

    res.status(200).json({
      success: true,
      message: `Created ${createdCoupons.length} coupons successfully`,
      data: {
        created: createdCoupons.length,
        errors: errors.length,
        errorDetails: errors
      }
    });
  } catch (error) {
    console.error("Error bulk creating coupons:", error);
    res.status(500).json({
      success: false,
      message: "Failed to bulk create coupons",
      error: error.message
    });
  }
};

module.exports = {
  createCoupon,
  getAllCoupons,
  getCouponById,
  updateCoupon,
  deleteCoupon,
  toggleCouponStatus,
  getCouponStats,
  validateCouponCode,
  getCouponUsageHistory,
  bulkCreateCoupons
};
