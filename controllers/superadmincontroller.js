const bcrypt=require("bcrypt");
const jwt = require('jsonwebtoken')
const Superadmin = require("../models/Superadmin");

const generateToken = (id) => {
    return jwt.sign({id},process.env.JWT_SECRET,{
        expiresIn:"1d",
    })
};

exports.registerSuperadmin = async (req,res) => {
    try {
    const { name, email, password, superadmincode } = req.body;
    console.log(superadmincode)
    if (superadmincode !== process.env.SUPERADMIN_REGISTRATION_CODE) {
      res.status(401).json({ message: "Invalid superadmin code" });
    }

    const existingSuperadmin = await Superadmin.findOne({email});
    if(existingSuperadmin)
    {
      res.status(400).json({ message: "Superadmin already exists" });
    }
    else 
    {
      const hashedPassword = await bcrypt.hash(password,10);
      const superadmin = await Superadmin.create({
        name,
        email,
        password:hashedPassword
      })
      const token = await generateToken(superadmin._id);

      res.status(200).json({
        success:true,
        message:"superadmin registered successfully",
        token:token,
        superadmin
      })
    }
}
    catch (error) {
        res.status(400).json({ message: error });
        console.log(error);
    }
}

exports.loginSuperadmin = async (req,res) => {
    try {
    const {email,password}=req.body;

    if(!email || !password)
    {
        return res.status(400).json({
        message: "Email and password are required",
        received: { email: !!email, password: !!password }        
    });
    }

    const superadmin = await Superadmin.findOne({email});

    console.log('Found superadmin:', superadmin ? 'Yes' : 'No');

    if(!superadmin) {
        return res.status(400).json({message:"Superadmin not found"});
    }

    const isPasswordValid = await bcrypt.compare(password,superadmin.password);

    if(!isPasswordValid)
    {
        res.status(400).json({message:"Invalid Credentials"})
    }
    const token = generateToken(superadmin._id);

    res.status(200).json({
        success:true,
        message:"login successfully",
        token:token,
        superadmin
    })
 
    } 
    catch (error) {
    res.status(500).json({message: "Internal server error"});
    console.log(error)
    }
}