const express = require('express');
const { loginSuperadmin, registerSuperadmin } = require('../controllers/superadmincontroller');

const router = express.Router();

router.post('/login',loginSuperadmin);

router.post('/register',registerSuperadmin);

module.exports = router;