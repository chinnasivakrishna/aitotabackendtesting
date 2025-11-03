const ApiKey = require('../models/ApiKey');

const plainKey = 'ak_k5oc5g5R5QQ6GvfntiwYcn';
const encrypted = ApiKey.encryptKey(plainKey);

console.log('Encrypted LMNT Key:', encrypted); 