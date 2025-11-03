const axios = require('axios');
const telephony = require('../config/telephony');
let sanpbxToken = null;

exports.callCzentrix = async ({ phone, agent, contact, campaignId, uniqueid }) => {
  const body = {
    transaction_id: "CTI_BOT_DIAL",
    phone_num: phone,
    uniqueid,
    callerid: agent.callerId,
    uuid: campaignId,
    custom_param: {
      a: contact.name,
      uniqueid
    },
    resFormat: 3
  };
  const headers = {
    "X-CLIENT": telephony.czentrix.client,
    "X-API-KEY": telephony.czentrix.apiKey,
    "Content-Type": "application/json"
  };
  const resp = await axios.post(telephony.czentrix.url, body, { headers });
  return resp.data;
};

exports.getSanpbxToken = async () => {
  if (sanpbxToken) return sanpbxToken;
  const resp = await axios.post(telephony.sanpbx.gentokenUrl, { access_key: "mob" }, {
    headers: { Accesstoken: process.env.SANPBX_ACCESS_TOKEN }
  });
  sanpbxToken = resp.data.Apittoken;
  return sanpbxToken;
};

exports.callSanpbx = async ({ phone, agent, contact, uniqueid }) => {
  const apiToken = await exports.getSanpbxToken();
  const body = {
    appid: 3,
    call_to: phone,
    custom_field: { uniqueid, name: contact.name },
    caller_id: agent.callerId
  };
  const headers = { Apitoken: apiToken };
  const resp = await axios.post(telephony.sanpbx.dialcallUrl, body, { headers });
  return resp.data;
};
