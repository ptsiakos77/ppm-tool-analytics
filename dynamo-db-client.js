const AWS = require('aws-sdk');
const AWSXRay = require('aws-xray-sdk');

require('dotenv').config();

AWS.config.update({
    region: process.env.AWS_REGION
});

const ddBClient = AWSXRay.captureAWSClient(new AWS.DynamoDB());
module.exports = ddBClient;
