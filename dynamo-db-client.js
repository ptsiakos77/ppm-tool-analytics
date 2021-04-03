const AWS = require('aws-sdk');
require('dotenv').config();

AWS.config.update({
    region: process.env.AWS_REGION
});

const ddBClient = new AWS.DynamoDB.DocumentClient();

module.exports = ddBClient;
