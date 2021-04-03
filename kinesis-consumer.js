'use strict';

require('dotenv').config();
const kcl = require('aws-kcl');
const ddBClient = require('./dynamo-db-client');
const log = require('./logger');

function recordProcessor() {
    let shardId;

    return {

        initialize: (initializeInput, completeCallback) => {
            shardId = initializeInput.shardId;
            completeCallback();
        },

        processRecords: (processRecordsInput, completeCallback) => {
            if (!processRecordsInput || !processRecordsInput.records) {
                completeCallback();
                return;
            }
            const records = processRecordsInput.records;
            let record, data, sequenceNumber, partitionKey;
            for (let i = 0; i < records.length; ++i) {
                record = records[i];
                data = new Buffer(record.data, 'base64').toString();
                sequenceNumber = record.sequenceNumber;
                partitionKey = record.partitionKey;
                const Item = {
                    userId: partitionKey, ...JSON.parse(data)
                };
                const params = {
                    TableName: process.env.DYNAMO_DB_TABLE,
                    Item
                };
                ddBClient.put(params, function(err, data) {
                    if (err) {
                        log.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2));
                    } else {
                        log.info('Added item:', JSON.stringify(data, null, 2));
                    }
                });
            }
            if (!sequenceNumber) {
                completeCallback();
                return;
            }
            // If checkpointing, completeCallback should only be called once checkpoint is complete.
            processRecordsInput.checkpointer.checkpoint(sequenceNumber, (err, sequenceNumber) => {
                log.info(`Checkpoint successful. ShardID: ${shardId}, SequenceNumber: ${sequenceNumber}`);
                completeCallback();
            });
        },

        leaseLost: (leaseLostInput, completeCallback) => {
            log.error(`'Lease was lost for ShardId: ${shardId}'`);
            completeCallback();
        },

        shardEnded: (shardEndedInput, completeCallback) => {
            log.info(`ShardId: ${shardId} has ended. Will checkpoint now.`);
            // eslint-disable-next-line handle-callback-err
            shardEndedInput.checkpointer.checkpoint(err => {
                completeCallback();
            });
        },

        shutdownRequested: (shutdownRequestedInput, completeCallback) => {
            // eslint-disable-next-line handle-callback-err
            shutdownRequestedInput.checkpointer.checkpoint(err => {
                completeCallback();
            });
        }
    };
}

kcl(recordProcessor()).run();
