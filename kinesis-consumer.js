'use strict';

require('dotenv').config()
const kcl = require('aws-kcl');

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
                console.log(`ShardID: ${shardId}, Record: ${data}, SequenceNumber:${sequenceNumber}, PartitionKey: ${partitionKey}`)
            }
            if (!sequenceNumber) {
                completeCallback();
                return;
            }
            // If checkpointing, completeCallback should only be called once checkpoint is complete.
            processRecordsInput.checkpointer.checkpoint(sequenceNumber, (err, sequenceNumber) => {
                console.log(`Checkpoint successful. ShardID: ${shardId}, SequenceNumber: ${sequenceNumber}`)
                completeCallback();
            });
        },

        leaseLost: (leaseLostInput, completeCallback) => {
            console.log(`'Lease was lost for ShardId: ${shardId}'`)
            completeCallback();
        },

        shardEnded: (shardEndedInput, completeCallback) => {
            console.log(`ShardId: ${shardId} has ended. Will checkpoint now.`)
            shardEndedInput.checkpointer.checkpoint(err => {
                completeCallback();
            });
        },

        shutdownRequested: (shutdownRequestedInput, completeCallback) => {
            shutdownRequestedInput.checkpointer.checkpoint(err => {
                completeCallback();
            });
        }
    };
}

kcl(recordProcessor()).run();
