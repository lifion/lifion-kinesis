'use strict';

/*!
 * Code adpted from aws-kinesis-agg 4.0.4
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

const ProtoBuf = require('protobufjs');
const aggJson = require('./aggregate-protobuf.json');

const KPL_MAGIC_NUMBER = 'f3899ac2';
const MESSAGE_NAME = 'AggregatedRecord';

const builder = ProtoBuf.Root.fromJSON(aggJson).lookupType(MESSAGE_NAME);

/** synchronous deaggregation interface */

/**
 * asynchronous deaggregation interface
 *
 * @param {object} kinesisRecord - The kinesis message
 * @param {Function} perRecordCallback - A callback invoked for each deaggregated record
 * @param {Function} afterRecordCallback - A callback invoked after all records have been deaggregated
 */
const deaggregate = (kinesisRecord, perRecordCallback, afterRecordCallback) => {
  // we receive the record data as a base64 encoded string
  const {
    ApproximateArrivalTimestamp,
    Data,
    ExplicitPartitionKey,
    PartitionKey,
    SequenceNumber
  } = kinesisRecord;
  const recordBuffer = Buffer.from(Data, 'base64');

  // first 4 bytes are the Kinesis Producer Library assigned magic number
  // https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
  if (recordBuffer.slice(0, 4).toString('hex') === KPL_MAGIC_NUMBER) {
    // decode the protobuf binary from byte offset 4 to length-16 (last
    // 16 are checksum)
    const protobufMessage = builder.decode(recordBuffer.slice(4, -16));

    // iterate over each User Record in order
    for (let i = 0; i < protobufMessage.records.length; i += 1) {
      const item = protobufMessage.records[i];

      // emit the per-record callback with the extracted partition
      // keys and sequence information
      perRecordCallback(null, {
        ApproximateArrivalTimestamp,
        Data: item.data,
        ExplicitPartitionKey: protobufMessage.explicit_hash_key_table[item.explicit_hash_key_index],
        PartitionKey: protobufMessage.partition_key_table[item.partition_key_index],
        SequenceNumber,
        SubSequenceNumber: i
      });
    }

    // finished processing the kinesis record
    afterRecordCallback();
  } else {
    // not a KPL encoded message - no biggie - emit the record with
    // the same interface as if it was. Customers can differentiate KPL
    // user records vs plain Kinesis Records on the basis of the
    // sub-sequence number

    perRecordCallback(null, {
      ApproximateArrivalTimestamp,
      Data,
      ExplicitPartitionKey,
      PartitionKey,
      SequenceNumber
    });
    afterRecordCallback();
  }
};

module.exports = (kinesisRecords) => {
  const userRecords = [];
  return new Promise((resolve) => {
    // use the async deaggregation interface, and accumulate user records into
    // the userRecords array

    if (!kinesisRecords || kinesisRecords.length === 0) {
      resolve(kinesisRecords);
      return;
    }
    for (let n = 0, completionCount = 0; n < kinesisRecords.length; n += 1) {
      const kinesisRecord = kinesisRecords[n];
      deaggregate(
        kinesisRecord,
        (err, userRecord) => {
          userRecords.push(userRecord);
        },
        () => {
          completionCount += 1;
          if (completionCount >= kinesisRecords.length) {
            resolve(userRecords);
          }
        }
      );
    }
  });
};
