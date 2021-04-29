# lifion-kinesis

[![npm version](https://badge.fury.io/js/lifion-kinesis.svg)](http://badge.fury.io/js/lifion-kinesis)

Lifion's Node.js client for [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/).

## Getting Started

To install the module:

```sh
npm install lifion-kinesis --save
```

The main module export is a Kinesis class that instantiates as a [readable stream](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams).

```js
const Kinesis = require('lifion-kinesis');

const kinesis = new Kinesis({
  streamName: 'sample-stream'
  /* other options from AWS.Kinesis */
});
kinesis.on('data', data => {
  console.log('Incoming data:', data);
});
kinesis.startConsumer();
```

To take advantage of back-pressure, the client can be piped to a writable stream:

```js
const Kinesis = require('lifion-kinesis');
const { pipeline } = require('stream');

pipeline([
  new Kinesis(/* options */),
  new Writable({
    objectMode: true,
    write(data, encoding, callback) {
      console.log(data);
      callback();
    }
  })
]);
```

## Features

- Standard [Node.js stream abstraction](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_stream) of Kinesis streams.
- Node.js implementation of the new enhanced fan-out feature.
- Optional auto-creation, encryption, and tagging of Kinesis streams.
- Support for a polling mode, using the [`GetRecords` API](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html), with automatic checkpointing.
- Support for multiple concurrent consumers through automatic assignment of shards.
- Support for sending messages to streams, with auto-retries.

## API Reference


* [lifion-kinesis](#module_lifion-kinesis)
    * [Kinesis](#exp_module_lifion-kinesis--Kinesis) ⇐ [<code>PassThrough</code>](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough) ⏏
        * [new Kinesis(options)](#new_module_lifion-kinesis--Kinesis_new)
        * _instance_
            * [.startConsumer()](#module_lifion-kinesis--Kinesis+startConsumer) ⇒ <code>Promise</code>
            * [.stopConsumer()](#module_lifion-kinesis--Kinesis+stopConsumer)
            * [.putRecord(params)](#module_lifion-kinesis--Kinesis+putRecord) ⇒ <code>Promise</code>
            * [.listShards(params)](#module_lifion-kinesis--Kinesis+listShards) ⇒ <code>Promise</code>
            * [.putRecords(params)](#module_lifion-kinesis--Kinesis+putRecords) ⇒ <code>Promise</code>
            * [.getStats()](#module_lifion-kinesis--Kinesis+getStats) ⇒ <code>object</code>
        * _static_
            * [.getStats()](#module_lifion-kinesis--Kinesis.getStats) ⇒ <code>object</code>

<a name="exp_module_lifion-kinesis--Kinesis"></a>

### Kinesis ⇐ [<code>PassThrough</code>](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough) ⏏
A [pass-through stream](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough) class specialization implementing a consumer
of Kinesis Data Streams using the [AWS SDK for JavaScript](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest). Incoming
data can be retrieved through either the `data` event or by piping the instance to other streams.

**Kind**: Exported class  
**Extends**: [<code>PassThrough</code>](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough)  
<a name="new_module_lifion-kinesis--Kinesis_new"></a>

#### new Kinesis(options)
Initializes a new instance of the Kinesis client.


| Param | Type | Default | Description |
| --- | --- | --- | --- |
| options | <code>object</code> |  | The initialization options. In addition to the below options, it        can also contain any of the [`AWS.Kinesis` options](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property). |
| [options.compression] | <code>string</code> |  | The kind of data compression to use with records.        The currently available compression options are either `"LZ-UTF8"` or none. |
| [options.consumerGroup] | <code>string</code> |  | The name of the group of consumers in which shards        will be distributed and checkpoints will be shared. If not provided, it defaults to        the name of the application/project using this module. |
| [options.createStreamIfNeeded] | <code>boolean</code> | <code>true</code> | Whether if the Kinesis stream should        be automatically created if it doesn't exist upon connection |
| [options.dynamoDb] | <code>object</code> | <code>{}</code> | The initialization options for the DynamoDB client        used to store the state of the consumers. In addition to `tableNames` and `tags`, it        can also contain any of the [`AWS.DynamoDB` options](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#constructor-property). |
| [options.dynamoDb.tableName] | <code>string</code> |  | The name of the table in which to store the        state of consumers. If not provided, it defaults to "lifion-kinesis-state". |
| [options.dynamoDb.tags] | <code>object</code> |  | If provided, the client will ensure that the        DynamoDB table where the state is stored is tagged with these tags. If the table        already has tags, they will be merged. |
| [options.encryption] | <code>object</code> |  | The encryption options to enforce in the stream. |
| [options.encryption.type] | <code>string</code> |  | The encryption type to use. |
| [options.encryption.keyId] | <code>string</code> |  | The GUID for the customer-managed AWS KMS key        to use for encryption. This value can be a globally unique identifier, a fully        specified ARN to either an alias or a key, or an alias name prefixed by "alias/". |
| [options.limit] | <code>number</code> | <code>10000</code> | The limit of records per get records call (only        applicable with `useEnhancedFanOut` is set to `false`) |
| [options.logger] | <code>object</code> |  | An object with the `warn`, `debug`, and `error` functions        that will be used for logging purposes. If not provided, logging will be omitted. |
| [options.maxEnhancedConsumers] | <code>number</code> | <code>5</code> | An option to set the number of enhanced        fan-out consumer ARNs that the module should initialize. Defaults to 5.        Providing a number above the AWS limit (20) or below 1 will result in using the default. |
| [options.noRecordsPollDelay] | <code>number</code> | <code>1000</code> | The delay in milliseconds before        attempting to get more records when there were none in the previous attempt (only        applicable when `useEnhancedFanOut` is set to `false`) |
| [options.pollDelay] | <code>number</code> | <code>250</code> | When the `usePausedPolling` option is `false`, this        option defines the delay in milliseconds in between poll requests for more records        (only applicable when `useEnhancedFanOut` is set to `false`) |
| [options.s3] | <code>object</code> | <code>{}</code> | The initialization options for the S3 client used        to store large items in buckets. In addition to `bucketName` and `endpoint`, it        can also contain any of the [`AWS.S3` options](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#constructor-property). |
| [options.s3.bucketName] | <code>string</code> |  | The name of the bucket in which to store        large messages. If not provided, it defaults to the name of the Kinesis stream. |
| [options.s3.largeItemThreshold] | <code>number</code> | <code>900</code> | The size in KB above which an item        should automatically be stored in s3. |
| [options.s3.nonS3Keys] | <code>Array.&lt;string&gt;</code> | <code>[]</code> | If the `useS3ForLargeItems` option is set to        `true`, the `nonS3Keys` option lists the keys that will be sent normally on the kinesis record. |
| [options.s3.tags] | <code>string</code> |  | If provided, the client will ensure that the        S3 bucket is tagged with these tags. If the bucket already has tags, they will be merged. |
| [options.shardCount] | <code>number</code> | <code>1</code> | The number of shards that the newly-created stream        will use (if the `createStreamIfNeeded` option is set) |
| [options.shouldDeaggregate] | <code>string</code> \| <code>boolean</code> | <code>&quot;auto&quot;</code> | Whethe the method retrieving the records             should expect aggregated records and deaggregate them appropriately. |
| [options.shouldParseJson] | <code>string</code> \| <code>boolean</code> | <code>&quot;auto&quot;</code> | Whether if retrieved records' data should be parsed as JSON or not.        Set to "auto" to only attempt parsing if data looks like JSON. Set to true to force data parse. |
| [options.statsInterval] | <code>number</code> | <code>30000</code> | The interval in milliseconds for how often to        emit the "stats" event. The event is only available while the consumer is running. |
| options.streamName | <code>string</code> |  | The name of the stream to consume data from (required) |
| [options.supressThroughputWarnings] | <code>boolean</code> | <code>false</code> | Set to `true` to make the client        log ProvisionedThroughputExceededException as debug rather than warning. |
| [options.tags] | <code>object</code> |  | If provided, the client will ensure that the stream is tagged        with these tags upon connection. If the stream is already tagged, the existing tags        will be merged with the provided ones before updating them. |
| [options.useAutoCheckpoints] | <code>boolean</code> | <code>true</code> | Set to `true` to make the client        automatically store shard checkpoints using the sequence number of the most-recently        received record. If set to `false` consumers can use the `setCheckpoint()` function to        store any sequence number as the checkpoint for the shard. |
| [options.useAutoShardAssignment] | <code>boolean</code> | <code>true</code> | Set to `true` to automatically assign        the stream shards to the active consumers in the same group (so only one client reads      from one shard at the same time). Set to `false` to make the client read from all shards. |
| [options.useEnhancedFanOut] | <code>boolean</code> | <code>false</code> | Set to `true` to make the client use        enhanced fan-out consumers to read from shards. |
| [options.usePausedPolling] | <code>boolean</code> | <code>false</code> | Set to `true` to make the client not to        poll for more records until the consumer calls `continuePolling()`. This option is        useful when consumers want to make sure the records are fully processed before        receiving more (only applicable when `useEnhancedFanOut` is set to `false`) |
| [options.useS3ForLargeItems] | <code>boolean</code> | <code>false</code> | Whether to automatically use an S3        bucket to store large items or not. |
| [options.leaseAcquisitionInterval] | <code>number</code> | <code>20000</code> | The interval in milliseconds for how often to        attempt lease acquisitions. |
| [options.leaseAcquisitionRecoveryInterval] | <code>number</code> | <code>5000</code> | The interval in milliseconds for how often        to re-attempt lease acquisitions when an error is returned from aws. |

<a name="module_lifion-kinesis--Kinesis+startConsumer"></a>

#### kinesis.startConsumer() ⇒ <code>Promise</code>
Starts the stream consumer, by ensuring that the stream exists, that it's ready, and
configured as requested. The internal managers that deal with heartbeats, state, and
consumers will also be started.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: <code>undefined</code> - Once the consumer has successfully started.  
**Reject**: <code>Error</code> - On any unexpected error while trying to start.  
<a name="module_lifion-kinesis--Kinesis+stopConsumer"></a>

#### kinesis.stopConsumer()
Stops the stream consumer. The internal managers will also be stopped.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
<a name="module_lifion-kinesis--Kinesis+putRecord"></a>

#### kinesis.putRecord(params) ⇒ <code>Promise</code>
Writes a single data record into a stream.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: <code>Object</code> - The de-serialized data returned from the request.  
**Reject**: <code>Error</code> - On any unexpected error while writing to the stream.  

| Param | Type | Description |
| --- | --- | --- |
| params | <code>object</code> | The parameters. |
| params.data | <code>\*</code> | The data to put into the record. |
| [params.explicitHashKey] | <code>string</code> | The hash value used to explicitly determine the        shard the data record is assigned to by overriding the partition key hash. |
| [params.partitionKey] | <code>string</code> | Determines which shard in the stream the data record        is assigned to. If omitted, it will be calculated based on a SHA-1 hash of the data. |
| [params.sequenceNumberForOrdering] | <code>string</code> | Set this to the sequence number obtained        from the last put record operation to guarantee strictly increasing sequence numbers,        for puts from the same client and to the same partition key. If omitted, records are        coarsely ordered based on arrival time. |
| [params.streamName] | <code>string</code> | If provided, the record will be put into the specified        stream instead of the stream name provided during the consumer instantiation. |

<a name="module_lifion-kinesis--Kinesis+listShards"></a>

#### kinesis.listShards(params) ⇒ <code>Promise</code>
List the shards of a stream.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: <code>Object</code> - The de-serialized data returned from the request.  
**Reject**: <code>Error</code> - On any unexpected error while writing to the stream.  

| Param | Type | Description |
| --- | --- | --- |
| params | <code>object</code> | The parameters. |
| [params.streamName] | <code>string</code> | If provided, the method will list the shards of the        specific stream instead of the stream name provided during the consumer instantiation. |

<a name="module_lifion-kinesis--Kinesis+putRecords"></a>

#### kinesis.putRecords(params) ⇒ <code>Promise</code>
Writes multiple data records into a stream in a single call.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: <code>Object</code> - The de-serialized data returned from the request.  
**Reject**: <code>Error</code> - On any unexpected error while writing to the stream.  

| Param | Type | Description |
| --- | --- | --- |
| params | <code>object</code> | The parameters. |
| params.records | <code>Array.&lt;object&gt;</code> | The records associated with the request. |
| params.records[].data | <code>\*</code> | The record data. |
| [params.records[].explicitHashKey] | <code>string</code> | The hash value used to explicitly        determine the shard the data record is assigned to by overriding the partition key hash. |
| [params.records[].partitionKey] | <code>string</code> | Determines which shard in the stream the        data record is assigned to. If omitted, it will be calculated based on a SHA-1 hash        of the data. |
| [params.streamName] | <code>string</code> | If provided, the record will be put into the specified        stream instead of the stream name provided during the consumer instantiation. |

<a name="module_lifion-kinesis--Kinesis+getStats"></a>

#### kinesis.getStats() ⇒ <code>object</code>
Returns statistics for the instance of the client.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Returns**: <code>object</code> - An object with the statistics.  
<a name="module_lifion-kinesis--Kinesis.getStats"></a>

#### Kinesis.getStats() ⇒ <code>object</code>
Returns the aggregated statistics of all the instances of the client.

**Kind**: static method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Returns**: <code>object</code> - An object with the statistics.  

## License

[MIT](./LICENSE)
