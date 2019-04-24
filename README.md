# lifion-kinesis

Lifion's Node.js client for [Amazon Kinesis Data Streams](https://aws.amazon.com/kinesis/data-streams/).

## Getting Started

To install the module:

```sh
npm install lifion-kinesis --save
```

The main module export is a Kinesis class that instantiates as a [readable stream](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams).

```js
const Kinesis = require('lifion-kinesis');

const client = new Kinesis({
  streamName: 'sample-stream',
  consumerName: 'sample-consumer'
  /* other options from AWS.Kinesis */
});
client.on('data', chunk => {
  console.log('Incoming data:', chunk);
});
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

**Incoming Features:**

- Support for a polling mode, using the [`GetRecords` API](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html), with automatic checkpointing.
- Support for multiple concurrent consumers through automatic assignment of shards.
- Support for sending messages to streams, with auto-retries.

## API Reference

- [lifion-kinesis](#module_lifion-kinesis)
  - [Kinesis](#exp_module_lifion-kinesis--Kinesis) ⇐ <code>external:Readable</code> ⏏
    - [new Kinesis(options)](#new_module_lifion-kinesis--Kinesis_new)
    - _instance_
      - [.startConsumer()](#module_lifion-kinesis--Kinesis+startConsumer) ⇒ <code>Promise</code>
      - [.putRecord(params)](#module_lifion-kinesis--Kinesis+putRecord) ⇒ <code>Promise</code>
      - [.putRecords(params)](#module_lifion-kinesis--Kinesis+putRecords) ⇒ <code>Promise</code>
    - _inner_
      - [~setUpEnhancedConsumers(instance)](#module_lifion-kinesis--Kinesis..setUpEnhancedConsumers) ⇒ <code>Promise</code>

<a name="exp_module_lifion-kinesis--Kinesis"></a>

### Kinesis ⇐ <code>external:Readable</code> ⏏

A [pass-through stream](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_class_stream_passthrough) class specialization implementing a
consumer of Kinesis Data Streams using the [AWS SDK for JavaScript](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest).
Incoming data can be retrieved through either the `data` event or by piping the instance to a
writable stream.

**Kind**: Exported class  
**Extends**: <code>external:Readable</code>  
<a name="new_module_lifion-kinesis--Kinesis_new"></a>

#### new Kinesis(options)

Initializes a new instance of the Kinesis client.

| Param                            | Type                 | Default            | Description                                                                                                                                                                                                                                                                           |
| -------------------------------- | -------------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| options                          | <code>Object</code>  |                    | The initialization options. In addition to the below options, it can also contain any of the [`AWS.Kinesis` options](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property).                                                                      |
| [options.compression]            | <code>string</code>  |                    | The kind of data compression to use with records. The currently available compression options are either `"LZ-UTF8"` or none.                                                                                                                                                         |
| [options.consumerGroup]          | <code>string</code>  |                    | The name of the group of consumers in which shards will be distributed and checkpoints will be shared. If not provided, it defaults to the name of the application/project using this module.                                                                                         |
| [options.createStreamIfNeeded]   | <code>boolean</code> | <code>true</code>  | Whether if the Kinesis stream should be automatically created if it doesn't exist upon connection                                                                                                                                                                                     |
| [options.dynamoDb]               | <code>Object</code>  | <code>{}</code>    | The initialization options for the DynamoDB client used to store the state of the stream consumers. In addition to `tableNames` and `tags`, it can also contain any of the [`AWS.DynamoDB` options](AwsJsSdkDynamoDb).                                                                |
| [options.dynamoDb.tableName]     | <code>string</code>  |                    | The name of the table in which to store the state of consumers. If not provided, it defaults to "lifion-kinesis-state".                                                                                                                                                               |
| [options.dynamoDb.tags]          | <code>Object</code>  |                    | If provided, the client will ensure that the DynamoDB table where the state is stored is tagged with these tags. If the table already has tags, they will be merged.                                                                                                                  |
| [options.encryption]             | <code>Object</code>  |                    | The encryption options to enforce in the stream.                                                                                                                                                                                                                                      |
| [options.encryption.type]        | <code>string</code>  |                    | The encryption type to use.                                                                                                                                                                                                                                                           |
| [options.encryption.keyId]       | <code>string</code>  |                    | The GUID for the customer-managed AWS KMS key to use for encryption. This value can be a globally unique identifier, a fully specified ARN to either an alias or a key, or an alias name prefixed by "alias/".                                                                        |
| [options.logger]                 | <code>Object</code>  |                    | An object with the `warn`, `debug`, and `error` functions that will be used for logging purposes. If not provided, logging will be omitted.                                                                                                                                           |
| [options.noRecordsPollDelay]     | <code>number</code>  | <code>1000</code>  | The delay in milliseconds before attempting to get more records when there were none in the previous attempt (only applicable when `useEnhancedFanOut` is set to `false`)                                                                                                             |
| [options.pollDelay]              | <code>number</code>  | <code>250</code>   | When the `usePausedPolling` option is `false`, this option defines the delay in milliseconds in between poll requests for more records (only applicable when `useEnhancedFanOut` is set to `false`)                                                                                   |
| [options.shardCount]             | <code>number</code>  | <code>1</code>     | The number of shards that the newly-created stream will use (if the `createStreamIfNeeded` option is set)                                                                                                                                                                             |
| options.streamName               | <code>string</code>  |                    | The name of the stream to consume data from (required)                                                                                                                                                                                                                                |
| [options.tags]                   | <code>Object</code>  |                    | If provided, the client will ensure that the stream is tagged with these tags upon connection. If the stream is already tagged, the existing tags will be merged with the provided ones before updating them.                                                                         |
| [options.useAutoCheckpoints]     | <code>boolean</code> | <code>true</code>  | Set to `true` to make the client automatically store shard checkpoints using the sequence number of the most-recently received record. If set to `false` consumers can use the `setCheckpoint()` function to store any sequence number as the checkpoint for the shard.               |
| [options.useAutoShardAssignment] | <code>boolean</code> | <code>true</code>  | Set to `true` to automatically assign the stream shards to the active consumers in the same group (so only one client reads from one shard at the same time). Set to `false` to make the client read from all shards.                                                                 |
| [options.useEnhancedFanOut]      | <code>boolean</code> | <code>false</code> | Set to `true` to make the client use enhanced fan-out consumers to read from shards.                                                                                                                                                                                                  |
| [options.usePausedPolling]       | <code>boolean</code> | <code>false</code> | Set to `true` to make the client not to poll for more records until the consumer calls `continuePolling()`. This option is useful when consumers want to make sure the records are fully processed before receiving more (only applicable when `useEnhancedFanOut` is set to `false`) |

<a name="module_lifion-kinesis--Kinesis+startConsumer"></a>

#### kinesis.startConsumer() ⇒ <code>Promise</code>

Initializes the client, by ensuring that the stream exists, it's ready, and configured as
requested. The internal managers that deal with heartbeats, state, and consumers will also
be started.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: Once the client has successfully started.  
**Reject**: <code>Error</code> - On any unexpected error while trying to start.  
<a name="module_lifion-kinesis--Kinesis+putRecord"></a>

#### kinesis.putRecord(params) ⇒ <code>Promise</code>

Puts a record to a stream.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: If record is successfully pushed to stream.  
**Reject**: <code>Error</code> - On any unexpected error while pushing to stream.

| Param               | Type                                       | Description                                                                                                                                                                                                                                                   |
| ------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| params              | <code>Object</code>                        | The putRecord parameters. In addition to the params described here, uses [`AWS.Kinesis.putRecord` parameters](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#putRecord-property) from the `AWS.Kinesis.putRecord` method in camel case. |
| params.data         | <code>Object</code> \| <code>string</code> | The data to be used as the Kinesis message.                                                                                                                                                                                                                   |
| [params.streamName] | <code>string</code>                        | If provided, overrides the stream name provided on client instantiation.                                                                                                                                                                                      |

<a name="module_lifion-kinesis--Kinesis+putRecords"></a>

#### kinesis.putRecords(params) ⇒ <code>Promise</code>

Batch puts multiple records to a stream.

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: If records are successfully pushed to stream.  
**Reject**: <code>Error</code> - On any unexpected error while pushing to stream.

| Param               | Type                                       | Description                                                                                                                                                                                                                                                       |
| ------------------- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| params              | <code>Object</code>                        | The putRecords parameters. In addition to the params described here, uses [`AWS.Kinesis.putRecords` parameters](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#putRecords-property) from the `AWS.Kinesis.putRecords` method in camel case. |
| params.records      | <code>Array</code>                         | A list of records to push to a Kinesis stream.                                                                                                                                                                                                                    |
| params.records.data | <code>Object</code> \| <code>string</code> | The data to be used as the Kinesis message.                                                                                                                                                                                                                       |
| [params.streamName] | <code>string</code>                        | If provided, overrides the stream name provided on client instantiation.                                                                                                                                                                                          |

<a name="module_lifion-kinesis--Kinesis..setUpEnhancedConsumers"></a>

#### Kinesis~setUpEnhancedConsumers(instance) ⇒ <code>Promise</code>

If the `useEnhancedFanOut` option is enabled, this function will be called to prepare for the
automated distribution of the Amazon-registered enhanced fan-out consumers into the consumers
of this module on the same consumer group. The preparation consist in the pre-registration of
the maximum allowed number of enhanced fan-out consumers in Amazon, and also in making sure
that the state of the stream, in DynamoDB, reflects the existing enhanced fan-out consumers.
Stale state will be removed, existing enhanced fan-out consumers not registered by this module
will be preserved and used in the automated distribution.

**Kind**: inner method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)

| Param    | Type                | Description                                       |
| -------- | ------------------- | ------------------------------------------------- |
| instance | <code>Object</code> | A reference to the instance of the Kinesis class. |

## License

[MIT](./LICENSE)
