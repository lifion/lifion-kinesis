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
  - [Kinesis](#exp_module_lifion-kinesis--Kinesis) ⇐ [<code>Readable</code>](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams) ⏏
    - [new Kinesis(options)](#new_module_lifion-kinesis--Kinesis_new)
    - [.connect()](#module_lifion-kinesis--Kinesis+connect) ⇒ <code>Promise</code>

<a name="exp_module_lifion-kinesis--Kinesis"></a>

### Kinesis ⇐ [<code>Readable</code>](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams) ⏏

A specialization of the [readable stream](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams) class implementing a
consumer of Kinesis Data Streams using the
[enhanced fan-out feature](https://docs.aws.amazon.com/streams/latest/dev/introduction-to-enhanced-consumers.html). Upon connection, instances of this
class will subscribe to receive data from all the shards of the given stream. Incoming data can
be retrieved through either the `data` event or by piping the instance to a writable stream.

**Kind**: Exported class  
**Extends**: [<code>Readable</code>](https://nodejs.org/dist/latest-v10.x/docs/api/stream.html#stream_readable_streams)  
<a name="new_module_lifion-kinesis--Kinesis_new"></a>

#### new Kinesis(options)

Initializes a new instance of the Kinesis client.

| Param                          | Type                 | Default            | Description                                                                                                                                                                                                                                                    |
| ------------------------------ | -------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| options                        | <code>Object</code>  |                    | The initialization options. In addition to the below options, this object can also contain the [`AWS.Kinesis` options](https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html#constructor-property).                                             |
| [options.compression]          | <code>string</code>  |                    | The kind of data compression to use with records. The currently available compression options are either `"LZ-UTF8"` or none.                                                                                                                                  |
| options.consumerName           | <code>string</code>  |                    | The unique name of the consumer for the given stream. This option is required.                                                                                                                                                                                 |
| [options.createStreamIfNeeded] | <code>boolean</code> | <code>false</code> | Whether if the Kinesis stream should be created if it doesn't exist upon connection.                                                                                                                                                                           |
| [options.compression]          | <code>Object</code>  |                    | The kind of compression to enforce in the stream.                                                                                                                                                                                                              |
| [options.compression.type]     | <code>string</code>  |                    | The encryption type to use.                                                                                                                                                                                                                                    |
| [options.compression.keyId]    | <code>string</code>  |                    | The GUID for the customer-managed AWS KMS key to use for encryption. This value can be a globally unique identifier, a fully specified ARN to either an alias or a key, or an alias name prefixed by "alias/".                                                 |
| [options.logger]               | <code>Object</code>  |                    | An object with the `warn`, `debug`, and `error` functions that will be used for logging purposes. If not provided, logging will be omitted.                                                                                                                    |
| [options.shardCount]           | <code>number</code>  | <code>1</code>     | The number of shards that the newly-created stream will use (if the `createStreamIfNeeded` option is set).                                                                                                                                                     |
| options.streamName             | <code>string</code>  |                    | The name of the stream to consume data from. This option is required.                                                                                                                                                                                          |
| [options.tags]                 | <code>Object</code>  |                    | If provided, the client will ensure that the stream is tagged with these hash of tags upon connection. If the stream is already tagged same tag keys, they won't be overriden. If the stream is already tagged with different tag keys, they won't be removed. |

<a name="module_lifion-kinesis--Kinesis+connect"></a>

#### kinesis.connect() ⇒ <code>Promise</code>

Initializes the Kinesis client, then it proceeds to:

1. Create the stream if asked for.
2. Ensure that the stream is active.
3. Ensure that the stream is encrypted as indicated.
4. Ensure that the stream is tagged as requested.
5. Ensure an enhanced fan-out consumer with the given name exists.
6. Ensure that the enhanced fan-out consumer is active.
7. A subscription for data is issued to all the shard in the stream.
8. Data will then be available in both [stream read modes](external:readModes).

**Kind**: instance method of [<code>Kinesis</code>](#exp_module_lifion-kinesis--Kinesis)  
**Fulfil**: Once the stream is active, encrypted, tagged, the enhanced fan-out consumer is active,
and the client is subscribed to the data in all the stream shards.  
**Reject**: <code>Error</code> - If at least one of the above steps fails to succeed.

## License

[MIT](./LICENSE)
