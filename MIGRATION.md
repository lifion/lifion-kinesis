# Migrating to v2

v2 raises the Node version, swaps in the AWS SDK v3, and ships as an ES module. The public API is the same as v1, so most of the upgrade comes down to the Node version you run and how you import the client.

## Node.js 22.12 or newer

v2 requires Node.js `>=22.12.0`. Older versions are no longer supported. If you can't move off an older Node yet, stay on v1 until you can.

## ESM-only package

v2 ships as an ES module. How you load it depends on your project.

From an ES module, use a default import:

```js
import Kinesis from 'lifion-kinesis';
```

From CommonJS, `require` still works on Node 22.12+, but it hands back the module namespace, so reach for the default export:

```js
const { default: Kinesis } = require('lifion-kinesis');
```

A bare `const Kinesis = require('lifion-kinesis')` (the v1 style) now gives you the namespace object instead of the class, and `new Kinesis(...)` fails with "Kinesis is not a constructor". Adding `.default` is the fix.

## AWS SDK for JavaScript v3

v2 uses the AWS SDK v3 internally, in place of the v1 client's aws-sdk v2. The client builds the `@aws-sdk/client-*` packages it needs on its own, so you no longer depend on `aws-sdk` for this. A few option shapes follow the SDK's v3 conventions now.

### Credentials

v2 no longer reads top-level `accessKeyId`, `secretAccessKey`, or `sessionToken`. The v3 SDK takes a `credentials` object or a credential provider, so passing those keys now raises a clear error instead of being silently ignored.

If you relied on the default provider chain (environment variables, shared config files, web identity tokens, the ECS/EC2 role), nothing changes. If you set keys directly, wrap them in a `credentials` object:

```js
// v1
const kinesis = new Kinesis({
  streamName: 'sample-stream',
  accessKeyId: '<your-access-key-id>',
  secretAccessKey: '<your-secret-access-key>'
});

// v2
const kinesis = new Kinesis({
  streamName: 'sample-stream',
  credentials: {
    accessKeyId: '<your-access-key-id>',
    secretAccessKey: '<your-secret-access-key>'
  }
});
```

You can also pass a provider from `@aws-sdk/credential-providers`:

```js
import { fromIni } from '@aws-sdk/credential-providers';

const kinesis = new Kinesis({
  streamName: 'sample-stream',
  credentials: fromIni({ profile: 'my-profile' })
});
```

A `region` also needs to be resolvable, from `AWS_REGION`, your shared config, or the `region` option.

### Other AWS options

AWS client options pass through to the v3 clients, so option names follow v3. The one to watch for is S3 path-style addressing under the `s3` options: it's `forcePathStyle` now, where the v2 SDK called it `s3ForcePathStyle`.

## Shard read errors recover instead of stopping the stream

In v1, a fatal error while polling a shard was emitted on the client's `error` event, which usually stopped consumption of that shard until the process restarted. In v2 the client logs a warning, releases the shard's lease, and lets the shard be acquired again so it resumes from the last stored checkpoint.

If you were listening on `error` to detect and recover from these yourself, you can lean on the built-in recovery instead. Producer errors, such as a rejected `putRecord`, still surface the same way as before.

## What stayed the same

The client options, methods, events, and the readable-stream interface match v1. Once you're on Node 22.12+ and importing the client the new way, existing consumer and producer code should keep working.
