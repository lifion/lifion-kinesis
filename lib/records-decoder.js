'use strict';

const { Transform } = require('stream');
const compressionLibs = require('./compression');
const { isJson } = require('./utils');

const privateData = new WeakMap();

function internal(instance) {
  if (!privateData.has(instance)) privateData.set(instance, {});
  return privateData.get(instance);
}

class RecordsDecoder extends Transform {
  constructor(options) {
    super({ objectMode: true });
    const { compression } = options;
    const compressionLib = compression && compressionLibs[compression];
    Object.assign(internal(this), { ...options, compressionLib });
  }

  async _transform({ headers, payload }, encoding, callback) {
    const { compressionLib, logger } = internal(this);
    const msgType = headers[':message-type'];
    const eventType = headers[':event-type'];

    if (msgType !== 'event') {
      this.emit('error', new Error(`Unknown event stream message type "${msgType}".`));
      return;
    }

    if (eventType === 'SubscribeToShardEvent') {
      try {
        const decodedRecords = await Promise.all(
          payload.Records.map(async record => {
            const { Data } = record;
            let parsedData;
            if (compressionLib) parsedData = await compressionLib.decompress(Data);
            else parsedData = Buffer.from(Data).toString('utf8');
            if (isJson(parsedData)) parsedData = JSON.parse(parsedData);
            return { ...record, Data: parsedData };
          })
        );
        this.push({ ...payload, Records: decodedRecords });
        callback();
        return;
      } catch (err) {
        this.emit('error', err);
      }
    }

    logger.debug(`Emiting "${eventType}"…`);
    this.emit(eventType, payload);
    callback();
  }
}

module.exports = RecordsDecoder;
