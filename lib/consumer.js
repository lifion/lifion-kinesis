/* eslint-disable no-await-in-loop */

'use strict';

const { wait } = require('./utils');

const CONSUMER_MAX_STATE_CHECKS = 18;
const CONSUMER_STATE_CHECK_DELAY = 10000;

module.exports.activate = async ctx => {
  const {
    client,
    consumerName: ConsumerName,
    logger,
    streamArn: StreamARN,
    streamName: StreamName
  } = ctx;

  logger.debug(`Checking if the "${ConsumerName}" consumer for "${StreamName}" exists…`);

  async function describeStreamConsumer() {
    const { Consumers } = await client.listStreamConsumers({ StreamARN }).promise();
    return Consumers.find(i => i.ConsumerName === ConsumerName) || {};
  }

  const consumer = await describeStreamConsumer(ConsumerName, client, StreamARN);
  let { ConsumerStatus, ConsumerARN } = consumer;

  if (ConsumerStatus === 'DELETING') {
    logger.debug('Waiting for the stream consumer to complete deletion…');
    let checks = 0;
    while ((await describeStreamConsumer()).ConsumerStatus) {
      await wait(CONSUMER_STATE_CHECK_DELAY);
      checks += 1;
      if (checks > CONSUMER_MAX_STATE_CHECKS) {
        throw new Error(
          `Maximum checks reached while waiting for consumer "${ConsumerName}" to get deleted.`
        );
      }
    }
    logger.debug('The stream consumer is now gone.');
    ConsumerStatus = '';
  }

  if (!ConsumerStatus) {
    logger.debug('Trying to register the consumer…');
    const { Consumer } = await client.registerStreamConsumer({ ConsumerName, StreamARN }).promise();
    ({ ConsumerStatus, ConsumerARN } = Consumer);
  }

  if (ConsumerStatus === 'CREATING') {
    logger.debug('Waiting until the stream consumer is active…');
    let checks = 0;
    while ((await describeStreamConsumer()).ConsumerStatus !== 'ACTIVE') {
      await wait(CONSUMER_STATE_CHECK_DELAY);
      checks += 1;
      if (checks > CONSUMER_MAX_STATE_CHECKS) {
        throw new Error(
          `Maximum checks reached while waiting for consumer "${ConsumerName}" to get active.`
        );
      }
    }
    logger.debug('The stream consumer is now active.');
  }

  if (ConsumerStatus === 'ACTIVE') {
    logger.debug('The stream consumer already exists.');
  }

  return ConsumerARN;
};
