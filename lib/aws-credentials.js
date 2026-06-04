/**
 * Module with a guard for the credentials handed to the AWS SDK v3 clients.
 *
 * @module aws-credentials
 * @private
 */

/**
 * Asserts that the given AWS client options don't carry credentials in the AWS SDK v2 shape. The
 * AWS SDK v3 only reads a `credentials` object or provider and silently ignores top-level
 * `accessKeyId`/`secretAccessKey`/`sessionToken`, so this turns that silent gap into a clear error.
 * When no credentials are supplied, the clients are left to the AWS SDK v3 default provider chain
 * (environment, shared config, web identity, and the ECS/EC2 IAM role metadata endpoints).
 *
 * @param {Object} [awsOptions] - The AWS client options to check.
 * @throws {TypeError} If `accessKeyId`, `secretAccessKey`, or `sessionToken` are passed at the top
 *         level, which the AWS SDK v2 accepted but v3 ignores.
 * @memberof module:aws-credentials
 */
function assertCredentialsSupported(awsOptions = {}) {
  const { accessKeyId, secretAccessKey, sessionToken } = awsOptions;
  if (accessKeyId || secretAccessKey || sessionToken) {
    throw new TypeError(
      'Top-level "accessKeyId", "secretAccessKey", and "sessionToken" options are not supported. ' +
        'Pass a "credentials" object (or a credentials provider function) instead.'
    );
  }
}

export { assertCredentialsSupported };
