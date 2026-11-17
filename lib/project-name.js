/**
 * Module that resolves the name of the application that is using this module.
 *
 * @module project-name
 * @private
 */

import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';

const ORIGIN = 'remote "origin"';
const SECTION = /^\[([^\]]*)\]$/;
const URL_AUTHORITY = /^[\d+.a-z-]+:(?:\/\/)?[^#/?\\]*/i;
const URL_VARIABLE = /^url\s*=\s*(?:"([^"]*)"|([^#;]*))/;

/**
 * Retrieves the name in the `package.json` file of the given directory. A file that exists
 * but can't be parsed throws, so a broken manifest never ends up as a different name.
 *
 * @param {string} cwd - The directory to look into.
 * @returns {string} The name of the package, or `undefined` if there isn't one.
 * @private
 */
function getPackageName(cwd) {
  const file = path.join(cwd, 'package.json');
  return existsSync(file) ? JSON.parse(readFileSync(file, 'utf8')).name : undefined;
}

/**
 * Retrieves the URL of a remote of the Git repository in the given directory. The remote
 * named "origin" is preferred, otherwise the first remote in the configuration is used.
 *
 * @param {string} cwd - The directory to look into.
 * @returns {string} The URL of the remote, or `undefined` if there isn't one.
 * @private
 */
function getRemoteUrl(cwd) {
  let config;
  try {
    config = readFileSync(path.join(cwd, '.git', 'config'), 'utf8');
  } catch {
    return undefined;
  }
  const remotes = new Map();
  let remote;
  for (const line of config.split(/[\n\r]+/)) {
    const section = SECTION.exec(line)?.[1].trim();
    if (section !== undefined) {
      remote = section.startsWith('remote ') ? section : undefined;
      if (remote && !remotes.has(remote)) remotes.set(remote, undefined);
    } else if (remote) {
      const match = URL_VARIABLE.exec(line.trim());
      if (match) remotes.set(remote, (match[1] ?? match[2]).trim());
    }
  }
  return remotes.has(ORIGIN) ? remotes.get(ORIGIN) : remotes.values().next().value;
}

/**
 * Retrieves the name of the Git repository in the given directory, which is the last segment
 * of the path in the URL of its remote, without the extension.
 *
 * @param {string} cwd - The directory to look into.
 * @returns {string} The name of the repository, or `undefined` if there isn't one.
 * @private
 */
function getRepositoryName(cwd) {
  const remoteUrl = getRemoteUrl(cwd);
  if (!remoteUrl) return undefined;
  const [pathname] = remoteUrl.replace(URL_AUTHORITY, '').split(/[#?]/);
  const segment = pathname.split(/[/\\]/).pop();
  return path.basename(segment, path.extname(segment));
}

/**
 * Resolves the name of the project in the given directory. The name is taken from, in order:
 * the `package.json` file, the Git remote of the repository, or the directory itself.
 *
 * @param {string} cwd - The directory of the project.
 * @returns {string} The name of the project.
 * @memberof module:project-name
 */
function getProjectName(cwd) {
  return getPackageName(cwd) || getRepositoryName(cwd) || path.basename(cwd);
}

export { getProjectName };
