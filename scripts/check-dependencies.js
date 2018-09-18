#!/usr/bin/env node

'use strict';

/* eslint-disable global-require, import/no-dynamic-require, no-process-exit */
/* eslint-disable security/detect-child-process, security/detect-non-literal-require */

const chalk = require('chalk');
const path = require('path');
const semver = require('semver');
const { exec } = require('child_process');
const { promisify } = require('util');

const { dependencies, devDependencies } = require(path.join(process.cwd(), 'package.json'));

const execAsync = promisify(exec);
const pkgs = [];

async function getLatestVersions(name) {
  const { stdout } = await execAsync(`npm view ${name} versions --json`);
  try {
    return JSON.parse(stdout);
  } catch (err) {
    return [];
  }
}

async function getLatestVersion(name, wanted) {
  const versions = await getLatestVersions(name);
  const applicableVersions = versions.filter(i => semver.satisfies(i, wanted));
  applicableVersions.sort((a, b) => semver.rcompare(a, b));
  return applicableVersions[0];
}

function getInstalledVersion(name) {
  try {
    return require(path.join(process.cwd(), 'node_modules', name, 'package.json')).version;
  } catch (err) {
    return null;
  }
}

function pushPkgs(deps = {}, type) {
  return Object.keys(deps).map(async name => {
    let wanted = deps[name];
    if (!wanted.startsWith('^')) wanted = `^${wanted}`;
    const installed = getInstalledVersion(name);
    const latest = await getLatestVersion(name, wanted);
    const wantedFixed = wanted.startsWith('^') ? wanted.substr(1) : wanted;
    const shouldBeInstalled =
      installed === null || wantedFixed !== installed || installed !== latest;
    if (shouldBeInstalled) {
      const warning =
        installed !== null
          ? `outdated: ${chalk.red(
              wantedFixed !== installed ? wantedFixed : installed
            )} → ${chalk.green(latest)}`
          : chalk.red('not installed');
      console.log(`${chalk.red(name)} is ${warning}`);
    }
    pkgs.push({ name, wanted, installed, type, latest, shouldBeInstalled });
  });
}

function getPkgIds(filteredPkgs) {
  return filteredPkgs.map(({ name, latest }) => `${name}@${latest}`).join(' ');
}

async function run() {
  console.log(chalk.blue('Checking NPM module versions…\n'));
  await Promise.all([...pushPkgs(dependencies, 'prod'), ...pushPkgs(devDependencies, 'dev')]);
  const toInstall = pkgs.filter(({ shouldBeInstalled }) => shouldBeInstalled);
  if (toInstall.length > 0) {
    console.log(`\n${chalk.bold('To resolve this, run:')}`);
    const prodPkgs = toInstall.filter(({ type }) => type === 'prod');
    if (prodPkgs.length > 0) {
      console.log(`npm i ${getPkgIds(prodPkgs)}`);
    }
    const devPkgs = toInstall.filter(({ type }) => type === 'dev');
    if (devPkgs.length > 0) {
      console.log(`npm i -D ${getPkgIds(devPkgs)}`);
    }
    if (prodPkgs.length > 0 || devPkgs.length > 0) {
      console.log();
    }
    process.exit(1);
  } else {
    console.log(chalk.green('All NPM modules are up to date.'));
    process.exit(0);
  }
}

run();
