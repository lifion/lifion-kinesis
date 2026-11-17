import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { afterEach, beforeEach, describe, expect, test } from 'vitest';

import * as projectNameStatic from './project-name';

describe('lib/project-name', () => {
  const { getProjectName } = projectNameStatic;

  let cwd;
  let root;

  function remote(name, url) {
    return `[remote "${name}"]\n\turl = ${url}\n\tfetch = +refs/heads/*:refs/remotes/${name}/*\n`;
  }

  function writeGitConfig(contents) {
    mkdirSync(path.join(cwd, '.git'));
    writeFileSync(path.join(cwd, '.git', 'config'), `[core]\n\tbare = false\n${contents}`);
  }

  function writePackage(contents) {
    writeFileSync(path.join(cwd, 'package.json'), contents);
  }

  beforeEach(() => {
    root = mkdtempSync(path.join(tmpdir(), 'lifion-kinesis-'));
    cwd = path.join(root, 'my-app');
    mkdirSync(cwd);
  });

  afterEach(() => {
    rmSync(root, { force: true, recursive: true });
  });

  test('the module exports the expected', () => {
    expect({ ...projectNameStatic }).toEqual({ getProjectName: expect.any(Function) });
  });

  test('getProjectName returns the name in package.json', () => {
    writePackage('{"name":"my-service"}');
    expect(getProjectName(cwd)).toBe('my-service');
  });

  test('getProjectName prefers the name in package.json over the Git remote', () => {
    writePackage('{"name":"my-service"}');
    writeGitConfig(remote('origin', 'git@github.com:acme/my-repo.git'));
    expect(getProjectName(cwd)).toBe('my-service');
  });

  test("getProjectName throws if package.json can't be parsed", () => {
    writePackage('{"name":');
    expect(() => getProjectName(cwd)).toThrow(SyntaxError);
  });

  test('getProjectName uses the Git remote if package.json has no name', () => {
    writePackage('{"version":"1.0.0"}');
    writeGitConfig(remote('origin', 'git@github.com:acme/my-repo.git'));
    expect(getProjectName(cwd)).toBe('my-repo');
  });

  test.each`
    url                                                              | expected
    ${'https://github.com/acme/my-repo.git'}                         | ${'my-repo'}
    ${'https://github.com/acme/my-repo'}                             | ${'my-repo'}
    ${'https://user:token@github.com:443/acme/my-repo.git'}          | ${'my-repo'}
    ${'https://github.com/acme/my-repo.git?ref=main#readme'}         | ${'my-repo'}
    ${'https://git-codecommit.us-east-1.amazonaws.com/v1/repos/Foo'} | ${'Foo'}
    ${'git@github.com:acme/my-repo.git'}                             | ${'my-repo'}
    ${'git@ssh.dev.azure.com:v3/org/project/my-repo'}                | ${'my-repo'}
    ${'ssh://git@github.com:22/acme/my-repo.git'}                    | ${'my-repo'}
    ${'git.example.com:acme/my-repo.git'}                            | ${'my-repo'}
    ${'file:///srv/git/my-repo.git'}                                 | ${'my-repo'}
    ${'/srv/git/my-repo.git'}                                        | ${'my-repo'}
    ${'../my-repo.git'}                                              | ${'my-repo'}
    ${'C:\\\\repos\\\\my-repo.git'}                                  | ${'my-repo'}
    ${'https://github.com/acme/my.repo.git'}                         | ${'my.repo'}
    ${'https://github.com/acme/socket.io'}                           | ${'socket'}
  `('getProjectName returns $expected for the remote $url', ({ expected, url }) => {
    writeGitConfig(remote('origin', url));
    expect(getProjectName(cwd)).toBe(expected);
  });

  test.each`
    url                                   | scenario
    ${'https://github.com/acme/my-repo/'} | ${'ends with a slash'}
    ${'https://github.com'}               | ${'has no path'}
    ${'codecommit://my-repo'}             | ${'has no path after its host'}
  `('getProjectName returns the directory name if the remote $scenario', ({ url }) => {
    writeGitConfig(remote('origin', url));
    expect(getProjectName(cwd)).toBe('my-app');
  });

  test.each`
    entry                                                 | scenario
    ${'url = "https://github.com/acme/my-repo.git"'}      | ${'in quotes'}
    ${'url = https://github.com/acme/my-repo.git ; main'} | ${'followed by a comment'}
    ${'url=https://github.com/acme/my-repo.git'}          | ${'without spaces'}
    ${'url = https://github.com/acme/my-repo.git\r'}      | ${'ending in a carriage return'}
  `('getProjectName reads a remote URL $scenario', ({ entry }) => {
    writeGitConfig(`[remote "origin"]\n\t${entry}\n`);
    expect(getProjectName(cwd)).toBe('my-repo');
  });

  test('getProjectName prefers the remote named origin', () => {
    writeGitConfig(
      remote('upstream', 'git@github.com:acme/first.git') +
        remote('origin', 'git@github.com:acme/second.git')
    );
    expect(getProjectName(cwd)).toBe('second');
  });

  test('getProjectName uses the first remote if none is named origin', () => {
    writeGitConfig(
      remote('upstream', 'git@github.com:acme/first.git') +
        remote('fork', 'git@github.com:acme/second.git')
    );
    expect(getProjectName(cwd)).toBe('first');
  });

  test('getProjectName uses the last URL of a remote that is defined more than once', () => {
    writeGitConfig(
      remote('origin', 'git@github.com:acme/first.git') +
        remote('origin', 'git@github.com:acme/second.git')
    );
    expect(getProjectName(cwd)).toBe('second');
  });

  test('getProjectName ignores the URLs outside of the chosen remote', () => {
    writeGitConfig(
      '[remote "origin"]\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n' +
        '[submodule "vendor"]\n\turl = git@github.com:acme/vendor.git\n' +
        remote('upstream', 'git@github.com:acme/upstream.git')
    );
    expect(getProjectName(cwd)).toBe('my-app');
  });

  test('getProjectName returns the directory name if the repository has no remotes', () => {
    writeGitConfig('');
    expect(getProjectName(cwd)).toBe('my-app');
  });

  test('getProjectName returns the directory name if .git is not a directory', () => {
    writeFileSync(path.join(cwd, '.git'), 'gitdir: ../.git/worktrees/my-app\n');
    expect(getProjectName(cwd)).toBe('my-app');
  });

  test('getProjectName returns the directory name if there is nothing else to go by', () => {
    expect(getProjectName(cwd)).toBe('my-app');
  });
});
