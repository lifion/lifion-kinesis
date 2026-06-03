import json from '@eslint/json';
import vitest from '@vitest/eslint-plugin';
import { defineConfig, globalIgnores } from 'eslint/config';
import arrayFunc from 'eslint-plugin-array-func';
import importPlugin from 'eslint-plugin-import';
import jsdoc from 'eslint-plugin-jsdoc';
import noSecrets from 'eslint-plugin-no-secrets';
import prettierRecommended from 'eslint-plugin-prettier/recommended';
import security from 'eslint-plugin-security';
import sonarjs from 'eslint-plugin-sonarjs';
import sortDestructureKeys from 'eslint-plugin-sort-destructure-keys';
import sortKeysFix from 'eslint-plugin-sort-keys-fix';
import eslintPluginUnicorn from 'eslint-plugin-unicorn';
import neostandard from 'neostandard';

export default defineConfig([
  globalIgnores(['coverage/**', 'lib/aggregate-protobuf.json']),

  {
    extends: [
      neostandard({ env: ['node'], noStyle: true, semi: true }),
      jsdoc.configs['flat/recommended'],
      eslintPluginUnicorn.configs.recommended,
      security.configs.recommended,
      sonarjs.configs.recommended,
      arrayFunc.configs.all,
      prettierRecommended
    ],
    files: ['**/*.js'],
    languageOptions: { ecmaVersion: 2025 },
    plugins: {
      import: importPlugin,
      'no-secrets': noSecrets,
      'sort-destructure-keys': sortDestructureKeys,
      'sort-keys-fix': sortKeysFix
    },
    rules: {
      'import/extensions': 'off',
      'import/no-extraneous-dependencies': [
        'error',
        {
          devDependencies: [
            '**/*.test.js',
            '**/__mocks__/**',
            'test/**',
            '*.config.js',
            'eslint.config.js'
          ],
          optionalDependencies: false
        }
      ],
      'import/no-unresolved': 'off',
      'import/order': [
        'error',
        {
          groups: [
            ['builtin', 'external'],
            ['index', 'parent', 'sibling']
          ],
          'newlines-between': 'always'
        }
      ],
      'jsdoc/check-tag-names': ['error', { definedTags: ['fulfil', 'reject', 'typicalname'] }],
      'jsdoc/no-defaults': 'off',
      'jsdoc/no-undefined-types': ['error', { definedTypes: ['NodeJS'] }],
      'jsdoc/reject-any-type': 'off',
      'jsdoc/require-hyphen-before-param-description': 'warn',
      'jsdoc/require-jsdoc': 'off',
      'jsdoc/require-returns-description': 'off',
      'jsdoc/tag-lines': ['warn', 'never', { startLines: 1 }],
      'n/handle-callback-err': 'off',
      'n/no-missing-import': 'error',
      'no-secrets/no-secrets': [
        'error',
        {
          ignoreContent: [
            'NoSuchLifecycleConfiguration',
            'Kinesis_20131202.SubscribeToShard',
            'ProvisionedThroughputExceededException',
            'InvalidArgumentException',
            'SubscribeToShardEvent',
            'params.provisionedThroughput.writeCapacityUnits',
            'eyJmb28iOiJiYXIsIGJhesQFxgoifQ==',
            'Qk2rZuty0pO/vptdjx3KZ2hUqVM=',
            'n0vR6WyiMI1VeNqoISbuIEPoMPM=',
            'hoKe98qHWTKVJg\\+g8IEsdvvnrLI=',
            'uM9dgBWF4OGL42Uqbr61Yyt5h58='
          ]
        }
      ],
      'no-unmodified-loop-condition': 'off',
      'security/detect-object-injection': 'off',
      'sonarjs/cognitive-complexity': ['error', 128],
      'sonarjs/hashing': 'off',
      'sonarjs/no-clear-text-protocols': 'off',
      'sonarjs/no-identical-functions': 'off',
      'sonarjs/no-nested-conditional': 'off',
      'sort-destructure-keys/sort-destructure-keys': 'error',
      'sort-keys-fix/sort-keys-fix': ['error', 'asc', { natural: true }],
      'unicorn/catch-error-name': ['error', { name: 'err' }],
      'unicorn/filename-case': [
        'error',
        { case: 'kebabCase', ignore: ['^README\\.md$', '^\\d+_\\d+\\.json$'] }
      ],
      'unicorn/no-anonymous-default-export': 'off',
      'unicorn/no-array-callback-reference': 'off',
      'unicorn/no-array-for-each': 'off',
      'unicorn/no-array-reduce': 'off',
      'unicorn/no-array-sort': 'off',
      'unicorn/no-await-expression-member': 'off',
      'unicorn/no-for-loop': 'off',
      'unicorn/no-negated-condition': 'off',
      'unicorn/no-null': 'off',
      'unicorn/no-useless-undefined': 'off',
      'unicorn/numeric-separators-style': 'off',
      'unicorn/prefer-at': 'off',
      'unicorn/prefer-flat-map': 'off',
      'unicorn/prefer-logical-operator-over-ternary': 'off',
      'unicorn/prefer-math-min-max': 'off',
      'unicorn/prefer-module': 'off',
      'unicorn/prefer-object-from-entries': 'off',
      'unicorn/prefer-spread': 'off',
      'unicorn/prefer-string-raw': 'off',
      'unicorn/prefer-string-replace-all': 'off',
      'unicorn/prevent-abbreviations': 'off'
    },
    settings: {
      jsdoc: {
        mode: 'permissive',
        preferredTypes: { object: 'Object', 'object.<>': 'Object' },
        tagNamePreference: { return: 'returns' }
      }
    }
  },

  {
    extends: [vitest.configs.recommended],
    files: ['**/*.test.js'],
    rules: { 'vitest/no-conditional-expect': 'off' }
  },

  {
    files: ['**/*.test.js', '__mocks__/**/*.js', 'test/**/*.js', '*.config.js'],
    rules: {
      'jsdoc/require-param': 'off',
      'jsdoc/require-returns': 'off',
      'sonarjs/cognitive-complexity': 'warn',
      'sonarjs/no-duplicate-string': 'off',
      'unicorn/consistent-function-scoping': 'off'
    }
  },

  {
    files: ['lib/records.js'],
    rules: { 'promise/catch-or-return': 'off' }
  },

  {
    files: ['eslint.config.js'],
    rules: { 'no-secrets/no-secrets': 'off' }
  },

  {
    extends: ['json/recommended'],
    files: ['**/*.json'],
    ignores: ['package-lock.json'],
    language: 'json/json',
    plugins: { json }
  }
]);
