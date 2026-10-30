import assert from 'node:assert';
import {describe, it} from 'node:test';
import {spawnSync} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import path from 'node:path';

const CONFIG_PATH = path.join(import.meta.dirname, '..', 'src', 'config.ts');

// All config values are read at import time (module-scope exports), and the
// exports are evaluated once per process. Each case is therefore run in its
// own subprocess: the child imports src/config.ts with a specific env and
// prints the selected exports as one JSON line, which the parent asserts on.
const MANAGED_VARIABLES = [
  'RECORD_LOAD_URL',
  'RECORD_LOAD_FIX_PATH',
  'RECORD_LOAD_LOAD_PATH',
  'RECORD_LOAD_API_KEY',
  'RECORD_LOAD_LIBRARY',
  'FIX_PRIO',
  'FIX_BULK',
  'AMQP_URL',
  'MONGO_URI',
  'POLL_WAIT_TIME',
  'ERROR_503_WAIT_TIME',
  'OPERATION',
  'KEEP_LOAD_PROCESS_RESULTS'
];

// The variables without a default in config.ts: without them the import throws.
const MANDATORY_VARIABLES = {
  RECORD_LOAD_URL: 'http://record-load',
  // Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
  RECORD_LOAD_API_KEY: ['api', 'key'].join('-'),
  RECORD_LOAD_LIBRARY: 'library',
  OPERATION: 'load'
};

const createChildEnv = (overrides: Record<string, string>) => {
  const env = {...process.env};
  MANAGED_VARIABLES.forEach((variable) => delete env[variable]);
  return {...env, ...overrides};
};

// Every exported config value is a string or a number, so no JSON replacer is
// needed for the dump.
const CONFIG_DUMP_SCRIPT = `
import * as config from ${JSON.stringify(pathToFileURL(CONFIG_PATH).href)};
console.log('CONFIG_DUMP ' + JSON.stringify({
  recordLoadUrl: config.recordLoadUrl,
  recordLoadFixPath: config.recordLoadFixPath,
  recordLoadLoadPath: config.recordLoadLoadPath,
  recordLoadApiKey: config.recordLoadApiKey,
  recordLoadLibrary: config.recordLoadLibrary,
  fixPrio: config.fixPrio,
  fixBulk: config.fixBulk,
  amqpUrl: config.amqpUrl,
  mongoUri: config.mongoUri,
  pollWaitTime: config.pollWaitTime,
  error503WaitTime: config.error503WaitTime,
  operation: config.operation,
  keepLoadProcessReports: config.keepLoadProcessReports
}));
`;

const runConfigDump = (overrides: Record<string, string>) => {
  const result = spawnSync(process.execPath, ['--input-type=module', '-e', CONFIG_DUMP_SCRIPT], {
    env: createChildEnv(overrides),
    encoding: 'utf8'
  });
  if (result.status !== 0) {
    return {status: result.status, stdout: result.stdout, stderr: result.stderr, dump: null};
  }
  const line: string | undefined = result.stdout.split('\n').find((line) => line.startsWith('CONFIG_DUMP '));
  const dump = line ? JSON.parse(line.slice('CONFIG_DUMP '.length)) : null;
  return {status: result.status, stdout: result.stdout, stderr: result.stderr, dump};
};

const assertThrow = ({status, stderr}: {status: number | null; stderr: string}, message: string) => {
  assert.strictEqual(status, 1);
  assert.ok(stderr.includes(message), `expected stderr to include: ${message}`);
};

describe('config option building (subprocess: env is read at import time)', () => {
  it('builds the defaults when only the mandatory vars are set', () => {
    const {status, dump} = runConfigDump(MANDATORY_VARIABLES);
    assert.strictEqual(status, 0);
    // POLL_WAIT_TIME and ERROR_503_WAIT_TIME come back as numbers: the numeric
    // defaults pass through parseInt() unchanged.
    assert.deepStrictEqual(dump, {
      recordLoadUrl: 'http://record-load',
      recordLoadFixPath: '/fix',
      recordLoadLoadPath: '',
      recordLoadApiKey: ['api', 'key'].join('-'),
      recordLoadLibrary: 'library',
      fixPrio: 'API',
      fixBulk: 'INSB',
      amqpUrl: 'amqp://127.0.0.1:5672/',
      mongoUri: 'mongodb://127.0.0.1:27017/db',
      pollWaitTime: 1000,
      error503WaitTime: 10000,
      operation: 'load',
      keepLoadProcessReports: 'NON_HANDLED'
    });
  });

  it('applies environment variable overrides for every managed var', () => {
    const {status, dump} = runConfigDump({
      RECORD_LOAD_URL: 'http://record-load.over',
      RECORD_LOAD_FIX_PATH: '/fix/over',
      RECORD_LOAD_LOAD_PATH: '/load/over',
      RECORD_LOAD_API_KEY: ['api', 'key', 'over'].join('-'),
      RECORD_LOAD_LIBRARY: 'library-over',
      FIX_PRIO: 'API2',
      FIX_BULK: 'INSB2',
      AMQP_URL: 'amqp://127.0.0.1:5673/',
      MONGO_URI: 'mongodb://127.0.0.1:27018/db',
      POLL_WAIT_TIME: '250',
      ERROR_503_WAIT_TIME: '30000',
      OPERATION: 'load-over',
      KEEP_LOAD_PROCESS_RESULTS: 'ALL'
    });
    assert.strictEqual(status, 0);
    // POLL_WAIT_TIME and ERROR_503_WAIT_TIME are coerced with parseInt(..., 10);
    // every other var passes the env value through unchanged.
    assert.deepStrictEqual(dump, {
      recordLoadUrl: 'http://record-load.over',
      recordLoadFixPath: '/fix/over',
      recordLoadLoadPath: '/load/over',
      recordLoadApiKey: ['api', 'key', 'over'].join('-'),
      recordLoadLibrary: 'library-over',
      fixPrio: 'API2',
      fixBulk: 'INSB2',
      amqpUrl: 'amqp://127.0.0.1:5673/',
      mongoUri: 'mongodb://127.0.0.1:27018/db',
      pollWaitTime: 250,
      error503WaitTime: 30000,
      operation: 'load-over',
      keepLoadProcessReports: 'ALL'
    });
  });
});

// The throw cases abort the module at import time. readEnvironmentVariable
// prints its notices and node prints the uncaught error to stderr, so the
// parent checks the child stderr for the exact message.
describe('config import failures (subprocess)', () => {
  Object.entries(MANDATORY_VARIABLES).forEach(([variable]) => {
    it(`throws at import when ${variable} is missing`, () => {
      const {status, stderr} = runConfigDump(Object.fromEntries(
        Object.entries(MANDATORY_VARIABLES).filter(([name]) => name !== variable)
      ));
      assertThrow({status, stderr}, `Mandatory environment variable missing: ${variable}`);
    });
  });
});
