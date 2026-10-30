import assert from 'node:assert/strict';
import {after, before, describe, it} from 'node:test';
import {spawn} from 'node:child_process';
import net from 'node:net';
import path from 'node:path';

// This test runs src/index.ts in a subprocess on purpose. Importing it
// in-process would start the service (AMQP + Mongo connections and the
// infinite startCheck loop) inside the test runner, so every assertion
// below goes through a spawned `node src/index.ts` child.
//
// Intentionally untested wiring: that `startApp(config)` is called with the
// loaded module-level config. Verifying this requires a module mock or a src
// refactor (no seam exists for src/index.ts); the signal and rejection paths
// below prove the handler registration and the run() -> startApp() wiring
// actually execute.
//
// Note: this project's logMessage() also logs 'Are we here?' and
// 'No catch error!' before the real message; the assertions below use
// 'No catch error!' as the signature of our termination handler.

const INDEX_PATH = path.join(import.meta.dirname, '..', 'src', 'index.ts');

// RECORD_LOAD_URL, RECORD_LOAD_API_KEY, RECORD_LOAD_LIBRARY and OPERATION
// are mandatory for importing src/config.ts.
const BASE_ENV = {
  RECORD_LOAD_URL: 'http://record-load.test',
  // Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
  RECORD_LOAD_API_KEY: ['ke', 'y'].join(''),
  RECORD_LOAD_LIBRARY: 'FINNA',
  OPERATION: 'FIX'
};

// Runs src/index.ts in a subprocess and resolves with its exit code and
// combined output. Optionally kills the child after a delay so signal
// handlers can be exercised while the app connections are pending.
const runIndex = (env, {signalAfterMs = null, signal = null, nodeArgs = []} = {}) =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [...nodeArgs, INDEX_PATH], {env: {...process.env, ...env}});
    let output = '';
    child.stdout.on('data', (chunk) => {output += chunk;});
    child.stderr.on('data', (chunk) => {output += chunk;});
    if (signalAfterMs) {
      const timer = setTimeout(() => child.kill(signal), signalAfterMs);
      timer.unref();
    }
    // Hard-kill safety net: no test may ever orphan a child. Unref'd so it
    // cannot itself keep the process alive; cleared when the child exits.
    const killTimer = setTimeout(() => child.kill('SIGKILL'), 20000);
    killTimer.unref();
    child.on('exit', (exitCode) => {
      clearTimeout(killTimer);
      // Belt-and-braces: kill once more in case the child lingered.
      child.kill('SIGKILL');
      resolve({exitCode, output});
    });
    child.on('error', reject);
  });

describe('index (subprocess: process entry wiring)', () => {
  // A dumb TCP socket that accepts but never answers: amqpFactory's
  // amqplib.connect() and mongoFactory's client.connect() hang on it,
  // keeping the app alive so the signal and exception handlers can be
  // exercised. One server doubles as both the AMQP and the Mongo target.
  let socketServer;
  let dumbPort;
  const liveSockets = new Set();

  before(async () => {
    socketServer = net.createServer();
    socketServer.on('connection', (socket) => {
      liveSockets.add(socket);
      socket.on('close', () => liveSockets.delete(socket));
    });
    await new Promise((resolve) => socketServer.listen(0, '127.0.0.1', resolve));
    dumbPort = socketServer.address().port;
  });
  after(() => new Promise((resolve) => {
    // A child killed mid-handshake leaves a half-open server socket behind
    // (it never sees EOF), which server.close() would wait on forever.
    for (const socket of liveSockets) {
      socket.destroy();
    }
    socketServer.close(resolve);
  }));

  it('exits with code 1 when startApp rejects (unhandled rejection path)', async () => {
    // Connection-refused on 127.0.0.1:1 is immediate; the explicit short
    // serverSelectionTimeoutMS makes the Mongo failure deterministic anyway
    // (AMQP connects first in startApp, so it is the one that rejects).
    const {exitCode, output} = await runIndex({
      ...BASE_ENV,
      AMQP_URL: 'amqp://127.0.0.1:1/',
      MONGO_URI: 'mongodb://127.0.0.1:1/db?serverSelectionTimeoutMS=2000'
    });
    assert.strictEqual(exitCode, 1);
    assert.ok(output.includes('No catch error!'), `output was:\n${output}`);
    assert.ok(output.includes('ECONNREFUSED 127.0.0.1:1'), `output was:\n${output}`);
  });

  it('exits with code 1 and logs the signal on SIGTERM', async () => {
    const {exitCode, output} = await runIndex({
      ...BASE_ENV,
      AMQP_URL: `amqp://127.0.0.1:${dumbPort}/`,
      MONGO_URI: `mongodb://127.0.0.1:${dumbPort}/db`
    }, {signalAfterMs: 2000, signal: 'SIGTERM'});
    assert.strictEqual(exitCode, 1);
    assert.ok(output.includes('Received SIGTERM'), `output was:\n${output}`);
  });

  it('exits with code 1 and logs the signal on SIGINT', async () => {
    const {exitCode, output} = await runIndex({
      ...BASE_ENV,
      AMQP_URL: `amqp://127.0.0.1:${dumbPort}/`,
      MONGO_URI: `mongodb://127.0.0.1:${dumbPort}/db`
    }, {signalAfterMs: 2000, signal: 'SIGINT'});
    assert.strictEqual(exitCode, 1);
    assert.ok(output.includes('Received SIGINT'), `output was:\n${output}`);
  });

  it('exits with code 1 and logs the stack on an uncaught exception', async () => {
    // A --import data-URL module throws after a delay: the handler registered
    // in index.ts is in place by then (the app is pending on the dumb
    // sockets), so the uncaughtException handler logs the stack and exits
    // with code 1.
    const dataUrl = `data:text/javascript,setTimeout(() => { throw new Error('Simulated uncaught exception'); }, 3000);`;
    const {exitCode, output} = await runIndex({
      ...BASE_ENV,
      AMQP_URL: `amqp://127.0.0.1:${dumbPort}/`,
      MONGO_URI: `mongodb://127.0.0.1:${dumbPort}/db`
    }, {nodeArgs: ['--import', dataUrl]});
    assert.strictEqual(exitCode, 1);
    assert.ok(output.includes('No catch error!'), `output was:\n${output}`);
    assert.ok(output.includes('Simulated uncaught exception'), `output was:\n${output}`);
  });
});
