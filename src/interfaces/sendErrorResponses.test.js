import {test} from 'node:test';
import assert from 'node:assert/strict';
import {sendErrorResponses} from './sendErrorResponses.js';

// Plain mock matching the @natlibfi/melinda-rest-api-commons amqp factory:
// checkQueue({queue, style, toRecord, purge}) -> {headers, messages} (or false when empty),
// ackMessages(messages) / nackMessages(messages) receive the raw message array.
// Every call is recorded as [methodName, ...args] so call order and arguments
// can be asserted, not just return values.
function createAmqpOperator({queueResult}) {
  const calls = [];
  return {
    calls,
    checkQueue: async (params) => {
      calls.push(['checkQueue', params]);
      return queueResult;
    },
    ackMessages: async (messages) => {
      calls.push(['ackMessages', messages]);
    },
    nackMessages: async (messages) => {
      calls.push(['nackMessages', messages]);
    }
  };
}

// Plain mock matching the commons mongo factory:
// setImportJobState({correlationId, operation, importJobState}),
// setState({correlationId, state, errorMessage, errorStatus})
function createMongoOperator() {
  const calls = [];
  return {
    calls,
    setImportJobState: async (params) => {
      calls.push(['setImportJobState', params]);
    },
    setState: async (params) => {
      calls.push(['setState', params]);
    }
  };
}

const CORRELATION_ID = 'corr-123';
const QUEUE = 'PROCESS-queue';
const OPERATION = 'CREATE';
const messages = [{id: 'msg-1'}, {id: 'msg-2'}];
const CHECK_QUEUE_PARAMS = {queue: QUEUE, style: 'basic', toRecords: false, purge: false};

async function send({amqpOperator, mongoOperator, error, prio, error503WaitTime = 30}) {
  return sendErrorResponses({
    error,
    correlationId: CORRELATION_ID,
    queue: QUEUE,
    mongoOperator,
    prio,
    error503WaitTime,
    amqpOperator,
    operation: OPERATION
  });
}

test('returns silently when the queue is empty and makes no operator calls', async () => {
  const amqpOperator = createAmqpOperator({queueResult: false});
  const mongoOperator = createMongoOperator();
  const result = await send({amqpOperator, mongoOperator, error: {status: 500, payload: 'boom'}, prio: true});

  assert.equal(result, undefined);
  // Only the checkQueue call; nothing acked, nacked or written to mongo
  assert.deepEqual(amqpOperator.calls, [['checkQueue', CHECK_QUEUE_PARAMS]]);
  assert.deepEqual(mongoOperator.calls, []);
});

test('acks and writes ERROR states for a prio error', async () => {
  const amqpOperator = createAmqpOperator({queueResult: {headers: {}, messages}});
  const mongoOperator = createMongoOperator();
  const error = {status: 500, payload: 'prio boom'};
  const result = await send({amqpOperator, mongoOperator, error, prio: true});

  assert.equal(result, undefined);
  // Order matters: checkQueue, ack, setImportJobState, setState
  assert.deepEqual(amqpOperator.calls, [
    ['checkQueue', CHECK_QUEUE_PARAMS],
    ['ackMessages', messages]
  ]);
  assert.deepEqual(mongoOperator.calls, [
    ['setImportJobState', {correlationId: CORRELATION_ID, operation: OPERATION, importJobState: 'ERROR'}],
    [
      'setState',
      {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: 'prio boom',
        errorStatus: 500
      }
    ]
  ]);
});

test('prio takes precedence over 503: acks and writes ERROR states', async () => {
  const amqpOperator = createAmqpOperator({queueResult: {headers: {}, messages}});
  const mongoOperator = createMongoOperator();
  const error = {status: 503, payload: 'not ready'};
  await send({amqpOperator, mongoOperator, error, prio: true});

  assert.deepEqual(amqpOperator.calls, [
    ['checkQueue', CHECK_QUEUE_PARAMS],
    ['ackMessages', messages]
  ]);
  assert.deepEqual(mongoOperator.calls, [
    ['setImportJobState', {correlationId: CORRELATION_ID, operation: OPERATION, importJobState: 'ERROR'}],
    [
      'setState',
      {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: 'not ready',
        errorStatus: 503
      }
    ]
  ]);
});

test('acks and writes ERROR states for a bulk error with a non-503/non-422 status', async () => {
  const amqpOperator = createAmqpOperator({queueResult: {headers: {}, messages}});
  const mongoOperator = createMongoOperator();
  const error = {status: 400, payload: 'bad request'};
  await send({amqpOperator, mongoOperator, error, prio: false});

  assert.deepEqual(amqpOperator.calls, [
    ['checkQueue', CHECK_QUEUE_PARAMS],
    ['ackMessages', messages]
  ]);
  assert.deepEqual(mongoOperator.calls, [
    ['setImportJobState', {correlationId: CORRELATION_ID, operation: OPERATION, importJobState: 'ERROR'}],
    [
      'setState',
      {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: 'bad request',
        errorStatus: 400
      }
    ]
  ]);
});

test('falls back to 500 status and Unknown error payload when error lacks both', async () => {
  const amqpOperator = createAmqpOperator({queueResult: {headers: {}, messages}});
  const mongoOperator = createMongoOperator();
  await send({amqpOperator, mongoOperator, error: {}, prio: true});

  assert.deepEqual(amqpOperator.calls, [
    ['checkQueue', CHECK_QUEUE_PARAMS],
    ['ackMessages', messages]
  ]);
  assert.deepEqual(mongoOperator.calls, [
    ['setImportJobState', {correlationId: CORRELATION_ID, operation: OPERATION, importJobState: 'ERROR'}],
    [
      'setState',
      {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: 'Unknown error',
        errorStatus: 500
      }
    ]
  ]);
});

test('acks and sleeps for a bulk 422 without any mongo writes or nack', async () => {
  const amqpOperator = createAmqpOperator({queueResult: {headers: {}, messages}});
  const mongoOperator = createMongoOperator();
  const error = {status: 422, payload: 'unprocessable'};
  const start = process.hrtime.bigint();
  const result = await send({amqpOperator, mongoOperator, error, prio: false});
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

  assert.equal(result, undefined);
  // The 422 bulk path acks (not nacks) and does not touch mongo
  assert.deepEqual(amqpOperator.calls, [
    ['checkQueue', CHECK_QUEUE_PARAMS],
    ['ackMessages', messages]
  ]);
  assert.deepEqual(mongoOperator.calls, []);
  // Hard-coded 200 ms wait before the next batch is attempted
  assert.ok(elapsedMs >= 150, `expected the 422 path to wait ~200 ms, waited ${elapsedMs} ms`);
});

test('nacks and sleeps error503WaitTime for a bulk 503 without ack or mongo writes', async () => {
  const amqpOperator = createAmqpOperator({queueResult: {headers: {}, messages}});
  const mongoOperator = createMongoOperator();
  const error = {status: 503, payload: 'service unavailable'};
  const error503WaitTime = 40;
  const start = process.hrtime.bigint();
  const result = await send({amqpOperator, mongoOperator, error, prio: false, error503WaitTime});
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;

  assert.equal(result, undefined);
  // The 503 bulk path nacks the same message chunk and does not touch mongo
  assert.deepEqual(amqpOperator.calls, [
    ['checkQueue', CHECK_QUEUE_PARAMS],
    ['nackMessages', messages]
  ]);
  assert.deepEqual(mongoOperator.calls, []);
  assert.ok(
    elapsedMs >= error503WaitTime - 10,
    `expected the 503 path to wait ${error503WaitTime} ms, waited ${elapsedMs} ms`
  );
});
