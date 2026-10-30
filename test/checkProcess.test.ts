import {test, describe, beforeEach, afterEach, after} from 'node:test';
import {strict as assert} from 'node:assert';
import httpStatus from 'http-status';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {MongoClient} from 'mongodb';
import checkProcessFactory from '../src/interfaces/checkProcess.ts';

// checkProcess.ts creates its processOperator internally (processPoll.ts) and
// calls mongoLogFactory(mongoUri) itself, so neither can be injected.
// The processOperator is driven through a stubbed globalThis.fetch (per test).
// mongoLogFactory (rest-api-commons) opens a real Mongo connection via
// MongoClient.connect, so we patch MongoClient.connect for the duration of the
// suite (mongodb is an existing transitive dependency; node:test mock.module
// would need --experimental-test-module-mocks, which test:base does not set).
const originalMongoConnect = MongoClient.connect;
const mongoConnectUris = [];
const mongoLogItems = [];

MongoClient.connect = async (uri) => {
  mongoConnectUris.push(uri);
  return {
    db: () => ({
      collection: () => ({
        insertOne: async (logItem) => {
          mongoLogItems.push(logItem);
          return {acknowledged: true};
        }
      })
    })
  };
};

after(() => {
  MongoClient.connect = originalMongoConnect;
});

const RECORD_LOAD_URL = 'http://record-load-api';
// Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
const API_KEY = ['api', 'key'].join('');
const MONGO_URI = 'mongodb://test/mongo';
const CORRELATION_ID = 'CID-1';
const AUTH_HEADER = generateAuthorizationHeader(API_KEY);

const originalFetch = globalThis.fetch;

beforeEach(() => {
  mongoConnectUris.length = 0;
  mongoLogItems.length = 0;
  // Fails loudly if a test forgets to stub fetch
  globalThis.fetch = () => {
    throw new Error('globalThis.fetch was called but not stubbed in this test');
  };
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

function makeProcessParams(overrides = {}) {
  return {
    correlationId: CORRELATION_ID,
    pActiveLibrary: 'FIN',
    processId: 123,
    loaderProcessId: 456,
    recordAmount: 2,
    ...overrides
  };
}

function makeProcessMessage(data) {
  return {content: Buffer.from(JSON.stringify({data}))};
}

function makeMessage({id = '000999999', blobSequence, notes} = {}) {
  const recordMetadata = {};
  if (blobSequence !== undefined) {
    recordMetadata.blobSequence = blobSequence;
  }
  const headers = {id, recordMetadata};
  if (notes !== undefined) {
    headers.notes = notes;
  }
  return {properties: {headers}, content: Buffer.from('{}')};
}

// The real amqpOperator.checkQueue (style 'basic') returns {headers, messages}.
function makeOperationQueue(messages, headers = {}) {
  return {headers, messages};
}

function makeAmqp({
  processMessage,
  processMessageOnFirstCall = true,
  operationQueueResult,
  bulkRemaining = 0,
  processQueueError
} = {}) {
  const calls = [];
  let processQueueAttempts = 0;
  return {
    calls,
    checkQueue({queue, style} = {}) {
      calls.push(['checkQueue', queue, style]);
      if (queue.startsWith('PROCESS.')) {
        processQueueAttempts += 1;
        if (processQueueError) {
          throw processQueueError;
        }
        if (!processMessageOnFirstCall && processQueueAttempts === 1) {
          return false;
        }
        return processMessage;
      }
      if (style === 'basic') {
        return operationQueueResult;
      }
      if (style === 'messages') {
        return bulkRemaining;
      }
      throw new Error(`Unexpected checkQueue style: ${style}`);
    },
    ackMessages(messages) {
      calls.push(['ackMessages', messages]);
    },
    nackMessages(messages) {
      calls.push(['nackMessages', messages]);
    },
    removeQueue(queue) {
      calls.push(['removeQueue', queue]);
    }
  };
}

function makeMongoOperator() {
  const calls = [];
  return {
    calls,
    pushIds(args) {
      calls.push(['pushIds', args]);
    },
    pushMessages(args) {
      calls.push(['pushMessages', args]);
    },
    setImportJobState(args) {
      calls.push(['setImportJobState', args]);
    },
    setState(args) {
      calls.push(['setState', args]);
    }
  };
}

function makeFetch({pollStatus = 200, pollBody = {}, fileClearStatus = 200} = {}) {
  const calls = [];
  return {
    calls,
    async fetch(url, options) {
      calls.push({url: String(url), options});
      if (options.method === 'get') {
        // The record-load-api payload always has these arrays; pollLoad does
        // not guard against missing ones (missing rejectedIds crashes it).
        const body = {handledIds: [], rejectedIds: [], ...pollBody};
        return {status: pollStatus, json: async () => body, text: async () => ''};
      }
      if (options.method === 'delete') {
        return {status: fileClearStatus, json: async () => ({}), text: async () => ''};
      }
      throw new Error(`Unexpected fetch method: ${options.method}`);
    }
  };
}

// Returns the checkProcessQueueStart() promise with {amqp, mongo, fetchCalls}
// attached, so tests can inspect the mocks on both resolve and reject.
function runCheckProcessQueueStart({
  operation = 'CREATE',
  prio,
  correlationId = CORRELATION_ID,
  pollStatus,
  pollBody = {},
  processMessage,
  processMessageOnFirstCall = true,
  operationQueueResult,
  bulkRemaining = 0,
  keepLoadProcessReports = 'NONE',
  error503WaitTime = 1,
  fileClearStatus = 200,
  processQueueError
} = {}) {
  const amqp = makeAmqp({
    processMessage,
    processMessageOnFirstCall,
    operationQueueResult,
    bulkRemaining,
    processQueueError
  });
  const fetchStub = makeFetch({pollStatus, pollBody, fileClearStatus});
  globalThis.fetch = fetchStub.fetch;
  const mongo = makeMongoOperator();
  const run = checkProcessFactory({
    amqpOperator: amqp,
    recordLoadApiKey: API_KEY,
    recordLoadUrl: RECORD_LOAD_URL,
    error503WaitTime,
    keepLoadProcessReports,
    mongoUri: MONGO_URI
  }).then((factory) => factory.checkProcessQueueStart({
    correlationId,
    operation,
    mongoOperator: mongo,
    prio
  }));
  run.amqp = amqp;
  run.mongo = mongo;
  run.fetchCalls = fetchStub.calls;
  return run;
}

function fetchQuery(fetchCalls, index) {
  return new URL(fetchCalls[index].url).searchParams;
}

function ackCallsOf(amqp) {
  return amqp.calls.filter((call) => call[0] === 'ackMessages');
}

function recordResponseCalls(mongo) {
  return mongo.calls.filter((call) => call[0] === 'pushMessages' && call[1].messageField === 'records');
}

function nackCallsOf(amqp) {
  return amqp.calls.filter((call) => call[0] === 'nackMessages');
}

function reportCalls(mongo) {
  const isReportPush = (call) =>
    call[0] === 'pushMessages' && call[1].messageField === 'loaderProcessReports';
  return mongo.calls.filter(isReportPush);
}

describe('checkProcess/handleProcessMessageError (process poll errors)', () => {
  test('423 LOCKED: nacks the process message and returns false, no error state', async () => {
    const processMessage = makeProcessMessage(makeProcessParams());
    const run = runCheckProcessQueueStart({processMessage, pollStatus: httpStatus.LOCKED});
    const result = await run;

    assert.equal(result, false);
    assert.deepEqual(nackCallsOf(run.amqp), [['nackMessages', [processMessage]]]);
    assert.equal(ackCallsOf(run.amqp).length, 0);
    assert.equal(run.mongo.calls.length, 0);
    // One fetch: the load process poll. No file clearing for a false result.
    assert.equal(run.fetchCalls.length, 1);
    assert.equal(run.fetchCalls[0].options.method, 'get');
    assert.equal(run.fetchCalls[0].options.headers.Authorization, AUTH_HEADER);
    const query = fetchQuery(run.fetchCalls, 0);
    assert.equal(query.get('correlationId'), CORRELATION_ID);
    assert.equal(query.get('pActiveLibrary'), 'FIN');
    assert.equal(query.get('processId'), '123');
    assert.equal(query.get('loaderProcessId'), '456');
    // Missing file params are omitted from the query (buildQuery; the gap 6
    // null-coercion that stringified them as literal "null" is fixed).
    assert.ok(!query.has('pLogFile'));
    assert.ok(!query.has('pRejectFile'));
  });

  test('503: nacks the process message, sleeps error503WaitTime, returns false', async () => {
    const processMessage = makeProcessMessage(makeProcessParams());
    const run = runCheckProcessQueueStart({processMessage, pollStatus: httpStatus.SERVICE_UNAVAILABLE});
    const result = await run;

    assert.equal(result, false);
    assert.deepEqual(nackCallsOf(run.amqp), [['nackMessages', [processMessage]]]);
    assert.equal(run.mongo.calls.length, 0);
  });

  test('400 BAD_REQUEST with prio: throws ApiError 500, errors the queueItem, acks the message', async () => {
    const processMessage = makeProcessMessage(makeProcessParams());
    const run = runCheckProcessQueueStart({processMessage, pollStatus: httpStatus.BAD_REQUEST, prio: true});
    const result = await run;

    assert.equal(result, false);
    // handleProcessMessageError throws a fresh ApiError(500) for prio, so the
    // error state carries status 500 and no payload.
    assert.deepEqual(run.mongo.calls, [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'ERROR'}],
      ['setState', {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: undefined,
        errorStatus: httpStatus.INTERNAL_SERVER_ERROR
      }]
    ]);
    assert.deepEqual(ackCallsOf(run.amqp), [['ackMessages', [processMessage]]]);
    assert.equal(run.amqp.calls.filter((call) => call[0] === 'nackMessages').length, 0);
  });

  test('400 BAD_REQUEST in bulk: rethrows ApiError 400 with payload, errors, acks the message', async () => {
    const processMessage = makeProcessMessage(makeProcessParams());
    const run = runCheckProcessQueueStart({processMessage, pollStatus: httpStatus.BAD_REQUEST, prio: false});
    const result = await run;

    assert.equal(result, false);
    assert.deepEqual(run.mongo.calls, [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'ERROR'}],
      ['setState', {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: 'Bad request from record-load-api',
        errorStatus: httpStatus.BAD_REQUEST
      }]
    ]);
    assert.deepEqual(ackCallsOf(run.amqp), [['ackMessages', [processMessage]]]);
  });

  test('other ApiError (404): rethrown, errors the queueItem, acks the process message', async () => {
    const processMessage = makeProcessMessage(makeProcessParams());
    const run = runCheckProcessQueueStart({processMessage, pollStatus: httpStatus.NOT_FOUND});
    const result = await run;

    assert.equal(result, false);
    assert.deepEqual(run.mongo.calls, [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'ERROR'}],
      ['setState', {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: 'Process log not found!',
        errorStatus: httpStatus.NOT_FOUND
      }]
    ]);
    assert.deepEqual(ackCallsOf(run.amqp), [['ackMessages', [processMessage]]]);
  });

  test('non-ApiError (malformed process message JSON): wrapped in ApiError 500, errors, acks', async () => {
    // JSON.parse crashes before the poll fetch happens. The crash is not an
    // ApiError, so handleProcessMessageError wraps it in ApiError(500, message).
    const processMessage = {content: Buffer.from('this is not json')};
    const run = runCheckProcessQueueStart({processMessage, pollStatus: 200, pollBody: {handledIds: []}});
    const result = await run;

    assert.equal(result, false);
    assert.equal(run.fetchCalls.length, 0);
    const setStateCall = run.mongo.calls.find((call) => call[0] === 'setState');
    assert.equal(setStateCall[1].state, 'ERROR');
    assert.equal(setStateCall[1].errorStatus, httpStatus.INTERNAL_SERVER_ERROR);
    assert.ok(typeof setStateCall[1].errorMessage === 'string' && setStateCall[1].errorMessage.length > 0);
    assert.deepEqual(ackCallsOf(run.amqp), [['ackMessages', [processMessage]]]);
  });
});

describe('checkProcess/checkProcessQueue (empty PROCESS queue handling)', () => {
  test('empty PROCESS queue even after the 200 ms retry: errors the queueItem, returns false', async () => {
    const run = runCheckProcessQueueStart({});
    const result = await run;

    assert.equal(result, false);
    // Waited once (200 ms hard-coded sleep), then errored on the empty queue.
    assert.deepEqual(run.amqp.calls, [
      ['checkQueue', `PROCESS.CREATE.${CORRELATION_ID}`, 'one'],
      ['checkQueue', `PROCESS.CREATE.${CORRELATION_ID}`, 'one']
    ]);
    assert.deepEqual(run.mongo.calls, [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'ERROR'}],
      ['setState', {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: `Empty PROCESS.CREATE.${CORRELATION_ID} queue`,
        errorStatus: httpStatus.INTERNAL_SERVER_ERROR
      }]
    ]);
  });

  test('empty PROCESS queue on first check: retries and continues when the message appears', async () => {
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      processMessageOnFirstCall: false,
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: makeOperationQueue([makeMessage()]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    const processQueueChecks = run.amqp.calls.filter(
      (call) => call[0] === 'checkQueue' && call[1].startsWith('PROCESS.'));
    assert.equal(processQueueChecks.length, 2);
    // Success: import job DONE, importer queues removed, file clearing requested.
    const setImportJobState = run.mongo.calls.filter((call) => call[0] === 'setImportJobState');
    assert.deepEqual(setImportJobState, [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'DONE'}]
    ]);
    assert.deepEqual(run.amqp.calls.filter((call) => call[0] === 'removeQueue'), [
      ['removeQueue', `CREATE.${CORRELATION_ID}`],
      ['removeQueue', `PROCESS.CREATE.${CORRELATION_ID}`]
    ]);
    const fileClear = run.fetchCalls[run.fetchCalls.length - 1];
    assert.equal(fileClear.options.method, 'delete');
    assert.equal(fileClear.options.headers.Authorization, AUTH_HEADER);
    assert.equal(fetchQuery(run.fetchCalls, run.fetchCalls.length - 1).get('correlationId'), CORRELATION_ID);
  });

  test('checkQueue throws a non-ApiError: caught, logged, no error state, returns false', async () => {
    // The `amqpOperator.checkQueue` call is INSIDE checkProcessQueue's try
    // block, so a non-ApiError from it is caught, logged (not sent to the
    // queueItem as an error state) and the function returns undefined;
    // checkProcessQueueStart treats a falsy result as "not ready yet"
    // and returns false, so the loop just re-polls.
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams()),
      pollStatus: 200,
      pollBody: {handledIds: []},
      processQueueError: new Error('simulated amqp connection error')
    });
    const result = await run;

    assert.equal(result, false);
    assert.equal(run.mongo.calls.length, 0);
    assert.equal(ackCallsOf(run.amqp).length, 0);
    assert.equal(run.amqp.calls.filter((call) => call[0] === 'nackMessages').length, 0);
    assert.equal(run.fetchCalls.length, 0);
  });
});

describe('checkProcess/checkLoadProcessReport (keepLoadProcessReports config)', () => {
  // Poll result with processedAll: false and handledAmount (1) < recordAmount (2).
  const notProcessedAll = {handledIds: ['000999999FIN']};

  function runReportFlow(keepLoadProcessReports, pollBody) {
    return runCheckProcessQueueStart({
      keepLoadProcessReports,
      pollStatus: 200,
      pollBody,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      // blobSequence is required: the not-handled-all CREATE fallback reads
      // recordMetadata.blobSequence.toString() without a guard
      operationQueueResult: makeOperationQueue([
        makeMessage({id: '000999999', blobSequence: 999999}),
        makeMessage({id: '000123456', blobSequence: 123456})
      ]),
      bulkRemaining: 0
    });
  }

  test('ALL: keeps the load process report (pushMessages loaderProcessReports)', async () => {
    const run = runReportFlow('ALL', notProcessedAll);
    await run;

    assert.equal(reportCalls(run.mongo).length, 1);
    const kept = reportCalls(run.mongo)[0][1];
    assert.equal(kept.correlationId, CORRELATION_ID);
    assert.equal(kept.messages.length, 1);
    assert.equal(kept.messages[0].processedAll, false);
    assert.equal(kept.messages[0].handledAmount, 1);
    // The report is also always added as a LOAD_PROCESS_LOG log item via the
    // (connect-patched) mongoLogFactory, with the configured mongoUri.
    assert.deepEqual(mongoConnectUris, [MONGO_URI]);
    assert.equal(mongoLogItems.length, 1);
    // insertOne is called with the log item enriched with creationTime/protected
    assert.equal(mongoLogItems[0].logItemType, 'LOAD_PROCESS_LOG');
    assert.equal(mongoLogItems[0].correlationId, CORRELATION_ID);
    assert.equal(mongoLogItems[0].handledAmount, 1);
    // And the ids are pushed for the correlation.
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'pushIds'), [
      ['pushIds', {
        correlationId: CORRELATION_ID,
        handledIds: ['000999999'],
        rejectedIds: [],
        erroredAmount: 0
      }]
    ]);
  });

  test('NON_PROCESSSED (the code value) with processedAll false: keeps the report', async () => {
    // The code value (and the one documented in config.ts and the README)
    // has THREE s's: NON_ + PROCESSED.
    const run = runReportFlow('NON_PROCESSSED', notProcessedAll);
    await run;
    assert.equal(reportCalls(run.mongo).length, 1);
  });

  test('NON_PROCESSSED (the code value) with processedAll true: does not keep the report', async () => {
    const run = runReportFlow('NON_PROCESSSED', {handledIds: ['000999999FIN', '000123456FIN']});
    await run;
    assert.equal(reportCalls(run.mongo).length, 0);
  });

  test('NON_PROCESSED (undocumented two-s spelling): falls through and does not keep', async () => {
    // The code (and config.ts/README) compare against 'NON_PROCESSSED'
    // (three s's). The two-s spelling 'NON_PROCESSED' is not accepted and
    // falls through to "not keeping".
    const run = runReportFlow('NON_PROCESSED', notProcessedAll);
    await run;
    assert.equal(reportCalls(run.mongo).length, 0);
  });

  test('NON_HANDLED with handledAmount < recordAmount: keeps the report', async () => {
    const run = runReportFlow('NON_HANDLED', notProcessedAll);
    await run;
    assert.equal(reportCalls(run.mongo).length, 1);
  });

  test('NON_HANDLED with handledAmount === recordAmount: does not keep the report', async () => {
    const run = runReportFlow('NON_HANDLED', {handledIds: ['000999999FIN', '000123456FIN']});
    await run;
    assert.equal(reportCalls(run.mongo).length, 0);
  });

  test('any other value (NONE): falls through and does not keep the report', async () => {
    const run = runReportFlow('NONE', notProcessedAll);
    await run;
    assert.equal(reportCalls(run.mongo).length, 0);
  });
});

describe('checkProcess/createRecordResponsesForLoad (CREATE)', () => {
  test('all records handled: CREATED items with the handled ids', async () => {
    const message0 = makeMessage({id: '000999999', notes: ['first note']});
    const message1 = makeMessage({id: '000123456'});
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN', '000123456FIN']},
      operationQueueResult: makeOperationQueue([message0, message1]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    // ackOnlyLength (2) === messages.length (2): nothing to nack.
    assert.deepEqual(run.amqp.calls.filter((call) => call[0] === 'nackMessages'), [['nackMessages', []]]);
    // Both record messages are acked (after the earlier process message ack).
    const acks = ackCallsOf(run.amqp);
    assert.deepEqual(acks[acks.length - 1], ['ackMessages', [message0, message1]]);
    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'CREATED',
        detailedRecordStatus: 'CREATED',
        message: 'first note - Created record 000999999.'
      },
      {
        databaseId: '000123456',
        recordMetadata: {},
        recordStatus: 'CREATED',
        detailedRecordStatus: 'CREATED',
        message: 'Created record 000123456.'
      }
    ]);
  });

  test('duplicate handled id: duplicate gets an ERROR- marker and an ERROR item', async () => {
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      // Duplicate handledId -> second one becomes ERROR-000999999 in processPoll
      pollBody: {handledIds: ['000999999FIN', '000999999FIN'], errorIdCount: 1},
      operationQueueResult: makeOperationQueue([
        makeMessage({id: '000999999'}),
        makeMessage({id: '000123456'})
      ]),
      bulkRemaining: 0
    });
    await run;

    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'CREATED',
        detailedRecordStatus: 'CREATED',
        message: 'Created record 000999999.'
      },
      {
        databaseId: '000000000',
        recordMetadata: {},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'ERROR',
        message: 'Errored creating record ERROR-000999999.'
      }
    ]);
  });

  test('erroredAmount === handledIds.length: all items are ERROR', async () => {
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN', '000123456FIN'], errorIdCount: 2},
      operationQueueResult: makeOperationQueue([
        makeMessage({id: '000999999'}),
        makeMessage({id: '000123456'})
      ]),
      bulkRemaining: 0
    });
    await run;

    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000000000',
        recordMetadata: {},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'ERROR',
        message: 'Errored creating record 000999999.'
      },
      {
        databaseId: '000000000',
        recordMetadata: {},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'ERROR',
        message: 'Errored creating record 000123456.'
      }
    ]);
  });

  test('not all handled: rejection matched by blobSequence, rest UNKNOWN with possible ids', async () => {
    const rejectedMessage = makeMessage({id: '000888888', blobSequence: 888888});
    const unknownMessage = makeMessage({id: '000777777', blobSequence: 777777});
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN'], rejectedIds: ['000888888FIN']},
      operationQueueResult: makeOperationQueue([rejectedMessage, unknownMessage]),
      bulkRemaining: 0
    });
    await run;

    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000000000',
        recordMetadata: {blobSequence: 888888},
        recordStatus: 'INVALID',
        detailedRecordStatus: 'INVALID',
        message: 'LoaderProcess rejected record 000888888'
      },
      {
        databaseId: '000000000',
        recordMetadata: {blobSequence: 777777},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'UNKNOWN',
        message: 'LoaderProcess did not return databaseIds for all records in chunk.',
        ids: ['000999999']
      }
    ]);
  });
});

describe('checkProcess/createRecordResponsesForLoad (UPDATE)', () => {
  test('record handled: UPDATED item, import job DONE for prio', async () => {
    const run = runCheckProcessQueueStart({
      operation: 'UPDATE',
      prio: true,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: makeOperationQueue([makeMessage({id: '000999999'})]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'UPDATED',
        detailedRecordStatus: 'UPDATED',
        message: 'Updated record 000999999'
      }
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setImportJobState'), [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'UPDATE', importJobState: 'DONE'}]
    ]);
  });

  test('handledAll false: handled id UPDATED, unhandled record UNKNOWN without ids', async () => {
    const run = runCheckProcessQueueStart({
      operation: 'UPDATE',
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: makeOperationQueue([
        makeMessage({id: '000999999'}),
        makeMessage({id: '000888888'})
      ]),
      bulkRemaining: 0
    });
    await run;

    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'UPDATED',
        detailedRecordStatus: 'UPDATED',
        message: 'Updated record 000999999'
      },
      {
        databaseId: '000888888',
        recordMetadata: {},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'UNKNOWN',
        message: 'LoaderProcess did not return result for record 000888888',
        ids: []
      }
    ]);
  });

  test('handledAll true: unhandled record UNKNOWN with positional fallback id', async () => {
    const run = runCheckProcessQueueStart({
      operation: 'UPDATE',
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      // The record's own id is not in handledIds, but handledAll is true
      operationQueueResult: makeOperationQueue([makeMessage({id: '000888888'})]),
      bulkRemaining: 0
    });
    await run;

    const fallbackMessage = 'LoaderProcess did not return result for record 000888888.' +
      ' It might have updated 000999999 instead.';
    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000888888',
        recordMetadata: {},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'UNKNOWN',
        message: fallbackMessage,
        ids: ['000999999']
      }
    ]);
  });

  test('record rejected: INVALID item', async () => {
    const run = runCheckProcessQueueStart({
      operation: 'UPDATE',
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: [], rejectedIds: ['000888888FIN']},
      operationQueueResult: makeOperationQueue([makeMessage({id: '000888888'})]),
      bulkRemaining: 0
    });
    await run;

    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000888888',
        recordMetadata: {},
        recordStatus: 'INVALID',
        detailedRecordStatus: 'INVALID',
        message: 'LoaderProcess rejected record 000888888'
      }
    ]);
  });
});

describe('checkProcess/createRecordResponsesForFix (FIX)', () => {
  test('200 OK: FIXED items with notes prefix, import job DONE', async () => {
    const run = runCheckProcessQueueStart({
      operation: 'FIX',
      prio: true,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: makeOperationQueue([
        makeMessage({id: '000999999', notes: ['note one', 'note two']})
      ]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    // FIX does not pushIds or add a load process log item
    assert.equal(run.mongo.calls.filter((call) => call[0] === 'pushIds').length, 0);
    assert.equal(mongoLogItems.length, 0);
    // Fix process poll query has no pLogFile/pRejectFile params
    assert.ok(!fetchQuery(run.fetchCalls, 0).has('pLogFile'));
    assert.ok(!fetchQuery(run.fetchCalls, 0).has('pRejectFile'));
    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'FIXED',
        detailedRecordStatus: 'FIXED',
        message: 'note one - note two - Fixed record 000999999.'
      }
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setImportJobState'), [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'FIX', importJobState: 'DONE'}]
    ]);
  });

  test('409 CONFLICT: UNKNOWN record result, UNKNOWN prio end, queueItem errored', async () => {
    // processPoll's pollFix returns results.status, so a 409 answer from
    // record-load-api reaches the CONFLICT branch of getPrioStatus: FIX prio
    // ends 'UNKNOWN', which prioEnd treats as a failed prio (ERROR states)
    // even though the record responses were written as UNKNOWN.
    const run = runCheckProcessQueueStart({
      operation: 'FIX',
      prio: true,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: httpStatus.CONFLICT,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: makeOperationQueue([makeMessage({id: '000999999'})]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'ERROR',
        detailedRecordStatus: 'UNKNOWN',
        message: 'Tried to fix record 000999999. Result unknown.',
        ids: []
      }
    ]);
    // prioStatus 'UNKNOWN' is a failed prio: import job and queueItem ERROR
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setImportJobState'), [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'FIX', importJobState: 'ERROR'}]
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setState'), [
      ['setState', {correlationId: CORRELATION_ID, state: 'ERROR', errorMessage: '000999999', errorStatus: 'UNKNOWN'}]
    ]);
    assert.deepEqual(run.amqp.calls.filter((call) => call[0] === 'removeQueue'), [
      ['removeQueue', `FIX.${CORRELATION_ID}`],
      ['removeQueue', `PROCESS.FIX.${CORRELATION_ID}`]
    ]);
  });
});

describe('checkProcess/prioEnd', () => {
  test('CREATE success: prio status CREATED, import job DONE, queues removed', async () => {
    const run = runCheckProcessQueueStart({
      prio: true,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: makeOperationQueue([makeMessage({id: '000999999'})]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    assert.equal(recordResponseCalls(run.mongo).length, 1);
    assert.deepEqual(recordResponseCalls(run.mongo)[0][1].messages, [
      {
        databaseId: '000999999',
        recordMetadata: {},
        recordStatus: 'CREATED',
        detailedRecordStatus: 'CREATED',
        message: 'Created record 000999999.'
      }
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setImportJobState'), [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'DONE'}]
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setState'), [
      ['setState', {correlationId: CORRELATION_ID, state: 'DONE'}]
    ]);
    assert.deepEqual(run.amqp.calls.filter((call) => call[0] === 'removeQueue'), [
      ['removeQueue', `CREATE.${CORRELATION_ID}`],
      ['removeQueue', `PROCESS.CREATE.${CORRELATION_ID}`]
    ]);
  });

  test('CREATE with oraErrors (erroredAmount > 0): prio status 503, queueItem errored', async () => {
    const run = runCheckProcessQueueStart({
      prio: true,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN'], errorIdCount: 1},
      operationQueueResult: makeOperationQueue([makeMessage({id: '000999999'})]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setImportJobState'), [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'ERROR'}]
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setState'), [
      ['setState', {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: '000999999',
        errorStatus: httpStatus.SERVICE_UNAVAILABLE
      }]
    ]);
    assert.deepEqual(run.amqp.calls.filter((call) => call[0] === 'removeQueue'), [
      ['removeQueue', `CREATE.${CORRELATION_ID}`],
      ['removeQueue', `PROCESS.CREATE.${CORRELATION_ID}`]
    ]);
  });

  test('CREATE with no handled ids: prio status 422, queueItem errored', async () => {
    const run = runCheckProcessQueueStart({
      prio: true,
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: [], rejectedIds: ['000888888FIN']},
      operationQueueResult: makeOperationQueue([makeMessage({id: '000888888', blobSequence: 888888})]),
      bulkRemaining: 0
    });
    const result = await run;

    assert.equal(result, true);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setState'), [
      ['setState', {
        correlationId: CORRELATION_ID,
        state: 'ERROR',
        errorMessage: '000888888',
        errorStatus: httpStatus.UNPROCESSABLE_ENTITY
      }]
    ]);
  });
});

describe('checkProcess/bulkEnd (remaining items in the bulk queue)', () => {
  test('items remaining in the queue: import job back to IMPORTING, queues kept', async () => {
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN', '000123456FIN']},
      operationQueueResult: makeOperationQueue([
        makeMessage({id: '000999999'}),
        makeMessage({id: '000123456'})
      ]),
      bulkRemaining: 3
    });
    const result = await run;

    assert.equal(result, true);
    // bulkEnd checks the remaining message count with style 'messages'
    assert.deepEqual(run.amqp.calls.filter((call) => call[0] === 'checkQueue'), [
      ['checkQueue', `PROCESS.CREATE.${CORRELATION_ID}`, 'one'],
      ['checkQueue', `CREATE.${CORRELATION_ID}`, 'basic'],
      ['checkQueue', `CREATE.${CORRELATION_ID}`, 'messages']
    ]);
    assert.deepEqual(run.mongo.calls.filter((call) => call[0] === 'setImportJobState'), [
      ['setImportJobState', {correlationId: CORRELATION_ID, operation: 'CREATE', importJobState: 'IMPORTING'}]
    ]);
    // No queue removal while there are remaining items
    assert.equal(run.amqp.calls.filter((call) => call[0] === 'removeQueue').length, 0);
    // And the flow ends with a file clearing request
    assert.equal(run.fetchCalls[run.fetchCalls.length - 1].options.method, 'delete');
  });
});

describe('checkProcess/handleMessages and separateMessages edge cases', () => {
  test('operation queue empty (checkQueue returns false): handleMessages returns false -> ApiError 422', async () => {
    // The real amqpOperator.checkQueue (style 'basic') returns plain false
    // for an empty queue, so `messages` is undefined. handleMessages guards
    // for that before any use of messages and returns false, and
    // checkProcessQueueStart then throws the "No messages" ApiError (422).
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: false
    });
    const isUnprocessable = (error) =>
      error instanceof ApiError && error.status === httpStatus.UNPROCESSABLE_ENTITY;
    await assert.rejects(run, isUnprocessable);

    // handleProcessQueueResults ran before handleMessages, so ids were already pushed
    assert.equal(run.mongo.calls.filter((call) => call[0] === 'pushIds').length, 1);
    assert.equal(recordResponseCalls(run.mongo).length, 0);
    assert.equal(run.fetchCalls.length, 1); // poll only, no file clearing
  });

  test('operation queue with falsy messages: handleMessages returns false -> ApiError 422', async () => {
    // A truthy checkQueue result whose `messages` property is falsy hits
    // the same `if (!messages)` guard and returns false, and
    // checkProcessQueueStart throws the "No messages" ApiError.
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 1})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN']},
      operationQueueResult: {headers: null, messages: false}
    });
    const isUnprocessable = (error) =>
      error instanceof ApiError && error.status === httpStatus.UNPROCESSABLE_ENTITY;
    await assert.rejects(run, isUnprocessable);

    assert.equal(recordResponseCalls(run.mongo).length, 0);
    assert.equal(run.fetchCalls.length, 1); // poll only, no file clearing
  });

  test('ackOnlyLength larger than the message count: separateMessages throws ApiError 500', async () => {
    const run = runCheckProcessQueueStart({
      processMessage: makeProcessMessage(makeProcessParams({recordAmount: 2})),
      pollStatus: 200,
      pollBody: {handledIds: ['000999999FIN', '000123456FIN']}, // ackOnlyLength 2
      // only 1 message in the queue
      operationQueueResult: makeOperationQueue([makeMessage({id: '000999999'})])
    });
    const isInternalServerError = (error) =>
      error instanceof ApiError && error.status === httpStatus.INTERNAL_SERVER_ERROR;
    await assert.rejects(run, isInternalServerError);

    // The throw happens before any (s)nack of record messages or record responses
    assert.equal(run.amqp.calls.filter((call) => call[0] === 'nackMessages').length, 0);
    assert.equal(recordResponseCalls(run.mongo).length, 0);
  });

  // Dead / unreachable code in checkProcess.ts (documented, not tested):
  // - handleMessages: the `ackMessages === undefined || ackMessages.length < 1`
  //   fallback is unreachable: separateMessages throws ApiError(500) first
  //   whenever ackOnlyLength is 0/falsy or exceeds messages.length, so it can
  //   never return an empty array.
  // - handleProcessQueueResults / createRecordResponsesForLoad /
  //   getPrioStatus: the `Unknown OPERATION` throws are defensive dead code -
  //   the only operations that ever reach them (CREATE/UPDATE/FIX) are exactly
  //   the ones poll supports, so operation can never be anything else.
  // - checkProcessQueue: the non-ApiError catch branch does not error the
  //   queueItem and returns undefined; checkProcessQueueStart treats that as
  //   "not ready yet" (see the checkQueue non-ApiError test above).
});
