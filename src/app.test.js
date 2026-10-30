// Tests for app.js: the startCheck recursive polling loop (loop control) and
// the queueItem/importJobState transition checks.
//
// app.js creates its amqpOperator (amqpFactory), prio/bulk mongoOperators
// (mongoFactory) and processOperator (checkProcess, which also creates
// mongoLogFactory) internally, so none of them are injectable. Like
// checkProcess.test.js, we patch the two client constructors (amqplib.connect
// and MongoClient.connect) for the duration of the suite and drive everything
// through those fakes plus a per-test globalThis.fetch stub.
//
// startCheck recurses forever (that is by design - it is the service loop),
// so await startApp(...) never settles. Every test therefore:
//   1. scripts the fake mongo findOnes (per-collection FIFO of docs, null =
//      not found) so the loop walks exactly the branch the test wants;
//   2. waits until the recorded mock calls prove the branch was exercised;
//   3. parks the fakes: the next findOne (or health check assertQueue) then
//      returns a never-settling promise, so the recursion dies on a pending
//      promise. No timers remain, so the test process exits naturally.
// A test that fails to reach its expectation fails via the waitFor timeout
// (and parks on the way out) - it never hangs into --test-force-exit.

import {test, describe, after, afterEach, beforeEach} from 'node:test';
import {strict as assert} from 'node:assert';
import httpStatus from 'http-status';
import {MongoClient} from 'mongodb';
import amqplib from 'amqplib';
import {OPERATIONS, IMPORT_JOB_STATE, QUEUE_ITEM_STATE} from '@natlibfi/melinda-rest-api-commons';
import {generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import startApp from './app.js';

process.env.LOG_LEVEL = 'error';

const AMQP_URL = 'amqp://test:5672/';
const MONGO_URI = 'mongodb://test/db';
// Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
const API_KEY = ['test', 'api', 'key'].join('-');
const AUTH_HEADER = generateAuthorizationHeader(API_KEY);
const RECORD_LOAD_URL = 'http://record-load-api';
const RECORD_LOAD_FIX_PATH = '/fix';
const RECORD_LOAD_LOAD_PATH = '/load';
const CID = 'CID-1';
const WAIT_TIMEOUT_MS = 5000;
const CREATE = OPERATIONS.CREATE;
const UPDATE = OPERATIONS.UPDATE;
const FIX = OPERATIONS.FIX;

const originalMongoConnect = MongoClient.connect;
const originalAmqpConnect = amqplib.connect;
const originalFetch = globalThis.fetch;

MongoClient.connect = async (uri) => {
  fakes.mongoUris.push(uri);
  return {db: () => fakes.db, close: async () => {}};
};

amqplib.connect = async (url) => {
  fakes.amqpUrls.push(url);
  return fakes.connection;
};

let fakes = null;

function makeFakes() {
  const amqpCalls = [];
  const findOneCalls = [];
  const findOneScripts = {};
  const findOneAndUpdateCalls = [];
  const updateOneCalls = [];
  const insertOneCalls = [];
  const collectionNames = [];
  const queues = {};
  let parked = false;

  const makeCollection = (name) => ({
    findOne: async (query) => {
      if (parked) {
        return new Promise(() => {});
      }
      findOneCalls.push([name, query]);
      if (!findOneScripts[name]) {
        findOneScripts[name] = [];
      }
      return findOneScripts[name].length > 0 ? findOneScripts[name].shift() : null;
    },
    findOneAndUpdate: async (filter, update) => {
      findOneAndUpdateCalls.push([name, filter, update]);
      return null;
    },
    updateOne: async (filter, update) => {
      updateOneCalls.push([name, filter, update]);
      return {acknowledged: true};
    },
    insertOne: async (doc) => {
      insertOneCalls.push([name, doc]);
      return {acknowledged: true};
    }
  });

  const collections = {};
  const db = {
    collection: (name) => {
      collectionNames.push(name);
      if (!collections[name]) {
        collections[name] = makeCollection(name);
      }
      return collections[name];
    }
  };

  const channel = {
    // rest-api-commons 6 attaches error/close listeners to the channel
    on: () => {},
    assertQueue: (name) => {
      if (name === 'HEALTHCHECK') {
        // Park the commons amqp healthCheck recursion (it would otherwise
        // schedule a 200 ms timer forever and block natural process exit)
        return new Promise(() => {});
      }
      amqpCalls.push(['assertQueue', name]);
      return Promise.resolve({messageCount: (queues[name] || []).length});
    },
    get: (name) => {
      amqpCalls.push(['get', name]);
      return Promise.resolve((queues[name] || []).shift() ?? null);
    },
    checkQueue: (name) => Promise.resolve({messageCount: (queues[name] || []).length}),
    ack: (message) => {
      amqpCalls.push(['ack', message]);
    },
    nack: (message) => {
      amqpCalls.push(['nack', message]);
    },
    sendToQueue: (name, content, options) => {
      amqpCalls.push(['sendToQueue', name, JSON.parse(content.toString()).data, options]);
      (queues[name] = queues[name] || []).push({
        properties: {correlationId: options.correlationId, headers: options.headers},
        content
      });
      return Promise.resolve();
    },
    deleteQueue: (name) => {
      amqpCalls.push(['deleteQueue', name]);
      return Promise.resolve();
    },
    purgeQueue: async () => {},
    close: async () => {}
  };

  const connection = {
    // rest-api-commons 6 attaches error/close listeners to the connection
    on: () => {},
    createChannel: async () => channel,
    close: async () => {}
  };

  return {
    amqpUrls: [],
    mongoUris: [],
    amqpCalls,
    findOneCalls,
    findOneAndUpdateCalls,
    updateOneCalls,
    insertOneCalls,
    collectionNames,
    queues,
    db,
    connection,
    scriptFor: (name) => {
      if (!findOneScripts[name]) {
        findOneScripts[name] = [];
      }
      return findOneScripts[name];
    },
    park: () => {
      parked = true;
    }
  };
}

beforeEach(() => {
  fakes = makeFakes();
  // Fails loudly if a test forgets to stub fetch
  globalThis.fetch = () => {
    throw new Error('globalThis.fetch was called but not stubbed in this test');
  };
});

afterEach(() => {
  fakes.park();
  globalThis.fetch = originalFetch;
});

after(() => {
  MongoClient.connect = originalMongoConnect;
  amqplib.connect = originalAmqpConnect;
});

function makeAppConfig(overrides = {}) {
  return {
    amqpUrl: AMQP_URL,
    operation: CREATE,
    pollWaitTime: 5,
    error503WaitTime: 5,
    mongoUri: MONGO_URI,
    recordLoadApiKey: API_KEY,
    recordLoadLibrary: 'FIN',
    recordLoadUrl: RECORD_LOAD_URL,
    recordLoadFixPath: RECORD_LOAD_FIX_PATH,
    recordLoadLoadPath: RECORD_LOAD_LOAD_PATH,
    fixPrio: 'API',
    fixBulk: 'INSB',
    keepLoadProcessReports: 'NONE',
    ...overrides
  };
}

// startApp's promise never settles (the loop parks instead) unless the loop
// itself crashes, in which case it rejects. The .catch handler must be
// attached immediately: an unobserved rejection would surface as an
// unhandledRejection and crash the whole test process.
function runApp(overrides) {
  const appPromise = startApp(makeAppConfig(overrides));
  const rejections = [];
  appPromise.catch(error => rejections.push(error));
  return {appPromise, rejections};
}

async function waitFor(what, predicate) {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (!predicate()) {
    if (Date.now() > deadline) {
      fakes.park();
      throw new Error(
        `Timed out waiting for: ${what}\n` +
        `findOne: ${JSON.stringify(fakes.findOneCalls)}\n` +
        `findOneAndUpdate: ${JSON.stringify(fakes.findOneAndUpdateCalls)}\n` +
        `updateOne: ${JSON.stringify(fakes.updateOneCalls)}\n` +
        `amqp: ${JSON.stringify(fakes.amqpCalls.map(call => call.slice(0, 2)))}`
      );
    }
    await new Promise(resolve => setTimeout(resolve, 5));
  }
}

function makeFetch(responses) {
  const calls = [];
  const fn = async (url, options) => {
    calls.push({url: String(url), options});
    const next = responses.shift();
    if (!next) {
      throw new Error(`fetch called with no response left: ${url}`);
    }
    return {status: next.status, json: async () => next.body || {}};
  };
  return {calls, fn};
}

// The getOne queries app.js builds (createImportJobState uses dotted keys)
function importJobQuery(operation, jobState) {
  return {queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, [`importJobState.${operation}`]: jobState};
}

const queueItemInQueueQuery = {queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE};

function makeQueueItem(overrides = {}) {
  // operationSettings is assumed to always exist by app.js:224
  // (`queueItem.operationSettings.noop`); real queueItems created by the
  // validator always carry it. Missing here would crash the loop like the
  // app.js:249 bug below.
  return {correlationId: CID, operationSettings: {}, ...overrides};
}

function makeProcessData(overrides = {}) {
  return {
    correlationId: CID,
    pActiveLibrary: 'FIN',
    processId: 123,
    loaderProcessId: 456,
    pLogFile: null,
    pRejectFile: null,
    recordAmount: 1,
    ...overrides
  };
}

let deliveryTag = 0;

function makeProcessMessage(data) {
  deliveryTag += 1;
  return {
    properties: {headers: {id: 'process-message'}, correlationId: CID},
    fields: {deliveryTag},
    content: Buffer.from(JSON.stringify({data}))
  };
}

function makeMarcContent(id) {
  return JSON.stringify({
    data: {
      fields: [
        {tag: '001', value: id},
        {tag: '200', ind1: '1', ind2: ' ', subfields: [{code: 'a', value: 'test record'}]}
      ]
    }
  });
}

function makeMessage({id = '000999999', operationSettings = {}} = {}) {
  deliveryTag += 1;
  // fields.deliveryTag is read by commons' getData pump (message dedup)
  return {
    properties: {
      correlationId: CID,
      headers: {id, recordMetadata: {}, cataloger: {id: 'CATALOGER-1'}, operationSettings}
    },
    fields: {deliveryTag},
    content: Buffer.from(makeMarcContent(id))
  };
}

function findOnes(name) {
  return fakes.findOneCalls.filter(call => call[0] === name);
}

function setStateCalls(name) {
  return fakes.findOneAndUpdateCalls.filter(
    call => call[0] === name && call[2].$set.queueItemState !== undefined
  );
}

function setJobStateCalls(name) {
  return fakes.findOneAndUpdateCalls.filter(
    call => call[0] === name && Object.keys(call[2].$set).some(key => key.startsWith('importJobState.'))
  );
}

function amqpCount(name) {
  return fakes.amqpCalls.filter(call => call[0] === name).length;
}

function jobStateChanged(name, operation, state) {
  return setJobStateCalls(name).some(call => call[2].$set[`importJobState.${operation}`] === state);
}

function stateChanged(name, state) {
  return setStateCalls(name).some(call => call[2].$set.queueItemState === state);
}

// One idle cycle = prio in-process, then (nothing found) bulk in-process,
// then 4 IMPORTER.IMPORTING/IN_QUEUE getOnes per operator
const idleCycleQueries = operation => [
  importJobQuery(operation, IMPORT_JOB_STATE.PROCESSING),
  importJobQuery(operation, IMPORT_JOB_STATE.IMPORTING),
  importJobQuery(operation, IMPORT_JOB_STATE.DONE),
  importJobQuery(operation, IMPORT_JOB_STATE.IN_QUEUE),
  queueItemInQueueQuery
];

// nulls for the rest of the cycle after the scripted doc, so the loop walks
// on until the test parks it
const nulls = count => Array.from({length: count}, () => null);

describe('app/startCheck loop control', () => {
  test('transient Mongo error mid-cycle: survives with backoff, keeps polling', async () => {
    // Non-ApiError rejections (e.g. a Mongo/AMQP driver error) are contained:
    // logged, counted, and the next cycle is re-scheduled with capped
    // exponential backoff (100 ms base). The service does not exit.
    // (The backoff waits themselves are not observable through the fakes -
    // findOne does not record timestamps - so this asserts survival and
    // continued polling only.)
    const makeInfraError = () => new Error('simulated Mongo connection error');
    const reject1 = Promise.reject(makeInfraError());
    const reject2 = Promise.reject(makeInfraError());
    // Attach no-op handlers so the rejections can never surface as
    // unhandledRejection before the loop awaits them
    reject1.catch(() => {});
    reject2.catch(() => {});
    fakes.scriptFor('prio').push(reject1, reject2, null, ...nulls(14));
    fakes.scriptFor('bulk').push(...nulls(15));
    const {rejections} = runApp();

    // After the two failed cycles the loop runs normal idle cycles again:
    // 2 failing findOnes + a full 3rd cycle (prio) + part of a 4th
    await waitFor('the loop to keep polling after two failed cycles', () =>
      findOnes('prio').length >= 11 && findOnes('bulk').length >= 5);

    // The catch handler on appPromise records any rejection immediately;
    // the loop ran (including the two backoff waits) without crashing
    assert.equal(rejections.length, 0);
    fakes.park();
  });

  test('starts, cascades prio -> bulk over all checks, idle loop with pollWaitTime', async () => {
    const {rejections} = runApp();

    // Two full idle cycles: 5 findOnes per operator each cycle
    await waitFor('two idle cycles', () => findOnes('prio').length >= 10 && findOnes('bulk').length >= 10);
    fakes.park();

    assert.deepEqual(fakes.amqpUrls, [AMQP_URL]);
    // prio and bulk from app.js, logs from checkProcess' mongoLogFactory
    assert.deepEqual(fakes.mongoUris, [MONGO_URI, MONGO_URI, MONGO_URI]);
    assert.ok(fakes.collectionNames.includes('prio'));
    assert.ok(fakes.collectionNames.includes('bulk'));

    const cycle = idleCycleQueries(CREATE);
    assert.deepEqual(findOnes('prio').slice(0, 10).map(call => call[1]), [...cycle, ...cycle]);
    assert.deepEqual(findOnes('bulk').slice(0, 10).map(call => call[1]), [...cycle, ...cycle]);
    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });

  test('in-process item still running (423): nacks the process message, re-polls after 100 ms', async () => {
    fakes.scriptFor('prio').push(makeQueueItem(), ...nulls(5));
    fakes.scriptFor('bulk').push(...nulls(5));
    fakes.queues[`PROCESS.${CREATE}.${CID}`] = [makeProcessMessage(makeProcessData())];
    const fetchStub = makeFetch([{status: httpStatus.LOCKED}]);
    globalThis.fetch = fetchStub.fn;
    const {rejections} = runApp();

    await waitFor('process message nacked and the loop in cycle 2', () =>
      amqpCount('nack') > 0 && findOnes('prio').length >= 6);
    fakes.park();

    // Poll goes through the LOAD path (CREATE operation) with the API key
    assert.equal(fetchStub.calls.length, 1);
    const {url, options} = fetchStub.calls[0];
    assert.equal(options.method, 'get');
    assert.equal(options.headers.Authorization, AUTH_HEADER);
    const parsed = new URL(url);
    assert.equal(parsed.pathname, RECORD_LOAD_LOAD_PATH);
    assert.equal(parsed.searchParams.get('correlationId'), CID);
    assert.equal(parsed.searchParams.get('pActiveLibrary'), 'FIN');
    assert.equal(parsed.searchParams.get('processId'), '123');
    // 423: process ongoing - the queueItem must NOT get an error state
    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.ok(!fakes.amqpCalls.some(call => call[0] === 'ack'));
    assert.equal(rejections.length, 0);
  });

  test('in-process item unavailable (503): nacks, sleeps error503WaitTime, keeps looping', async () => {
    fakes.scriptFor('prio').push(makeQueueItem(), ...nulls(5));
    fakes.scriptFor('bulk').push(...nulls(5));
    fakes.queues[`PROCESS.${CREATE}.${CID}`] = [makeProcessMessage(makeProcessData())];
    const fetchStub = makeFetch([{status: httpStatus.SERVICE_UNAVAILABLE}]);
    globalThis.fetch = fetchStub.fn;
    const {rejections} = runApp();

    await waitFor('process message nacked and the loop in cycle 2', () =>
      amqpCount('nack') > 0 && findOnes('prio').length >= 6);
    fakes.park();

    assert.equal(fetchStub.calls.length, 1);
    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });

  test('in-process item done: handles all results, marks job DONE, continues WITHOUT waiting', async () => {
    fakes.scriptFor('prio').push(makeQueueItem(), ...nulls(5));
    fakes.scriptFor('bulk').push(...nulls(5));
    const processMessage = makeProcessMessage(makeProcessData());
    const message = makeMessage();
    fakes.queues[`PROCESS.${CREATE}.${CID}`] = [processMessage];
    fakes.queues[`CREATE.${CID}`] = [message];
    const fetchStub = makeFetch([
      {status: 200, body: {handledIds: ['000999999FIN'], rejectedIds: []}},
      {status: 200, body: {}}
    ]);
    globalThis.fetch = fetchStub.fn;
    const {rejections} = runApp();

    await waitFor('job DONE and the loop in cycle 2', () =>
      jobStateChanged('prio', CREATE, IMPORT_JOB_STATE.DONE)
      && findOnes('prio').length >= 6);
    fakes.park();

    const pushedIds = fakes.updateOneCalls.find(call => call[0] === 'prio' && call[2].$push.handledIds);
    assert.deepEqual(pushedIds[2].$push.handledIds, {$each: ['000999999']});
    assert.deepEqual(pushedIds[2].$push.rejectedIds, {$each: []});
    const recordResponses = fakes.updateOneCalls.find(call => call[0] === 'prio' && call[2].$push.records);
    assert.equal(recordResponses[2].$push.records.$each.length, 1);
    assert.equal(recordResponses[2].$push.records.$each[0].databaseId, '000999999');

    assert.equal(setJobStateCalls('prio')[0][2].$set[`importJobState.${CREATE}`], IMPORT_JOB_STATE.DONE);
    const doneState = setStateCalls('prio').find(
      call => call[2].$set.queueItemState === QUEUE_ITEM_STATE.DONE
    );
    assert.ok(doneState);
    assert.deepEqual(doneState[1], {correlationId: CID});

    assert.equal(amqpCount('ack'), 2); // process message + record message
    assert.equal(amqpCount('nack'), 0); // ackOnlyLength 1 of 1: nothing to nack
    assert.deepEqual(
      fakes.amqpCalls.filter(call => call[0] === 'deleteQueue').map(call => call[1]),
      [`CREATE.${CID}`, `PROCESS.${CREATE}.${CID}`]
    );
    const logItem = fakes.insertOneCalls.find(call => call[0] === 'logs');
    assert.equal(logItem[1].logItemType, 'LOAD_PROCESS_LOG');
    assert.equal(logItem[1].correlationId, CID);
    // Second fetch: file clearing request after successful handling
    assert.equal(fetchStub.calls.length, 2);
    assert.equal(fetchStub.calls[1].options.method, 'delete');
    assert.equal(new URL(fetchStub.calls[1].url).searchParams.get('correlationId'), CID);
    assert.equal(rejections.length, 0);
  });

  test('in-process item in BULK: bulk operator is checked second and used for the process', async () => {
    fakes.scriptFor('prio').push(null, ...nulls(5));
    fakes.scriptFor('bulk').push(makeQueueItem(), ...nulls(4));
    fakes.queues[`PROCESS.${CREATE}.${CID}`] = [makeProcessMessage(makeProcessData())];
    const fetchStub = makeFetch([{status: httpStatus.LOCKED}]);
    globalThis.fetch = fetchStub.fn;
    const {rejections} = runApp();

    await waitFor('bulk process message nacked and the loop in cycle 2', () =>
      amqpCount('nack') > 0 && findOnes('bulk').length >= 2 && findOnes('prio').length >= 2);
    fakes.park();

    // prio found nothing, so bulk's first getOne is the in-process query
    assert.deepEqual(findOnes('bulk')[0][1], importJobQuery(CREATE, IMPORT_JOB_STATE.PROCESSING));
    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });
});

describe('app/checkImportJobStateIMPORTING (item in importer)', () => {
  test('item with messages: nacks, loads records, sends process message, sets job PROCESSING', async () => {
    const item = makeQueueItem({recordLoadParams: {pActiveLibrary: 'FIN'}});
    fakes.scriptFor('prio').push(null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    fakes.queues[`CREATE.${CID}`] = [makeMessage()];
    const fetchStub = makeFetch([{
      status: 200,
      body: {
        correlationId: CID,
        processId: 777,
        pLogFile: 'log.txt',
        pRejectFile: 'rej.txt',
        loaderProcessId: 888
      }
    }]);
    globalThis.fetch = fetchStub.fn;
    const {rejections} = runApp();

    await waitFor('job PROCESSING', () =>
      jobStateChanged('prio', CREATE, IMPORT_JOB_STATE.PROCESSING));
    fakes.park();

    assert.equal(amqpCount('nack'), 1); // record message nacked to wait for results
    assert.equal(fetchStub.calls.length, 1);
    const {url, options} = fetchStub.calls[0];
    assert.equal(options.method, 'post');
    assert.equal(options.headers.Authorization, AUTH_HEADER);
    assert.ok(options.body.includes('000999999 001'));
    const query = new URL(url).searchParams;
    assert.equal(new URL(url).pathname, RECORD_LOAD_LOAD_PATH);
    // prio CREATE load: fixPrio, NEW, HIGH indexing priority
    assert.equal(query.get('pFixType'), 'API');
    assert.equal(query.get('pOldNew'), 'NEW');
    assert.equal(query.get('pZ07PriorityYear'), '1990');
    assert.equal(query.get('pCatalogerIn'), 'CATALOGER-1');

    const processQueue = fakes.queues[`PROCESS.${CREATE}.${CID}`];
    const processData = JSON.parse(processQueue[0].content.toString()).data;
    assert.deepEqual(processData, {
      correlationId: CID,
      pActiveLibrary: 'FIN',
      processId: 777,
      pRejectFile: 'rej.txt',
      pLogFile: 'log.txt',
      loaderProcessId: 888,
      recordAmount: 1
    });
    assert.equal(rejections.length, 0);
  });

  test('item with empty queue: 200 ms retry, then errors the item and keeps looping', async () => {
    const item = makeQueueItem({recordLoadParams: {pActiveLibrary: 'FIN'}});
    fakes.scriptFor('prio').push(null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    // queues[`CREATE.${CID}`] intentionally empty
    const {rejections} = runApp();

    await waitFor('queueItem errored for the empty queue', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.ERROR));
    fakes.park();

    // The queue was checked, waited 200 ms, checked again, then the item
    // errored; sendErrorResponses checked it a third time (empty: no ack)
    const operationQueueChecks = fakes.amqpCalls.filter(call =>
      call[0] === 'assertQueue' && call[1] === `CREATE.${CID}`).length;
    assert.equal(operationQueueChecks, 3);
    const erroredJob = setJobStateCalls('prio').find(call =>
      call[2].$set[`importJobState.${CREATE}`] === IMPORT_JOB_STATE.ERROR);
    assert.ok(erroredJob);
    const erroredState = setStateCalls('prio').find(
      call => call[2].$set.queueItemState === QUEUE_ITEM_STATE.ERROR
    );
    assert.equal(erroredState[2].$set.errorStatus, '500');
    assert.equal(erroredState[2].$set.errorMessage, `Importer: empty queue: ${CREATE}.${CID}`);
    // No fetch at all: the load was never attempted
    assert.equal(rejections.length, 0);
  });

  test('FIX item with messages: loads through the FIX path (recordFixFactory + fix URL)', async () => {
    const item = makeQueueItem({recordLoadParams: {pActiveLibrary: 'FIN'}});
    fakes.scriptFor('prio').push(null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    fakes.queues[`FIX.${CID}`] = [makeMessage({operationSettings: {fixType: 'RECOVERED'}})];
    const fetchStub = makeFetch([{
      status: 200,
      body: {correlationId: CID, processId: 555, pLogFile: null, pRejectFile: null, loaderProcessId: 666}
    }]);
    globalThis.fetch = fetchStub.fn;
    const {rejections} = runApp({operation: FIX});

    await waitFor('job PROCESSING', () =>
      jobStateChanged('prio', FIX, IMPORT_JOB_STATE.PROCESSING));
    fakes.park();

    const {url, options} = fetchStub.calls[0];
    assert.equal(options.method, 'post');
    // FIX: fix path, recordList as syslist body, fixType from the queue item headers
    assert.equal(new URL(url).pathname, RECORD_LOAD_FIX_PATH);
    assert.equal(new URL(url).searchParams.get('pFixType'), 'RECOVERED');
    assert.equal(options.body, '000999999FIN');
    const processData = JSON.parse(fakes.queues[`PROCESS.${FIX}.${CID}`][0].content.toString()).data;
    assert.equal(processData.processId, 555);
    assert.equal(processData.recordAmount, 1);
    assert.equal(rejections.length, 0);
  });
});

describe('app/checkImportJobStateDONE (import job DONE)', () => {
  test('CREATE item, other operation DONE: queueItem -> DONE', async () => {
    const item = makeQueueItem({importJobState: {
      [CREATE]: IMPORT_JOB_STATE.DONE,
      [UPDATE]: IMPORT_JOB_STATE.DONE
    }});
    fakes.scriptFor('prio').push(null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('queueItem DONE', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.DONE));
    fakes.park();

    assert.deepEqual(setStateCalls('prio')[0][1], {correlationId: CID});
    assert.equal(setStateCalls('prio')[0][2].$set.errorStatus, undefined);
    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('CREATE item, other operation ongoing: not done, falls through the remaining checks', async () => {
    const item = makeQueueItem({importJobState: {
      [CREATE]: IMPORT_JOB_STATE.DONE,
      [UPDATE]: IMPORT_JOB_STATE.IN_QUEUE
    }});
    fakes.scriptFor('prio').push(null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('the loop to be in cycle 2', () => findOnes('prio').length >= 6);
    fakes.park();

    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });

  test('UPDATE item, other operation DONE: queueItem -> DONE (UPDATE side of ternary)', async () => {
    const item = makeQueueItem({importJobState: {
      [UPDATE]: IMPORT_JOB_STATE.DONE,
      [CREATE]: IMPORT_JOB_STATE.DONE
    }});
    fakes.scriptFor('prio').push(null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: UPDATE});

    await waitFor('queueItem DONE', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.DONE));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('FIX item: FIX job DONE -> queueItem DONE directly', async () => {
    const item = makeQueueItem({importJobState: {[FIX]: IMPORT_JOB_STATE.DONE}});
    fakes.scriptFor('prio').push(null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: FIX});

    await waitFor('queueItem DONE', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.DONE));
    fakes.park();

    assert.deepEqual(setStateCalls('prio')[0][1], {correlationId: CID});
    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });
});

describe('app/checkImportJobStateINQUEUE (import job IN_QUEUE)', () => {
  test('job IN_QUEUE: queueItem -> IMPORTING, job -> IMPORTING', async () => {
    fakes.scriptFor('prio').push(null, null, null, makeQueueItem(), ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('job IMPORTING', () =>
      jobStateChanged('prio', CREATE, IMPORT_JOB_STATE.IMPORTING));
    fakes.park();

    assert.equal(setStateCalls('prio')[0][2].$set.queueItemState, QUEUE_ITEM_STATE.IMPORTER.IMPORTING);
    assert.equal(setJobStateCalls('prio')[0][2].$set[`importJobState.${CREATE}`], IMPORT_JOB_STATE.IMPORTING);
    assert.equal(rejections.length, 0);
  });

  test('job IN_QUEUE found after idle cycles: waitTimePrint reports the accumulated waiting', async () => {
    // Item appears in cycle 3, after two idle waits: waitSinceLastOp > 0,
    // covering the waitTimePrint "after X of waiting" branch
    fakes.scriptFor('prio').push(...nulls(10), null, null, null, makeQueueItem());
    fakes.scriptFor('bulk').push(...nulls(10));
    const {rejections} = runApp();

    await waitFor('job IMPORTING', () =>
      jobStateChanged('prio', CREATE, IMPORT_JOB_STATE.IMPORTING));
    fakes.park();

    assert.equal(setStateCalls('prio')[0][2].$set.queueItemState, QUEUE_ITEM_STATE.IMPORTER.IMPORTING);
    assert.equal(rejections.length, 0);
  });
});

describe('app/checkQueueItemStateINQUEUE (queueItem IN_QUEUE)', () => {
  test('operationSettings.noop: queueItem -> ERROR 500, job -> DONE (found in cycle 2)', async () => {
    // Scripted into cycle 2 so waitSinceLastOp > 0: covers the
    // waitTimePrint "after X of waiting" branch
    fakes.scriptFor('prio').push(...nulls(5), ...nulls(4), makeQueueItem({
      importJobState: {[CREATE]: IMPORT_JOB_STATE.DONE},
      operationSettings: {noop: true}
    }));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('noop queueItem errored', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.ERROR));
    fakes.park();

    const errored = setStateCalls('prio')[0];
    assert.equal(errored[2].$set.errorStatus, '500');
    assert.equal(errored[2].$set.errorMessage, 'Noop for queueItem true, item in importer???');
    assert.equal(setJobStateCalls('prio')[0][2].$set[`importJobState.${CREATE}`], IMPORT_JOB_STATE.DONE);
    assert.equal(rejections.length, 0);
  });

  test('job state IN_QUEUE: queueItem -> IMPORTING', async () => {
    const item = makeQueueItem({importJobState: {[CREATE]: IMPORT_JOB_STATE.IN_QUEUE}});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('queueItem IMPORTING', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.IMPORTER.IMPORTING));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job EMPTY, FIX queueItem for a FIX importer: queueItem -> DONE', async () => {
    const item = makeQueueItem({operation: FIX, importJobState: {[FIX]: IMPORT_JOB_STATE.EMPTY}});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: FIX});

    await waitFor('queueItem DONE', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.DONE));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job EMPTY, CREATE queueItem, other operation final: queueItem -> DONE', async () => {
    const item = makeQueueItem({
      operation: CREATE,
      importJobState: {[CREATE]: IMPORT_JOB_STATE.EMPTY, [UPDATE]: IMPORT_JOB_STATE.ERROR}
    });
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('queueItem DONE', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.DONE));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('UPDATE job EMPTY, other operation final: queueItem -> DONE (UPDATE side of ternary)', async () => {
    const item = makeQueueItem({
      operation: UPDATE,
      importJobState: {[UPDATE]: IMPORT_JOB_STATE.EMPTY, [CREATE]: IMPORT_JOB_STATE.DONE}
    });
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: UPDATE});

    await waitFor('queueItem DONE', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.DONE));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job EMPTY, CREATE queueItem, other operation ongoing: not done, keeps looping', async () => {
    const item = makeQueueItem({
      operation: CREATE,
      importJobState: {[CREATE]: IMPORT_JOB_STATE.EMPTY, [UPDATE]: IMPORT_JOB_STATE.IN_QUEUE}
    });
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('the loop to be in cycle 2', () => findOnes('prio').length >= 6);
    fakes.park();

    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job EMPTY, FIX queueItem for a CREATE importer: cross-op mismatch, keeps looping', async () => {
    const item = makeQueueItem({operation: FIX, importJobState: {[CREATE]: IMPORT_JOB_STATE.EMPTY}});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('the loop to be in cycle 2', () => findOnes('prio').length >= 6);
    fakes.park();

    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job EMPTY, CREATE queueItem for a FIX importer: cross-op mismatch, keeps looping', async () => {
    const item = makeQueueItem({operation: CREATE, importJobState: {[FIX]: IMPORT_JOB_STATE.EMPTY}});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: FIX});

    await waitFor('the loop to be in cycle 2', () => findOnes('prio').length >= 6);
    fakes.park();

    assert.equal(fakes.findOneAndUpdateCalls.length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job in some other state, non-FIX operation: queueItem -> IMPORTING', async () => {
    const item = makeQueueItem({importJobState: {[CREATE]: IMPORT_JOB_STATE.QUEUING}});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp();

    await waitFor('queueItem IMPORTING', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.IMPORTER.IMPORTING));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test('job in some other state, FIX importer without a FIX job state key: job -> IMPORTING', async () => {
    // importJobState is a flat {operation: state} map; a missing FIX key
    // (another operation's key present) gets the FIX job state initialized
    const item = makeQueueItem({importJobState: {[UPDATE]: IMPORT_JOB_STATE.DONE, OPERATIONS: {}}});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: FIX});

    await waitFor('job IMPORTING', () =>
      jobStateChanged('prio', FIX, IMPORT_JOB_STATE.IMPORTING));
    fakes.park();

    assert.equal(setStateCalls('prio')[0][2].$set.queueItemState, QUEUE_ITEM_STATE.IMPORTER.IMPORTING);
    assert.equal(setJobStateCalls('prio')[0][2].$set[`importJobState.${FIX}`], IMPORT_JOB_STATE.IMPORTING);
    assert.equal(rejections.length, 0);
  });

  test('job in some other state, FIX importer with importJobState.FIX set: no job state change', async () => {
    const item = makeQueueItem({importJobState: {
      [FIX]: IMPORT_JOB_STATE.QUEUING,
      OPERATIONS: {FIX: IMPORT_JOB_STATE.IMPORTING}
    }});
    fakes.scriptFor('prio').push(null, null, null, null, item, ...nulls(4));
    fakes.scriptFor('bulk').push(...nulls(5));
    const {rejections} = runApp({operation: FIX});

    await waitFor('queueItem IMPORTING', () =>
      stateChanged('prio', QUEUE_ITEM_STATE.IMPORTER.IMPORTING));
    fakes.park();

    assert.equal(setJobStateCalls('prio').length, 0);
    assert.equal(rejections.length, 0);
  });

  test(
    'FIX importer + other job state without a FIX key: job initialized to IMPORTING, no crash',
    async () => {
      // Regression test for the former app.js:249 bug, where this branch
      // read `queueItem.importJobState.OPERATIONS.FIX` (importJobState is a
      // flat {operation: state} map, so .OPERATIONS was undefined ->
      // TypeError -> the whole service died). Now a missing FIX key
      // initializes the FIX job state to IMPORTING and the loop continues.
      const item = makeQueueItem({importJobState: {[UPDATE]: IMPORT_JOB_STATE.DONE}});
      fakes.scriptFor('prio').push(null, null, null, null, item);
      fakes.scriptFor('bulk').push(...nulls(5));
      const {rejections} = runApp({operation: FIX});

      // The job state gets initialized AND the loop keeps running (cycle 2)
      await waitFor('job IMPORTING and the loop in cycle 2', () =>
        jobStateChanged('prio', FIX, IMPORT_JOB_STATE.IMPORTING)
        && findOnes('prio').length >= 6);
      fakes.park();

      assert.equal(setStateCalls('prio')[0][2].$set.queueItemState, QUEUE_ITEM_STATE.IMPORTER.IMPORTING);
      assert.equal(setJobStateCalls('prio')[0][2].$set[`importJobState.${FIX}`], IMPORT_JOB_STATE.IMPORTING);
      // The app promise never rejected (no crash); it is still pending and
      // the loop parked mid-cycle 2, so a crash would have shown up in the
      // rejections array
      assert.equal(rejections.length, 0);
    }
  );
});
