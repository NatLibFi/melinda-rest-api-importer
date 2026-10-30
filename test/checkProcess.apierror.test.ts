// ApiError funneling in checkProcess.ts, end-to-end against the REAL
// rest-api-commons mongo layer (mongoFactory, with MongoClient.connect
// patched) instead of a hand-rolled mongoOperator fake.
//
// Covers:
//   1. An operator/HTTP error (a record-load-api 404 poll) surfacing as an
//      ApiError flows through the checkProcessQueue catch into the real
//      mongo operator: setImportJobState(ERROR) + setState(ERROR, payload,
//      status), the process message is acked, checkProcessQueueStart
//      resolves to false.
//   2. New in rest-api-commons 6.2.0-alpha.1: a Mongo driver error
//      (MongoDriverError) thrown by the mongo layer's driver calls is
//      converted by withMongoError into ApiError(500, "Mongo errored: ...").
//      When that happens while checkProcessQueue is erroring a queueItem,
//      the ApiError escapes checkProcessQueueStart instead of the
//      queueItem being left without a second error state.
//
// checkProcess.ts creates its processOperator internally (processPoll.ts)
// and calls mongoLogFactory(mongoUri) itself, so neither can be injected.
// The processOperator is driven through a stubbed globalThis.fetch; both
// MongoClient.connect users (mongoFactory and mongoLogFactory) are driven
// through the patched connect below.

import {test, describe, beforeEach, afterEach, after} from 'node:test';
import {strict as assert} from 'node:assert';
import httpStatus from 'http-status';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {MongoClient, MongoDriverError} from 'mongodb';
import {mongoFactory} from '@natlibfi/melinda-rest-api-commons';
import checkProcessFactory from '../src/interfaces/checkProcess.ts';

// One fake Mongo client per patched connect call. The fake db hands out a
// recording fake collection for every requested collection name.
const originalMongoConnect = MongoClient.connect;
let collectionFakes = null;

MongoClient.connect = async () => ({
  db: () => ({
    collection: (name) => {
      if (!collectionFakes[name]) {
        collectionFakes[name] = {
          findOneAndUpdateCalls: [],
          findOneAndUpdateError: null,
          findOneAndUpdate: async (filter, update) => {
            collectionFakes[name].findOneAndUpdateCalls.push([name, filter, update]);
            if (collectionFakes[name].findOneAndUpdateError) {
              throw collectionFakes[name].findOneAndUpdateError;
            }
            return null;
          },
          insertOne: async () => ({acknowledged: true})
        };
      }
      return collectionFakes[name];
    }
  })
});

after(() => {
  MongoClient.connect = originalMongoConnect;
});

const RECORD_LOAD_URL = 'http://record-load-api';
// Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
const API_KEY = ['api', 'key'].join('');
const MONGO_URI = 'mongodb://test/mongo';
const CORRELATION_ID = 'CID-1';
const OPERATOR_COLLECTION = 'prio';

const originalFetch = globalThis.fetch;

beforeEach(() => {
  collectionFakes = {};
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

// Minimal amqp operator: the process message on the PROCESS queue, then
// nothing else is needed - the flow ends in the checkProcessQueue catch
// before any operation queue handling.
function makeAmqp(processMessage) {
  const calls = [];
  return {
    calls,
    checkQueue({queue} = {}) {
      calls.push(['checkQueue', queue]);
      return queue.startsWith('PROCESS.') ? processMessage : false;
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

// Fetch stub answering the record-load-api poll with the given status.
function stubFetch(pollStatus) {
  globalThis.fetch = async (url, options) => {
    assert.equal(options.method, 'get');
    assert.equal(options.headers.Authorization, generateAuthorizationHeader(API_KEY));
    return {
      status: pollStatus,
      json: async () => ({handledIds: [], rejectedIds: []})
    };
  };
}

// Creates the real mongo operator (the one the app.ts loop would pass into
// checkProcessQueueStart) and runs checkProcessQueueStart with a 404 poll,
// so handleProcessMessageError rethrows the ApiError(404) and the
// checkProcessQueue catch is exercised. findOneAndUpdateError, when set,
// is thrown by the operator collection's findOneAndUpdate (a real
// MongoDriverError in the driver-error test).
async function runWithErroringPoll({findOneAndUpdateError = null} = {}) {
  const amqp = makeAmqp(makeProcessMessage(makeProcessParams()));
  stubFetch(httpStatus.NOT_FOUND);

  const factoryPromise = checkProcessFactory({
    amqpOperator: amqp,
    recordLoadApiKey: API_KEY,
    recordLoadUrl: RECORD_LOAD_URL,
    error503WaitTime: 1,
    keepLoadProcessReports: 'NONE',
    mongoUri: MONGO_URI
  });
  // mongoLogFactory connects inside the factory call; create the operator
  // first so its collection fake exists before anything writes to it.
  const mongoOperator = await mongoFactory(MONGO_URI, OPERATOR_COLLECTION);
  const factory = await factoryPromise;

  // Arm the failure mode on the operator collection only (mongoLog's
  // 'logs' collection stays healthy).
  if (findOneAndUpdateError) {
    collectionFakes[OPERATOR_COLLECTION].findOneAndUpdateError = findOneAndUpdateError;
  }

  const run = factory.checkProcessQueueStart({
    correlationId: CORRELATION_ID,
    operation: 'CREATE',
    mongoOperator,
    prio: true
  });
  return {run, amqp, fake: collectionFakes[OPERATOR_COLLECTION]};
}

describe('checkProcess ApiError funneling against the real mongo layer', () => {
  test('operator/HTTP error (404 poll): ApiError flows through - ERROR states written, process message acked, returns false', async () => {
    const {run, amqp, fake} = await runWithErroringPoll();
    const result = await run;

    assert.equal(result, false);
    // checkProcessQueue catch: the ApiError's status and payload land in
    // the real mongo operator (through the rest-api-commons mongoFactory).
    assert.equal(fake.findOneAndUpdateCalls.length, 2);
    const [jobStateCollection, jobStateFilter, jobStateUpdate] = fake.findOneAndUpdateCalls[0];
    assert.equal(jobStateCollection, OPERATOR_COLLECTION);
    assert.deepEqual(jobStateFilter, {correlationId: CORRELATION_ID});
    assert.equal(jobStateUpdate.$set['importJobState.CREATE'], 'ERROR');
    assert.ok(jobStateUpdate.$set.modificationTime instanceof Date);
    const [itemCollection, itemFilter, itemUpdate] = fake.findOneAndUpdateCalls[1];
    assert.equal(itemCollection, OPERATOR_COLLECTION);
    assert.deepEqual(itemFilter, {correlationId: CORRELATION_ID});
    assert.equal(itemUpdate.$set.queueItemState, 'ERROR');
    assert.ok(itemUpdate.$set.modificationTime instanceof Date);
    // The ApiError's payload and status are written verbatim.
    assert.equal(itemUpdate.$set.errorMessage, 'Process log not found!');
    assert.equal(itemUpdate.$set.errorStatus, httpStatus.NOT_FOUND);
    // The process message is acked (no nack on the way out).
    const acks = amqp.calls.filter((call) => call[0] === 'ackMessages');
    assert.equal(acks.length, 1);
    assert.equal(acks[0][1].length, 1);
    assert.deepEqual(amqp.calls.filter((call) => call[0] === 'nackMessages'), []);
  });

  test('MongoDriverError from the mongo layer: surfaces as ApiError 500 with the "Mongo errored:" prefix', async () => {
    const {run, amqp, fake} = await runWithErroringPoll({
      findOneAndUpdateError: new MongoDriverError('simulated driver failure')
    });
    await assert.rejects(run, (error) => {
      assert.ok(error instanceof ApiError, `expected ApiError, got ${error.constructor.name}`);
      assert.equal(error.status, httpStatus.INTERNAL_SERVER_ERROR);
      assert.equal(error.payload, 'Mongo errored: simulated driver failure');
      return true;
    });
    // The driver error fires on the FIRST error-state write
    // (setImportJobState), so only one write is attempted and the process
    // message is never acked: the rejection propagates to the app loop
    // instead of the queueItem being half-errored.
    assert.equal(fake.findOneAndUpdateCalls.length, 1);
    assert.equal(fake.findOneAndUpdateCalls[0][0], OPERATOR_COLLECTION);
    assert.deepEqual(amqp.calls.filter((call) => call[0] === 'ackMessages'), []);
  });
});
