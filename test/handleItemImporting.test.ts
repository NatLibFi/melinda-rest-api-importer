import {describe, it} from 'node:test';
import assert from 'node:assert/strict';
import type {AmqpOperator, MongoOperator, QueueItem} from '@natlibfi/melinda-rest-api-commons';
import type {WithId} from 'mongodb';
import {createItemImportingHandler} from '../src/handleItemImporting.ts';

// The module does not fetch anything itself (recordLoadOperator is injected)
// and creates no MongoClient internally, so no fetch stub or mongodb patch is
// needed. The real sendErrorResponses module (imported by the module under
// test) is exercised as-is: it only talks to the mocked amqpOperator /
// mongoOperator.

const CORRELATION_ID = 'corr-123';
const RECORD_LOAD_LIBRARY = 'DEFAULT-LIB';

function createItem(extra: Record<string, unknown> = {}): WithId<QueueItem> {
  return {correlationId: CORRELATION_ID, ...extra} as unknown as WithId<QueueItem>;
}

// Message shape mirrors @natlibfi/melinda-rest-api-commons amqp.js, where
// headers come from message.properties.headers and the record id is read as
// message.properties.headers.id.
function createMessage(id: string) {
  return {content: Buffer.from(''), properties: {headers: {id}}};
}

function createHeaders() {
  return {operationSettings: {fixType: 'API'}, cataloger: 'cataloger-1'};
}

// Returns an object with the plain-object operator mocks and a `calls`
// recording object so tests can assert on call arguments, not just returns.
//
// checkQueueResult is an array: the i-th call gets checkQueueResult[i], and
// calls beyond the array keep getting the last element. An Error element is
// thrown. This mirrors the real amqpOperator.checkQueue contract
// ({headers, records, messages} chunk, or false for an empty queue).
function createMockOperators({checkQueueResult, loadRecordResult}: {
  checkQueueResult: unknown[];
  loadRecordResult: Record<string, unknown>;
}) {
  const calls: any = {
    checkQueue: [], ackMessages: [], nackMessages: [], sendToQueue: [],
    prioSetImportJobState: [], prioSetState: [],
    bulkSetImportJobState: [], bulkSetState: [],
    loadRecord: []
  };
  let checkQueueCount = 0;
  const amqpOperator = {
    async checkQueue(args: unknown) {
      calls.checkQueue.push(args);
      const current = checkQueueCount;
      checkQueueCount += 1;
      const result = checkQueueResult[current] !== undefined
        ? checkQueueResult[current]
        : checkQueueResult[checkQueueResult.length - 1];
      if (result instanceof Error) {
        throw result;
      }
      return result;
    },
    ackMessages(messages: unknown) {
      calls.ackMessages.push(messages);
    },
    nackMessages(messages: unknown) {
      calls.nackMessages.push(messages);
    },
    sendToQueue(args: unknown) {
      calls.sendToQueue.push(args);
    }
  };
  const mongoOperators = {
    prio: createMockMongoOperator(calls, 'prio'),
    bulk: createMockMongoOperator(calls, 'bulk')
  };
  const recordLoadOperator = {
    async loadRecord(args: unknown) {
      calls.loadRecord.push(args);
      return loadRecordResult;
    }
  };
  return {amqpOperator, mongoOperators, recordLoadOperator, calls};
}

function createMockMongoOperator(calls: any, name: string) {
  return {
    setImportJobState(args: unknown) {
      calls[`${name}SetImportJobState`].push(args);
    },
    setState(args: unknown) {
      calls[`${name}SetState`].push(args);
    }
  };
}

function createHandler(operators: ReturnType<typeof createMockOperators>) {
  // Same construction options shape as src/app.ts:
  // createItemImportingHandler(amqpOperator, mongoOperators, recordLoadOperator,
  // {error503WaitTime, recordLoadLibrary}). Boundary casts: the mocks only
  // implement the members the handler under test actually calls.
  return createItemImportingHandler(
    operators.amqpOperator as unknown as AmqpOperator,
    operators.mongoOperators as unknown as {prio: MongoOperator; bulk: MongoOperator},
    operators.recordLoadOperator as unknown as Parameters<typeof createItemImportingHandler>[2],
    {error503WaitTime: 10, recordLoadLibrary: RECORD_LOAD_LIBRARY}
  );
}

describe('createItemImportingHandler', () => {

  it('builds all importRecords parameters for CREATE (bulk) and sends the PROCESS message', async () => {
    const item = createItem({recordLoadParams: {pActiveLibrary: 'FINNA'}});
    const messages = [createMessage('000000001'), createMessage('000000002')];
    const chunk = {
      headers: createHeaders(),
      records: ['record-1', 'record-2'],
      messages
    };
    const operators = createMockOperators({
      checkQueueResult: [chunk],
      // Shape mirrors the documented record-load-api response in the source
      // comment of importRecords.
      loadRecordResult: {
        processId: 12014,
        pLogFile: 'file.log',
        pRejectFile: 'reject.log',
        loaderProcessId: 42
      }
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({item, operation: 'CREATE', prio: false});

    // checkQueue: queue name, style, toRecord (true for CREATE/UPDATE), purge
    // (hard-coded `const purgeQueues = false` in the module)
    assert.deepEqual(calls.checkQueue, [{
      queue: `CREATE.${CORRELATION_ID}`,
      style: 'basic',
      toRecord: true,
      purge: false
    }]);
    // The exact chunk messages are nacked before loading (nack-then-load).
    assert.equal(calls.nackMessages.length, 1);
    assert.equal(calls.nackMessages[0], messages);

    // Every loadRecord parameter, including the passthrough ones.
    assert.equal(calls.loadRecord.length, 1);
    assert.deepEqual(calls.loadRecord[0], {
      correlationId: CORRELATION_ID,
      records: ['record-1', 'record-2'],
      recordList: ['000000001', '000000002'],
      operation: 'CREATE',
      fixType: 'API',
      recordLoadParams: {pActiveLibrary: 'FINNA'},
      cataloger: 'cataloger-1',
      prio: false
    });

    // The PROCESS.<operation>.<correlationId> message: every data field.
    // recordList comes from message.properties.headers.id, recordAmount from
    // messages.length.
    assert.deepEqual(calls.sendToQueue, [{
      queue: `PROCESS.CREATE.${CORRELATION_ID}`,
      correlationId: CORRELATION_ID,
      headers: {queue: `CREATE.${CORRELATION_ID}`},
      data: {
        correlationId: CORRELATION_ID,
        pActiveLibrary: 'FINNA',
        processId: 12014,
        pRejectFile: 'reject.log',
        pLogFile: 'file.log',
        loaderProcessId: 42,
        recordAmount: 2
      }
    }]);

    // importJobState PROCESSING goes to the bulk operator when prio is false;
    // no state (queue item) write happens on the success path.
    assert.deepEqual(calls.bulkSetImportJobState, [{
      correlationId: CORRELATION_ID,
      operation: 'CREATE',
      importJobState: 'PROCESSING'
    }]);
    assert.deepEqual(calls.prioSetImportJobState, []);
    assert.deepEqual(calls.prioSetState, []);
    assert.deepEqual(calls.bulkSetState, []);
    assert.deepEqual(calls.ackMessages, []);
  });

  it('uses toRecord false for FIX and passes records through as undefined', async () => {
    const messages = [createMessage('000000001')];
    // No `records` key: the real amqpOperator.checkQueue with toRecord: false
    // returns only {headers, messages}.
    const chunk = {headers: createHeaders(), messages};
    const operators = createMockOperators({
      checkQueueResult: [chunk],
      loadRecordResult: {processId: 1}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({item: createItem(), operation: 'FIX', prio: false});

    assert.deepEqual(calls.checkQueue[0], {
      queue: `FIX.${CORRELATION_ID}`,
      style: 'basic',
      toRecord: false,
      purge: false
    });
    assert.equal(calls.loadRecord.length, 1);
    // Actual behavior: records reach loadRecord as undefined for FIX; the
    // module only guards this with `records?.length` in its debug log.
    assert.equal(calls.loadRecord[0].records, undefined);
    assert.deepEqual(calls.loadRecord[0].recordList, ['000000001']);
    assert.equal(calls.sendToQueue[0].queue, `PROCESS.FIX.${CORRELATION_ID}`);
    assert.equal(calls.sendToQueue[0].data.recordAmount, 1);
  });

  it('falls back to recordLoadLibrary when the item has no recordLoadParams', async () => {
    const operators = createMockOperators({
      checkQueueResult: [{headers: createHeaders(), records: ['r'], messages: [createMessage('000000001')]}],
      loadRecordResult: {processId: 1}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({item: createItem(), operation: 'CREATE', prio: false});

    // Undefined recordLoadParams also pass through to loadRecord as-is.
    assert.equal(calls.loadRecord[0].recordLoadParams, undefined);
    // pActiveLibrary is the constructor-level recordLoadLibrary (from config.ts
    // RECORD_LOAD_LIBRARY) only when recordLoadParams is falsy.
    assert.equal(calls.sendToQueue[0].data.pActiveLibrary, RECORD_LOAD_LIBRARY);
  });

  it('passes pActiveLibrary through as undefined for a truthy but empty recordLoadParams', async () => {
    const operators = createMockOperators({
      checkQueueResult: [{headers: createHeaders(), records: ['r'], messages: [createMessage('000000001')]}],
      loadRecordResult: {processId: 1}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({
      item: createItem({recordLoadParams: {}}),
      operation: 'CREATE',
      prio: false
    });

    // Actual (arguably surprising) behavior: the fallback only checks whether
    // recordLoadParams itself is truthy. An empty object is truthy, so the
    // PROCESS message data.pActiveLibrary is undefined and recordLoadLibrary
    // is NOT used as a fallback for the missing field.
    assert.equal(calls.sendToQueue[0].data.pActiveLibrary, undefined);
    assert.deepEqual(calls.loadRecord[0].recordLoadParams, {});
  });

  it('selects the prio mongoOperator and passes prio true to loadRecord', async () => {
    const operators = createMockOperators({
      checkQueueResult: [{headers: createHeaders(), records: ['r'], messages: [createMessage('000000001')]}],
      loadRecordResult: {processId: 1}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({item: createItem(), operation: 'UPDATE', prio: true});

    assert.equal(calls.loadRecord[0].prio, true);
    assert.equal(calls.loadRecord[0].operation, 'UPDATE');
    assert.deepEqual(calls.prioSetImportJobState, [{
      correlationId: CORRELATION_ID,
      operation: 'UPDATE',
      importJobState: 'PROCESSING'
    }]);
    assert.deepEqual(calls.bulkSetImportJobState, []);
    assert.deepEqual(calls.bulkSetState, []);
  });

  it('derives recordList and recordAmount from the messages (empty chunk gives recordAmount 0)', async () => {
    const operators = createMockOperators({
      checkQueueResult: [{headers: createHeaders(), records: [], messages: []}],
      loadRecordResult: {processId: 1}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({item: createItem(), operation: 'CREATE', prio: false});

    // Note: the real amqpOperator.checkQueue only returns a chunk when the
    // queue has at least one message, so this shape is defensive — but it
    // shows the actual coercion: recordList maps over the messages and
    // recordAmount is messages.length (0), with no minimum enforced.
    assert.deepEqual(calls.loadRecord[0].recordList, []);
    assert.equal(calls.sendToQueue[0].data.recordAmount, 0);
  });

  it('handleNoMessages: waits 200 ms and retries once, then imports when messages appear', async () => {
    const messages = [createMessage('000000001')];
    const chunk = {headers: createHeaders(), records: ['r'], messages};
    // First checkQueue: queue empty (the real amqpOperator returns false for
    // an empty basic-style check). Second call (after the 200 ms wait and the
    // internal retry with waited: true): messages are there.
    const operators = createMockOperators({
      checkQueueResult: [false, chunk],
      loadRecordResult: {processId: 1}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    await handleItemImporting({item: createItem(), operation: 'CREATE', prio: false});

    assert.equal(calls.checkQueue.length, 2);
    assert.deepEqual(calls.checkQueue[1], calls.checkQueue[0]);
    assert.deepEqual(calls.checkQueue[0], {
      queue: `CREATE.${CORRELATION_ID}`,
      style: 'basic',
      toRecord: true,
      purge: false
    });
    // The retry went through importRecords.
    assert.equal(calls.loadRecord.length, 1);
    assert.equal(calls.nackMessages.length, 1);
    assert.equal(calls.sendToQueue.length, 1);
    // No ERROR state was written on the recovered path; only the normal
    // PROCESSING importJobState from importRecords.
    assert.deepEqual(calls.prioSetImportJobState, []);
    assert.deepEqual(calls.prioSetState, []);
    assert.deepEqual(calls.bulkSetImportJobState, [{
      correlationId: CORRELATION_ID,
      operation: 'CREATE',
      importJobState: 'PROCESSING'
    }]);
    assert.deepEqual(calls.bulkSetState, []);
  });

  it('handleNoMessages: empty after wait: ERROR states + sendErrorResponses', async () => {
    // Queue stays empty for every checkQueue call. Called with waited: true
    // directly so the "give up" branch runs immediately.
    const operators = createMockOperators({checkQueueResult: [false], loadRecordResult: {}});
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    // handleNoMessages throws `Empty queue ...`, but the handler's own
    // try/catch swallows it and calls sendErrorResponses, so the promise
    // resolves. The throw never reaches the caller of handleItemImporting.
    await handleItemImporting({item: createItem(), operation: 'CREATE', prio: false, waited: true});

    assert.deepEqual(calls.bulkSetImportJobState, [{
      correlationId: CORRELATION_ID,
      operation: 'CREATE',
      importJobState: 'ERROR'
    }]);
    assert.deepEqual(calls.bulkSetState, [{
      correlationId: CORRELATION_ID,
      state: 'ERROR',
      errorStatus: '500',
      errorMessage: `Importer: empty queue: CREATE.${CORRELATION_ID}`
    }]);
    assert.deepEqual(calls.prioSetImportJobState, []);
    assert.deepEqual(calls.prioSetState, []);
    // The second checkQueue call comes from the real sendErrorResponses,
    // which got false (no messages back) and therefore acks and nacks nothing
    // and writes no state of its own — the error response is dropped.
    assert.equal(calls.checkQueue.length, 2);
    assert.deepEqual(calls.ackMessages, []);
    assert.deepEqual(calls.nackMessages, []);
    assert.equal(calls.loadRecord.length, 0);
  });

  it('handleNoMessages: errored checkQueue via sendErrorResponses, no state writes', async () => {
    // First checkQueue rejects (e.g. AMQP connection error); the catch block
    // calls the real sendErrorResponses, whose checkQueue (second call)
    // returns false, so it only logs and returns.
    const operators = createMockOperators({
      checkQueueResult: [new Error('amqp connection down'), false],
      loadRecordResult: {}
    });
    const handleItemImporting = createHandler(operators);
    const calls = operators.calls;

    // The error is handled, not rethrown.
    await handleItemImporting({item: createItem(), operation: 'CREATE', prio: false});

    assert.equal(calls.checkQueue.length, 2);
    assert.deepEqual(calls.prioSetImportJobState, []);
    assert.deepEqual(calls.prioSetState, []);
    assert.deepEqual(calls.bulkSetImportJobState, []);
    assert.deepEqual(calls.bulkSetState, []);
    assert.deepEqual(calls.ackMessages, []);
    assert.deepEqual(calls.nackMessages, []);
    assert.equal(calls.loadRecord.length, 0);
  });

});
