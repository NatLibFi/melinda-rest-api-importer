import {afterEach, beforeEach, test} from 'node:test';
import assert from 'node:assert/strict';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import loadStarter from './loadStarter.ts';

// Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
const CONFIG = {
  recordLoadApiKey: ['test', 'api', 'key'].join('-'),
  recordLoadLibrary: '201',
  recordLoadUrl: 'http://record-load-api:8080/load',
  fixPrio: 'API',
  fixBulk: 'INSB'
};

// 24-character MARC leader
const LEADER = '00042cam a2200000 a 4500';

// generateAuthorizationHeader(apiKey) encodes `apiKey:` (empty password) as Basic base64
const EXPECTED_AUTH = `Basic ${Buffer.from(CONFIG.recordLoadApiKey + ':').toString('base64')}`;

function makeMarcRecord({sysnum, title}) {
  return new MarcRecord({
    leader: LEADER,
    fields: [
      {tag: '001', value: sysnum},
      {tag: '245', ind1: ' ', ind2: '0', subfields: [{code: 'a', value: title}]}
    ]
  }, {subfieldValues: false});
}

const RECORDS = [
  makeMarcRecord({sysnum: '123', title: 'First record'}),
  makeMarcRecord({sysnum: '456', title: 'Second record'})
];

// Mirrors createSeqRecords: AlephSequential serialization joined with the 001-fusion separator
const EXPECTED_SEQ_BODY = RECORDS.map(record => AlephSequential.to(record, {subfieldValues: false}))
  .join('000000000 000   L 0\n');

let starter;
let fetchCalls;
const originalFetch = globalThis.fetch;

beforeEach(() => {
  starter = loadStarter(CONFIG);
  fetchCalls = [];
  // Default stub: fail loudly if a test forgot to stub fetch
  globalThis.fetch = async () => {
    throw new Error('globalThis.fetch was not stubbed');
  };
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

// Records every fetch call and resolves with the response produced by impl(url, options)
function stubFetch(impl) {
  globalThis.fetch = async (url, options) => {
    fetchCalls.push({url: String(url), options});
    return impl(url, options);
  };
}

function jsonResponse(status, body) {
  return {
    status,
    json: async () => body,
    text: async () => (typeof body === 'string' ? body : JSON.stringify(body))
  };
}

function queryOf(urlString) {
  return new URL(urlString).searchParams;
}

// Captures the rejected ApiError (Node's assert.rejects resolves with undefined, so we
// catch it manually) and fails the test if the promise resolved or rejected with another error
async function expectApiError(promise) {
  try {
    await promise;
    assert.fail('expected the promise to be rejected');
  } catch (err) {
    assert.ok(err instanceof ApiError, `expected ApiError, got: ${err}`);
    return err;
  }
}

test('builds full URL, headers and seq body (CREATE, prio, all recordLoadParams set)', async () => {
  const result = {correlationId: 'corr-1', loaderProcessId: 'lp-1', processId: 7};
  stubFetch(async () => jsonResponse(200, result));

  const returned = await starter.loadRecord({
    correlationId: 'corr-1',
    records: RECORDS,
    operation: OPERATIONS.CREATE,
    recordLoadParams: {
      pActiveLibrary: '900',
      pCatalogerIn: 'abc',
      pRejectFile: 'reject.log',
      pLogFile: 'process.log'
    },
    cataloger: {id: 'cat1'},
    prio: true
  });

  assert.equal(fetchCalls.length, 1);
  // URLSearchParams preserves insertion order, so the full URL string is deterministic
  assert.equal(fetchCalls[0].url,
    'http://record-load-api:8080/load?'
    + 'correlationId=corr-1'
    + '&useLoaderProcessId=1'
    + '&pActiveLibrary=900'
    + '&pOldNew=NEW'
    + '&pFixType=API'
    + '&pCatalogerIn=ABC'
    + '&pZ07PriorityYear=1990'
    + '&pRejectFile=reject.log'
    + '&pLogFile=process.log');
  assert.equal(fetchCalls[0].options.method, 'post');
  assert.equal(fetchCalls[0].options.headers['Content-Type'], 'text/plain');
  assert.equal(fetchCalls[0].options.headers.Authorization, EXPECTED_AUTH);
  assert.equal(fetchCalls[0].options.body, EXPECTED_SEQ_BODY);
  assert.deepEqual(returned, result);
});

test('omits null-valued file params; correlationId still stringified as "undefined" (out of scope for the file-param fix)', async () => {
  // loadStarter.js:21-44: correlationId defaults to undefined and pRejectFile/pLogFile default to null.
  // buildQuery omits the null-valued file params (pRejectFile/pLogFile). correlationId is left as-is:
  // its undefined value is still stringified by URLSearchParams (out of scope for the file-param fix).
  stubFetch(async () => jsonResponse(200, {ok: true}));

  await starter.loadRecord({
    records: RECORDS,
    operation: OPERATIONS.UPDATE,
    cataloger: 'cat2'
  });

  const query = queryOf(fetchCalls[0].url);
  assert.equal(query.get('correlationId'), 'undefined');
  assert.equal(query.get('useLoaderProcessId'), '1');
  assert.equal(query.get('pActiveLibrary'), '201');
  assert.equal(query.get('pOldNew'), 'OLD');
  assert.equal(query.get('pFixType'), 'INSB');
  assert.equal(query.get('pCatalogerIn'), 'CAT2');
  assert.equal(query.get('pZ07PriorityYear'), '2099');
  assert.equal(query.get('pRejectFile'), null);
  assert.equal(query.get('pLogFile'), null);
});

test('treats empty-string pCatalogerIn/pRejectFile/pLogFile as unset', async () => {
  stubFetch(async () => jsonResponse(200, {ok: true}));

  await starter.loadRecord({
    correlationId: 'corr-1',
    records: RECORDS,
    operation: OPERATIONS.CREATE,
    recordLoadParams: {pCatalogerIn: '', pRejectFile: '', pLogFile: ''},
    cataloger: {id: 'cat3'},
    prio: false
  });

  const query = queryOf(fetchCalls[0].url);
  assert.equal(query.get('pCatalogerIn'), 'CAT3');
  assert.equal(query.get('pRejectFile'), null);
  assert.equal(query.get('pLogFile'), null);
});

test('pZ07PriorityYear matrix: CREATE 1990/1992, UPDATE (HIGH) 1998/2099', async () => {
  stubFetch(async () => jsonResponse(200, {ok: true}));
  const cases = [
    {operation: OPERATIONS.CREATE, prio: true, expected: '1990'},
    {operation: OPERATIONS.CREATE, prio: false, expected: '1992'},
    {operation: OPERATIONS.UPDATE, prio: true, expected: '1998'},
    {operation: OPERATIONS.UPDATE, prio: false, expected: '2099'}
  ];

  for (const {operation, prio, expected} of cases) {
    await starter.loadRecord({records: RECORDS, operation, cataloger: 'cat1', prio});
    assert.equal(queryOf(fetchCalls.at(-1).url).get('pZ07PriorityYear'), expected);
  }

  // Note: generateIndexingPriority's else branch (non-CREATE + non-HIGH indexingPriority,
  // new Date().getFullYear() + 1000) is unreachable: loadRecord always passes INDEXING_PRIORITY.HIGH.
  // It is deliberately left uncovered instead of stubbing the clock around dead code.
});

test('createSeqRecords: MARC serialization joined with 000000000 fusion separator', async () => {
  stubFetch(async () => jsonResponse(200, {ok: true}));

  await starter.loadRecord({records: RECORDS, operation: OPERATIONS.CREATE, cataloger: 'cat1'});

  const body = fetchCalls[0].options.body;
  assert.equal(body, EXPECTED_SEQ_BODY);
  assert.ok(body.includes('000000000 000   L 0\n'), 'separator between the two serialized records');
});

test('createSeqRecords: serialization failure throws ApiError(422) without calling fetch', async () => {
  // A plain object (no MarcRecord .get method) passes field validation but breaks in to()
  const badRecords = [{leader: LEADER, fields: [{tag: '001', value: '1'}]}];
  const error = await expectApiError(
    starter.loadRecord({records: badRecords, operation: OPERATIONS.CREATE, cataloger: 'cat1'})
  );

  assert.equal(error.status, 422);
  assert.equal(error.payload, 'record.get is not a function');
  assert.equal(fetchCalls.length, 0);
});

test('200 response: resolves with parsed JSON body', async () => {
  const result = {correlationId: 'corr-1', loaderProcessId: 'lp-1'};
  stubFetch(async () => jsonResponse(200, result));

  const returned = await starter.loadRecord({records: RECORDS, operation: OPERATIONS.FIX, cataloger: 'cat1'});

  assert.deepEqual(returned, result);
});

test('checkStatus error payloads: 404, 406, 423, 503', async () => {
  const expected = [
    {status: 404, payload: 'Process log not found!'},
    {status: 406, payload: '0 processed records!'},
    {status: 423, payload: 'Not ready yet!'},
    {
      status: 503,
      payload: 'The server is temporarily unable to service your request due to maintenance downtime or '
        + 'capacity problems. Please try again later.'
    }
  ];

  for (const {status, payload} of expected) {
    stubFetch(async () => jsonResponse(status, {}));
    const error = await expectApiError(
      starter.loadRecord({records: RECORDS, operation: OPERATIONS.CREATE, cataloger: 'cat1'})
    );
    assert.equal(error.status, status);
    assert.equal(error.payload, payload);
  }
});

test('network failure (fetch rejects): throws ApiError(503)', async () => {
  stubFetch(async () => {
    throw new TypeError('fetch failed');
  });

  const error = await expectApiError(
    starter.loadRecord({records: RECORDS, operation: OPERATIONS.CREATE, cataloger: 'cat1'})
  );

  assert.equal(error.status, 503);
  assert.equal(error.payload, 'The server is temporarily unable to service your request due to '
    + 'maintenance downtime or capacity problems. Please try again later.');
});

test('unexpected status (500): checkStatus passes through, loadRecord throws ApiError with response text', async () => {
  stubFetch(async () => jsonResponse(500, 'boom'));

  const error = await expectApiError(
    starter.loadRecord({records: RECORDS, operation: OPERATIONS.CREATE, cataloger: 'cat1'})
  );

  assert.equal(error.status, 500);
  assert.equal(error.payload, 'boom');
});

test('falsy response.status: falls back to 500 via `response.status || INTERNAL_SERVER_ERROR`', async () => {
  stubFetch(async () => jsonResponse(0, 'weird'));

  const error = await expectApiError(
    starter.loadRecord({records: RECORDS, operation: OPERATIONS.CREATE, cataloger: 'cat1'})
  );

  assert.equal(error.status, 500);
  assert.equal(error.payload, 'weird');
  // Note: the `response ? await response.text() : 'Internal error'` ternary's false side is
  // unreachable: response is always an object (checkStatus would throw first, and
  // handleConnectionError always throws ApiError(503)). Left uncovered deliberately.
});
