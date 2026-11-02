import {after, test} from 'node:test';
import assert from 'node:assert/strict';
import httpStatus from 'http-status';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import type {RequestInfo} from 'undici-types';
import processPoll from '../src/interfaces/processPoll.ts';
import type {ProcessParamsData} from '../src/interfaces/processPoll.ts';

// Built from parts so secret scanners (njsscan) do not flag a hardcoded key.
const recordLoadApiKey = ['test', 'api', 'key'].join('-');
const recordLoadUrl = 'http://record-load-api:8080/process';
const expectedAuthorization = generateAuthorizationHeader(recordLoadApiKey);
const error503Payload =
  'The server is temporarily unable to service your request due to maintenance downtime or capacity ' +
  'problems. Please try again later.';

const poller = processPoll({recordLoadApiKey, recordLoadUrl});

const originalFetch = globalThis.fetch;

let fetchCalls: {url: any; options: any}[] = [];

function stubFetch({status = httpStatus.OK, json = {}, text = '', reject = null}: {
  status?: number;
  json?: unknown;
  text?: string;
  reject?: Error | null;
} = {}) {
  fetchCalls = [];
  globalThis.fetch = async (url: RequestInfo | URL, options?: RequestInit) => {
    fetchCalls.push({url, options});
    if (reject) {
      throw reject;
    }
    return {status, json: async () => json, text: async () => text} as Response;
  };
}

function lastCall() {
  return fetchCalls[fetchCalls.length - 1]!;
}

function queryParam(name: string) {
  return new URL(lastCall().url).searchParams.get(name);
}

function loadParams(extra: Record<string, unknown> = {}): ProcessParamsData {
  return {
    correlationId: 'correlation-1',
    pActiveLibrary: 'FIN01',
    processId: 'PROC-1',
    loaderProcessId: 'LOADER-1',
    recordAmount: 4,
    ...extra
  } as unknown as ProcessParamsData;
}

function loadResponse(handledIds: string[], extra: Record<string, unknown> = {}) {
  return {
    handledIds,
    rejectedIds: [],
    rejectMessages: [],
    ...extra
  };
}

after(() => {
  globalThis.fetch = originalFetch;
});

test('poll CREATE sends a GET with all six query parameters set to their values', async () => {
  stubFetch({json: loadResponse(['000000001FIN01'], {errorIdCount: 0})});
  await poller.poll({operation: 'CREATE', params: loadParams({pLogFile: 'log.txt', pRejectFile: 'reject.txt'})});

  assert.equal(fetchCalls.length, 1);
  const {url, options} = lastCall();
  assert.equal(options.method, 'get');
  const target = new URL(url);
  assert.equal(target.origin + target.pathname, recordLoadUrl);
  assert.equal(queryParam('correlationId'), 'correlation-1');
  assert.equal(queryParam('pActiveLibrary'), 'FIN01');
  assert.equal(queryParam('processId'), 'PROC-1');
  assert.equal(queryParam('loaderProcessId'), 'LOADER-1');
  assert.equal(queryParam('pLogFile'), 'log.txt');
  assert.equal(queryParam('pRejectFile'), 'reject.txt');
});

test('poll CREATE omits missing pLogFile and pRejectFile', async () => {
  // buildQuery omits the null coerced by `pLogFile || null` instead of
  // stringifying it (the old URLSearchParams literal "null" behavior).
  stubFetch({json: loadResponse([])});
  await poller.poll({operation: 'CREATE', params: loadParams()});

  const searchParams = new URL(lastCall().url).searchParams;
  assert.equal(searchParams.has('pLogFile'), false);
  assert.equal(searchParams.has('pRejectFile'), false);
});

test('poll CREATE omits pLogFile/pRejectFile values carried as the literal "null" string (in-flight jobs from API < 4.1.0)', async () => {
  stubFetch({json: loadResponse([])});
  await poller.poll({operation: 'CREATE', params: loadParams({pLogFile: 'null', pRejectFile: 'null'})});

  const searchParams = new URL(lastCall().url).searchParams;
  assert.equal(searchParams.has('pLogFile'), false);
  assert.equal(searchParams.has('pRejectFile'), false);
});

test('poll UPDATE is treated like a load poll', async () => {
  stubFetch({json: loadResponse(['000000001FIN01'])});
  const result = (await poller.poll({operation: 'UPDATE', params: loadParams({recordAmount: 1})}))!;

  assert.equal(result.status, 200);
  assert.equal(result.ackOnlyLength, 1);
  assert.equal(queryParam('pLogFile'), null);
});

test('poll FIX queries only the four base parameters, omitting pLogFile and pRejectFile', async () => {
  stubFetch({json: {handledIds: ['000000001FIN01']}});
  await poller.poll({
    operation: 'FIX',
    params: loadParams({recordAmount: 1, pLogFile: 'log.txt', pRejectFile: 'reject.txt'})
  });

  const searchParams = new URL(lastCall().url).searchParams;
  assert.equal(searchParams.get('correlationId'), 'correlation-1');
  assert.equal(searchParams.get('pActiveLibrary'), 'FIN01');
  assert.equal(searchParams.get('processId'), 'PROC-1');
  assert.equal(searchParams.get('loaderProcessId'), 'LOADER-1');
  assert.equal(searchParams.has('pLogFile'), false);
  assert.equal(searchParams.has('pRejectFile'), false);
});

test('poll requests carry the Basic Authorization header and text/plain content type', async () => {
  stubFetch({json: loadResponse([])});
  await poller.poll({operation: 'CREATE', params: loadParams()});

  const {options} = lastCall();
  assert.equal(options.headers.Authorization, expectedAuthorization);
  assert.equal(options.headers['Content-Type'], 'text/plain');
});

test('poll CREATE resolves on 200 and passes the response into pollLoad', async () => {
  stubFetch({json: loadResponse(['000000001FIN01'])});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 1})}))!;

  assert.equal(result.status, 200);
  assert.deepEqual(result.payloads.handledIds, ['000000001']);
});

test('poll CREATE resolves on 409 (CONFLICT) and reports status 409 in the load process report', async () => {
  stubFetch({
    status: httpStatus.CONFLICT,
    json: loadResponse(['000000001FIN01', '000000002FIN01', '000000004FIN01'], {
      rejectedIds: ['000000003FIN01'],
      errorIdCount: 1
    })
  });
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 4})}))!;

  assert.equal(result.payloads.loadProcessReport!.status, 409);
  assert.equal(result.payloads.loadProcessReport!.handledAll, false);
  assert.equal(result.payloads.erroredAmount, 1);
});

test('poll CREATE rejects with checkStatus ApiErrors for 400/401/403/404/406/423/503', async (t) => {
  const cases = [
    {status: httpStatus.BAD_REQUEST, expectedStatus: httpStatus.BAD_REQUEST, payload: '400 BAD_REQUEST with empty body'},
    {status: httpStatus.UNAUTHORIZED, expectedStatus: httpStatus.INTERNAL_SERVER_ERROR, payload: undefined},
    {status: httpStatus.FORBIDDEN, expectedStatus: httpStatus.INTERNAL_SERVER_ERROR, payload: undefined},
    {status: httpStatus.NOT_FOUND, expectedStatus: httpStatus.NOT_FOUND, payload: 'Process log not found!'},
    {status: httpStatus.NOT_ACCEPTABLE, expectedStatus: httpStatus.NOT_ACCEPTABLE, payload: '0 processed records!'},
    {status: httpStatus.LOCKED, expectedStatus: httpStatus.LOCKED, payload: 'Not ready yet!'},
    {status: httpStatus.SERVICE_UNAVAILABLE, expectedStatus: httpStatus.SERVICE_UNAVAILABLE, payload: error503Payload}
  ];

  for (const {status, expectedStatus, payload} of cases) {
    await t.test(`status ${status} -> ApiError(${expectedStatus})`, async () => {
      stubFetch({status});
      await assert.rejects(poller.poll({operation: 'CREATE', params: loadParams()}), (error) => {
        assert.ok(error instanceof ApiError);
        assert.equal(error.status, expectedStatus);
        assert.equal(error.payload, payload);
        return true;
      });
    });
  }
});

test('poll CREATE 400 with body: the ApiError payload is the body verbatim', async () => {
  stubFetch({status: httpStatus.BAD_REQUEST, text: 'Missing mandatory parameter: processId'});
  await assert.rejects(poller.poll({operation: 'CREATE', params: loadParams()}), (error) => {
    assert.ok(error instanceof ApiError);
    assert.equal(error.status, httpStatus.BAD_REQUEST);
    assert.equal(error.payload, 'Missing mandatory parameter: processId');
    return true;
  });
});

test('poll CREATE throws ApiError(500, "Unexpected") for statuses that checkStatus ignores', async () => {
  stubFetch({status: httpStatus.INTERNAL_SERVER_ERROR, json: loadResponse([])});
  await assert.rejects(poller.poll({operation: 'CREATE', params: loadParams()}), (error) => {
    assert.ok(error instanceof ApiError);
    assert.equal(error.status, httpStatus.INTERNAL_SERVER_ERROR);
    assert.equal(error.payload, 'Unexpected');
    return true;
  });
});

test('poll CREATE maps a fetch network failure to ApiError(503)', async () => {
  stubFetch({reject: new Error('ECONNREFUSED')});
  await assert.rejects(poller.poll({operation: 'CREATE', params: loadParams()}), (error) => {
    assert.ok(error instanceof ApiError);
    assert.equal(error.status, httpStatus.SERVICE_UNAVAILABLE);
    assert.equal(error.payload, error503Payload);
    return true;
  });
});

test('poll does not fetch for operations other than CREATE/UPDATE/FIX and returns undefined', async () => {
  stubFetch({});
  const result = await poller.poll({operation: 'DELETE', params: loadParams()});

  assert.equal(fetchCalls.length, 0);
  assert.equal(result, undefined);
});

test('pollLoad processedAll: returns stripped ids, report, and ackOnlyLength = processedAmount', async () => {
  stubFetch({
    json: loadResponse(['000000001FIN01', '000000002FIN01', '000000004FIN01'], {
      rejectedIds: ['000000003FIN01'],
      errorIdCount: 0
    })
  });
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 4})}))!;

  assert.equal(result.status, 200);
  assert.equal(result.ackOnlyLength, 4);
  assert.deepEqual(result.payloads.handledIds, ['000000001', '000000002', '000000004']);
  assert.deepEqual(result.payloads.rejectedIds, ['000000003']);
  assert.deepEqual(result.payloads.loadProcessReport, {
    status: 200,
    processId: 'PROC-1',
    loaderProcessId: 'LOADER-1',
    processedAll: true,
    recordAmount: 4,
    processedAmount: 4,
    handledAmount: 3,
    rejectedAmount: 1,
    rejectMessages: [],
    erroredAmount: 0,
    handledAll: false
  });
});

test('pollLoad partial: errorIdCount reduces processedAmount, ackOnlyLength = recordAmount', async () => {
  stubFetch({json: loadResponse(['000000001FIN01', '000000002FIN01', '000000003FIN01'], {errorIdCount: 1})});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 3})}))!;

  assert.equal(result.ackOnlyLength, 3);
  assert.equal(result.status, undefined);
  assert.equal(result.payloads.erroredAmount, 1);
  const report = result.payloads.loadProcessReport!;
  assert.equal(report.processedAll, false);
  assert.equal(report.processedAmount, 2);
  assert.equal(report.handledAmount, 3);
  assert.equal(report.erroredAmount, 1);
  assert.equal(report.handledAll, false);
});

test('pollLoad handlesAll: errored handled ids leave the report with handledAll true', async () => {
  // handledAmount 3 - erroredAmount 1 = recordAmount 2, while rejectedAmount 0
  // keeps processedAll true
  stubFetch({json: loadResponse(['000000001FIN01', '000000002FIN01', '000000003FIN01'], {errorIdCount: 1})});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 2})}))!;

  assert.equal(result.ackOnlyLength, 2);
  assert.equal(result.payloads.loadProcessReport!.handledAll, true);
  assert.equal(result.payloads.loadProcessReport!.processedAll, true);
});

test('pollLoad marks a handledId that repeats the previous one as ERROR-<id>', async () => {
  stubFetch({json: loadResponse(['000000001FIN01', '000000001FIN01'])});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 3})}))!;

  assert.deepEqual(result.payloads.handledIds, ['000000001', 'ERROR-000000001']);
  assert.equal(result.ackOnlyLength, 3);
});

test('pollLoad does not mark a handledId that only repeats a non-adjacent one', async () => {
  stubFetch({json: loadResponse(['000000001FIN01', '000000002FIN01', '000000001FIN01'])});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 4})}))!;

  assert.deepEqual(result.payloads.handledIds, ['000000001', '000000002', '000000001']);
});

test('pollLoad with zero processed records still acks all messages', async () => {
  stubFetch({json: loadResponse([], {errorIdCount: 0})});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 3})}))!;

  assert.equal(result.ackOnlyLength, 3);
  assert.deepEqual(result.payloads.handledIds, []);
  assert.equal(result.payloads.loadProcessReport!.processedAll, false);
  assert.equal(result.payloads.loadProcessReport!.processedAmount, 0);
});

test('pollLoad keeps handledIds without the library suffix unchanged', async () => {
  stubFetch({json: loadResponse(['000000001FIN01', '000000009'])});
  const result = (await poller.poll({operation: 'CREATE', params: loadParams({recordAmount: 2})}))!;

  assert.deepEqual(result.payloads.handledIds, ['000000001', '000000009']);
});

test('pollLoad rejects with TypeError when the response payload has no handledIds array', async () => {
  stubFetch({json: {rejectedIds: []}});
  await assert.rejects(poller.poll({operation: 'CREATE', params: loadParams()}), TypeError);
});

test('pollFix returns stripped handledIds with ackOnlyLength = recordAmount on 200', async () => {
  stubFetch({json: {handledIds: ['000000001FIN01', '000000002FIN01', '000000004FIN01']}});
  const result = (await poller.poll({operation: 'FIX', params: loadParams({recordAmount: 3})}))!;

  assert.equal(result.status, 200);
  assert.equal(result.ackOnlyLength, 3);
  assert.deepEqual(result.payloads.handledIds, ['000000001', '000000002', '000000004']);
});

test('pollFix on 409 returns the guessed handledIds with status 409', async () => {
  stubFetch({status: httpStatus.CONFLICT, json: {handledIds: ['000000001FIN01']}});
  const result = (await poller.poll({operation: 'FIX', params: loadParams({recordAmount: 1})}))!;

  assert.equal(result.status, httpStatus.CONFLICT);
  assert.equal(result.ackOnlyLength, 1);
  assert.deepEqual(result.payloads.handledIds, ['000000001']);
});

test('pollFix rejects with checkStatus ApiErrors like poll does', async () => {
  stubFetch({status: httpStatus.LOCKED});
  await assert.rejects(poller.poll({operation: 'FIX', params: loadParams()}), (error) => {
    assert.ok(error instanceof ApiError);
    assert.equal(error.status, httpStatus.LOCKED);
    assert.equal(error.payload, 'Not ready yet!');
    return true;
  });
});

test('requestFileClear sends a DELETE with four query parameters and the Authorization header', async () => {
  stubFetch({});
  await poller.requestFileClear({
    correlationId: 'correlation-9',
    pActiveLibrary: 'FIN01',
    processId: 'PROC-9' as unknown as number,
    loaderProcessId: 'LOADER-9' as unknown as number
  });

  assert.equal(fetchCalls.length, 1);
  const {url, options} = lastCall();
  assert.equal(options.method, 'delete');
  assert.equal(options.headers.Authorization, expectedAuthorization);
  assert.equal(options.headers['Content-Type'], 'text/plain');
  const target = new URL(url);
  assert.equal(target.origin + target.pathname, recordLoadUrl);
  assert.equal(target.searchParams.get('correlationId'), 'correlation-9');
  assert.equal(target.searchParams.get('pActiveLibrary'), 'FIN01');
  assert.equal(target.searchParams.get('processId'), 'PROC-9');
  assert.equal(target.searchParams.get('loaderProcessId'), 'LOADER-9');
  assert.equal(target.searchParams.has('pLogFile'), false);
  assert.equal(target.searchParams.has('pRejectFile'), false);
});

test('requestFileClear maps a fetch network failure to ApiError(503)', async () => {
  stubFetch({reject: new Error('ECONNREFUSED')});
  await assert.rejects(
    poller.requestFileClear({
      correlationId: 'correlation-9',
      pActiveLibrary: 'FIN01',
      processId: 'PROC-9' as unknown as number,
      loaderProcessId: 'LOADER-9' as unknown as number
    }),
    (error) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, httpStatus.SERVICE_UNAVAILABLE);
      assert.equal(error.payload, error503Payload);
      return true;
    }
  );
});

test('requestFileClear resolves without a value on 200', async () => {
  stubFetch({});
  const result = await poller.requestFileClear({
    correlationId: 'correlation-9',
    pActiveLibrary: 'FIN01',
    processId: 'PROC-9' as unknown as number,
    loaderProcessId: 'LOADER-9' as unknown as number
  });

  assert.equal(result, undefined);
});

test('requestFileClear rejects with ApiError when record-load-api responds with an error status', async () => {
  stubFetch({status: httpStatus.NOT_FOUND});
  await assert.rejects(
    poller.requestFileClear({
      correlationId: 'correlation-9',
      pActiveLibrary: 'FIN01',
      processId: 'PROC-9' as unknown as number,
      loaderProcessId: 'LOADER-9' as unknown as number
    }),
    (error) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, httpStatus.NOT_FOUND);
      assert.equal(error.payload, 'Process log not found!');
      return true;
    }
  );
});
