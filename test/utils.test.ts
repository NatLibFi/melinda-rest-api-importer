import {test} from 'node:test';
import assert from 'node:assert/strict';
import httpStatus from 'http-status';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import type {Response} from 'undici-types';
import {buildQuery, checkStatus, handleConnectionError} from '../src/utils.ts';

type ConnectionErrorParam = Parameters<typeof handleConnectionError>[0];

test('checkStatus: throws ApiError 400 with the response body verbatim for 400', async () => {
  const response = {status: httpStatus.BAD_REQUEST, text: async () => 'Missing mandatory parameter: correlationId'} as Response;
  // The API body arrives as a quoted bare-JSON string (res.json(string)) and
  // is stored verbatim, un-parsed.
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.BAD_REQUEST);
  assert.equal(thrown.payload, 'Missing mandatory parameter: correlationId');
});

test('checkStatus: 400 with an empty body falls back to a descriptive payload', async () => {
  const response = {status: httpStatus.BAD_REQUEST, text: async () => ''} as Response;
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.BAD_REQUEST);
  assert.equal(thrown.payload, '400 BAD_REQUEST with empty body');
});

test('checkStatus: throws ApiError 500 with no payload for 401', async () => {
  const response = {status: httpStatus.UNAUTHORIZED} as Response;
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.INTERNAL_SERVER_ERROR);
  assert.equal(thrown.payload, undefined);
});

test('checkStatus: throws ApiError 500 with no payload for 403', async () => {
  const response = {status: httpStatus.FORBIDDEN} as Response;
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.INTERNAL_SERVER_ERROR);
  assert.equal(thrown.payload, undefined);
});

test('checkStatus: throws ApiError 404 with "Process log not found!" for 404', async () => {
  const response = {status: httpStatus.NOT_FOUND} as Response;
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.NOT_FOUND);
  assert.equal(thrown.payload, 'Process log not found!');
});

test('checkStatus: throws ApiError 406 with "0 processed records!" for 406', async () => {
  const response = {status: httpStatus.NOT_ACCEPTABLE} as Response;
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.NOT_ACCEPTABLE);
  assert.equal(thrown.payload, '0 processed records!');
});

test('checkStatus: throws ApiError 423 with "Not ready yet!" for 423', async () => {
  const response = {status: httpStatus.LOCKED} as Response;
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.LOCKED);
  assert.equal(thrown.payload, 'Not ready yet!');
});

test('checkStatus: throws ApiError 503 with maintenance message for 503', async () => {
  const response = {status: httpStatus.SERVICE_UNAVAILABLE} as Response;
  const expectedPayload = 'The server is temporarily unable to service your request'
    + ' due to maintenance downtime or capacity problems. Please try again later.';
  let thrown: ApiError;
  try {
    await checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    thrown = error as ApiError;
  }
  assert.ok(thrown instanceof ApiError, 'error is an ApiError');
  assert.equal(thrown.status, httpStatus.SERVICE_UNAVAILABLE);
  assert.equal(thrown.payload, expectedPayload);
});

test('checkStatus: falls through without throwing for any other status', async () => {
  for (const status of [httpStatus.OK, httpStatus.MOVED_PERMANENTLY, httpStatus.INTERNAL_SERVER_ERROR]) {
    const response = {status} as Response;
    assert.equal(await checkStatus(response), undefined);
  }
});

test('handleConnectionError: throws ApiError 503 for a plain Error (no cause)', () => {
  const error = new Error('network down');
  const expectedPayload = 'The server is temporarily unable to service your request'
    + ' due to maintenance downtime or capacity problems. Please try again later.';
  try {
    handleConnectionError(error as ConnectionErrorParam);
    assert.fail('Expected handleConnectionError to throw');
  } catch (thrown) {
    assert.ok(thrown instanceof ApiError, 'error is an ApiError');
    assert.equal(thrown.status, httpStatus.SERVICE_UNAVAILABLE);
    assert.equal(thrown.payload, expectedPayload);
  }
});

test('handleConnectionError: maps a fetch TypeError carrying an undici cause to ApiError(503) and logs the cause', () => {
  const error = new TypeError('fetch failed');
  error.cause = Object.assign(new Error('getaddrinfo ENOTFOUND record-load-api'), {code: 'ENOTFOUND'});
  const expectedPayload = 'The server is temporarily unable to service your request'
    + ' due to maintenance downtime or capacity problems. Please try again later.';
  try {
    handleConnectionError(error as ConnectionErrorParam);
    assert.fail('Expected handleConnectionError to throw');
  } catch (thrown) {
    assert.ok(thrown instanceof ApiError, 'error is an ApiError');
    assert.equal(thrown.status, httpStatus.SERVICE_UNAVAILABLE);
    assert.equal(thrown.payload, expectedPayload);
  }
});

test('handleConnectionError: a cause without code or message still maps to ApiError(503)', () => {
  const error = new TypeError('fetch failed');
  error.cause = {};
  const expectedPayload = 'The server is temporarily unable to service your request'
    + ' due to maintenance downtime or capacity problems. Please try again later.';
  try {
    handleConnectionError(error as ConnectionErrorParam);
    assert.fail('Expected handleConnectionError to throw');
  } catch (thrown) {
    assert.ok(thrown instanceof ApiError, 'error is an ApiError');
    assert.equal(thrown.status, httpStatus.SERVICE_UNAVAILABLE);
    assert.equal(thrown.payload, expectedPayload);
  }
});

test('buildQuery: omits null, empty string and literal "null" values, keeps everything else in order', () => {
  const query = buildQuery({a: '1', pLogFile: null, pRejectFile: '', other: 'null', zero: '0', u: undefined});
  assert.equal(query.toString(), 'a=1&zero=0&u=undefined');
});
