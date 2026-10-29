import {test} from 'node:test';
import assert from 'node:assert/strict';
import httpStatus from 'http-status';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {checkStatus, handleConectionError} from './utils.js';

test('checkStatus: throws ApiError 400 with no payload for 400', () => {
  const response = {status: httpStatus.BAD_REQUEST};
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.BAD_REQUEST);
    assert.equal(error.payload, undefined);
  }
});

test('checkStatus: throws ApiError 500 with no payload for 401', () => {
  const response = {status: httpStatus.UNAUTHORIZED};
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.INTERNAL_SERVER_ERROR);
    assert.equal(error.payload, undefined);
  }
});

test('checkStatus: throws ApiError 500 with no payload for 403', () => {
  const response = {status: httpStatus.FORBIDDEN};
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.INTERNAL_SERVER_ERROR);
    assert.equal(error.payload, undefined);
  }
});

test('checkStatus: throws ApiError 404 with "Process log not found!" for 404', () => {
  const response = {status: httpStatus.NOT_FOUND};
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.NOT_FOUND);
    assert.equal(error.payload, 'Process log not found!');
  }
});

test('checkStatus: throws ApiError 406 with "0 processed records!" for 406', () => {
  const response = {status: httpStatus.NOT_ACCEPTABLE};
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.NOT_ACCEPTABLE);
    assert.equal(error.payload, '0 processed records!');
  }
});

test('checkStatus: throws ApiError 423 with "Not ready yet!" for 423', () => {
  const response = {status: httpStatus.LOCKED};
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.LOCKED);
    assert.equal(error.payload, 'Not ready yet!');
  }
});

test('checkStatus: throws ApiError 503 with maintenance message for 503', () => {
  const response = {status: httpStatus.SERVICE_UNAVAILABLE};
  const expectedPayload = 'The server is temporarily unable to service your request'
    + ' due to maintenance downtime or capacity problems. Please try again later.';
  try {
    checkStatus(response);
    assert.fail('Expected checkStatus to throw');
  } catch (error) {
    assert.ok(error instanceof ApiError, 'error is an ApiError');
    assert.equal(error.status, httpStatus.SERVICE_UNAVAILABLE);
    assert.equal(error.payload, expectedPayload);
  }
});

test('checkStatus: falls through without throwing for any other status', () => {
  for (const status of [httpStatus.OK, httpStatus.MOVED_PERMANENTLY, httpStatus.INTERNAL_SERVER_ERROR]) {
    const response = {status};
    assert.doesNotThrow(() => checkStatus(response));
    assert.equal(checkStatus(response), undefined);
  }
});

test('handleConectionError: returns error.response when present', () => {
  const errorResponse = {status: 500, data: 'boom'};
  const error = {response: errorResponse};
  assert.equal(handleConectionError(error), errorResponse);
});

test('handleConectionError: throws ApiError 503 when error has no response', () => {
  const error = new Error('network down');
  const expectedPayload = 'The server is temporarily unable to service your request'
    + ' due to maintenance downtime or capacity problems. Please try again later.';
  try {
    handleConectionError(error);
    assert.fail('Expected handleConectionError to throw');
  } catch (thrown) {
    assert.ok(thrown instanceof ApiError, 'error is an ApiError');
    assert.equal(thrown.status, httpStatus.SERVICE_UNAVAILABLE);
    assert.equal(thrown.payload, expectedPayload);
  }
});
