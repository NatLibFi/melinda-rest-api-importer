import httpStatus from 'http-status';
import {createLogger, Error as ApiError} from '@natlibfi/melinda-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons';

export async function checkStatus(response) {
  const logger = createLogger();

  // Bad Request (400)
  if (response.status === httpStatus.BAD_REQUEST) {
    // The API sends 400 bodies as quoted bare-JSON strings (res.json(string)) -
    // store verbatim, do NOT JSON.parse.
    const body = await response.text();
    logger.info(`Got "BAD_REQUEST" (400) response from record-load-api: ${body}`);
    throw new ApiError(httpStatus.BAD_REQUEST, body || '400 BAD_REQUEST with empty body');
  }

  // Unauthorized (401)
  if (response.status === httpStatus.UNAUTHORIZED) {
    logger.info('Got "UNAUTHORIZED" (401) response from record-load-api.');
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
  }

  // Forbidden (403)
  if (response.status === httpStatus.FORBIDDEN) {
    logger.info('Got "FORBIDDEN" (403) response from record-load-api.');
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
  }

  // Not found (404)
  if (response.status === httpStatus.NOT_FOUND) {
    logger.info('Got "NOT_FOUND" (404) response from record-load-api. Process log files missing!');
    throw new ApiError(httpStatus.NOT_FOUND, 'Process log not found!');
  }

  // Not acceptable (406)
  if (response.status === httpStatus.NOT_ACCEPTABLE) {
    logger.info('Got "NOT_ACCEPTABLE" (406) response from record-load-api. 0 processed records!');
    throw new ApiError(httpStatus.NOT_ACCEPTABLE, '0 processed records!');
  }

  // Locked (423) too early
  if (response.status === httpStatus.LOCKED) {
    // Do not spam LOCKED responses
    logger.silly('Got "LOCKED" (423) response from record-load-api. Process is still going on!');
    throw new ApiError(httpStatus.LOCKED, 'Not ready yet!');
  }

  // Service unavailable (503)
  if (response.status === httpStatus.SERVICE_UNAVAILABLE) {
    logger.info('Got "SERVICE_UNAVAILABLE" (503) response from record-load-api.');
    throw new ApiError(httpStatus.SERVICE_UNAVAILABLE, 'The server is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.');
  }
}

// URLSearchParams stringifies null/undefined instead of omitting keys. Omit the
// values the record-load API treats as "no file" (null, '', and the literal
// 'null' string echoed by API < 4.1.0 in in-flight AMQP messages).
// Values are compared explicitly - never use a truthiness test, so '0' survives.
// undefined values are kept on purpose: the API tolerates the string
// "undefined" only for the ids (handleId shim); for pFixType an absent
// value silently falls back to the default "API" fixroutine, which would
// mask a missing fixPrio/fixBulk config that currently 400s loudly.
export function buildQuery(params) {
  const searchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== '' && value !== 'null') {
      searchParams.set(key, value);
    }
  }
  return searchParams;
}

export function handleConectionError(error) {
  const logger = createLogger();
  if (error.response) {
    return error.response;
  }

  logger.info('No connection to aleph-record-load-api.');
  logError(error);
  throw new ApiError(httpStatus.SERVICE_UNAVAILABLE, 'The server is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.');
}
