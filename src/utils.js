import httpStatus from 'http-status';
import {Error as ApiError, Utils} from '@natlibfi/melinda-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons';

export function checkStatus(response) {
  const {createLogger} = Utils;
  const logger = createLogger();

  // Unauthorized (401)
  if (response.status === httpStatus.UNAUTHORIZED) { // eslint-disable-line functional/no-conditional-statement
    logger.log('info', 'Got "UNAUTHORIZED" (401) response from record-load-api.');
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
  }

  // Forbidden (403)
  if (response.status === httpStatus.FORBIDDEN) { // eslint-disable-line functional/no-conditional-statement
    logger.log('info', 'Got "FORBIDDEN" (403) response from record-load-api.');
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
  }

  // Not found (404)
  if (response.status === httpStatus.NOT_FOUND) { // eslint-disable-line functional/no-conditional-statement
    logger.log('info', 'Got "NOT_FOUND" (404) response from record-load-api. Process log files missing!');
    throw new ApiError(httpStatus.NOT_FOUND, 'Process log not found!');
  }

  // Not acceptable (406)
  if (response.status === httpStatus.NOT_ACCEPTABLE) { // eslint-disable-line functional/no-conditional-statement
    logger.log('info', 'Got "NOT_ACCEPTABLE" (406) response from record-load-api. 0 processed records!');
    throw new ApiError(httpStatus.NOT_ACCEPTABLE, '0 processed records!');
  }

  // Locked (423) too early
  if (response.status === httpStatus.LOCKED) { // eslint-disable-line functional/no-conditional-statement
    logger.log('info', 'Got "LOCKED" (423) response from record-load-api. Process is still going on!');
    throw new ApiError(httpStatus.LOCKED, 'Not ready yet!');
  }

  // Service unavailable (503)
  if (response.status === httpStatus.SERVICE_UNAVAILABLE) { // eslint-disable-line functional/no-conditional-statement
    logger.log('info', 'Got "SERVICE_UNAVAILABLE" (503) response from record-load-api.');
    throw new ApiError(httpStatus.SERVICE_UNAVAILABLE, 'The server is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.');
  }
}

export function handleConectionError(error) {
  const {createLogger} = Utils;
  const logger = createLogger();
  if (error.response) { // eslint-disable-line functional/no-conditional-statement
    // Toimii checkStatus(error.response);
    return error.response;
  }

  logger.log('info', 'No connection to aleph-record-load-api.');
  logError(error);
  throw new ApiError(httpStatus.SERVICE_UNAVAILABLE, 'The server is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.');
}

export function urlQueryParams(params) {
  const esc = encodeURIComponent;
  return Object.keys(params)
    .map(k => `${k}=${esc(params[k])}`)
    .join('&');
}
