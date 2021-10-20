import httpStatus from 'http-status';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons';

// eslint-disable-next-line max-statements
export function checkStatus(response) {
  const logger = createLogger();

  // Unauthorized (401)
  if (response.status === httpStatus.UNAUTHORIZED) { // eslint-disable-line functional/no-conditional-statement
    logger.info('Got "UNAUTHORIZED" (401) response from record-load-api.');
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
  }

  // Forbidden (403)
  if (response.status === httpStatus.FORBIDDEN) { // eslint-disable-line functional/no-conditional-statement
    logger.info('Got "FORBIDDEN" (403) response from record-load-api.');
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
  }

  // Not found (404)
  if (response.status === httpStatus.NOT_FOUND) { // eslint-disable-line functional/no-conditional-statement
    logger.info('Got "NOT_FOUND" (404) response from record-load-api. Process log files missing!');
    throw new ApiError(httpStatus.NOT_FOUND, 'Process log not found!');
  }

  // Not acceptable (406)
  if (response.status === httpStatus.NOT_ACCEPTABLE) { // eslint-disable-line functional/no-conditional-statement
    logger.info('Got "NOT_ACCEPTABLE" (406) response from record-load-api. 0 processed records!');
    throw new ApiError(httpStatus.NOT_ACCEPTABLE, '0 processed records!');
  }

  // Locked (423) too early
  if (response.status === httpStatus.LOCKED) { // eslint-disable-line functional/no-conditional-statement
    logger.info('Got "LOCKED" (423) response from record-load-api. Process is still going on!');
    throw new ApiError(httpStatus.LOCKED, 'Not ready yet!');
  }

  // Service unavailable (503)
  if (response.status === httpStatus.SERVICE_UNAVAILABLE) { // eslint-disable-line functional/no-conditional-statement
    logger.info('Got "SERVICE_UNAVAILABLE" (503) response from record-load-api.');
    throw new ApiError(httpStatus.SERVICE_UNAVAILABLE, 'The server is temporarily unable to service your request due to maintenance downtime or capacity problems. Please try again later.');
  }
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
