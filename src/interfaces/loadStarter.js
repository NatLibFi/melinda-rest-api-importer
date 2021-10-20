/* eslint-disable max-statements */
import httpStatus from 'http-status';
import moment from 'moment';
import fetch from 'node-fetch';
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {checkStatus, handleConectionError} from '../utils';

export default function (recordLoadApiKey, recordLoadLibrary, recordLoadUrl) {
  const logger = createLogger();
  const INDEXING_PRIORITY = {
    HIGH: 1,
    LOW: 2
  };

  return {loadRecord};

  async function loadRecord({correlationId = undefined, records, operation, cataloger, recordLoadParams = {}, prio}) {
    const recordCount = records.length;
    const seqRecords = records.map(record => AlephSequential.to(record)).join('');

    const query = new URLSearchParams({
      correlationId,
      pActiveLibrary: recordLoadParams.pActiveLibrary || recordLoadLibrary,
      pOldNew: operation === OPERATIONS.CREATE ? 'NEW' : 'OLD',
      pFixType: prio ? 'API' : 'INSB',
      pCatalogerIn: recordLoadParams.pCatalogerIn ? recordLoadParams.pCatalogerIn.toUpperCase() : cataloger.toUpperCase(),
      pZ07PriorityYear: generateIndexingPriority(INDEXING_PRIORITY.HIGH, operation, prio),
      pRejectFile: recordLoadParams.pRejectFile && recordLoadParams.pRejectFile !== '' ? recordLoadParams.pRejectFile : null,
      pLogFile: recordLoadParams.pLogFile && recordLoadParams.pLogFile !== '' ? recordLoadParams.pLogFile : null
    });
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.log('debug', `Loading ${recordCount} records to: ${url.toString()}`);

    const response = await fetch(url, {
      method: 'post',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      },
      body: seqRecords
    }).catch(error => handleConectionError(error));

    logger.log('info', 'Got response for load record');
    logger.log('debug', `Status: ${response.status}`);

    checkStatus(response);

    if (response.status === httpStatus.OK) {
      logger.log('info', 'Got "OK" (200) response from record-load-api.');
      const result = await response.json();
      logger.log('debug', `Response: ${JSON.stringify(result)}`);
      return result;
    }

    // Unexpected! Retry?
    throw new ApiError(response.status || httpStatus.INTERNAL_SERVER_ERROR, response ? await response.text() : 'Internal error');

    function generateIndexingPriority(indexingPriority, operation, forPriority) {
      if (operation === OPERATIONS.CREATE) {
        return forPriority ? '1990' : '1992';
      }

      if (indexingPriority === INDEXING_PRIORITY.HIGH) {
        // These are values Aleph assigns for records modified in the cataloging GUI
        return forPriority ? '1998' : '2099';
      }

      return moment().add(1000, 'years')
        .year();
    }
  }
}
