import httpStatus from 'http-status';
import fetch from 'node-fetch';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {checkStatus, handleConectionError} from '../utils';

export default function ({recordLoadApiKey, recordLoadLibrary, recordLoadUrl}) {
  const logger = createLogger();

  return {fixLoadRecord};

  async function fixLoadRecord({correlationId = undefined, recordList, operation, cataloger, fixType, recordLoadParams = {}, prio}) {
    //logger.debug(`Records: ${JSON.stringify(records)}`);
    logger.debug(`RecordList: ${JSON.stringify(recordList)}`);
    const pActiveLibrary = recordLoadParams.pActiveLibrary || recordLoadLibrary;
    const recordSysList = createRecordSysListString(recordList);
    logger.debug(correlationId, recordList.length, operation, cataloger, recordLoadParams, prio);

    // why we had here cataloger.toUpperCase - where have we made a change?
    // cataloger.id from prio, cataloger from bulk

    const catalogerName = cataloger.id || cataloger;

    // useLoaderProcessId: name aleph-record-load-api:s files by unique loaderProcessId instead of correlationId
    const query = new URLSearchParams({
      correlationId,
      useLoaderProcessId: '1',
      pActiveLibrary,
      pFixType: fixType,
      pCatalogerIn: recordLoadParams.pCatalogerIn ? recordLoadParams.pCatalogerIn.toUpperCase() : catalogerName.toUpperCase()
    });
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.debug(`Loading ${recordList.length} recordIds to: ${url.toString()}`);

    const response = await fetch(url, {
      method: 'post',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      },
      body: recordSysList
    }).catch(error => handleConectionError(error));

    logger.silly(`Got response for load record. Status: ${response.status}`);

    checkStatus(response);

    if (response.status === httpStatus.OK) {
      const result = await response.json();
      logger.info(`Got "OK" (200) response from record-load-api. correlationId: ${result.correlationId}, loaderProcessId: ${result.loaderProcessId}`);
      logger.debug(`Response: ${JSON.stringify(result)}`);
      return result;
    }

    // Unexpected! Retry?
    throw new ApiError(response.status || httpStatus.INTERNAL_SERVER_ERROR, response ? await response.text() : 'Internal error');
  }

  function createRecordSysListString(recordList, pActiveLibrary) {
    return recordList.map(record => `${record}${pActiveLibrary}`).join('\n');
  }
}
