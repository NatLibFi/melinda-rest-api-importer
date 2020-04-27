import axios from 'axios';
import httpStatus from 'http-status';
import {Error as ApiError, Utils} from '@natlibfi/melinda-commons';
import {checkStatus, handleConectionError} from '../utils';

export default function ({recordLoadApiKey, recordLoadUrl}) {
  const {createLogger, generateAuthorizationHeader} = Utils;
  const logger = createLogger();

  return {poll, requestFileClear};

  async function poll({correlationId, pActiveLibrary, processId, pLogFile, pRejectFile}) {

    // Pass correlationId to record-load-api so it can use same name in log files
    const params = {
      correlationId,
      pActiveLibrary,
      processId,
      pLogFile: pLogFile || null,
      pRejectFile: pRejectFile || null
    };

    const response = await axios({
      method: 'get',
      baseURL: recordLoadUrl,
      url: '',
      headers: {
        'Content-Type': 'text/plain',
        Authorization: generateAuthorizationHeader(recordLoadApiKey)
      },
      params
    }).catch(error => handleConectionError(error));

    logger.log('info', 'Got response for process poll!');
    logger.log('debug', `Status: ${response.status}`);

    checkStatus(response);

    // OK (200)
    if (response.status === httpStatus.OK) {
      const array = await response.data;
      const idList = array.map(id => formatRecordId(pActiveLibrary, id));
      logger.log('info', `Got "OK" (200) response from record-load-api. Ids: ${idList}`);
      return {payloads: idList, ackOnlyLength: idList.length};
    }

    // R-L-A has crashed (409)
    if (response.status === httpStatus.CONFLICT) {
      const array = await response.data;
      if (array.length > 0) {
        const idList = array.map(id => formatRecordId(pActiveLibrary, id));
        logger.log('info', `Got "conflict" (409) response from record-load-api. Ids:  ${idList}`);
        return {payloads: idList, ackOnlyLength: idList.length};
      }

      return {payloads: [], ackOnlyLength: 0};
    }

    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, 'Unexpected');
  }

  async function requestFileClear({correlationId, pActiveLibrary, processId}) {
    // Pass correlationId to record-load-api so it can use same name in log files
    const params = {
      correlationId,
      pActiveLibrary,
      processId
    };

    const response = await axios({
      method: 'delete',
      baseURL: recordLoadUrl,
      url: '',
      headers: {
        'Content-Type': 'text/plain',
        Authorization: generateAuthorizationHeader(recordLoadApiKey)
      },
      params
    }).catch(error => handleConectionError(error));

    checkStatus(response);

    logger.log('debug', 'Got response for file clear!');
    logger.log('debug', response.status);
  }

  function formatRecordId(library, id) {
    const pattern = new RegExp(`${library.toUpperCase()}$`, 'u');
    return id.replace(pattern, '');
  }
}
