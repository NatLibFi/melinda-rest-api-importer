/* eslint-disable max-statements */
import fetch from 'node-fetch';
import httpStatus from 'http-status';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {checkStatus, handleConectionError} from '../utils';

export default function ({recordLoadApiKey, recordLoadUrl}) {
  const logger = createLogger();

  return {poll, requestFileClear};

  async function poll({correlationId, pActiveLibrary, processId, loaderProcessId, pLogFile, pRejectFile, recordAmount}) {

    // Pass correlationId to record-load-api so it can use same name in log files
    const query = new URLSearchParams({
      correlationId,
      pActiveLibrary,
      processId,
      loaderProcessId,
      pLogFile: pLogFile || null,
      pRejectFile: pRejectFile || null
    });
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.silly(`Polling recordLoadUrl: ${url.toString()}`);

    const response = await fetch(url, {
      method: 'get',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      }
    }).catch(error => handleConectionError(error));

    logger.silly(`Got response for process poll! Status: ${response.status}`);

    // Bad Request (400)
    if (response.status === httpStatus.BAD_REQUEST) { // eslint-disable-line functional/no-conditional-statement
      logger.info('Got "BAD_REQUEST" (400) response from record-load-api.');
      throw new ApiError(httpStatus.BAD_REQUEST);
    }

    // response: {"status":200,"payload":{"handledIds":[],"rejectedIds":["000000001"],"rejectMessages": []}}
    // response: {"status":409,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"],"rejectedIds":[],"rejectMessages": [], "errorIdCount": 1}}

    // 401, 403, 404, 406, 423, 503 responses from checkStatus
    checkStatus(response);

    // OK (200)
    // R-L-A has crashed (409) or encountered one or more oraErrors

    if (response.status === httpStatus.OK || response.status === httpStatus.CONFLICT) {
      const {handledIds, rejectedIds, rejectMessages, errorIdCount} = await response.json();
      // errorIdCount: amount ids that didn't get created even if their id is listed as handledIds
      logger.silly(`processPoll/poll handledIds: ${handledIds} rejectedIds: ${rejectedIds} rejectMessages: ${rejectMessages}: errorIdCount: ${errorIdCount}`);

      // This should check that payloads make sense (ie. are arrays)
      const handledIdList = handledIds.map(id => formatRecordId(pActiveLibrary, id));

      // Mark here those handledIds that are identical to the previous handledId as ERRORs
      const handledIdListWithErrors = await handledIdList.map((id, index, array) => {
        if (array[index - 1] === id) {
          return `ERROR-${id}`;
        }
        return id;
      });

      const rejectedIdList = rejectedIds.map(id => formatRecordId(pActiveLibrary, id));

      const handledAmount = handledIdList.length || 0;
      const rejectedAmount = rejectedIdList.length || 0;
      // if we didn't get errorIdCount => erroredAmount = 0
      const erroredAmount = errorIdCount || 0;
      // errored records were NOT handled, even if they are listed in handledId list (Aleph updates syslog before trying to create the record)
      const processedAmount = handledAmount + rejectedAmount - erroredAmount;
      const notProcessedAmout = recordAmount - processedAmount;
      const processedAll = processedAmount === recordAmount;
      const handledAll = handledAmount - erroredAmount === recordAmount;

      logger.silly(`processPoll/poll recordAmount: ${recordAmount}, processedAmount: ${processedAmount}, notProcessedAmount: ${notProcessedAmout}, erroredAmount: ${erroredAmount}`);

      logger.debug(`handledAmount: ${handledAmount}, erroredAmount: ${erroredAmount}`);

      const loadProcessReport = {status: response.status, processId, loaderProcessId, processedAll, recordAmount, processedAmount, handledAmount, rejectedAmount, rejectMessages, erroredAmount, handledAll};
      const responseStatusString = response.status === httpStatus.OK ? '"OK" (200)' : '"CONFLICT" (409)';
      logger.silly(`processPoll/poll Created loadProcessReport: ${JSON.stringify(loadProcessReport)}`);

      if (processedAll) {
        logger.info(`Got ${responseStatusString} response from record-load-api. All records processed ${processedAmount}/${recordAmount}. HandledIds (${handledIdList.length}). RejectedIds (${rejectedIdList.length}).`);
        logger.silly(`Ids (${handledIdList.length}): ${handledIdList}. RejectedIds (${rejectedIdList.length}): ${rejectedIdList}`);
        return {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount};
      }

      // What should we do in cases where R-L-A crashed and did not process any/all records?
      // Ack all messages for sent records

      // eslint-disable-next-line functional/no-conditional-statement
      if (processedAmount === 0 || processedAmount < 0) {
        logger.info(`Got ${responseStatusString} response from record-load-api, but NO records were processed ${processedAmount}/${recordAmount}. HandledIds (${handledIdList.length}). RejectedIds (${rejectedIdList.length}). ErroredAmount: ${erroredAmount}`);
      }
      // eslint-disable-next-line functional/no-conditional-statement
      if (processedAmount > 0) {
        logger.info(`Got ${responseStatusString} response from record-load-api, but all records were NOT processed ${processedAmount}/${recordAmount}. HandledIds (${handledIdList.length}). RejectedIds (${rejectedIdList.length}). ErroredAmount (${erroredAmount})`);
      }

      logger.debug(`Ids (${handledIdListWithErrors.length}): ${handledIdListWithErrors}. RejectedIds (${rejectedIdList.length}): ${rejectedIdList}. ErroredAmount: ${erroredAmount}`);
      return {payloads: {handledIds: handledIdListWithErrors, rejectedIds: rejectedIdList, erroredAmount, loadProcessReport}, ackOnlyLength: recordAmount};
    }

    // 500 from aleph-record-load-api goes here (not in utils::checkStatus)
    // Also other not statuses not handled by checkStatus
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, 'Unexpected');
  }

  async function requestFileClear({correlationId, pActiveLibrary, processId}) {
    // Pass correlationId to record-load-api so it can use same name in log files
    const query = new URLSearchParams({
      correlationId,
      pActiveLibrary,
      processId
    });
    const url = new URL(`${recordLoadUrl}?${query}`);

    logger.debug(`Sending file clearing request: ${url.toString()}`);

    const response = await fetch(url, {
      method: 'delete',
      headers: {
        'Content-Type': 'text/plain',
        Authorization: generateAuthorizationHeader(recordLoadApiKey)
      }
    }).catch(error => handleConectionError(error));

    logger.silly(`Got file clearing response ${JSON.stringify(response)}`);

    checkStatus(response);
    logger.silly(`Got response for file clear! Status: ${response.status}`);
  }

  function formatRecordId(library, id) {
    const pattern = new RegExp(`${library.toUpperCase()}$`, 'u');
    return id.replace(pattern, '');
  }
}
