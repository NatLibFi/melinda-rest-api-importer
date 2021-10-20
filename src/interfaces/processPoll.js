/* eslint-disable max-statements */
import fetch from 'node-fetch';
import httpStatus from 'http-status';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {checkStatus, handleConectionError} from '../utils';

export default function ({recordLoadApiKey, recordLoadUrl}) {
  const logger = createLogger();

  return {poll, requestFileClear};

  async function poll({correlationId, pActiveLibrary, processId, pLogFile, pRejectFile, recordAmount}) {

    // Pass correlationId to record-load-api so it can use same name in log files
    const query = new URLSearchParams({
      correlationId,
      pActiveLibrary,
      processId,
      pLogFile: pLogFile || null,
      pRejectFile: pRejectFile || null
    });
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.log('debug', `Polling recordLoadUrl: ${url.toString()}`);

    const response = await fetch(url, {
      method: 'get',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      }
    }).catch(error => handleConectionError(error));

    logger.log('info', 'Got response for process poll!');
    logger.log('debug', `Status: ${response.status}`);

    // response: {"status":200,"payload":{"handledIds":[],"rejectedIds":["000000001"],"rejectMessages": []}}
    // response: {"status":409,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"],"rejectedIds":[],"rejectMessages": []}}

    // 401, 403, 404, 406, 423, 503 responses from checkStatus
    checkStatus(response);

    // OK (200)
    // R-L-A has crashed (409)

    if (response.status === httpStatus.OK || response.status === httpStatus.CONFLICT) {
      const {handledIds, rejectedIds, rejectMessages} = await response.json();
      logger.debug(`processPoll/poll handledIds: ${handledIds} rejectedIds: ${rejectedIds} rejectMessages: ${rejectMessages}`);

      // This should check that payloads make sense (ie. are arrays)
      const handledIdList = handledIds.map(id => formatRecordId(pActiveLibrary, id));
      const rejectedIdList = rejectedIds.map(id => formatRecordId(pActiveLibrary, id));

      const handledAmount = handledIdList.length || 0;
      const rejectedAmount = rejectedIdList.length || 0;
      const processedAmount = handledAmount + rejectedAmount;
      const notProcessedAmout = recordAmount - processedAmount;
      const processedAll = processedAmount === recordAmount;

      logger.debug(`processPoll/poll recordAmount: ${recordAmount}, processedAmount: ${processedAmount}, notProcessedAmount: ${notProcessedAmout}`);

      const loadProcessReport = {status: response.status, processId, processedAll, recordAmount, processedAmount, handledAmount, rejectedAmount, rejectMessages};
      logger.debug(`processPoll/poll Created loadProcessReport: ${JSON.stringify(loadProcessReport)}`);

      if (processedAll) {
        logger.log('info', `Got "OK" (200) or "CONFLICT" (409) response from record-load-api. All records processed ${processedAmount}/${recordAmount}. Ids (${handledIdList.length}): ${handledIdList}. RejectedIds (${rejectedIdList.length}): ${rejectedIdList}`);
        return {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount};
      }

      // eslint-disable-next-line functional/no-conditional-statement
      if (processedAmount === 0) {
        logger.info(`Got "OK" (200) or "CONFLICT" (409) response from record-load-api, but NO records were processed ${processedAmount}/${recordAmount}.`);
      }

      // What should we do in cases where R-L-A crashed and did not process any/all records?
      // Ack all messages for sent records
      logger.info(`Got "OK" (200) or "CONFLICT" (409) response from record-load-api, but all records were NOT processed ${processedAmount}/${recordAmount}.`);
      return {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: recordAmount};
    }

    // This never happens??? utils:checkStatus handles also 404.
    if (response.status === httpStatus.NOT_FOUND) {
      // P_manage_18 inputfile missing
      return {payloads: ['ERROR'], ackOnlyLength: 1};
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

    logger.log('debug', `Sending file clearing request: ${url.toString()}`);

    const response = await fetch(url, {
      method: 'delete',
      headers: {
        'Content-Type': 'text/plain',
        Authorization: generateAuthorizationHeader(recordLoadApiKey)
      }
    }).catch(error => handleConectionError(error));

    logger.log('debug', `Got file clearing response ${JSON.stringify(response)}`);
    checkStatus(response);

    logger.log('debug', 'Got response for file clear!');
    logger.log('debug', response.status);
  }

  function formatRecordId(library, id) {
    const pattern = new RegExp(`${library.toUpperCase()}$`, 'u');
    return id.replace(pattern, '');
  }
}
