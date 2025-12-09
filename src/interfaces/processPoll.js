import httpStatus from 'http-status';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {checkStatus, handleConectionError} from '../utils.js';

export default function ({recordLoadApiKey, recordLoadUrl}) {
  const logger = createLogger();

  return {poll, requestFileClear};

  async function poll({operation, params}) {

    const {correlationId, pActiveLibrary, processId, loaderProcessId, pLogFile = null, pRejectFile = null} = params;
    logger.silly(`poll parameters: operation: ${JSON.stringify(operation)}, params: ${JSON.stringify(params)}`);
    logger.silly(`${JSON.stringify(OPERATIONS)}`);
    if ([OPERATIONS.CREATE, OPERATIONS.UPDATE].includes(operation)) {
      logger.silly(`We have loadProcess operation: ${operation}`);
      const query = new URLSearchParams({
        correlationId,
        pActiveLibrary,
        processId,
        loaderProcessId,
        pLogFile: pLogFile || null,
        pRejectFile: pRejectFile || null
      });

      const response = await pollQuery(query);
      logger.silly(`We got a loadProcess response.`);
      return pollLoad({response, operation, ...params});
    }
    if ([OPERATIONS.FIX].includes(operation)) {
      logger.silly(`We have fixProcess operation: ${operation}`);
      const query = new URLSearchParams({
        correlationId,
        pActiveLibrary,
        processId,
        loaderProcessId
      });

      const response = await pollQuery(query);
      logger.silly(`We got a fixProcess response.`);
      return pollFix({response, operation, ...params});
    }
  }


  async function pollQuery(query) {
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

    // 400, 401, 403, 404, 406, 423, 503 responses from checkStatus
    checkStatus(response);

    // OK (200)
    // R-L-A has crashed (409) or encountered one or more oraErrors
    if (response.status === httpStatus.OK || response.status === httpStatus.CONFLICT) {
      return response;
    }

    // 500 from aleph-record-load-api goes here (not in utils::checkStatus)
    // Also other not statuses not handled by checkStatus
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, 'Unexpected');
  }

  async function pollLoad({response, pActiveLibrary, processId, loaderProcessId, recordAmount}) {
    // OK (200)
    // R-L-A has crashed (409) or encountered one or more oraErrors

    //if (response.status === httpStatus.OK || response.status === httpStatus.CONFLICT) {

    // response: {"status":200,"payload":{"handledIds":[],"rejectedIds":["000000001"],"rejectMessages": []}}
    // response: {"status":409,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"],"rejectedIds":[],"rejectMessages": [], "errorIdCount": 1}}

    const {handledIds, rejectedIds, rejectMessages, errorIdCount} = await response.json();
    // errorIdCount: amount ids that didn't get created even if their id is listed as handledIds
    logger.silly(`processPoll/poll handledIds: ${handledIds} rejectedIds: ${rejectedIds} rejectMessages: ${rejectMessages}: errorIdCount: ${errorIdCount}`);

    // This should check that payloads make sense (ie. are arrays)
    const handledIdList = await mapHandledIds(handledIds, pActiveLibrary);

    // Mark here those handledIds that are identical to the previous handledId as ERRORs
    const handledIdListWithErrors = await handledIdList.map((id, index, array) => {
      if (array[index - 1] === id) {
        return `ERROR-${id}`;
      }
      return id;
    });

    const rejectedIdList = mapHandledIds(rejectedIds, pActiveLibrary);

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

    logger.silly(`handledAmount: ${handledAmount}, erroredAmount: ${erroredAmount}`);

    const loadProcessReport = {status: response.status, processId, loaderProcessId, processedAll, recordAmount, processedAmount, handledAmount, rejectedAmount, rejectMessages, erroredAmount, handledAll};
    const responseStatusString = response.status === httpStatus.OK ? '"OK" (200)' : '"CONFLICT" (409)';
    logger.silly(`processPoll/poll Created loadProcessReport: ${JSON.stringify(loadProcessReport)}`);

    if (processedAll) {
      logger.info(`Got ${responseStatusString} response from record-load-api. All records processed ${processedAmount}/${recordAmount}. HandledIds (${handledIdList.length}). RejectedIds (${rejectedIdList.length}).`);
      logger.silly(`Ids (${handledIdList.length}): ${handledIdList}. RejectedIds (${rejectedIdList.length}): ${rejectedIdList}`);
      return {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount, status: response.status};
    }

    // What should we do in cases where R-L-A crashed and did not process any/all records?
    // Ack all messages for sent records

    if (processedAmount === 0 || processedAmount < 0) {
      logger.info(`Got ${responseStatusString} response from record-load-api, but NO records were processed ${processedAmount}/${recordAmount}. HandledIds (${handledIdList.length}). RejectedIds (${rejectedIdList.length}). ErroredAmount: ${erroredAmount}`);
    }

    if (processedAmount > 0) {
      logger.info(`Got ${responseStatusString} response from record-load-api, but all records were NOT processed ${processedAmount}/${recordAmount}. HandledIds (${handledIdList.length}). RejectedIds (${rejectedIdList.length}). ErroredAmount (${erroredAmount})`);
    }

    logger.silly(`Ids (${handledIdListWithErrors.length}): ${handledIdListWithErrors}. RejectedIds (${rejectedIdList.length}): ${rejectedIdList}. ErroredAmount: ${erroredAmount}`);
    return {payloads: {handledIds: handledIdListWithErrors, rejectedIds: rejectedIdList, erroredAmount, loadProcessReport}, ackOnlyLength: recordAmount};
  }

  async function pollFix({response, operation, pActiveLibrary, recordAmount}) {
    logger.silly(`Got response for process poll (${operation})! Status: ${response.status}`);

    // response: {"status":200,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"]}}
    // response: {"status":409,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"]}}

    const {handledIds} = await response.json();
    logger.silly(`processPoll/pollFix handledIds: ${handledIds}`);
    const handledIdList = mapHandledIds(handledIds, pActiveLibrary);

    // OK (200)
    // R-L-A has crashed (409) or encountered one or more oraErrors

    // FixProcessess handledIds is a guess from inputFile
    if (response.status === httpStatus.CONFLICT) {
      // we should somehow return also status - these are all status UNKNOWN
      return {payloads: {handledIds: handledIdList}, ackOnlyLength: recordAmount, status: response.status};
    }

    if (response.status === httpStatus.OK) {
      return {payloads: {handledIds: handledIdList}, ackOnlyLength: recordAmount, status: response.status};
    }

  }

  function mapHandledIds(handledIds, pActiveLibrary) {
    // This should check that payloads make sense (ie. are arrays)
    return handledIds.map(id => formatRecordId(pActiveLibrary, id));
  }

  // Note that aleph-record-load-api doesn't currently do anything with delete requests!!!
  async function requestFileClear({correlationId, pActiveLibrary, processId, loaderProcessId}) {
    // Pass correlationId to record-load-api so it can use same name in log files
    const query = new URLSearchParams({
      correlationId,
      pActiveLibrary,
      processId,
      loaderProcessId
    });
    const url = new URL(`${recordLoadUrl}?${query}`);

    logger.silly(`Sending file clearing request: ${url.toString()}`);

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
