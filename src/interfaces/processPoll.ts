import httpStatus from 'http-status';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {createLogger, Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {buildQuery, checkStatus, handleConnectionError} from '../utils.ts';
import type {Response} from 'undici-types';

export interface LoadProcessReport {
  status: number;
  processId: number | undefined;
  loaderProcessId: number | undefined;
  processedAll: boolean;
  recordAmount: number;
  processedAmount: number;
  handledAmount: number;
  rejectedAmount: number;
  rejectMessages: unknown[];
  erroredAmount: number;
  handledAll: boolean;
}

// unified return shape of pollLoad / pollFix:
export interface ProcessPollResults {
  payloads: {
    handledIds: string[];
    rejectedIds?: string[];
    erroredAmount?: number;
    loadProcessReport?: LoadProcessReport;
  };
  ackOnlyLength: number;
  status?: number; // only present on the pollLoad shapes
}

// processParams.data as parsed from the AMQP PROCESS message
export interface ProcessParamsData {
  correlationId: string;
  pActiveLibrary?: string;
  processId?: number;
  loaderProcessId?: number;
  pLogFile?: string | null;
  pRejectFile?: string | null;
  recordAmount?: number;
}

export default function ({recordLoadApiKey, recordLoadUrl}: {recordLoadApiKey: string; recordLoadUrl: string}) {
  const logger = createLogger();

  return {poll, requestFileClear};

  async function poll({operation, params}: {operation: string; params: ProcessParamsData}): Promise<ProcessPollResults | undefined> {

    const {correlationId, pActiveLibrary, processId, loaderProcessId, pLogFile = null, pRejectFile = null} = params;
    logger.silly(`poll parameters: operation: ${JSON.stringify(operation)}, params: ${JSON.stringify(params)}`);
    logger.silly(`${JSON.stringify(OPERATIONS)}`);
    if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation)) {
      logger.silly(`We have loadProcess operation: ${operation}`);
      const query = buildQuery({
        correlationId,
        pActiveLibrary,
        processId,
        loaderProcessId,
        pLogFile: pLogFile || null,
        pRejectFile: pRejectFile || null
      });

      const response = await pollQuery(query);
      logger.silly(`We got a loadProcess response.`);
      // params.pActiveLibrary/recordAmount are optional per ProcessParamsData but always present in PROCESS payloads; cast at the JSON boundary.
      return pollLoad({response, operation, ...params, pActiveLibrary: params.pActiveLibrary as string, recordAmount: params.recordAmount as number});
    }
    if (([OPERATIONS.FIX] as string[]).includes(operation)) {
      logger.silly(`We have fixProcess operation: ${operation}`);
      // URLSearchParams init coerces numbers/undefined to strings; cast at the boundary.
      const query = new URLSearchParams({
        correlationId,
        pActiveLibrary,
        processId,
        loaderProcessId
      } as unknown as Record<string, string>);

      const response = await pollQuery(query);
      logger.silly(`We got a fixProcess response.`);
      // params.pActiveLibrary/recordAmount are optional per ProcessParamsData but always present in PROCESS payloads; cast at the JSON boundary.
      return pollFix({response, operation, ...params, pActiveLibrary: params.pActiveLibrary as string, recordAmount: params.recordAmount as number});
    }
    return undefined;
  }


  async function pollQuery(query: URLSearchParams): Promise<Response> {
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.silly(`Polling recordLoadUrl: ${url.toString()}`);

    // handleConnectionError always throws; the catch path never resolves to a value.
    const response = (await fetch(url, {
      method: 'get',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      }
    }).catch(error => handleConnectionError(error))) as Response;

    logger.silly(`Got response for process poll! Status: ${response.status}`);

    // 400, 401, 403, 404, 406, 423, 503 responses from checkStatus
    await checkStatus(response);

    // OK (200)
    // R-L-A has crashed (409) or encountered one or more oraErrors
    if (response.status === httpStatus.OK || response.status === httpStatus.CONFLICT) {
      return response;
    }

    // 500 from aleph-record-load-api goes here (not in utils::checkStatus)
    // Also other not statuses not handled by checkStatus
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, 'Unexpected');
  }

  async function pollLoad({response, pActiveLibrary, processId, loaderProcessId, recordAmount}: {
    response: Response;
    operation: string;
    pActiveLibrary: string;
    processId?: number;
    loaderProcessId?: number;
    recordAmount: number;
  }): Promise<ProcessPollResults> {
    // OK (200)
    // R-L-A has crashed (409) or encountered one or more oraErrors

    //if (response.status === httpStatus.OK || response.status === httpStatus.CONFLICT) {

    // response: {"status":200,"payload":{"handledIds":[],"rejectedIds":["000000001"],"rejectMessages": []}}
    // response: {"status":409,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"],"rejectedIds":[],"rejectMessages": [], "errorIdCount": 1}}

    const {handledIds, rejectedIds, rejectMessages, errorIdCount} = (await response.json()) as {
      handledIds: string[];
      rejectedIds: string[];
      rejectMessages: unknown[];
      errorIdCount?: number;
    };
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

    const loadProcessReport: LoadProcessReport = {status: response.status, processId, loaderProcessId, processedAll, recordAmount, processedAmount, handledAmount, rejectedAmount, rejectMessages, erroredAmount, handledAll};
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

  async function pollFix({response, operation, pActiveLibrary, recordAmount}: {
    response: Response;
    operation: string;
    pActiveLibrary: string;
    recordAmount: number;
  }): Promise<ProcessPollResults | undefined> {
    logger.silly(`Got response for process poll (${operation})! Status: ${response.status}`);

    // response: {"status":200,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"]}}
    // response: {"status":409,"payload":{"handledIds":["000000001FIN01","000000002FIN01","000000004FIN01"]}}

    const {handledIds} = (await response.json()) as {handledIds: string[]};
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

    return undefined;
  }

  function mapHandledIds(handledIds: string[], pActiveLibrary: string): string[] {
    // This should check that payloads make sense (ie. are arrays)
    return handledIds.map(id => formatRecordId(pActiveLibrary, id));
  }

  // Note that aleph-record-load-api doesn't currently do anything with delete requests!!!
  async function requestFileClear({correlationId, pActiveLibrary, processId, loaderProcessId}: {correlationId: string; pActiveLibrary?: string; processId?: number; loaderProcessId?: number}): Promise<void> {
    // Pass correlationId to record-load-api so it can use same name in log files
    // URLSearchParams init coerces numbers/undefined to strings; cast at the boundary.
    const query = new URLSearchParams({
      correlationId,
      pActiveLibrary,
      processId,
      loaderProcessId
    } as unknown as Record<string, string>);
    const url = new URL(`${recordLoadUrl}?${query}`);

    logger.silly(`Sending file clearing request: ${url.toString()}`);

    // handleConnectionError always throws; the catch path never resolves to a value.
    const response = (await fetch(url, {
      method: 'delete',
      headers: {
        'Content-Type': 'text/plain',
        Authorization: generateAuthorizationHeader(recordLoadApiKey)
      }
    }).catch(error => handleConnectionError(error))) as Response;

    logger.silly(`Got file clearing response ${JSON.stringify(response)}`);

    await checkStatus(response);
    logger.silly(`Got response for file clear! Status: ${response.status}`);
  }

  function formatRecordId(library: string, id: string) {
    const pattern = new RegExp(`${library.toUpperCase()}$`, 'u');
    return id.replace(pattern, '');
  }
}
