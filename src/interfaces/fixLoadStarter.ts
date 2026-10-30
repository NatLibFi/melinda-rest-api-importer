import httpStatus from 'http-status';
import {createLogger, Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {checkStatus, handleConnectionError} from '../utils.ts';
import type {Response} from 'undici-types';

export interface RecordLoadParams {
  pActiveLibrary?: string;
  pCatalogerIn?: string;
  pRejectFile?: string;
  pLogFile?: string;
}
export type Cataloger = string | {id: string};
export interface LoadRecordResult {
  correlationId: string;
  processId?: number;
  pLogFile?: string | null;
  pRejectFile?: string | null;
  loaderProcessId?: number;
}

export default function ({recordLoadApiKey, recordLoadLibrary, recordLoadUrl}: {recordLoadApiKey: string; recordLoadLibrary: string; recordLoadUrl: string}) {
  const logger = createLogger();

  return {loadRecord};

  // -> loadRecord({correlationId, records, recordList, fixType, recordLoadParams, cataloger: headers.cataloger, prio})
  // -> records are not used but fix-type loads, but are used for load-type loads
  async function loadRecord({correlationId = undefined, recordList, operation, fixType, recordLoadParams = {}, cataloger, prio}: {
    correlationId?: string;
    recordList: string[];
    operation?: string;
    records?: unknown[];
    fixType?: string;
    recordLoadParams?: RecordLoadParams;
    cataloger: Cataloger;
    prio?: boolean;
  }): Promise<LoadRecordResult> {
    logger.silly(`loadRecord from fixLoadStarter`);
    logger.silly(`RecordList: ${JSON.stringify(recordList)}`);
    logger.silly(`RecordLoadParams: ${JSON.stringify(recordLoadParams)}`);
    logger.silly(`RecordLoadLibrary: ${recordLoadLibrary}`);
    logger.silly(`FixType: ${fixType}`);
    const pActiveLibrary = recordLoadParams.pActiveLibrary || recordLoadLibrary;
    const recordSysList = createRecordSysListString(recordList, pActiveLibrary);
    logger.silly(`${correlationId}, ${recordList.length}, ${operation}, ${cataloger}, ${fixType} ${recordLoadParams}, ${prio}`);
    logger.silly(`${recordSysList}`);

    // why we had here cataloger.toUpperCase - where have we made a change?
    // cataloger.id from prio, cataloger from bulk (DEVELOP: more consistent handling of cataloger?)

    // cataloger is a plain string in bulk and {id} in prio; the boundary casts keep the original
    // `cataloger.id || cataloger` expression byte-identical: .id is undefined for strings at runtime,
    // so the expression is a string either way.
    const catalogerName: string = (cataloger as {id?: string}).id || (cataloger as string);

    // useLoaderProcessId: name aleph-record-load-api:s files by unique loaderProcessId instead of correlationId
    // URLSearchParams stringifies the undefined values (correlationId, pFixType) as the literal "undefined" -
    // documented and asserted in tests, not a fix; the boundary cast keeps the init object byte-identical.
    const query = new URLSearchParams({
      correlationId,
      useLoaderProcessId: '1',
      pActiveLibrary,
      pFixType: fixType,
      pCatalogerIn: recordLoadParams.pCatalogerIn ? recordLoadParams.pCatalogerIn.toUpperCase() : catalogerName.toUpperCase()
    } as Record<string, string>);
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.debug(`Loading ${recordList.length} recordIds to: ${url.toString()}`);

    // handleConnectionError always throws; the catch path never resolves to a value.
    const response = (await fetch(url, {
      method: 'post',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      },
      body: recordSysList
    }).catch(error => handleConnectionError(error))) as Response;

    logger.silly(`Got response for load record. Status: ${response.status}`);

    await checkStatus(response);

    if (response.status === httpStatus.OK) {
      const result = (await response.json()) as LoadRecordResult;
      logger.info(`Got "OK" (200) response from record-load-api. correlationId: ${result.correlationId}, loaderProcessId: ${result.loaderProcessId}`);
      logger.debug(`Response: ${JSON.stringify(result)}`);
      return result;
    }

    // Unexpected! Retry?
    throw new ApiError(response.status || httpStatus.INTERNAL_SERVER_ERROR, response ? await response.text() : 'Internal error');
  }

  function createRecordSysListString(recordList: string[], pActiveLibrary: string) {
    return recordList.map(record => `${record}${pActiveLibrary.toUpperCase()}`).join('\n');
  }
}
