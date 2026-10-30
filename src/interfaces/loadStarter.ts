import httpStatus from 'http-status';
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import {createLogger, Error as ApiError, generateAuthorizationHeader} from '@natlibfi/melinda-commons';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {buildQuery, checkStatus, handleConnectionError} from '../utils.ts';
import type {Cataloger, LoadRecordResult, RecordLoadParams} from './fixLoadStarter.ts';
import type {MarcRecord} from '@natlibfi/marc-record';
import type {Response} from 'undici-types';

export default function ({recordLoadApiKey, recordLoadLibrary, recordLoadUrl, fixPrio, fixBulk}: {recordLoadApiKey: string; recordLoadLibrary: string; recordLoadUrl: string; fixPrio: string; fixBulk: string}) {
  const logger = createLogger();
  const INDEXING_PRIORITY = {
    HIGH: 1,
    LOW: 2
  };

  return {loadRecord};

  // -> loadRecord({correlationId, records, recordList, fixType, recordLoadParams, cataloger: headers.cataloger, prio})
  // -> recordList and fixType from parameters are not used but load-type loads, but are used for fix-type loads
  async function loadRecord({correlationId = undefined, records, operation, recordLoadParams = {}, cataloger, prio}: {
    correlationId?: string;
    records: MarcRecord[];
    operation?: string;
    recordLoadParams?: RecordLoadParams;
    cataloger: Cataloger;
    prio?: boolean;
  }): Promise<LoadRecordResult> {
    const recordCount = records.length;
    const seqRecords = createSeqRecords(records);

    logger.silly(`${correlationId}, ${records.length}, ${operation}, ${cataloger}, ${recordLoadParams}, ${prio}`);
    // This should check that concurrent update and create jobs with the same correlationId won't mix up their files
    // pOldNew from recordLoadParams is not used, its caught from operation

    // why we had here cataloger.toUpperCase - where have we made a change?
    // cataloger.id from prio, cataloger from bulk

    // cataloger is a plain string in bulk and {id} in prio; the boundary casts keep the original
    // `cataloger.id || cataloger` expression byte-identical: .id is undefined for strings at runtime,
    // so the expression is a string either way.
    const catalogerName: string = (cataloger as {id?: string}).id || (cataloger as string);

    // Note: pRejectFile & pLogFile are combined reject and logFiles for whole batch of records
    // useLoaderProcessId: name aleph-record-load-api:s files by unique loaderProcessId instead of correlationId
    const query = buildQuery({
      correlationId,
      useLoaderProcessId: '1',
      pActiveLibrary: recordLoadParams.pActiveLibrary || recordLoadLibrary,
      pOldNew: operation === OPERATIONS.CREATE ? 'NEW' : 'OLD',
      pFixType: prio ? fixPrio : fixBulk,
      pCatalogerIn: recordLoadParams.pCatalogerIn ? recordLoadParams.pCatalogerIn.toUpperCase() : catalogerName.toUpperCase(),
      pZ07PriorityYear: generateIndexingPriority(INDEXING_PRIORITY.HIGH, operation, prio),
      pRejectFile: recordLoadParams.pRejectFile && recordLoadParams.pRejectFile !== '' ? recordLoadParams.pRejectFile : null,
      pLogFile: recordLoadParams.pLogFile && recordLoadParams.pLogFile !== '' ? recordLoadParams.pLogFile : null
    });
    const url = new URL(`${recordLoadUrl}?${query}`);
    logger.debug(`Loading ${recordCount} records to: ${url.toString()}`);

    // handleConnectionError always throws; the catch path never resolves to a value.
    const response = (await fetch(url, {
      method: 'post',
      headers: {
        'Content-Type': 'text/plain',
        'Authorization': generateAuthorizationHeader(recordLoadApiKey)
      },
      body: seqRecords
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

    function generateIndexingPriority(indexingPriority: number, operation?: string, forPriority?: boolean): string | number {
      if (operation === OPERATIONS.CREATE) {
        return forPriority ? '1990' : '1992';
      }

      if (indexingPriority === INDEXING_PRIORITY.HIGH) {
        // These are values Aleph assigns for records modified in the cataloging GUI
        return forPriority ? '1998' : '2099';
      }

      return new Date().getFullYear() + 1000;
    }
  }

  function createSeqRecords(records: MarcRecord[]) {
    // Note: we error the whole batch in bulkjob, if even one of them errors serialization
    // Note: errored batch of records is not getting a recordResponse currently
    // Validator tries conversion for single records, so they should not end up here
    try {
      // If incoming records do not have 001, they all get aleph seq sys '000000000' and fuse together as one record
      //
      // Also, if there are two records with the same 001 after each other, they get fused together
      // We avoid this by adding a separator line between records
      const seqRecords = records.map(record => AlephSequential.to(record), {subfieldValues: false}).join('000000000 000   L 0\n');
      //  const seqRecords = records.map(record => AlephSequential.to(record)).join('');
      logger.silly(seqRecords);
      return seqRecords;
    } catch (err) {
      logger.debug(`createSeqRecords errored: ${err}`);
      // The serializers throw Error; the cast keeps the original `err.message` read on the catch-passed unknown.
      throw new ApiError(httpStatus.UNPROCESSABLE_ENTITY, (err as Error).message);
    }
  }
}

