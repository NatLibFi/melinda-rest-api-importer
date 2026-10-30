import {logError, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import type {AmqpOperator, MongoOperator, QueueItem} from '@natlibfi/melinda-rest-api-commons';
import {createLogger} from '@natlibfi/melinda-commons';
import {promisify} from 'util';
import type {Message, MessagePropertyHeaders} from 'amqplib';
import type {WithId} from 'mongodb';
import type {MarcRecord} from '@natlibfi/marc-record';
import type {Cataloger, LoadRecordResult, RecordLoadParams} from './interfaces/fixLoadStarter.ts';
import {sendErrorResponses} from './interfaces/sendErrorResponses.ts';

// The AMQP header contract the chunk `headers` follow (commons amqp.js builds the
// chunk headers from the first message's properties.headers): the record id,
// cataloger and operationSettings on every chunk, plus recordMetadata and notes
// on record messages. The only members this module reads are operationSettings
// (fixType) and cataloger.
export interface MessageHeaders {
  id: string;
  cataloger: Cataloger;
  operationSettings: {fixType?: string};
  recordMetadata: {blobSequence?: number; [key: string]: unknown};
  notes?: string[];
}

export function createItemImportingHandler(amqpOperator: AmqpOperator, mongoOperators: {prio: MongoOperator; bulk: MongoOperator}, recordLoadOperator: {
  loadRecord: (params: {
    correlationId: string;
    records?: MarcRecord[];
    recordList: string[];
    operation: string;
    fixType?: string;
    recordLoadParams?: RecordLoadParams;
    cataloger: Cataloger;
    prio: boolean;
  }) => Promise<LoadRecordResult>
}, {error503WaitTime, recordLoadLibrary}: {error503WaitTime: number; recordLoadLibrary: string}) {
  const purgeQueues = false;
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);

  return handleItemImporting;

  async function handleItemImporting({item, operation, prio, waited = false}: {
    item: WithId<QueueItem>;
    operation: string;
    prio: boolean;
    waited?: boolean;
  }): Promise<void> {

    logger.silly(`Item in importing: ${JSON.stringify(item)}`);
    const {correlationId} = item;

    const mongoOperator = prio ? mongoOperators.prio : mongoOperators.bulk;

    try {
      // load-type operations need records, fix-type operations do not need records
      // string[] widening (boundary cast, no logic change) so .includes(operation: string) typechecks
      const toRecords = ([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation);

      // basic: get next chunk of 100 messages {headers, records, messages}
      const checkQueueResult = await amqpOperator.checkQueue({queue: `${operation}.${correlationId}`, style: 'basic', toRecord: toRecords, purge: purgeQueues});
      // H2: checkQueue returns CheckQueueResult = number | boolean |
      // QueueItemRecordPayload | QueueItemMessagePayload | GetMessage. With
      // style 'basic' the runtime returns a QueueItemRecordPayload
      // {headers, records, messages} when toRecord is true (CREATE/UPDATE), a
      // QueueItemMessagePayload {headers, messages} when it is false (FIX), or
      // false for an empty queue. `headers`/`messages` exist only on the two
      // payload objects and `records` only on the record payload.
      // `typeof === 'object'` drops the primitive (number/boolean) members so
      // the `in` checks typecheck, and the existing `if (headers && messages)`
      // guard below stays the effective type guard (C8/C9 precedent in
      // sendErrorResponses.ts / checkProcess.ts). `headers` is the chunk's
      // first-message headers, so it is cast once here to the MessageHeaders
      // boundary type; everything downstream then typechecks.
      const headers = (checkQueueResult && typeof checkQueueResult === 'object' && 'headers' in checkQueueResult ? checkQueueResult.headers : undefined) as MessageHeaders | undefined;
      const records = checkQueueResult && typeof checkQueueResult === 'object' && 'records' in checkQueueResult ? checkQueueResult.records : undefined;
      const messages = checkQueueResult && typeof checkQueueResult === 'object' && 'headers' in checkQueueResult ? checkQueueResult.messages : undefined;
      /// 1-100 messages from 1-10000 messages
      if (headers && messages) {
        await importRecords({mongoOperator, headers, operation, records, messages, item, correlationId, prio});
        return;
      }
      await handleNoMessages({item, operation, prio, waited});

    } catch (error) {
      logger.error('app/handleItemImporting errored: ');
      logger.silly(JSON.stringify(error));
      logError(error);
      logger.debug(`we have ${operation} , ${correlationId}`);
      // The catch variable is unknown; sendErrorResponses declares the
      // structural union it handles. Single boundary cast, no logic change.
      await sendErrorResponses({error: error as {status?: number; payload?: unknown; message?: string}, correlationId, queue: `${operation}.${correlationId}`, mongoOperator, prio, error503WaitTime, operation, amqpOperator});

      return;
    }

    async function handleNoMessages({item, operation, prio, waited}: {
      item: WithId<QueueItem>;
      operation: string;
      prio: boolean;
      waited: boolean;
    }): Promise<void> {
      logger.debug(`app/handleItemImporting: No messages found in ${operation}.${correlationId}`);

      if (!waited) {
        logger.debug(`app/handleItemImporting: Waiting 200 ms and trying again`);
        await setTimeoutPromise(200); // Let's see if we have messages after waiting
        return handleItemImporting({item, operation, prio, waited: true});
      }

      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Importer: empty queue: ${operation}.${correlationId}`});
      throw new Error(`Empty queue ${operation}.${correlationId}`);
    }
  }

  async function importRecords({mongoOperator, headers, operation, records, messages, item, prio}: {
    mongoOperator: MongoOperator;
    headers: MessageHeaders;
    operation: string;
    records: MarcRecord[] | undefined;
    messages: Message[];
    item: WithId<QueueItem>;
    correlationId: string;
    prio: boolean;
  }): Promise<void> {
    logger.debug(`app/handleItemImporting: Headers: ${JSON.stringify(headers)}, Messages (${messages.length}), Records: ${records?.length}`);
    // The record id lives in the AMQP message headers: amqplib types
    // properties.headers as MessagePropertyHeaders | undefined (the cast drops
    // the undefined; bracket access per noPropertyAccessFromIndexSignature).
    // The string[] annotation is the boundary marker.
    const recordList: string[] = messages.map(message => (message.properties.headers as MessagePropertyHeaders)['id']);
    const recordAmount = messages.length;
    const {correlationId} = item;
    // The published 6.2 QueueItem type declares recordLoadParams as unknown;
    // the runtime shape is RecordLoadParams. Single boundary cast. The type
    // keeps `| undefined` (no `?? {}` default) so the falsy passthrough is
    // byte-identical: an absent recordLoadParams reaches loadRecord as
    // undefined and the PROCESS message data falls back to recordLoadLibrary.
    const recordLoadParams = item.recordLoadParams as RecordLoadParams | undefined;
    const {fixType} = headers.operationSettings;
    const {cataloger} = headers;

    // messages nacked to wait results - should these go to some other queue PROCESS.correaltionId ?
    await amqpOperator.nackMessages(messages);
    await setTimeoutPromise(200); // (S)Nack time! - we need this timeout here to catch errors from loadRecord

    // what happens if recordLoadOperator errors?
    // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}
    const {processId, pLogFile, pRejectFile, loaderProcessId} = await recordLoadOperator.loadRecord({correlationId, records, recordList, operation, fixType, recordLoadParams, cataloger, prio});

    logger.silly(`app/handleItemImporting: setState and send to process queue`);

    // send here to queue PROCESS.<OPERATION>.correlationId
    const processQueue = `PROCESS.${operation}.${correlationId}`;
    logger.debug(`Sending process information for loading process ${processId} / ${loaderProcessId} to ${processQueue}`);

    // what happens if sendToQueue errors?
    await amqpOperator.sendToQueue({
      queue: processQueue,
      correlationId,
      headers: {queue: `${operation}.${correlationId}`},
      data: {
        correlationId,
        pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
        processId, pRejectFile, pLogFile, loaderProcessId,
        recordAmount
      }
    });
    await setTimeoutPromise(200); // (S)Nack time! - wait here to avoid polling of a process queue that would have not received process message yet

    // set here importJobState: {<OPERATION>: PROCESSING}
    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.PROCESSING});

    return;
  }

}
