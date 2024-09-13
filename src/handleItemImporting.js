import {logError, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {promisify} from 'util';
import {sendErrorResponses} from './interfaces/sendErrorResponses';

export function createItemImportingHandler(amqpOperator, mongoOperators, recordLoadOperator, {error503WaitTime, recordLoadLibrary}) {
  const purgeQueues = false;
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);

  return handleItemImporting;

  async function handleItemImporting({item, operation, prio}) {

    logger.silly(`Item in importing: ${JSON.stringify(item)}`);
    const {correlationId} = item;

    const mongoOperator = prio ? mongoOperators.prio : mongoOperators.bulk;

    try {
      // load-type operations need records, fix-type operations do not need records
      const toRecords = [OPERATIONS.CREATE, OPERATIONS.UPDATE].includes(operation);

      // basic: get next chunk of 100 messages {headers, records, messages}
      const {headers, records, messages} = await amqpOperator.checkQueue({queue: `${operation}.${correlationId}`, style: 'basic', toRecord: toRecords, purge: purgeQueues});
      /// 1-100 messages from 1-10000 messages
      if (headers && messages) {
        await importRecords({mongoOperator, headers, operation, records, messages, item, correlationId, prio});
        return;
      }

      logger.debug(`app/handleItemImporting: No messages found in ${operation}.${correlationId}`);
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Importer: empty queue: ${operation}.${correlationId}`});
      throw new Error(`Empty queue ${operation}.${correlationId}`);
    } catch (error) {
      logger.error('app/handleItemImporting errored: ');
      logger.silly(JSON.stringify(error));
      logError(error);
      logger.debug(`we have ${operation} , ${correlationId}`);
      await sendErrorResponses({error, correlationId, queue: `${operation}.${correlationId}`, mongoOperator, prio, error503WaitTime, operation, amqpOperator});

      return;
    }
  }

  async function importRecords({mongoOperator, headers, operation, records, messages, item, prio}) {
    logger.debug(`app/handleItemImporting: Headers: ${JSON.stringify(headers)}, Messages (${messages.length}), Records: ${records?.length}`);
    const recordList = messages.map(message => message.properties.headers.id);
    const recordAmount = messages.length;
    const {correlationId, recordLoadParams} = item;
    const {fixType} = headers.operationSettings;
    const {cataloger} = headers;

    // messages nacked to wait results - should these go to some other queue PROCESS.correaltionId ?
    await amqpOperator.nackMessages(messages);
    await setTimeoutPromise(200); // (S)Nack time! - we need this timeout here to catch errors from loadRecord

    // what happens if recordLoadOperator errors?
    // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}
    const {processId, pLogFile, pRejectFile, loaderProcessId} = await recordLoadOperator.loadRecord({correlationId, records, recordList, fixType, recordLoadParams, cataloger, prio});

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
