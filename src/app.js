import {createLogger} from '@natlibfi/melinda-backend-commons';
import {amqpFactory, mongoFactory, logError, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {inspect, promisify} from 'util';
import recordLoadFactory from './interfaces/loadStarter';
import checkProcess from './interfaces/checkProcess';
import httpStatus from 'http-status';

export default async function ({
  amqpUrl, operation, pollWaitTime, error503WaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl, keepLoadProcessReports
}) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const purgeQueues = false;
  const amqpOperator = await amqpFactory(amqpUrl);
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports});

  logger.info(`Started Melinda-rest-api-importer with operation ${operation}`);
  startCheck({});

  async function startCheck({checkInProcessItems = true, wait = false}) {
    if (wait) {
      await setTimeoutPromise(5000);
      return startCheck({});
    }

    if (checkInProcessItems) {
      return checkInProcess({});
    }

    return checkItemImportingAndInQueue({});
  }

  async function checkInProcess({prio = true}) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;
    // Items in aleph-record-load-api

    const itemInProcess = await mongoOperator.getOne({importOperation: operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: IMPORT_JOB_STATE.IN_PROCESS});
    logger.silly(`checkInProcess: itemInProcess: ${JSON.stringify(itemInProcess)}`);
    if (itemInProcess) {
      // Do not spam logs
      logger.silly(`Found item in process ${itemInProcess.correlationId}`);
      return handleItemInProcess({item: itemInProcess, operation, mongoOperator, prio});
    }

    if (prio) {
      logger.silly(`app/checkInProcess: Nothing found in process for PRIO -> checkInProcess for BULK `);
      return checkInProcess({prio: false});
    }

    logger.silly(`app/checkInProcess: Nothing found for BULK -> startCheck for importing without waiting`);
    return startCheck({checkInProcessItems: false, wait: false});
  }

  // eslint-disable-next-line max-statements
  async function checkItemImportingAndInQueue({prio = true}) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;

    // Items in importer to be send to aleph-record-load-api
    // get here IMPORT_JOB_STATE.<OPERATION>.IMPORTING
    const itemImportingImporting = await mongoOperator.getOne({importOperation: operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: IMPORT_JOB_STATE.IMPORTING});

    /*
      EMPTY: 'EMPTY',
      QUEUING: 'QUEUING',
      IN_QUEUE: 'IN_QUEUE',
      PROCESSING: 'PROCESSING',
      DONE: 'DONE',
      ERROR: 'ERROR',
      ABORT: 'ABORT'
    */

    if (itemImportingImporting) {
      logger.debug(`Found item in importing ${itemImportingImporting.correlationId}, ${operation}ImportJobState: IMPORTING`);
      return handleItemImporting({item: itemImportingImporting, operation, mongoOperator, prio});
    }

    const itemImportingInQueue = await mongoOperator.getOne({importOperation: operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: IMPORT_JOB_STATE.IN_QUEUE});

    if (itemImportingInQueue) {
      logger.debug(`Found item in importing ${itemImportingInQueue.correlationId}, ${operation}ImportJobState: IN_QUEUE`);
      return handleItemInQueue({item: itemImportingInQueue, operation, mongoOperator, prio});
    }

    const itemImportingDone = await mongoOperator.getOne({importOperation: operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: IMPORT_JOB_STATE.DONE});
    if (itemImportingDone) {
      logger.debug(`Found item in importing ${itemImportingDone.correlationId}, ${operation}ImportJobState: DONE`);
      logger.debug(inspect(itemImportingDone));

      const otherOperationImportJobState = operation === OPERATIONS.CREATE ? 'updateImportJobState' : 'createImportJobState';
      const otherOperationImportJobStateResult = itemImportingDone[otherOperationImportJobState];
      logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

      if ([IMPORT_JOB_STATE.NULL, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
        logger.debug(`Other importJob in not ongoing/pending, importing done`);
        await mongoOperator.setState({correlationId: itemImportingDone.correlationId, state: QUEUE_ITEM_STATE.DONE});
        return startCheck({checkInProcessItems: true, wait: true});
      }
    }

    // This fails in cases where the operation in queueItem is not same as the importOperation
    // either if operation in queueItem is BOTH or when its UPDATE for CREATE importer
    const itemInQueue = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE});
    if (itemInQueue) {
      logger.debug(`Found item in queue to be imported ${itemInQueue.correlationId}`);
      return handleItemInQueue({item: itemInQueue, mongoOperator, operation});
    }

    if (prio) {
      logger.silly(`app/checkItemImportingAndInQueue: Nothing found: PRIO -> checkItemImportingAndInQueue `);
      return checkItemImportingAndInQueue({prio: false});
    }

    logger.silly(`app/checkItemImportingAndInQueue: Nothing found: BULK -> startCheck `);
    return startCheck({checkInProcessItems: true, wait: true});
  }


  async function handleItemInProcess({item, operation, mongoOperator, prio}) {
    logger.silly(`Item in process: ${JSON.stringify(item)}`);
    await processOperator.loopCheck({correlationId: item.correlationId, operation, mongoOperator, prio});
    await setTimeoutPromise(100);
    return startCheck({});
  }

  async function handleItemImporting({item, mongoOperator, prio, operation}) {
    logger.silly(`Item in importing: ${JSON.stringify(item)}`);
    const {correlationId} = item;

    try {
      // basic: get next chunk of 100 messages {headers, records, messages}
      const {headers, records, messages} = await amqpOperator.checkQueue({queue: `${operation}.${correlationId}`, style: 'basic', toRecord: true, purge: purgeQueues});
      /// 1-100 messages from 1-10000 messages
      if (headers && messages) {
        await importRecords({headers, operation, records, messages, item, correlationId, mongoOperator, amqpOperator, recordLoadOperator, prio});
        return startCheck({});
      }

      logger.debug(`app/handleItemImporting: No messages found in ${operation}.${correlationId}`);
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Importer: empty queue: ${operation}.${correlationId}`});
      throw new Error(`Empty queue ${operation}.${correlationId}`);
      //return startCheck();

    } catch (error) {
      logger.error('app/handleItemImporting errored: ');
      logError(error);
      await sendErrorResponses({error, correlationId, queue: `${operation}.${correlationId}`, mongoOperator, prio});

      return startCheck({});
    }

  }

  async function importRecords({headers, operation, records, messages, item, mongoOperator, amqpOperator, recordLoadOperator, prio}) {
    logger.debug(`app/handleItemImporting: Headers: ${JSON.stringify(headers)}, Messages (${messages.length}), Records: ${records.length}`);
    const recordAmount = records.length;
    // recordLoadParams have pOldNew - is this used or is operation caught from importer?
    const {correlationId, recordLoadParams} = item;
    // messages nacked to wait results - should these go to some other queue IN_PROCESS.correaltionId ?
    await amqpOperator.nackMessages(messages);

    await setTimeoutPromise(200); // (S)Nack time!
    // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}

    const {processId, pLogFile, pRejectFile} = await recordLoadOperator.loadRecord({correlationId, ...headers, records, recordLoadParams, prio});

    logger.silly(`app/handleItemImporting: setState and send to process queue`);

    // set here IMPORT_JOB_STATE.<OPERATION>.IN_PROCESS
    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.IN_PROCESS});
    //await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IN_PROCESS});

    // send here to queue PROCESS.<OPERATION>.correlationId
    const processQueue = `PROCESS.${operation}.${correlationId}`;

    await amqpOperator.sendToQueue({
      queue: processQueue,
      correlationId,
      headers: {queue: `${operation}.${correlationId}`},
      data: {
        correlationId,
        pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
        processId, pRejectFile, pLogFile,
        recordAmount
      }
    });
    return startCheck({});
  }

  async function handleItemInQueue({item, mongoOperator, operation}) {
    logger.silly(`app/handleItemInQueue: QueueItem: ${JSON.stringify(item)}`);
    // set here IMPORT_JOB_STATE.<OPERATION>.IMPORTING
    await mongoOperator.setState({correlationId: item.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
    await mongoOperator.setImportJobStates({correlationId: item.correlationId, importJobState: {[operation]: IMPORT_JOB_STATE.IMPORTING}});
    return startCheck({});
  }

  async function sendErrorResponses({error, correlationId, queue, mongoOperator, prio = false}) {
    logger.debug('app/sendErrorResponses: Sending error responses');

    // get next chunk of 100 messages {headers, messages} where cataloger is the same
    // no need for transforming messages to records
    const {messages} = await amqpOperator.checkQueue({queue, style: 'basic', toRecords: false, purge: false});

    if (messages) { // eslint-disable-line functional/no-conditional-statement
      logger.debug(`Got back messages (${messages.length}) for ${correlationId} from ${queue}`);

      const responseStatus = error.status ? error.status : httpStatus.INTERNAL_SERVER_ERROR;
      const responsePayload = error.payload ? error.payload : 'unknown error';

      logger.silly(`app/sendErrorResponses Status: ${responseStatus}, Messages: ${messages.length}, Payloads: ${responsePayload}`);
      // Send response back if PRIO
      // Send responses back if BULK and error is something else than 503

      // eslint-disable-next-line no-extra-parens
      if (prio || (!prio && error.status !== 503)) {

        await amqpOperator.ackMessages(messages);
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: responsePayload, errorStatus: responseStatus});
        return;
      }

      // Nack messages and sleep, if BULK and error is 503
      if (!prio && error.status === 503) {
        await amqpOperator.nackMessages(messages);
        logger.debug(`app/sendErrorResponses Got 503 for bulk. Nack messages to try loading/polling again after sleeping ${error503WaitTime} ms`);
        await setTimeoutPromise(error503WaitTime);
        return;
      }

      throw new Error('app/sendErrorMessages: What to do with these error responses?');
    }
    logger.debug(`app/sendErrorResponses: Did not get back any messages: ${messages} from ${queue}`);
    // should this throw an error?
  }
}
