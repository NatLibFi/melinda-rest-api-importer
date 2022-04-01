import {createLogger} from '@natlibfi/melinda-backend-commons';
import recordLoadFactory from './interfaces/loadStarter';
import {amqpFactory, mongoFactory, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS, createImportJobState} from '@natlibfi/melinda-rest-api-commons';
import {inspect, promisify} from 'util';
import {createItemImportingHandler} from './handleItemImporting';
import checkProcess from './interfaces/checkProcess';

export default async function ({
  amqpUrl, operation, pollWaitTime, error503WaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl, keepLoadProcessReports
}) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const amqpOperator = await amqpFactory(amqpUrl);
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports});
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
  const prioItemImportingHandler = createItemImportingHandler(amqpOperator, mongoOperatorPrio, recordLoadOperator, {prio: true, error503WaitTime, recordLoadLibrary});
  const bulkItemImportingHandler = createItemImportingHandler(amqpOperator, mongoOperatorBulk, recordLoadOperator, {prio: false, error503WaitTime, recordLoadLibrary});

  logger.info(`Started Melinda-rest-api-importer with operation ${operation}`);
  startCheck({});

  async function startCheck(checkInProcessItems = true, wait = false) {
    if (wait) {
      await setTimeoutPromise(wait);
      return startCheck();
    }

    if (checkInProcessItems) {
      return checkInProcess();
    }

    return checkItemImportingAndInQueue();
  }

  async function checkInProcess(prio = true) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;
    // Items in aleph-record-load-api

    const queueItemInProcess = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.PROCESSING, true)});

    // logger.silly(`checkInProcess: itemInProcess: ${correlationId}`);
    if (queueItemInProcess) {
      // Do not spam logs
      logger.silly(`Found item in process ${queueItemInProcess.correlationId}`);
      await processOperator.loopCheck({correlationId: queueItemInProcess.correlationId, operation, mongoOperator, prio});
      return startCheck(true, 100);
    }

    if (prio) {
      logger.silly(`app/checkInProcess: Nothing found in process for PRIO -> checkInProcess for BULK `);
      return checkInProcess(false);
    }

    logger.silly(`app/checkInProcess: Nothing found for BULK -> startCheck for importing without waiting`);
    return startCheck(false);
  }

  // eslint-disable-next-line max-statements
  async function checkItemImportingAndInQueue(prio = true) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;
    const itemImportingHandler = prio ? prioItemImportingHandler : bulkItemImportingHandler;
    // Items in importer to be send to aleph-record-load-api
    // ImportJobStates: EMPTY, QUEUING, IN_QUEUE, PROCESSING, DONE, ERROR, ABORT
    // get here {<OPERATION>: IN_QUEUE}

    if (await checkImportJobStateIMPORTING()) {
      return startCheck();
    }

    if (await checkImportJobStateDONE()) {
      return startCheck();
    }

    if (await checkImportJobStateINQUEUE()) {
      return startCheck();
    }

    if (await checkQueueItemStateINQUEUE()) {
      return startCheck();
    }

    if (prio) {
      logger.silly(`app/checkItemImportingAndInQueue: Nothing found: PRIO -> checkItemImportingAndInQueue `);
      return checkItemImportingAndInQueue(false);
    }

    logger.silly(`app/checkItemImportingAndInQueue: Nothing found: BULK -> startCheck `);
    return startCheck(true, 3000);

    async function checkImportJobStateIMPORTING() {
      const itemImportingImporting = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.IMPORTING, true)});

      if (itemImportingImporting) {
        logger.debug(`Found item in importing ${itemImportingImporting.correlationId}, ImportJobState: {${operation}: PROCESSING}`);
        await itemImportingHandler({item: itemImportingImporting, operation});
        return true;
      }

      return false;
    }

    async function checkImportJobStateDONE() {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.DONE, true)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: DONE}`);
        logger.debug(inspect(queueItem));

        const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
        const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
        logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

        if ([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
          logger.debug(`Other importJob in not ongoing/pending, importing done`);
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }
      }

      return false;
    }

    async function checkImportJobStateINQUEUE() {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.IN_QUEUE, true)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: IN_QUEUE}`);
        // set here IMPORT_JOB_STATE: {CREATE: PROCESSING, UPDATE: PROCESSING} based of operation
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.IMPORTING});
        return true;
      }
      return false;
    }

    // eslint-disable-next-line max-statements
    async function checkQueueItemStateINQUEUE() {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE});

      if (queueItem && queueItem.operationSettings.noop === true) {
        logger.verbose(`QueueItem ${queueItem.correlationId} has operationSettings.noop ${queueItem.operationSettings.noop} - not running importer for this job`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Noop for queueItem true, item in importer???`});
        await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
        return true;
      }

      if (queueItem && queueItem.importJobState[operation] === IMPORT_JOB_STATE.IN_QUEUE) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId}`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        return true;
      }

      if (queueItem && queueItem.importJobState[operation] === IMPORT_JOB_STATE.EMPTY) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId} but importJobState for ${operation} is ${queueItem.importJobState[operation]}`);
        logger.debug(JSON.stringify(queueItem.importJobState));

        // check whether also the other queue is EMPTY or a final state
        const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
        const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
        logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

        if ([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
          logger.debug(`Other importJob in not ongoing/pending, importing done`);
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }

        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        return true;
      }
      return false;
    }
  }
}
