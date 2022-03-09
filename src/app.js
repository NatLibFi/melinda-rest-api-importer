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

<<<<<<< HEAD
    const {correlationId} = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.PROCESSING)});
    logger.silly(`checkInProcess: itemInProcess: ${correlationId}`);
    if (correlationId) {
=======
    const itemInProcess = await mongoOperator.getOne({importOperation: operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: IMPORT_JOB_STATE.IN_PROCESS});
    logger.silly(`checkInProcess: itemInProcess: ${JSON.stringify(itemInProcess)}`);
    if (itemInProcess) {
>>>>>>> origin/add-merge
      // Do not spam logs
      logger.silly(`Found item in process ${correlationId}`);
      await processOperator.loopCheck({correlationId, operation, mongoOperator, prio});
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
<<<<<<< HEAD
    // ImportJobStates: EMPTY, QUEUING, IN_QUEUE, PROCESSING, DONE, ERROR, ABORT
    // get here {<OPERATION>: IN_QUEUE}
=======
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
>>>>>>> origin/add-merge

    if (await checkImportJobStatePROCESSING()) {
      return startCheck();
    }

<<<<<<< HEAD
    if (await checkImportJobStateDONE()) {
      return startCheck();
    }

    if (await checkImportJobStateINQUEUE()) {
      return startCheck();
    }

    if (await checkQueueItemStateINQUEUE()) {
      return startCheck();
=======
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
>>>>>>> origin/add-merge
    }

    if (prio) {
      logger.silly(`app/checkItemImportingAndInQueue: Nothing found: PRIO -> checkItemImportingAndInQueue `);
      return checkItemImportingAndInQueue(false);
    }

    logger.silly(`app/checkItemImportingAndInQueue: Nothing found: BULK -> startCheck `);
    return startCheck(true, 3000);

    async function checkImportJobStatePROCESSING() {
      const itemImportingImporting = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.PROCESSING)});

      if (itemImportingImporting) {
        logger.debug(`Found item in importing ${itemImportingImporting.correlationId}, ImportJobState: {${operation}: PROCESSING}`);
        await itemImportingHandler({item: itemImportingImporting, operation});
        return true;
      }

      return false;
    }

    async function checkImportJobStateDONE() {
      const queueItem = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.DONE)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: DONE}`);
        logger.debug(inspect(queueItem));

        const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.CREATE : OPERATIONS.UPDATE;
        const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
        logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

        if ([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
          logger.debug(`Other importJob in not ongoing/pending, importing done`);
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }
      }
<<<<<<< HEAD
=======
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
>>>>>>> origin/add-merge

      return false;
    }

    async function checkImportJobStateINQUEUE() {
      const queueItem = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.IN_QUEUE)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: IN_QUEUE}`);
        // set here IMPORT_JOB_STATE: {CREATE: PROCESSING, UPDATE: PROCESSING} based of operation
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.PROCESSING});
        return true;
      }

      return false;
    }

    async function checkQueueItemStateINQUEUE() {
      const queueItem = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE});
      if (queueItem) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId}`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        return true;
      }

      return false;
    }
  }
}
