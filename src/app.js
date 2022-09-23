import {createLogger} from '@natlibfi/melinda-backend-commons';
import recordLoadFactory from './interfaces/loadStarter';
import {amqpFactory, mongoFactory, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS, createImportJobState} from '@natlibfi/melinda-rest-api-commons';
import {inspect, promisify} from 'util';
import {createItemImportingHandler} from './handleItemImporting';
import checkProcess from './interfaces/checkProcess';
//import {prettyMilliseconds} from 'pretty-ms';
import prettyPrint from 'pretty-print-ms';

export default async function ({
  amqpUrl, operation, pollWaitTime, error503WaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl, keepLoadProcessReports
}) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const amqpOperator = await amqpFactory(amqpUrl);
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports, mongoUri});
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
  const prioItemImportingHandler = createItemImportingHandler(amqpOperator, mongoOperatorPrio, recordLoadOperator, {prio: true, error503WaitTime, recordLoadLibrary});
  const bulkItemImportingHandler = createItemImportingHandler(amqpOperator, mongoOperatorBulk, recordLoadOperator, {prio: false, error503WaitTime, recordLoadLibrary});

  logger.info(`Started Melinda-rest-api-importer with operation ${operation}`);
  startCheck({});

  async function startCheck({checkInProcessItems = true, wait = false, waitSinceLastOp = 0}) {
    if (wait) {
      await setTimeoutPromise(wait);
      const nowWaited = parseInt(wait, 10) + parseInt(waitSinceLastOp, 10);
      logWait(nowWaited);
      return startCheck({waitSinceLastOp: nowWaited});
    }

    if (checkInProcessItems) {
      return checkInProcess({waitSinceLastOp});
    }

    return checkItemImportingAndInQueue({waitSinceLastOp});
  }

  async function checkInProcess({prio = true, waitSinceLastOp}) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;
    // Items in aleph-record-load-api

    const queueItemInProcess = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.PROCESSING, true)});

    // logger.silly(`checkInProcess: itemInProcess: ${correlationId}`);
    if (queueItemInProcess) {
      // Do not spam logs
      logger.silly(`Found item in process ${queueItemInProcess.correlationId}`);
      // processOperator return false if process is still ongoing (or it errored) and true if the process is done
      const result = await processOperator.checkProcessQueueStart({correlationId: queueItemInProcess.correlationId, operation, mongoOperator, prio});
      if (result) {
        logger.debug(`Process done with ${prettyPrint(waitSinceLastOp)} of waiting`);
        return startCheck({checkInProcessItems: true});
      }
      // Hard coded wait if the items loaderProcess is still ongoing or if it errored (100 ms = 0,1 s)
      return startCheck({checkInProcessItems: true, wait: 100, waitSinceLastOp});
    }

    if (prio) {
      logger.silly(`app/checkInProcess: Nothing found in process for PRIO -> checkInProcess for BULK `);
      return checkInProcess({prio: false, waitSinceLastOp});
    }

    logger.silly(`app/checkInProcess: Nothing found for BULK -> startCheck for importing without waiting`);
    return startCheck({checkInProcessItems: false, waitSinceLastOp});
  }

  // eslint-disable-next-line max-statements
  async function checkItemImportingAndInQueue({prio = true, waitSinceLastOp}) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;
    const itemImportingHandler = prio ? prioItemImportingHandler : bulkItemImportingHandler;
    // Items in importer to be send to aleph-record-load-api
    // ImportJobStates: EMPTY, QUEUING, IN_QUEUE, PROCESSING, DONE, ERROR, ABORT
    // get here {<OPERATION>: IN_QUEUE}

    // Next checks return true if they found and handled a queueItem, false, if they didn't find one

    // queueItemState: IMPORTING, importJob: IMPORTING
    if (await checkImportJobStateIMPORTING({waitSinceLastOp})) {
      return startCheck({});
    }

    // queueItemState: IMPORTING, importJob: DONE
    if (await checkImportJobStateDONE({waitSinceLastOp})) {
      return startCheck({});
    }

    // queueItemState: IMPORTING, importJob: IN_QUEUE
    if (await checkImportJobStateINQUEUE({waitSinceLastOp})) {
      return startCheck({});
    }

    // queueItemState: IN_QUEUE
    if (await checkQueueItemStateINQUEUE({waitSinceLastOp})) {
      return startCheck({});
    }

    if (prio) {
      logger.silly(`app/checkItemImportingAndInQueue: Nothing found: PRIO -> checkItemImportingAndInQueue for bulk`);
      return checkItemImportingAndInQueue({prio: false, waitSinceLastOp});
    }

    logger.silly(`app/checkItemImportingAndInQueue: Nothing found: BULK -> startCheck, waiting ${pollWaitTime} ms = ${pollWaitTime / 1000} s`);
    return startCheck({checkInProcess: true, wait: pollWaitTime, waitSinceLastOp});

    async function checkImportJobStateIMPORTING({waitSinceLastOp}) {
      const itemImportingImporting = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.IMPORTING, true)});

      if (itemImportingImporting) {

        logger.debug(`Found item in importing ${itemImportingImporting.correlationId}, ImportJobState: {${operation}: PROCESSING} ${waitTimePrint(waitSinceLastOp)}`);
        await itemImportingHandler({item: itemImportingImporting, operation});
        return true;
      }

      return false;
    }

    async function checkImportJobStateDONE({waitSinceLastOp}) {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.DONE, true)});
      if (queueItem) {

        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: DONE} ${waitTimePrint(waitSinceLastOp)}`);
        logger.silly(inspect(queueItem));

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

    async function checkImportJobStateINQUEUE({waitSinceLastOp}) {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.IN_QUEUE, true)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: IN_QUEUE} ${waitTimePrint(waitSinceLastOp)}`);
        // set here IMPORT_JOB_STATE: {CREATE: IMPORTING, UPDATE: IMPORTING} based of operation
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.IMPORTING});
        return true;
      }
      return false;
    }

    function waitTimePrint(waitTime) {
      if (waitTime > 0) {
        return `after ${prettyPrint(waitTime)} of waiting`;
      }
      return '';
    }

    // eslint-disable-next-line max-statements
    async function checkQueueItemStateINQUEUE({waitSinceLastOp}) {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE});

      if (queueItem && queueItem.operationSettings.noop === true) {
        logger.verbose(`QueueItem ${queueItem.correlationId} has operationSettings.noop ${queueItem.operationSettings.noop} - not running importer for this job`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Noop for queueItem true, item in importer???`});
        await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
        return true;
      }

      if (queueItem && queueItem.importJobState[operation] === IMPORT_JOB_STATE.IN_QUEUE) {
        logger.debug(`Found item in q ueue to be imported${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)}`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        return true;
      }

      if (queueItem && queueItem.importJobState[operation] === IMPORT_JOB_STATE.EMPTY) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)} but importJobState for ${operation} is ${queueItem.importJobState[operation]}`);
        logger.silly(JSON.stringify(queueItem.importJobState));

        // check whether also the other queue is EMPTY or a final state
        const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
        const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
        logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

        if ([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
          logger.debug(`Other importJob (${otherOperationImportJobState}) in not ongoing/pending, importing done`);
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }

        logger.debug(`Found item in queue to be imported ${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)}`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        return true;
      }
      return false;
    }
  }

  // We should move this to some commons
  function logWait(waitTime) {
    // 3600000ms = 1h
    if (waitTime % 3600000 === 0) {
      return logger.verbose(`Total wait: ${prettyPrint(waitTime)}`);
    }
    // 60000ms = 1min
    if (waitTime % 60000 === 0) {
      return logger.debug(`Total wait: ${prettyPrint(waitTime)}`);
    }
    return logger.silly(`Total wait: ${prettyPrint(waitTime)}`);
  }
}
