import {createLogger, logWait} from '@natlibfi/melinda-backend-commons';
import recordLoadFactory from './interfaces/loadStarter';
import recordFixFactory from './interfaces/fixLoadStarter';
import {amqpFactory, mongoFactory, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS, createImportJobState} from '@natlibfi/melinda-rest-api-commons';
import {inspect, promisify} from 'util';
import {createItemImportingHandler} from './handleItemImporting';
import checkProcess from './interfaces/checkProcess';
import prettyPrint from 'pretty-print-ms';

export default async function ({
  amqpUrl, operation, pollWaitTime, error503WaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl, recordLoadFixPath, recordLoadLoadPath, fixPrio, fixBulk,
  keepLoadProcessReports
}) {

  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  // second parameter for running amqpHealthCheck
  const amqpOperator = await amqpFactory(amqpUrl, true);
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');

  const mongoOperators = {
    prio: mongoOperatorPrio,
    bulk: mongoOperatorBulk
  };

  logger.silly(`URL: ${recordLoadUrl}, paths: fix ${recordLoadFixPath}, load: ${recordLoadLoadPath}`);
  const recordLoadUrlWithPath = operation === OPERATIONS.FIX ? `${recordLoadUrl}${recordLoadFixPath}` : `${recordLoadUrl}${recordLoadLoadPath}`;
  logger.debug(`Using URL ${recordLoadUrlWithPath} for operation ${operation}`);

  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl: recordLoadUrlWithPath, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports, mongoUri});
  const recordLoadOperator = recordLoadFactory({recordLoadApiKey, recordLoadLibrary, recordLoadUrl: recordLoadUrlWithPath, fixPrio, fixBulk});
  const recordFixLoadOperator = recordFixFactory({recordLoadApiKey, recordLoadLibrary, recordLoadUrl: recordLoadUrlWithPath});

  const recordLoadOperators = {
    load: recordLoadOperator,
    fix: recordFixLoadOperator
  };

  const itemImportingHandler = createItemImportingHandler(amqpOperator, mongoOperators, recordLoadOperators, {prio: undefined, error503WaitTime, recordLoadLibrary});

  logger.info(`Started Melinda-rest-api-importer with operation ${operation}`);

  const server = await startCheck({});
  return server;

  async function startCheck({checkInProcessItems = true, wait = false, waitSinceLastOp = 0}) {
    if (wait) {
      await setTimeoutPromise(wait);
      const nowWaited = parseInt(wait, 10) + parseInt(waitSinceLastOp, 10);
      logWait(logger, nowWaited);
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
    //const itemImportingHandler = getItemImportingHandler(operation, prio);

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
        logger.debug(`Found item in importing ${itemImportingImporting.correlationId}, ImportJobState: {${operation}: IMPORTING} ${waitTimePrint(waitSinceLastOp)}`);
        await itemImportingHandler({item: itemImportingImporting, operation, prio});
        return true;
      }
      return false;
    }

    async function checkImportJobStateDONE({waitSinceLastOp}) {
      // We get queueItem, whose import job state for this importer is DONE
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.DONE, true)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: DONE} ${waitTimePrint(waitSinceLastOp)}`);
        logger.silly(inspect(queueItem));

        // FIXes are DONE when ImportJobState.FIX is DONE
        if ([OPERATIONS.FIX].includes(operation)) {
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }

        // Only CREATEs and UPDATEs can exist in the same queueItem, they are not DONE if the other job is not also done
        if ([OPERATIONS.CREATE, OPERATIONS.UPDATE].includes(operation)) {
          const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
          const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
          logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

          if ([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
            logger.debug(`Other importJob in not ongoing/pending, importing done`);
            await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
            return true;
          }
          return false;
        }
        logger.debug(`WARNING! unknown operation ${operation}`);
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
      logger.silly(`checkQueueItemStateINQUEUE:  ${JSON.stringify(queueItem)}`);

      if (queueItem && queueItem.operationSettings.noop === true) {
        logger.verbose(`QueueItem ${queueItem.correlationId} has operationSettings.noop ${queueItem.operationSettings.noop} - not running importer for this job`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Noop for queueItem true, item in importer???`});
        await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
        return true;
      }

      if (queueItem && queueItem.importJobState[operation] === IMPORT_JOB_STATE.IN_QUEUE) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)}`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        return true;
      }

      if (queueItem && queueItem.importJobState[operation] === IMPORT_JOB_STATE.EMPTY) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)} but importJobState for ${operation} is ${queueItem.importJobState[operation]}`);
        logger.silly(JSON.stringify(queueItem.importJobState));
        logger.debug(`QueueItem has operation: ${queueItem.operation}`);

        if ([OPERATIONS.FIX].includes(operation) && [OPERATIONS.FIX].includes(queueItem.operation)) {
          logger.debug(`Fix process has just FIX importer, we shouldn't have empty import job state!`);
          // Should we error this?
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }

        // Only CREATEs and UPDATEs can exist in the same queueItem
        if ([OPERATIONS.CREATE, OPERATIONS.UPDATE].includes(operation) && [OPERATIONS.CREATE, OPERATIONS.UPDATE].includes(queueItem.operation)) {
          // check whether also the other queues are EMPTY or a final state
          const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
          const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
          logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

          if ([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT].includes(otherOperationImportJobStateResult)) {
            logger.debug(`Other importJob (${otherOperationImportJobState}) in not ongoing/pending, importing done`);
            await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
            return true;
          }
          // WHY THIS? Shouldn't other LOAD-IMPORTER CATCH this?
          //logger.debug(`Found item in queue to be imported ${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)}`);
          //await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
          return false;
        }
        return false;
      }
      if (queueItem) {
        logger.debug(`Found item in queue to be imported ${queueItem.correlationId} ${waitTimePrint(waitSinceLastOp)}`);
        await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
        if (operation === OPERATIONS.FIX && queueItem.importJobState.OPERATIONS.FIX === undefined) {
          await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.IMPORTING});
          return true;
        }
        return true;
      }
      return false;
    }
  }
}
