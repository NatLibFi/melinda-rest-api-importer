import {createLogger, logWait, millisecondsToString, Error as ApiError} from '@natlibfi/melinda-commons';
import {amqpFactory, mongoFactory, QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS, createImportJobState} from '@natlibfi/melinda-rest-api-commons';
import type {ImportJobStateValue} from '@natlibfi/melinda-rest-api-commons';
import {inspect, promisify} from 'util';
import {createItemImportingHandler} from './handleItemImporting.ts';
import checkProcess from './interfaces/checkProcess.ts';
import recordFixFactory from './interfaces/fixLoadStarter.ts';
import recordLoadFactory from './interfaces/loadStarter.ts';

export type Operation = (typeof OPERATIONS)[keyof typeof OPERATIONS];

export interface StartAppOptions {
  amqpUrl: string;
  operation: Operation;
  pollWaitTime: number;
  error503WaitTime: number;
  mongoUri: string;
  recordLoadApiKey: string;
  recordLoadLibrary: string;
  recordLoadUrl: string;
  recordLoadFixPath: string;
  recordLoadLoadPath: string;
  fixPrio: string;
  fixBulk: string;
  keepLoadProcessReports: string;
}

// Option objects of the recursive startCheck state machine
export interface StartCheckOptions {
  checkInProcessItems?: boolean;
  // the pre-converted code defaults `wait` to `false` (any falsy value skips the wait block)
  wait?: number | false;
  waitSinceLastOp?: number;
}

export interface CheckOptions {
  prio?: boolean;
  waitSinceLastOp: number;
}

// The handler's record-load contract. The factories' inferred `loadRecord`
// param types are not directly comparable to it (loadStarter's `records` is
// required, the handler's is optional), so the pre-existing ternary result is
// boundary-cast to it once at the createItemImportingHandler call site.
type RecordLoadOperator = Parameters<typeof createItemImportingHandler>[2];

// Containment of infrastructure errors (Mongo/AMQP driver rejections) in the
// startCheck loop: retry with capped exponential backoff, and re-throw after
// too many consecutive failed cycles (the unhandledRejection handler in
// index.js then exits the process so a supervisor can restart it).
// Tuned values, revisit together with the stuck-423 safety-net work.
const INFRA_ERROR_RETRY_BASE_WAIT_MS = 100;
const INFRA_ERROR_RETRY_MAX_WAIT_MS = 30000;
const INFRA_ERROR_RETRY_MAX_CONSECUTIVE_FAILURES = 30;

// eslint-disable-next-line max-lines-per-function
export default async function ({
  amqpUrl, operation, pollWaitTime, error503WaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl, recordLoadFixPath, recordLoadLoadPath, fixPrio, fixBulk,
  keepLoadProcessReports
}: StartAppOptions): Promise<void> {

  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  let consecutiveInfraFailures = 0;

  const recordLoadUrlWithPath = getUrlWithPath({operation, recordLoadUrl, recordLoadFixPath, recordLoadLoadPath});

  // second parameter for running amqpHealthCheck
  const amqpOperator = await amqpFactory(amqpUrl, true);

  const mongoOperators = {
    prio: await mongoFactory(mongoUri, 'prio'),
    bulk: await mongoFactory(mongoUri, 'bulk')
  };

  // Boundary cast: the pre-converted call also passes `pollWaitTime` and
  // `operation`, which checkProcess never reads (absent from its declared
  // params); the existing object is asserted to the declared contract.
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl: recordLoadUrlWithPath, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports, mongoUri} as Parameters<typeof checkProcess>[0]);

  // string[] widening (boundary cast, no logic change): the literal constant
  // arrays infer narrow literal element types, while `operation` is the
  // broader `Operation` union
  const recordLoadOperator =
    ([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation)
      ? recordLoadFactory({recordLoadApiKey, recordLoadLibrary, recordLoadUrl: recordLoadUrlWithPath, fixPrio, fixBulk})
      : recordFixFactory({recordLoadApiKey, recordLoadLibrary, recordLoadUrl: recordLoadUrlWithPath});

  const itemImportingHandler = createItemImportingHandler(amqpOperator, mongoOperators, recordLoadOperator as unknown as RecordLoadOperator, {error503WaitTime, recordLoadLibrary});

  logger.info(`Started Melinda-rest-api-importer with operation ${operation}`);

  const server = await startCheck({});
  return server;

  function getUrlWithPath({operation, recordLoadUrl, recordLoadLoadPath, recordLoadFixPath}: {
    operation: Operation;
    recordLoadUrl: string;
    recordLoadLoadPath: string;
    recordLoadFixPath: string;
  }): string {
    logger.silly(`URL: ${recordLoadUrl}, paths: fix ${recordLoadFixPath}, load: ${recordLoadLoadPath}`);
    const recordLoadUrlWithPath = operation === OPERATIONS.FIX ? `${recordLoadUrl}${recordLoadFixPath}` : `${recordLoadUrl}${recordLoadLoadPath}`;
    logger.debug(`Using URL ${recordLoadUrlWithPath} for operation ${operation}`);
    return recordLoadUrlWithPath;
  }

  async function startCheck({checkInProcessItems = true, wait = false, waitSinceLastOp = 0}: StartCheckOptions): Promise<void> {
    if (wait) {
      await setTimeoutPromise(wait);
      // Boundary cast: the pre-converted code passes numbers to parseInt (JS
      // coerces them to strings); no logic change.
      const nowWaited = parseInt(wait as unknown as string, 10) + parseInt(waitSinceLastOp as unknown as string, 10);
      logWait(logger, nowWaited);
      return startCheck({waitSinceLastOp: nowWaited});
    }

    try {
      const result = checkInProcessItems
        ? await checkInProcess({waitSinceLastOp})
        : await checkItemImportingAndInQueue({waitSinceLastOp});
      consecutiveInfraFailures = 0;
      return result;
    } catch (error) {
      // ApiErrors keep the existing handling path: propagate unchanged
      if (error instanceof ApiError) {
        throw error;
      }

      // Boundary cast: the catch-passed variable is unknown; the
      // non-ApiError (infrastructure) path only reads `cause` (Error.cause,
      // unknown). No logic change.
      const infraError = error as {cause?: unknown};

      // Infrastructure (e.g. Mongo/AMQP driver) error: contain, log,
      // and re-schedule the next cycle with capped exponential backoff
      consecutiveInfraFailures += 1;
      if (consecutiveInfraFailures >= INFRA_ERROR_RETRY_MAX_CONSECUTIVE_FAILURES) {
        logger.error(`app/startCheck: ${consecutiveInfraFailures} consecutive cycles failed, giving up:`, error);
        throw error;
      }
      const waitMs = Math.min(
        INFRA_ERROR_RETRY_BASE_WAIT_MS * 2 ** (consecutiveInfraFailures - 1),
        INFRA_ERROR_RETRY_MAX_WAIT_MS
      );
      logger.error(
        `app/startCheck: cycle failed with a non-ApiError (consecutive infrastructure failures: ${consecutiveInfraFailures}/${INFRA_ERROR_RETRY_MAX_CONSECUTIVE_FAILURES}), retrying in ${waitMs} ms:`,
        error
      );
      if (infraError.cause) {
        logger.error(`app/startCheck: cause of the cycle failure:`, infraError.cause);
      }
      return startCheck({checkInProcessItems: true, wait: waitMs, waitSinceLastOp});
    }
  }

  async function checkInProcess({prio = true, waitSinceLastOp}: CheckOptions): Promise<void> {
    const mongoOperator = prio ? mongoOperators.prio : mongoOperators.bulk;
    // Items in aleph-record-load-api

    const queueItemInProcess = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.PROCESSING, true)});

    // logger.silly(`checkInProcess: itemInProcess: ${correlationId}`);
    if (queueItemInProcess) {
      // Do not spam logs
      logger.silly(`Found item in process ${queueItemInProcess.correlationId}`);

      // processOperator return false if process is still ongoing (or it errored) and true if the process is done
      const result = await processOperator.checkProcessQueueStart({correlationId: queueItemInProcess.correlationId, operation, mongoOperator, prio});

      if (result) {
        logger.debug(`Process done with ${millisecondsToString(waitSinceLastOp)} of waiting`);
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

  // eslint-disable-next-line max-lines-per-function
  async function checkItemImportingAndInQueue({prio = true, waitSinceLastOp}: CheckOptions): Promise<void> {
    const mongoOperator = prio ? mongoOperators.prio : mongoOperators.bulk;

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
    // Boundary cast: the pre-converted code passes a dead `checkInProcess`
    // key (a typo of `checkInProcessItems`, whose default `true` it
    // duplicates); the key is kept verbatim, so the literal is asserted to
    // StartCheckOptions.
    return startCheck({checkInProcess: true, wait: pollWaitTime, waitSinceLastOp} as StartCheckOptions);

    async function checkImportJobStateIMPORTING({waitSinceLastOp}: CheckOptions): Promise<boolean> {
      const itemImportingImporting = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.IMPORTING, true)});

      if (itemImportingImporting) {
        logger.debug(`Found item in importing ${itemImportingImporting.correlationId}, ImportJobState: {${operation}: IMPORTING} ${waitTimePrint(waitSinceLastOp)}`);
        await itemImportingHandler({item: itemImportingImporting, operation, prio});
        return true;
      }
      return false;
    }

    async function checkImportJobStateDONE({waitSinceLastOp}: CheckOptions): Promise<boolean> {
      // We get queueItem, whose import job state for this importer is DONE
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING, importJobState: createImportJobState(operation, IMPORT_JOB_STATE.DONE, true)});
      if (queueItem) {
        logger.debug(`Found item in importing ${queueItem.correlationId}, ImportJobState: {${operation}: DONE} ${waitTimePrint(waitSinceLastOp)}`);
        logger.silly(inspect(queueItem));

        // FIXes are DONE when ImportJobState.FIX is DONE
        if (([OPERATIONS.FIX] as string[]).includes(operation)) {
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }

        // Only CREATEs and UPDATEs can exist in the same queueItem, they are not DONE if the other job is not also done
        if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation)) {
          const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
          const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
          logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

          if (([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT] as string[]).includes(otherOperationImportJobStateResult)) {
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

    async function checkImportJobStateINQUEUE({waitSinceLastOp}: CheckOptions): Promise<boolean> {
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

    function waitTimePrint(waitTime: number): string {
      if (waitTime > 0) {
        return `after ${millisecondsToString(waitTime)} of waiting`;
      }
      return '';
    }

    async function checkQueueItemStateINQUEUE({waitSinceLastOp}: CheckOptions): Promise<boolean> {
      const queueItem = await mongoOperator.getOne({queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE});
      logger.silly(`checkQueueItemStateINQUEUE:  ${JSON.stringify(queueItem)}`);

      // Boundary cast (H7): the published 6.2 `QueueItem.operationSettings`
      // type has no `noop` member (only prio/noStream), but the validator
      // writes it and this branch depends on it. Single cast, no logic change.
      const operationSettingsNoop = queueItem ? (queueItem.operationSettings as {noop?: boolean}).noop : undefined;

      if (queueItem && operationSettingsNoop === true) {
        logger.verbose(`QueueItem ${queueItem.correlationId} has operationSettings.noop ${operationSettingsNoop} - not running importer for this job`);
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

        if (([OPERATIONS.FIX] as string[]).includes(operation) && ([OPERATIONS.FIX] as string[]).includes(queueItem.operation as string)) {
          logger.debug(`Fix process has just FIX importer, we shouldn't have empty import job state!`);
          // Should we error this?
          await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE});
          return true;
        }

        // Only CREATEs and UPDATEs can exist in the same queueItem
        if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation) && ([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(queueItem.operation as string)) {
          // check whether also the other queues are EMPTY or a final state
          const otherOperationImportJobState = operation === OPERATIONS.CREATE ? OPERATIONS.UPDATE : OPERATIONS.CREATE;
          const otherOperationImportJobStateResult = queueItem.importJobState[otherOperationImportJobState];
          logger.debug(`Checking importerJobState for other operation: ${otherOperationImportJobState}: ${otherOperationImportJobStateResult}`);

          if (([IMPORT_JOB_STATE.EMPTY, IMPORT_JOB_STATE.DONE, IMPORT_JOB_STATE.ERROR, IMPORT_JOB_STATE.ABORT] as string[]).includes(otherOperationImportJobStateResult)) {
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

        // importJobState is a flat {operation: state} map; initialize a missing
        // FIX import-job state for a FIX item reaching this branch
        // Boundary cast: the published type requires `importJobState.FIX`,
        // but a FIX document may lack it - that missing state is exactly what
        // this check initializes.
        if (operation === OPERATIONS.FIX && (queueItem.importJobState as {FIX?: ImportJobStateValue})[OPERATIONS.FIX] === undefined) {
          await mongoOperator.setImportJobState({correlationId: queueItem.correlationId, operation, importJobState: IMPORT_JOB_STATE.IMPORTING});
          return true;
        }
        return true;
      }
      return false;
    }
  }
}
