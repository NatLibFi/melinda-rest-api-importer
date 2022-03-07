import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {QUEUE_ITEM_STATE, IMPORT_JOB_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import httpStatus from 'http-status';
import {promisify} from 'util';
import processOperatorFactory from './processPoll';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';

export default function ({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, keepLoadProcessReports}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const processOperator = processOperatorFactory({recordLoadApiKey, recordLoadUrl});

  return {loopCheck};

  async function loopCheck({correlationId, operation, mongoOperator, prio, wait = false}) {

    if (wait) {
      logger.debug(`Waiting ${pollWaitTime}`);
      await setTimeoutPromise(pollWaitTime);
      return loopCheck({correlationId, operation, mongoOperator, prio, wait: false});
    }

    logger.silly(`loopCheck -> checkProcessQueue for ${operation} (${correlationId})`);
    const processQueueResults = await checkProcessQueue({correlationId, operation, mongoOperator});
    // handle checkProcessQueue errors ???
    // if checkProcessQueue errored with error that's not an ApiError, processMessage is not acked/nacked
    logger.silly(`processQueueResults: ${JSON.stringify(processQueueResults)}`);

    if (!processQueueResults) {
      // false: there's a process in process queue that is not ready yet (importJobState: IN_PROCESS) or
      // false: processQueue errored (queueItemState was set to ERROR)
      return;
    }

    await handleProcessQueueResults({processQueueResults, correlationId, operation, mongoOperator});

    await setTimeoutPromise(100); // (S)Nack time!

    // results: {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount};
    // if process poll results resulted in less processed results than processes recordAmount -> ackOnlyLength is recordAmount

    const {results, processParams} = processQueueResults;
    logger.silly(`loopCheck -> handleMessages`);
    // HandleMessages returns false if there are no messages in queue to handle
    // HandleMessages returns true, if there were messages and they were handled
    const messagesHandled = await handleMessages({operation, results, processParams, queue: `${operation}.${correlationId}`, mongoOperator, prio});
    logger.silly(`messagesHandled: ${messagesHandled}`);

    if (messagesHandled) {
      logger.verbose(`Requesting file cleaning for ${JSON.stringify(processParams.data)}`);
      await processOperator.requestFileClear(processParams.data);
      return;
    }

    // There was a message in processQueue for queueItem, but no messages in operationQueue for correlation id
    throw new ApiError(httpStatus.UNPROCESSABLE_ENTITY, `No messages in ${operation}.${correlationId}`);
  }

  async function handleProcessQueueResults({processQueueResults, correlationId, operation, mongoOperator}) {
    const {results, processParams} = processQueueResults;
    logger.silly(`results: ${JSON.stringify(results)}`);
    logger.silly(`processParams: ${JSON.stringify(processParams)}`);

    // Assume that all messages are for same correlation id, no need to pushId for every message

    logger.silly(`Pushing load process results to mongo for ${operation} ${correlationId}.`);
    const {handledIds, rejectedIds, loadProcessReport} = results.payloads;

    // Make here handledRecords & rejectedRecords

    await mongoOperator.pushIds({correlationId, handledIds, rejectedIds});

    const keepLoadProcessReport = checkLoadProcessReport(keepLoadProcessReports, loadProcessReport);
    if (keepLoadProcessReport) {
      await mongoOperator.pushMessages({correlationId, messageField: 'loaderProcessReports', messages: [loadProcessReport]});
      return;
    }

    return;
  }

  function checkLoadProcessReport(keepLoadProcessReports, loadProcessReport) {
    logger.silly(`Checking if loadProcessReport should be kept.`);

    if (keepLoadProcessReports === 'ALL') {
      logger.debug(`Keeping loadProcessReport. (${keepLoadProcessReports})`);
      return true;
    }
    if (keepLoadProcessReports === 'NON_PROCESSSED' && !loadProcessReport.processedAll) {
      logger.debug(`Keeping loadProcessReport. (${keepLoadProcessReports}): processedAll: ${loadProcessReport.processedAll}`);
      return true;
    }
    if (keepLoadProcessReports === 'NON_HANDLED' && loadProcessReport.handledAmount < loadProcessReport.recordAmount) {
      logger.debug(`Keeping loadProcessReport. (${keepLoadProcessReports}): ${loadProcessReport.handledAmount}/${loadProcessReport.recordAmount}`);
      return true;
    }
    logger.debug(`Not keeping loadProcessReport. (${keepLoadProcessReports}): ${loadProcessReport.processedAll}, ${loadProcessReport.handledAmount}/${loadProcessReport.recordAmount}`);
    return false;
  }

  async function checkProcessQueue({correlationId, operation, mongoOperator}) {
    const processQueue = `PROCESS.${operation}.${correlationId}`;
    logger.silly(`Checking process queue: ${processQueue} for ${correlationId}`);
    const processMessage = await amqpOperator.checkQueue({queue: processQueue, style: 'one', toRecord: false, purge: false});

    try {
      if (processMessage) {
        logger.silly(`We have processMessage: ${processMessage}`);
        logger.silly(`checkProcessQueue -> handleProcessMessage`);

        // handleProcessMessage returns: {results, processParams} if successful - ack processMessage
        // false if process is locked - nack processMessage
        // otherwise throws error
        const result = await handleProcessMessage(processMessage, correlationId);
        return result;
      }
      // This could remove empty PROCESS.operation.correlationId queue
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Empty ${processQueue} queue`);

    } catch (error) {

      logger.debug(`checkProcessQueue for queue ${operation} ${correlationId} errored: ${error}`);
      logError(error);

      if (error instanceof ApiError) {
        // should this do something for importJobState?
        await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
        // We are erroring the whole job here
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload, errorStatus: error.status});
        await amqpOperator.ackMessages([processMessage]);
        await setTimeoutPromise(100);
        return false;
      }
      // processMessage get un-(n)acked in this case?
      logger.debug(`Error is not ApiError, not sending error responses`);
    }
  }

  async function handleProcessMessage(processMessage, correlationId) {
    logger.silly(`handleProcessMessage: ${JSON.stringify(processMessage)} for ${correlationId}`);
    try {
      const processParams = await JSON.parse(processMessage.content.toString());
      logger.silly(`handleProcessMessage:processParams: ${JSON.stringify(processParams)}`);

      // Ask aleph-record-load-api about the process
      const results = await processOperator.poll(processParams.data);
      logger.silly(`ProcessPoll results: ${JSON.stringify(results)}`);
      // should this check that results exist/are sane?

      await amqpOperator.ackMessages([processMessage]);
      await setTimeoutPromise(100);

      return {results, processParams};
    } catch (error) {

      return handleProcessMessageError(error, processMessage, amqpOperator);
    }

    async function handleProcessMessageError(error, processMessage, amqpOperator) {
      if (error instanceof ApiError) {
        if (error.status === httpStatus.LOCKED) {
          // Nack message and loop back if process was ongoing
          await amqpOperator.nackMessages([processMessage]);
          logger.silly('Process in progress @ server, back to loop!');
          return false;
        }
        if (error.status === httpStatus.SERVICE_UNAVAILABLE) {
          await amqpOperator.nackMessages([processMessage]);
          logger.debug(`Server temporarily unavailable, sleeping ${error503WaitTime} and back to loop!`);
          await setTimeoutPromise(error503WaitTime);
          return false;
        }
        logger.error('checkProcess/handleProcessMessage errored');
        logError(error);
        // should we ack processMessage for other errors than LOCKED/SERVICE_UNAVAILABEL?
        // processMessage un-(n)acked
        throw error;
      }
      // Unexpected
      // processMessage un-(n)acked
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, error.message);
    }
  }

  async function handleMessages({mongoOperator, operation, results, processParams, queue, prio}) {
    const {correlationId} = processParams.data;
    logger.debug(`handleMessages for ${operation} ${correlationId}`);
    logger.silly(`handleMessages for ${JSON.stringify(results)}, ${JSON.stringify(JSON.stringify(processParams))}, ${queue}, ${correlationId}`);
    logger.silly(`Check queue: ${JSON.stringify(queue)}`);
    // note: headers should be same?
    const {headers, messages} = await amqpOperator.checkQueue({queue, style: 'basic', toRecord: false, purge: false});
    logger.debug(`headers: ${JSON.stringify(headers)}, messages: ${messages.length}`);
    logger.silly(`messages: ${messages}`);

    if (messages) {
      // Could this set status to REJECTED if record-load-api rejected the record?
      // This would need the message to have a record identifier

      logger.verbose('Handling operation.correlationId messages based on results got from process polling');
      // Handle separation of all ready done records
      const ackMessages = await separateMessages(messages, results.ackOnlyLength);

      if (ackMessages === undefined || ackMessages.length < 1) {
        // If there are no messages to ack, continue the loop
        logger.verbose(`There was no messages to ack!!!`);
        return false;
      }

      if (prio) {
        logger.debug(`${queue} is PRIO ***`);
        return handleMessagesPrio({operation, headers, messages: ackMessages, results, correlationId, mongoOperator, amqpOperator});
      }

      logger.debug(`${queue} is BULK ***`);
      return handleMessagesBulk({operation, headers, messages: ackMessages, queue, correlationId, mongoOperator, amqpOperator});
    }

    logger.verbose(`No messages in ${queue} to handle: ${messages}. Continuing the loop`);
    return false;
  }

  async function separateMessages(messages, ackOnlyLength) {
    const ack = messages.slice(0, ackOnlyLength);
    const nack = messages.slice(ackOnlyLength);
    logger.debug(`Message separation: ack: ${ack.length}, nack: ${nack.length}`);
    await amqpOperator.nackMessages(nack);
    await setTimeoutPromise(100); // (S)Nack time!
    return ack;
  }

  async function handleMessagesPrio({headers, operation, messages, results, correlationId, mongoOperator, amqpOperator}) {

    logger.debug(`Replying for ${messages.length} messages.`);
    const status = headers.operation === OPERATIONS.CREATE ? 'CREATED' : 'UPDATED';

    const prioStatus = results.payloads.handledIds.length < 1 ? httpStatus.UNPROCESSABLE_ENTITY : status;
    const prioPayloads = results.payloads.handledIds[0] || results.payloads.rejectedIds[0] || 'No loadProcess information for record';

    await amqpOperator.ackMessages(messages);

    if (prioStatus !== 'UPDATED' && prioStatus !== 'CREATED') {
      logger.debug(`prioStatus: ${prioStatus}`);
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: prioPayloads, errorStatus: prioStatus});
      removeImporterQueues({amqpOperator, operation: headers.operation, correlationId});
      return true;
    }

    // prio has always just one record
    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
    await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.DONE});
    removeImporterQueues({amqpOperator, operation: headers.operation, correlationId});
    return true;
  }

  async function handleMessagesBulk({operation, messages, queue, correlationId, mongoOperator, amqpOperator}) {
    logger.debug(`Acking for ${messages.length} messages.`);
    await amqpOperator.ackMessages(messages);

    // If Bulk queue has more records/messages waiting in the queue resume to them.
    //logger.silly(`Checking remaining items in ${queue}`);
    const queueMessagesCount = await amqpOperator.checkQueue({queue, style: 'messages'});
    //logger.silly(`Remaining items in ${queue}: ${queueItemsCount}`);

    if (queueMessagesCount > 0) {
      logger.debug(`All messages in ${queue} NOT handled.`);
      // Note: this assumes that all messages in the queue are related to the same correlationId
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.IMPORTING});
      return true;
    }

    logger.debug(`All messages in ${queue} handled`);

    // Note: cases, where aleph-record-load-api has rejected all or some records get state DONE here
    // Note: this assumes that all messages in the queue are related to the same correlationId

    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
    removeImporterQueues({amqpOperator, operation, correlationId});
    return true;
  }

  function removeImporterQueues({amqpOperator, operation, correlationId}) {
    const operationQueue = `${operation}.${correlationId}`;
    const processQueue = `PROCESS.${operation}.${correlationId}`;
    amqpOperator.removeQueue(operationQueue);
    amqpOperator.removeQueue(processQueue);
    return;
  }
}
