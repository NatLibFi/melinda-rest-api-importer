import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError, toAlephId} from '@natlibfi/melinda-commons';
import {IMPORT_JOB_STATE, OPERATIONS, QUEUE_ITEM_STATE, createRecordResponseItem, addRecordResponseItems, mongoLogFactory} from '@natlibfi/melinda-rest-api-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';
import httpStatus from 'http-status';
import {promisify, inspect} from 'util';
import processOperatorFactory from './processPoll';

export default async function ({amqpOperator, recordLoadApiKey, recordLoadUrl, error503WaitTime, keepLoadProcessReports, mongoUri}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const processOperator = processOperatorFactory({recordLoadApiKey, recordLoadUrl});
  const mongoLogOperator = await mongoLogFactory(mongoUri);

  return {checkProcessQueueStart};

  async function checkProcessQueueStart({correlationId, operation, mongoOperator, prio}) {

    logger.silly(`loopCheck -> checkProcessQueue for ${operation} (${correlationId})`);
    const processQueueResults = await checkProcessQueue({correlationId, operation, mongoOperator, prio});
    // handle checkProcessQueue errors ???
    // if checkProcessQueue errored with error that's not an ApiError, processMessage is not acked/nacked
    logger.silly(`processQueueResults: ${JSON.stringify(processQueueResults)}`);

    if (!processQueueResults) {
      // false: there's a process in process queue that is not ready yet (importJobState: IN_PROCESS) or
      // false: processQueue errored (queueItemState was set to ERROR)
      return false;
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
      return true;
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
    const {handledIds, rejectedIds, loadProcessReport, erroredAmount} = results.payloads;

    await mongoOperator.pushIds({correlationId, handledIds, rejectedIds, erroredAmount});

    const loadProcessLogItem = {
      logItemType: 'LOAD_PROCESS_LOG',
      correlationId,
      ...loadProcessReport
    };

    logger.silly(`${inspect(loadProcessLogItem, {depth: 6})}`);
    const result = mongoLogOperator.addLogItem(loadProcessLogItem);
    logger.debug(result);

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

  // eslint-disable-next-line max-statements
  async function checkProcessQueue({correlationId, operation, mongoOperator, prio}) {
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
        const result = await handleProcessMessage(processMessage, correlationId, prio);
        return result;
      }
      // This could remove empty PROCESS.operation.correlationId queue
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Empty ${processQueue} queue`);

    } catch (error) {

      logger.debug(`checkProcessQueue for queue ${operation} ${correlationId} errored: ${error}`);
      logError(error);

      if (error instanceof ApiError) {

        // We error the whole queueItem here
        await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload, errorStatus: error.status});

        // If we errored because we didn't have a process message, let's not try to ack non-existing message
        if (processMessage) {
          await amqpOperator.ackMessages([processMessage]);
          await setTimeoutPromise(100);
          return false;
        }

        return false;
      }
      // processMessage get un-(n)acked in this case?
      logger.debug(`Error is not ApiError, not sending error responses`);
    }
  }

  async function handleProcessMessage(processMessage, correlationId, prio) {
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

      return handleProcessMessageError(error, processMessage, amqpOperator, prio);
    }

    async function handleProcessMessageError(error, processMessage, amqpOperator, prio) {
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

        if (error.status === httpStatus.BAD_REQUEST) {
          if (prio) {
            throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
          }
          throw new ApiError(error.status, 'Bad request from record-load-api');
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
    // note: headers are headers for the first message in chunk
    const {headers: firstMessageHeaders, messages} = await amqpOperator.checkQueue({queue, style: 'basic', toRecord: false, purge: false});
    logger.debug(`firstMessageHeaders: ${JSON.stringify(firstMessageHeaders)}, messages: ${messages.length}`);
    logger.silly(`messages: ${messages}`);

    if (messages) {
      logger.verbose(`Handling ${operation}.${correlationId} messages based on results got from process polling`);
      // Handle separation of all ready done records
      const ackMessages = await separateMessages(messages, results.ackOnlyLength);

      if (ackMessages === undefined || ackMessages.length < 1) {
        // If there are no messages to ack, continue the loop
        logger.verbose(`There was no messages to ack!!!`);
        return false;
      }

      return handleMessagesBoth({operation, messages: ackMessages, results, queue, correlationId, mongoOperator, amqpOperator, prio});
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

  async function handleMessagesBoth({operation, messages, results, queue, correlationId, mongoOperator, amqpOperator, prio}) {
    logger.debug(`Acking for ${messages.length} messages.`);
    logger.silly(JSON.stringify(results));

    //{"payloads":{"handledIds":["000999999"],"rejectedIds":[],"loadProcessReport":{"status":200,"processId":31930,"processedAll":false,"recordAmount":2,"processedAmount":1,"handledAmount":1,"rejectedAmount":0,"rejectMessages":[]}},"ackOnlyLength":2, "handledAll": false}
    const {handledIds, rejectedIds, erroredAmount} = results.payloads;
    const {handledAll} = results.payloads.loadProcessReport;

    await createRecordResponses({messages, operation, handledAll, mongoOperator, correlationId, handledIds, rejectedIds, erroredAmount});
    // ack messages
    await amqpOperator.ackMessages(messages);

    // handle queueItem and amqp queues
    if (prio) {
      const status = operation === OPERATIONS.CREATE ? 'CREATED' : 'UPDATED';
      const prioStatus = getPrioStatus(status, handledIds, erroredAmount);
      const prioPayloads = handledIds[0] || rejectedIds[0] || 'No loadProcess information for record';

      return prioEnd({prioStatus, prioPayloads, correlationId, operation, mongoOperator, amqpOperator});
    }
    return bulkEnd({correlationId, operation, queue, mongoOperator, amqpOperator});
  }

  function getPrioStatus(status, handledIds, erroredAmount) {
    // OraErrors: return 503 so that the client can try again
    if (erroredAmount > 0) {
      return httpStatus.SERVICE_UNAVAILABLE;
    }
    // We did not get an id, probably rejected
    if (handledIds.length < 1) {
      return httpStatus.UNPROCESSABLE_ENTITY;
    }
    return status;
  }


  // eslint-disable-next-line max-statements
  async function createRecordResponses({messages, operation, handledAll, mongoOperator, correlationId, handledIds, rejectedIds, erroredAmount}) {
    logger.debug(`${messages.length}, ${operation}, ${handledAll}, ${handledIds.length}, ${rejectedIds.length}, ${erroredAmount}`);

    if (operation === OPERATIONS.CREATE) {
      if (handledAll) {
        await createRecordResponsesForCreateOperationHandledAll({mongoOperator, correlationId, messages, handledIds});
        return;
      }
      await createRecordResponsesForCreateOperationNotHandledAll({mongoOperator, correlationId, messages, handledIds, rejectedIds, erroredAmount});
      return;
    }

    if (operation === OPERATIONS.UPDATE) {
      await createRecordResponsesForUpdateOperation({mongoOperator, correlationId, messages, handledAll, handledIds, rejectedIds});
      return;
    }

    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Unknown OPERATION: ${operation} in ${correlationId}`);

    async function createRecordResponsesForCreateOperationHandledAll({mongoOperator, correlationId, messages, handledIds}) {
    // CREATEs which have handledId for all records and no oraErrors
      logger.debug(`We have a CREATE operation which handled all records normally.`);
      const status = 'CREATED';
      const recordResponseItems = await messages.map((message, index) => {
        logger.silly(JSON.stringify(message));
        const {id, recordMetadata, notes} = message.properties.headers;
        const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';

        const idFromHandledIds = handledIds[index];

        logger.debug(`headers.id: ${id} got handledId for ${index}: ${idFromHandledIds}`);
        const responsePayload = {message: `${notesString}Created record ${idFromHandledIds}.`};

        return createRecordResponseItem({responseStatus: status, responsePayload, recordMetadata, id: idFromHandledIds});
      });

      addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
      return;
    }

    async function createRecordResponsesForCreateOperationNotHandledAll({mongoOperator, correlationId, messages, handledIds, rejectedIds, erroredAmount}) {

      // CREATEs which have a (possible) handled id for all records (and !handledAll because of oraErrors)
      if (handledIds.length === messages.length) {
        logger.debug(`We have a CREATE operation which did not handle all records the records normally, but has a possible id for all records.`);
        const status = 'CREATED';
        const recordResponseItems = await messages.map((message, index) => {
          logger.silly(JSON.stringify(message));
          const {id, recordMetadata, notes} = message.properties.headers;
          const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';

          const idFromHandledIds = handledIds[index];
          logger.debug(`headers.id: ${id} got handledId for ${index}: ${idFromHandledIds}`);

          const errorRegex = /^ERROR-/u;
          // eslint-disable-next-line no-extra-parens
          if ((idFromHandledIds && errorRegex.test(idFromHandledIds)) || erroredAmount === handledIds.length) {
            const responsePayload = {message: `${notesString}Errored creating record ${idFromHandledIds}.`};
            return createRecordResponseItem({responseStatus: 'ERROR', responsePayload, recordMetadata, id: '000000000'});
          }

          const responsePayload = {message: `${notesString}Created record ${idFromHandledIds}.`};
          return createRecordResponseItem({responseStatus: status, responsePayload, recordMetadata, id: idFromHandledIds});
        });

        addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
        return;
      }

      logger.debug(`We have a CREATE operation which did not handle all records normally - other cases.`);
      // remove errorIds from handledIds
      const errorRegex = /^ERROR-/u;
      const possibleIds = handledIds.filter((id) => id && !errorRegex.test(id));

      // We could add rejetedRecords to the handledIds based on the blobSequence (CREATEs always have blobSequence as sysnro)
      // for each rejectId add an REJECT-<id> to handledIds[id-1]
      // and the we could solve ids for cases where handledIds.length + rejectedIds.length === messages.length

      const recordResponseItems = await messages.map((message) => {
        const {recordMetadata, notes} = message.properties.headers;
        const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';
        const {blobSequence} = recordMetadata;
        const alephSeqId = toAlephId(blobSequence.toString());

        if (rejectedIds.includes(alephSeqId)) {
          const rejectedStatus = 'INVALID';
          const responsePayload = {message: `${notesString}LoaderProcess rejected record ${alephSeqId}`};
          return createRecordResponseItem({responseStatus: rejectedStatus, recordMetadata, id: '000000000', responsePayload});
        }

        const status = 'UNKNOWN';
        const ids = possibleIds;
        const responsePayload = {
          message: `LoaderProcess did not return databaseIds for all records in chunk.`,
          ids
        };

        return createRecordResponseItem({responseStatus: status, recordMetadata, id: '000000000', responsePayload});
      });
      addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
      return;
    }

    async function createRecordResponsesForUpdateOperation({mongoOperator, correlationId, messages, handledAll, handledIds, rejectedIds}) {

      // eslint-disable-next-line max-statements
      const recordResponseItems = await messages.map((message, index) => {
        logger.silly(`${index}: ${JSON.stringify(message)}`);
        const {id, recordMetadata, notes} = message.properties.headers;
        const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';
        const paddedId = toAlephId(id);

        // Note: if a record is in chunk to be updated several times, all of them get status UPDATED
        if (handledIds.includes(paddedId)) {
          const responseStatus = 'UPDATED';
          const responsePayload = {message: `${notesString}Updated record ${paddedId}`};
          return createRecordResponseItem({responseStatus, recordMetadata, id, responsePayload});
        }

        if (rejectedIds.includes(paddedId)) {
          const responseStatus = 'INVALID';
          const responsePayload = {message: `${notesString}LoaderProcess rejected record ${paddedId}`};
          return createRecordResponseItem({responseStatus, recordMetadata, id, responsePayload});
        }

        if (handledAll) {
          const status = 'UNKNOWN';
          const idFromHandledIds = handledIds[index];
          const ids = [idFromHandledIds];
          const responsePayload = {
            message: `${notesString}LoaderProcess did not return result for record ${paddedId}. It might have updated ${idFromHandledIds} instead.`,
            ids
          };
          return createRecordResponseItem({responseStatus: status, recordMetadata, id, responsePayload});
        }

        const status = 'UNKNOWN';
        const responsePayload = {message: `${notesString}LoaderProcess did not return result for record ${paddedId}`};
        return createRecordResponseItem({responseStatus: status, recordMetadata, id, responsePayload});
      });

      addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
      return;
    }
  }
  async function prioEnd({prioStatus, prioPayloads, correlationId, operation, mongoOperator, amqpOperator}) {

    // failed prios
    if (prioStatus !== 'UPDATED' && prioStatus !== 'CREATED') {
      logger.debug(`prioStatus: ${prioStatus}`);
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: prioPayloads, errorStatus: prioStatus});

      removeImporterQueues({amqpOperator, operation, correlationId});
      return true;
    }

    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
    await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.DONE});
    removeImporterQueues({amqpOperator, operation, correlationId});
    return true;
  }

  async function bulkEnd({correlationId, operation, queue, mongoOperator, amqpOperator}) {
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
