/* eslint-disable max-lines */
/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import httpStatus from 'http-status';
import {promisify} from 'util';
import processOperatorFactory from './processPoll';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';

export default function ({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const processOperator = processOperatorFactory({recordLoadApiKey, recordLoadUrl});

  return {loopCheck};

  async function loopCheck({correlationId, mongoOperator, prio, wait = false}) {
    // eslint-disable-next-line functional/no-conditional-statement
    if (wait) {
      logger.debug(`Waiting ${pollWaitTime}`);
      await setTimeoutPromise(pollWaitTime);
    }

    logger.silly(`loopCheck -> checkProcessQueue (${correlationId})`);
    const processQueueResults = await checkProcessQueue(correlationId, mongoOperator, prio);
    // handle checkProcessQueue errors ???
    // if checkProcessQueue errored with error that's not an ApiError, processMessage is not acked/nacked
    logger.debug(`processQueueResults: ${JSON.stringify(processQueueResults)}`);

    if (!processQueueResults) {
      // false: there's a process in process queue that is not ready yet (queueItemState: IMPORTING.IN_PROCESS) or
      // false: processQueue errored (queueItemState was set to ERROR)
      return;
    }

    const {results, processParams} = processQueueResults;
    logger.debug(`results: ${JSON.stringify(results)}`);
    logger.debug(`processParams: ${JSON.stringify(processParams)}`);
    // results: {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount};
    // if process poll results resulted in less processed results than processes recordAmount -> ackOnlyLength is recordAmount

    // Assume that all messages are for same correlation id, no need to pushId for every message

    logger.debug(`Setting status in mongo for ${correlationId}.`);
    const {handledIds, rejectedIds, loadProcessReport} = results.payloads;
    mongoOperator.pushIds({correlationId, handledIds, rejectedIds});

    logger.debug(`Check if loadProcessReport should be kept.`);
    const keepLoadProcessReport = checkLoadProcessReport(keepLoadProcessReports, loadProcessReport);

    // eslint-disable-next-line functional/no-conditional-statement
    if (keepLoadProcessReport) {
      logger.debug(`Keeping loadProcessReport.`);
      mongoOperator.pushMessages({correlationId, messageField: 'loaderProcessReports', messages: [loadProcessReport]});
    }

    await setTimeoutPromise(100); // (S)Nack time!


    logger.silly(`loopCheck -> handleMessages`);
    // HandleMessages returns false if there are no messages in queue to handle
    // HandleMessages returns true, if there were messages and they were handled
    const messagesHandled = await handleMessages({results, processParams, queue: `${operation}.${correlationId}`, mongoOperator, prio});
    logger.debug(`messagesHandled: ${messagesHandled}`);

    if (messagesHandled) {
      logger.verbose(`Requesting file cleaning for ${JSON.stringify(processParams.data)}`);
      await processOperator.requestFileClear(processParams.data);
      return;
    }

    // There was a message in processQueue for queueItem, but no messages in operationQueue for correlation id
    throw new ApiError(httpStatus.UNPROCESSABLE_ENTITY, `No messages in ${operation}.${correlationId}`);
  }

  function checkLoadProcessReport(keepLoadProcessReports, loadProcessReport) {
    logger.debug(`Checking if loadProcessReport should be kept.`);

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

  async function checkProcessQueue(correlationId, mongoOperator, prio) {
    const processQueue = `PROCESS.${correlationId}`;
    logger.verbose(`Checking process queue: ${processQueue} for ${correlationId}`);
    const processMessage = await amqpOperator.checkQueue(processQueue, 'raw');

    try {
      if (processMessage) {
        logger.debug(`We have processMessage: ${processMessage}`);
        logger.silly(`checkProcessQueue -> handleProcessMessage`);

        // handleProcessMessage returns: {results, processParams} if successful - ack processMessage
        // false if process is locked - nack processMessage
        // otherwise throws error
        const result = await handleProcessMessage(processMessage, correlationId);
        return result;
      }
      // This could remove empty PROCESS.correlationId queu
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Empty ${processQueue} queue`);

    } catch (error) {

      logger.debug(`checkProcessQueue for queue ${correlationId} errored: ${error}`);
      logError(error);

      if (error instanceof ApiError) {
        // eslint-disable-next-line functional/no-conditional-statement
        if (prio) {
          logger.debug(`Error is from PRIO ${correlationId}, sending error responses`);
          await sendErrorResponses({error, correlationId, mongoOperator});
        }

        // eslint-disable-next-line functional/no-conditional-statement
        if (!prio) {
          // Does this set bulk state to error if any of loadProcesses result in error?
          logger.debug(`Error is from BULK ${correlationId}, not sending error responses`);
          await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload, errorStatus: error.status});
        }

        await amqpOperator.ackMessages([processMessage]);
        await setTimeoutPromise(100);
        return false;
      }
      // processMessage get un-(n)acked in this case?
      logger.debug(`Error is not ApiError, not sending error responses`);
    }
  }

  async function handleProcessMessage(processMessage, correlationId) {
    logger.debug(`handleProcessMessage: ${processMessage} for ${correlationId}`);
    try {
      const processParams = await JSON.parse(processMessage.content.toString());
      logger.debug(`handleProcessMessage:processParams: ${JSON.stringify(processParams)}`);

      // Ask aleph-record-load-api about the process
      const results = await processOperator.poll(processParams.data);
      logger.debug(`ProcessPoll results: ${JSON.stringify(results)}`);
      // should this check that results exist/are sane?

      await amqpOperator.ackMessages([processMessage]);
      await setTimeoutPromise(100);

      return {results, processParams};
    } catch (error) {
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

  async function handleMessages({mongoOperator, results, processParams, queue, prio}) {
    logger.debug(`handleMessages for ${JSON.stringify(results)}, ${JSON.stringify(JSON.stringify(processParams))}, ${queue}, ${processParams.data.correlationId}`);

    // this checks queue for correlationId -- queue is already operation.correlationId ?
    const {headers, messages} = await amqpOperator.checkQueue(queue, processParams.data.correlationId, false, false);
    logger.debug(`headers: ${headers}`);
    logger.debug(`messages: ${messages}`);

    if (messages) {
      // Could this set status to REJECTED if record-load-api rejected the record?
      // This would need the message to have a record identifier
      const status = headers.operation === OPERATIONS.CREATE ? 'CREATED' : 'UPDATED';
      logger.verbose('Handling process messages based on results got from process polling');
      // Handle separation of all ready done records
      const ack = messages.slice(0, results.ackOnlyLength);
      const nack = messages.slice(results.ackOnlyLength);
      logger.debug(`Message separation: ack: ${ack}, nack: ${nack}`);
      await amqpOperator.nackMessages(nack);
      await setTimeoutPromise(100); // (S)Nack time!

      if (ack === undefined || ack.length < 1) {
        // If there are no messages to ack, continue the loop
        logger.verbose(`There was no messages to ack!!!`);
        return false;
      }

      const distinctCorrelationIds = ack.map(message => message.properties.correlationId).filter((value, index, self) => self.indexOf(value) === index);
      logger.debug(`Found ${distinctCorrelationIds.length} distinct correlationIds from ${ack.length} messages.`);

      // IF PRIO -> DONE
      // IF BULK -> IF QUEUE EMPTY -> DONE  ELSE -> IMPORTING

      // eslint-disable-next-line functional/no-conditional-statement
      if (prio) {
        logger.debug(`${queue} is PRIO ***`);

        logger.debug(`Replying for ${ack.length} messages.`);

        const prioStatus = results.payloads.handledIds.length < 1 ? httpStatus.UNPROCESSABLE_ENTITY : status;
        const prioPayloads = results.payloads.handledIds[0] || results.payloads.rejectedIds[0] || 'No loadProcess information for record';

        await amqpOperator.ackMessages(ack);
        // This could remove the OPERATION.correlationID queue

        if (prioStatus !== 'UPDATED' && prioStatus !== 'CREATED') {
          logger.debug(`prioStatus: ${prioStatus}`);
          await distinctCorrelationIds.forEach(async correlationId => {
            await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: prioPayloads, errorStatus: prioStatus});
          });
          return true;
        }

        await distinctCorrelationIds.forEach(async correlationId => {
          await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.DONE});
        });
        return true;
      }

      // eslint-disable-next-line functional/no-conditional-statement
      logger.debug(`${queue} is BULK ***`);
      logger.debug(`Acking for ${ack.length} messages.`);
      await amqpOperator.ackMessages(ack);

      // If Bulk queue has more records/messages waiting in the queue resume to them.
      logger.debug(`Checking remaining items in ${queue}`);
      const queueItemsCount = await amqpOperator.checkQueue(queue, 'messages');
      logger.debug(`Remaining items in ${queue}: ${queueItemsCount}`);

      if (queueItemsCount > 0) {
        logger.debug(`All messages in ${queue} NOT handled.`);
        // Note: this assumes that all messages in the queue are related to the same correlationId
        await mongoOperator.setState({correlationId: messages[0].properties.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});

        return true;
      }

      // This could remove the OPERATION.correlationID queue
      logger.debug(`All messages in ${queue} handled`);

      // Combine loadProcessResults here

      // Note: cases, where aleph-record-load-api has rejected all or some records get state DONE here
      // Note: this assumes that all messages in the queue are related to the same correlationId
      await mongoOperator.setState({correlationId: messages[0].properties.correlationId, state: QUEUE_ITEM_STATE.DONE});

      return true;
    }

    logger.verbose(`No messages in ${queue} to handle: ${messages}. Continuing the loop`);
    return false;
  }

  async function sendErrorResponses({error, queue, mongoOperator}) {
    logger.debug('checkProcess/sendErrorResponses Sending error responses');

    const {messages} = await amqpOperator.checkQueue(queue, 'basic', false);

    // eslint-disable-next-line functional/no-conditional-statement
    if (messages) {
      logger.debug(`Got messages (${messages.length}): ${JSON.stringify(messages)}`);

      const distinctCorrelationIds = messages.map(message => message.properties.correlationId).filter((value, index, self) => self.indexOf(value) === index);
      logger.debug(`Found ${distinctCorrelationIds.length} distinct correlationIds from ${messages.length} messages.`);

      await distinctCorrelationIds.forEach(async correlationId => {
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload, errorStatus: error.status});
      });

      await amqpOperator.ackMessages(messages);

      return;
    }
    logger.debug(`checkProcess/sendErrorMessages Got no messages: ${JSON.stringify(messages)} from ${queue}`);
    // should this throw an error?
  }
}
