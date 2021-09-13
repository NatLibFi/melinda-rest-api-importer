/* eslint-disable max-lines */
/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import httpStatus from 'http-status';
import {promisify} from 'util';
import processOperatorFactory from './processPoll';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';

export default function ({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, operation}) {
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

    logger.log('silly', `loopCheck -> checkProcessQueue (${correlationId})`);
    const processQueueResults = await checkProcessQueue(correlationId, mongoOperator, prio);
    // handle checkProcessQueue errors ???
    // if checkProcessQueue errored, processMessage is not acked/nacked
    logger.log('debug', `processQueueResults: ${JSON.stringify(processQueueResults)}`);

    if (!processQueueResults) {
      // false: there's a process in process queue that is not ready yet
      return;
    }

    const {results, processParams} = processQueueResults;
    logger.log('debug', `results: ${JSON.stringify(results)}`);
    logger.log('debug', `processParams: ${JSON.stringify(processParams)}`);
    // results: {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount};
    // if process poll results resulted in less processed results than processes recordAmount -> ackOnlyLength is recordAmount

    // Assume that all messages are for same correlation id, no need to pushId for every message

    logger.log('debug', `Setting status in mongo for ${correlationId}.`);
    const {handledIds, rejectedIds, loadProcessReport} = results.payloads;
    mongoOperator.pushIds({correlationId, handledIds, rejectedIds});
    mongoOperator.pushMessages({correlationId, messageField: 'loaderProcessReports', messages: [loadProcessReport]});

    await setTimeoutPromise(100); // (S)Nack time!


    logger.log('silly', `loopCheck -> handleMessages`);
    // HandleMessages returns false if there are no messages in queue to handle
    // HandleMessages returns true, if there were messages and they were handled
    const messagesHandled = await handleMessages({results, processParams, queue: `${operation}.${correlationId}`, mongoOperator, prio});
    logger.log('debug', `messagesHandled: ${messagesHandled}`);

    if (messagesHandled) {
      logger.log('verbose', `Requesting file cleaning for ${JSON.stringify(processParams.data)}`);
      await processOperator.requestFileClear(processParams.data);
      return;
    }

    // There was a message in processQueue for queueItem, but no messages in operationQueue for correlation id
    throw new ApiError(httpStatus.UNPROCESSABLE_ENTITY, `No messages in ${operation}.${correlationId}`);
  }

  async function checkProcessQueue(correlationId, mongoOperator, prio) {
    const processQueue = `PROCESS.${correlationId}`;
    logger.log('verbose', `Checking process queue: ${processQueue} for ${correlationId}`);
    const processMessage = await amqpOperator.checkQueue(processQueue, 'raw');

    try {
      if (processMessage) {
        logger.log('debug', `We have processMessage: ${processMessage}`);
        logger.log('silly', `checkProcessQueue -> handleProcessMessage`);

        // handleProcessMessage returns: {results, processParams} if successful - ack processMessage
        // false if process is locked - nack processMessage
        // otherwise throws error
        const result = await handleProcessMessage(processMessage, correlationId);
        return result;
      }

      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Empty ${processQueue} queue`);

    } catch (error) {

      logger.debug(`checkProcessQueue for queue ${correlationId} errored: ${error}`);
      logError(error);

      if (error instanceof ApiError) {
        // eslint-disable-next-line functional/no-conditional-statement
        if (prio) {
          logger.debug(`Error is from PRIO ${correlationId}, sending error responses`);
          await sendErrorResponses(error, correlationId, mongoOperator);
        }

        // eslint-disable-next-line functional/no-conditional-statement
        if (!prio) {
          logger.debug(`Error is from BULK ${correlationId}, not sending error responses`);
          await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload});
        }

        await amqpOperator.ackMessages([processMessage]);
        await setTimeoutPromise(100);
        return false;
      }
      logger.debug(`Error is not ApiError, not sending error responses`);
    }
  }

  async function handleProcessMessage(processMessage, correlationId) {
    logger.log('debug', `handleProcessMessage: ${processMessage} for ${correlationId}`);
    try {
      const processParams = await JSON.parse(processMessage.content.toString());
      logger.log('debug', `handleProcessMessage:processParams: ${JSON.stringify(processParams)}`);

      // Ask aleph-record-load-api about the process
      const results = await processOperator.poll(processParams.data);
      logger.debug(`ProcessPoll results: ${JSON.stringify(results)}`);
      // should this check that results exist/are sane?

      // This could add loadProcessMessage to mongo for queueItem

      await amqpOperator.ackMessages([processMessage]);
      await setTimeoutPromise(100);

      return {results, processParams};
    } catch (error) {
      // should we ack processMessage for other errors than LOCKED?
      logger.error('handleProcessMessage errored');
      logError(error);
      if (error instanceof ApiError) {
        logger.log('debug', `Polling loader resulted in error: ${error.payload}`);
        if (error.status === httpStatus.LOCKED) {
          // Nack message and loop back if process was ongoing
          await amqpOperator.nackMessages([processMessage]);
          logger.debug('Process in progress @ server, back to loop!');
          return false;
        }
        if (error.status === httpStatus.SERVICE_UNAVAILABLE) {
          await amqpOperator.nackMessages([processMessage]);
          logger.debug(`Server temporarily unavailable, sleeping ${error503WaitTime} and back to loop!`);
          await setTimeoutPromise(error503WaitTime);
          return false;

        }
        throw error;
      }
      // Unexpected
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, error.message);
    }
  }

  async function handleMessages({mongoOperator, results, processParams, queue, prio}) {
    logger.log('debug', `handleMessages for ${JSON.stringify(results)}, ${JSON.stringify(JSON.stringify(processParams))}, ${queue}, ${processParams.data.correlationId}`);

    // this checks queue for correlationId -- queue is already operation.correlationId ?
    const {headers, messages} = await amqpOperator.checkQueue(queue, processParams.data.correlationId, false, false);
    logger.log('debug', `headers: ${headers}`);
    logger.log('debug', `messages: ${messages}`);

    if (messages) {
      // Could this set status to REJECTED if record-load-api rejected the record?
      // This would need the message to have a record identifier
      const status = headers.operation === OPERATIONS.CREATE ? 'CREATED' : 'UPDATED';
      logger.log('verbose', 'Handling process messages based on results got from process polling');
      // Handle separation of all ready done records
      const ack = messages.slice(0, results.ackOnlyLength);
      const nack = messages.slice(results.ackOnlyLength);
      logger.log('debug', `Message separation: ack: ${ack}, nack: ${nack}`);
      await amqpOperator.nackMessages(nack);
      await setTimeoutPromise(100); // (S)Nack time!

      if (ack === undefined || ack.length < 1) {
        // Should this error? Currently errors the same as if there was no messages in queue
        logger.debug(`There was no messages to ack!!!`);
        return false;
      }

      // IF PRIO -> DONE
      // IF BULK -> IF QUEUE EMPTY -> DONE  ELSE -> IMPORTING

      // eslint-disable-next-line functional/no-conditional-statement
      if (prio) {
        logger.log('debug', `${queue} is PRIO ***`);

        // Handle separation of all ready done records
        logger.log('debug', `Replying for ${ack.length} messages.`);
        logger.log('debug', `Parameters for ackNReplyMessages ${status}, ${ack}, ${JSON.stringify(results.payloads)}`);

        const prioStatus = results.payloads.handledIds.length < 1 ? httpStatus.UNPROCESSABLE_ENTITY : status;
        const prioPayloads = results.payloads.handledIds[0] || results.payloads.rejectedIds[0] || 'No loadProcess information for record';

        logger.log('debug', `New Parameters for ackNReplyMessages ${prioStatus}, ${ack}, ${JSON.stringify(prioPayloads)}`);

        // ackNReplyMessages sets status in message to 422 in case where there are no handled records and there are rejected records but does not set state as ERROR in mongo
        // ackNReplyMessages keep status as 200/201 even if there's no handledId

        await amqpOperator.ackNReplyMessages({status: prioStatus, messages: ack, payloads: prioPayloads});
      }
      // eslint-disable-next-line functional/no-conditional-statement
      if (!prio) {
        logger.log('debug', `${queue} is BULK ***`);
        logger.log('debug', `Acking for ${ack.length} messages.`);
        await amqpOperator.ackMessages(ack);
      }

      // If Bulk queue has more records in the line resume to them.
      logger.log('debug', `Checking remaining items in ${queue}`);
      const queueItemsCount = await amqpOperator.checkQueue(queue, 'messages');
      logger.log('debug', `Remaining items in ${queue}: ${queueItemsCount}`);

      if (queueItemsCount > 0) {
        logger.log('debug', `All messages in ${queue} NOT handled.`);
        await mongoOperator.setState({correlationId: messages[0].properties.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});

        return true;
      }

      logger.log('debug', `All messages in ${queue} handled`);
      // Note: cases, where aleph-record-load-api has rejected all or some records get state DONE here
      await mongoOperator.setState({correlationId: messages[0].properties.correlationId, state: QUEUE_ITEM_STATE.DONE});

      return true;
    }

    logger.log('debug', `No messages: ${messages}`);
    return false;
  }

  async function sendErrorResponses(error, queue, mongoOperator) {
    logger.log('debug', 'checkProcess/sendErrorResponses Sending error responses');
    const {messages} = await amqpOperator.checkQueue(queue, 'basic', false);
    // eslint-disable-next-line functional/no-conditional-statement
    if (messages) {
      logger.log('debug', `Got messages (${messages.length}): ${JSON.stringify(messages)}`);
      const {status} = error;
      const payloads = error.payload ? new Array(messages.length).fill(error.payload) : [];
      // Does this need to set itemState for all messages?
      messages.forEach(message => {
        mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.ERROR});
      });
      logger.log('debug', `checkProcess/sendErrorMessages Status: ${status}\nMessages: ${messages}\nPayloads:${payloads}`);
      // Send response back if PRIO
      await amqpOperator.ackNReplyMessages({status, messages, payloads});
      return;
    }
    logger.log('debug', `checkProcess/sendErrorMessages Got no messages: ${JSON.stringify(messages)} from ${queue}`);
  }
}
