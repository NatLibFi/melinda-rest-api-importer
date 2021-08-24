/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
/* eslint-disable max-lines */
/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import httpStatus from 'http-status';
import {promisify} from 'util';
import processOperatorFactory from './processPoll';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';

export default function ({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, operation}) {
  // note mongoOperator?
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const OPERATION_TYPES = [
    OPERATIONS.CREATE,
    OPERATIONS.UPDATE
  ];
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
    if (!processQueueResults) {
      return loopCheck({correlationId, mongoOperator, prio, wait: true});
    }
    const {results, processParams} = processQueueResults;
    logger.log('debug', `results: ${JSON.stringify(results)}`);
    logger.log('debug', `processParams: ${JSON.stringify(processParams)}`);

    // results: {payloads: [ids], ackOnlyLength: 0}
    try {
      const messagesHandled = await handleMessages({results, processParams, queue: `${operation}.${correlationId}`, mongoOperator, prio});
      logger.log('debug', `messagesHandled: ${messagesHandled}`);

      if (messagesHandled) {
        logger.log('verbose', `Requesting file cleaning for ${JSON.stringify(processParams.data)}`);
        await processOperator.requestFileClear(processParams.data);
        return;
      }

      throw new ApiError(httpStatus.UNPROCESSABLE_ENTITY, `No messages in ${operation}.${correlationId}`);
    } catch (error) { // TARVII MUUTOSTA
      logger.debug(`checkProcessQueue (${processQueue}) for queue ${correlationId} errored: ${error}`);
      logError(error);
      if (error instanceof ApiError) {
        if (prio) {
          logger.debug(`Error is from PRIO ${correlationId}, sending error responses`);
          await sendErrorResponses(error, correlationId);
          await amqpOperator.ackMessages([processMessage]);
          await setTimeoutPromise(100);
          return checkProcessQueue(correlationId, mongoOperator, prio);
        }

        logger.debug(`Error is from BULK ${correlationId}, not sending error responses`);
        await amqpOperator.ackMessages([processMessage]);
        await setTimeoutPromise(100);
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload});
        return checkProcessQueue(correlationId, mongoOperator, prio);
      }
      logger.debug(`Error is not ApiError, not sending error responses`);
    }
  }

  async function checkProcessQueue(correlationId, mongoOperator, prio) {
    const processQueue = `PROCESS.${correlationId}`;
    logger.log('verbose', `Checking process queue: ${processQueue} for ${correlationId}`);
    const processMessage = await amqpOperator.checkQueue(processQueue, 'raw');
    if (processMessage) {
      logger.log('debug', `We have processMessage: ${processMessage}`);
      //const {results, processParams} = await handleProcessMessage(processMessage, correlationId, mongoOperator, prio);
      return handleProcessMessage(processMessage, correlationId, mongoOperator, prio);
    }

    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Empty ${processQueue} queue`);
  }

  async function checkMongoInProcess(queueItem) {
    const messagesAmount = await amqpOperator.checkQueue(`PROCESS.${queueItem.correlationId}`, 'messages');
    logger.log('debug', `checkMongoInProcess: messagesAmount: ${messagesAmount}`);
    if (messagesAmount) {
      logger.log('silly', `checkMongoInProcess -> checkProcessQueue for ${queueItem.correlationId}`);
      return checkProcessQueue(`${queueItem.correlationId}`);
    }
  }

  async function handleProcessMessage(processMessage, correlationId, mongoOperator, prio) {
    logger.log('debug', `handleProcessMessage: ${processMessage} for ${correlationId}`);
    try {
      const processParams = await JSON.parse(processMessage.content.toString());
      logger.log('debug', `handleProcessMessage:processParams: ${JSON.stringify(processParams)}`);
      // Ask aleph-record-load-api about the process
      const results = await processOperator.poll(processParams.data);
      // results: {payloads: [ids], ackOnlyLength: 0}
      // should this check that results exist/are sane?

      await amqpOperator.ackMessages([processMessage]);
      await setTimeoutPromise(100);

      return {results, processParams};
    } catch (error) {
      logger.log('debug', `handleProcessMessage: Polling loader resulted in error: ${error}`);
      logError(error);
      if (error instanceof ApiError) {

        if (error.status === httpStatus.LOCKED) {
          // Nack message and loop back if process was ongoing
          await amqpOperator.nackMessages([processMessage]);
          logger.debug('Process in progress @ server, back to loop!');
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
    const {headers, messages} = await amqpOperator.checkQueue(queue, processParams.data.correlationId, false, false);
    logger.log('debug', `headers: ${headers}`);
    logger.log('debug', `messages: ${messages}`);
    if (messages) {
      const status = headers.operation === OPERATIONS.CREATE ? 'CREATED' : 'UPDATED';
      logger.log('verbose', 'Handling process messages based on results got from process polling');
      // Handle separation of all ready done records
      const ack = messages.slice(0, results.ackOnlyLength);
      const nack = messages.slice(results.ackOnlyLength);
      logger.log('debug', `Message separation: ack: ${ack}, nack: ${nack}`);
      await amqpOperator.nackMessages(nack);
      await setTimeoutPromise(100); // (S)Nack time!

      // IF PRIO -> DONE
      // IF BULK -> IF QUEUE EMPTY -> DONE, ELSE -> IMPORTING

      if (prio) {
        logger.log('debug', `${queue} is PRIO ***`);
        // Handle separation of all ready done records
        logger.log('debug', `Replying for ${ack.length} messages.`);
        logger.log('debug', `Parameters for ackNReplyMessages ${status}, ${ack}, ${results.payloads}`);
        await amqpOperator.ackNReplyMessages({status, messages: ack, payloads: results.payloads});

        logger.log('debug', `Setting status in mongo for ${ack.length} messages.`);
        await ack.forEach(message => {
          logger.log('debug', `Setting status in mongo for ${message.properties.correlationId}.`);
          mongoOperator.pushIds({correlationId: message.properties.correlationId, ids: results.payloads});
          mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.DONE});
        });
        await setTimeoutPromise(100); // (S)Nack time!

        return true;
      }

      // Ids to mongo for other errors?
      logger.log('debug', `${queue} is BULK ***`);
      logger.log('debug', `Pushing ids to mongo for with ${message.properties.correlationId} as correlationId, results.payloads ${results.payloads} as ids`);
      await mongoOperator.pushIds({correlationId: message.properties.correlationId, ids: results.payloads});
      await amqpOperator.ackMessages(ack);
      await setTimeoutPromise(100); // (S)Nack time!

      logger.log('debug', `Checking remaining items in ${queue}`);
      const queueItemsCount = await amqpOperator.checkQueue(queue, 'messages');
      logger.log('debug', `Remaining items in ${queue}: ${queueItemsCount}`);
      // eslint-disable-next-line functional/no-conditional-statement
      if (queueItemsCount > 0) {
        await mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
      }

      return true;
    }
    logger.log('debug', `No messages: ${messages}`);
    return false;
  }

  async function sendErrorResponses(error, queue) {
    logger.log('debug', 'Sending error responses');
    const {messages} = await amqpOperator.checkQueue(queue, 'basic', false);
    // eslint-disable-next-line functional/no-conditional-statement
    if (messages) {
      logger.log('debug', `Got messages (${messages.length}): ${JSON.stringify(messages)}`);
      const {status} = error;
      const payloads = error.payload ? new Array(messages.length).fill(error.payload) : [];
      messages.forEach(message => {
        mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.ERROR});
      });
      logger.log('debug', `Status: ${status}\nMessages: ${messages}\nPayloads:${payloads}`);
      // Send response back if PRIO
      await amqpOperator.ackNReplyMessages({status, messages, payloads});
      return;
    }
    logger.log('debug', `Got no messages: ${JSON.stringify(messages)}`);
  }
}
