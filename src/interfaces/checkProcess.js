/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {Error as ApiError} from '@natlibfi/melinda-commons';
import {PRIO_QUEUE_ITEM_STATE, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import httpStatus from 'http-status';
import {promisify} from 'util';
import processOperatorFactory from './processPoll';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';

export default function ({amqpOperator, mongoOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const OPERATION_TYPES = [
    OPERATIONS.CREATE,
    OPERATIONS.UPDATE
  ];
  const processOperator = processOperatorFactory({recordLoadApiKey, recordLoadUrl});

  return {intiCheck};

  async function intiCheck(queue, wait = false) {
    if (wait) {
      logger.debug(`Waiting ${pollWaitTime}`);
      await setTimeoutPromise(pollWaitTime);
      logger.log('silly', `intiCheck -> checkProcessQueue (${queue})`);
      return checkProcessQueue(queue);
    }

    logger.log('silly', `intiCheck -> checkProcessQueue (${queue})`);
    await checkProcessQueue(queue);

    logger.log('debug', `Process queue (${queue}) done, now checking mongo for items in process`);

    const queueItem = await mongoOperator.getOne({operation: queue, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});
    if (queueItem) {
      logger.log('debug', 'Got Mongo item in process');
      logger.log('silly', `intiCheck -> checkMongoInProcess`);
      return checkMongoInProcess(queueItem);
    }
  }

  async function checkProcessQueue(queue) {
    const processQueue = `PROCESS.${queue}`;
    logger.log('verbose', `Checking process queue: ${processQueue} for ${queue}`);
    const processMessage = await amqpOperator.checkQueue(processQueue, 'raw');

    try {
      if (processMessage) {
        logger.log('debug', `We have processMessage: ${processMessage}`);
        const {looped, results, processParams} = await handleProcessMessage(processMessage, queue);

        // If process was still busy handleProcessMessages looped and message was handled in a later loop
        if (looped) {
          logger.log('debug', `Looped, returning`);
          return;
        }

        // results: {payloads: [ids], ackOnlyLength: 0}
        logger.log('debug', `results: ${results}`);
        logger.log('debug', `processParams: ${processParams}`);

        const messagesHandled = await handleMessages({results, processParams, queue});
        logger.log('debug', `messagesHandled: ${messagesHandled}`);

        if (messagesHandled) {
          logger.log('verbose', `Requesting file cleaning for ${JSON.stringify(processParams.data)}`);
          await processOperator.requestFileClear(processParams.data);

          return;
        }
      }
      logger.log('debug', `No processMessages to handle`);
    } catch (error) {
      logger.debug(`checkProcessQueue (${processQueue}) for queue ${queue} errored: ${error}`);
      logError(error);
      if (error instanceof ApiError) {
        if (OPERATION_TYPES.includes(queue)) {
          await sendErrorResponses(error, queue);
          await amqpOperator.ackMessages([processMessage]);
          await setTimeoutPromise(100);
          return checkProcessQueue(queue);
        }
        logger.debug(`Error is from ${queue}, which is not operation type queue, not sending error responses`);
        await amqpOperator.ackMessages([processMessage]);
        await setTimeoutPromise(100);
        return checkProcessQueue(queue);
      }
      logger.debug(`Error is not ApiError, not sending error responses`);
    }
  }

  async function checkMongoInProcess(queueItem) {
    const messagesAmount = await amqpOperator.checkQueue(`PROCESS.${queueItem.correlationId}`, 'messages');
    logger.log('debug', `checkMongoInProcess: messagesAmount: ${messagesAmount}`);
    if (messagesAmount) {
      logger.log('silly', `checkMongoInProcess -> checkProcessQueue for ${queueItem.correlationId}`);
      return checkProcessQueue(`${queueItem.correlationId}`);
    }
  }

  async function handleProcessMessage(processMessage, queue) {
    logger.log('debug', `handleProcessMessage: ${processMessage} for ${queue}`);
    try {
      const processParams = await JSON.parse(processMessage.content.toString());
      logger.log('debug', `handleProcessMessage:processParams: ${JSON.stringify(processParams)}`);
      // Ask aleph-record-load-api about the process
      const results = await processOperator.poll(processParams.data);
      // results: {payloads: [ids], ackOnlyLength: 0}
      // should this check that results exist/are sane?

      await amqpOperator.ackMessages([processMessage]);
      await setTimeoutPromise(100);

      return {looped: false, results, processParams};
    } catch (error) {
      logger.log('debug', `handleProcessMessage: Polling loader resulted in error: ${error}`);
      logError(error);
      if (error instanceof ApiError) {

        if (error.status === httpStatus.LOCKED) {
          // Nack message and loop back if process was ongoing
          await amqpOperator.nackMessages([processMessage]);
          await intiCheck(queue, true);
          return {looped: true};
        }

        throw error;
      }
      // Unexpected
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  async function handleMessages({results, processParams, queue}) {
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

      if (OPERATION_TYPES.includes(queue)) {
        logger.log('debug', `${queue} is in OPERATION_TYPES`);
        // Handle separation of all ready done records
        logger.log('debug', `Setting status in mongo for ${ack.length} messages.`);
        await ack.forEach(message => {
          logger.log('debug', `Setting status in mongo for ${message.properties.correlationId}.`);
          mongoOperator.setState({correlationId: message.properties.correlationId, state: PRIO_QUEUE_ITEM_STATE.DONE});
        });
        logger.log('debug', `Replying for ${ack.length} messages.`);
        logger.log('debug', `Parameters for ackNReplyMessages ${status}, ${ack}, ${results.payloads}`);
        await amqpOperator.ackNReplyMessages({status, messages: ack, payloads: results.payloads});
        await setTimeoutPromise(100); // (S)Nack time!

        return true;
      }

      // Ids to mongo for other errors?
      logger.log('debug', `${queue} is not in OPERATION_TYPES`);
      logger.log('debug', `Pushing ids to mongo for with queue: ${queue} as correlationId, results.payloads ${results.payloads} as ids`);
      await mongoOperator.pushIds({correlationId: queue, ids: results.payloads});
      await amqpOperator.ackMessages(ack);
      await setTimeoutPromise(100); // (S)Nack time!

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
