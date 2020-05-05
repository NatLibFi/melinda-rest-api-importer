import {Error as ApiError, Utils} from '@natlibfi/melinda-commons';
import {PRIO_QUEUE_ITEM_STATE, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import httpStatus from 'http-status';
import {promisify} from 'util';
import processOperatorFactory from './processPoll';
import {logError} from '@natlibfi/melinda-rest-api-commons/dist/utils';

export default function ({amqpOperator, mongoOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime}) {
  const {createLogger} = Utils;
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
      await setTimeoutPromise(pollWaitTime);
      return checkProcessQueue(queue);
    }

    await checkProcessQueue(queue);

    const queueItem = await mongoOperator.getOne({queue, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});
    if (queueItem) {
      return checkMongoInProcess(queueItem);
    }
  }

  async function checkProcessQueue(queue) {
    logger.log('verbose', `Checking process queue: PROCESS.${queue}`);
    const processMessage = await amqpOperator.checkQueue(`PROCESS.${queue}`, 'raw');
    try {
      if (processMessage) {
        const {results, processParams} = await handleProcessMessage(processMessage, queue);
        const messagesHandled = await handleMessages(results, queue);
        if (messagesHandled) {
          await amqpOperator.ackMessages([processMessage]);
          logger.log('verbose', 'Requesting file cleaning');
          await processOperator.requestFileClear(processParams.data);
          await setTimeoutPromise(100); // (S)Nack time!

          return true;
        }
      }
    } catch (error) {
      if (error instanceof ApiError) {
        if (OPERATION_TYPES.includes(queue)) {
          await sendErrorResponses(error, queue);
          await amqpOperator.ackMessages([processMessage]);
          return checkProcessQueue(queue);
        }

        await amqpOperator.ackMessages([processMessage]);
        return checkProcessQueue(queue);
      }
    }
  }

  async function checkMongoInProcess(queueItem) {
    const messagesAmount = await amqpOperator.checkQueue(`PROCESS.${queueItem.correlationId}`, 'messages');
    if (messagesAmount) {
      return checkProcessQueue(`${queueItem.correlationId}`);
    }
  }

  async function handleProcessMessage(processMessage, queue) {
    try {
      const processParams = JSON.parse(processMessage.content.toString());
      return {results: await processOperator.poll(processParams.data), processParams};
    } catch (error) {
      logError(error);
      if (error instanceof ApiError) {
        if (error.status === httpStatus.LOCKED) {
          await amqpOperator.nackMessages([processMessage]);

          return intiCheck(queue, true);
        }

        throw error;
      }
      // Unexpected
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  async function handleMessages(results, queue) {
    const {headers, messages} = await amqpOperator.checkQueue(queue, 'basic');
    if (messages) {
      const status = headers.operation === OPERATIONS.CREATE ? 'CREATED' : 'UPDATED';
      logger.log('verbose', 'Handling process messages based on results got from process polling');
      // Handle separation of all ready done records
      const ack = messages.slice(0, results.ackOnlyLength);
      const nack = messages.slice(results.ackOnlyLength);
      await amqpOperator.nackMessages(nack);

      if (OPERATION_TYPES.includes(queue)) {
        // Handle separation of all ready done records
        await ack.forEach(message => {
          mongoOperator.setState({correlationId: message.properties.correlationId, state: PRIO_QUEUE_ITEM_STATE.DONE});
        });
        await amqpOperator.ackNReplyMessages({status, messages: ack, payloads: results.payloads});
        return true;
      }

      // Ids to mongo
      await mongoOperator.pushIds({correlationId: queue, ids: results.payloads});
      await amqpOperator.ackMessages(ack);

      return true;
    }

    return false;
  }

  async function sendErrorResponses(error, queue) {
    logger.log('debug', 'Sending error responses');
    const {messages} = await amqpOperator.checkQueue(queue, 'basic', false);
    const {status} = error;
    const payloads = error.payload ? new Array(messages.lenght).fill(error.payload) : [];
    messages.forEach(message => {
      mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.ERROR});
    });
    logger.log('silly', `Status: ${status}\nMessages: ${messages}\nPayloads:${payloads}`);
    // Send response back if PRIO
    await amqpOperator.ackNReplyMessages({status, messages, payloads});
  }
}
