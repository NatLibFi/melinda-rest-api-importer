/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {amqpFactory, mongoFactory, logError, PRIO_QUEUE_ITEM_STATE, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {promisify} from 'util';
import recordLoadFactory from './interfaces/datastore';
import checkProcess from './interfaces/checkProcess';

export default async function ({
  amqpUrl, operation, pollWaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl
}) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const OPERATION_TYPES = [
    OPERATIONS.CREATE,
    OPERATIONS.UPDATE
  ];
  const purgeQueues = false;
  const amqpOperator = await amqpFactory(amqpUrl);
  const mongoOperator = await mongoFactory(mongoUri);
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl, mongoOperator);
  const processOperator = await checkProcess({amqpOperator, mongoOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime});

  logger.log('info', `Started Melinda-rest-api-importer with operation ${operation}`);
  startCheck();

  async function startCheck() {
    await processOperator.intiCheck(operation);

    logger.log('debug', 'Process queue checked!');

    return checkAmqpQueue();
  }

  async function checkAmqpQueue(queue = operation, recordLoadParams = {}) {
    logger.log('debug', `Checking queue for ${queue}`);

    if (OPERATION_TYPES.includes(queue)) {
      const {headers, messages} = await amqpOperator.checkQueue(queue, 'rawChunk', purgeQueues);
      if (headers && messages) {
        logger.log('debug', `Headers: ${queue}, Messages (${messages.length}): ${messages}`);
        const {correlationId} = messages[0].properties;
        logger.log('debug', `Found correlationId: ${correlationId}`);
        const records = await checkMessages(messages);
        await setTimeoutPromise(200); // (S)Nack time!
        return checkAmqpQueuePrio({queue, headers, correlationId, records, recordLoadParams});
      }

      return checkMongoDB();
    }

    const {headers, records, messages} = await amqpOperator.checkQueue(queue, 'basic', purgeQueues);
    if (headers && records) {
      await amqpOperator.nackMessages(messages);
      await setTimeoutPromise(200); // (S)Nack time!
      return checkAmqpQueueBulk({queue, headers, records, recordLoadParams});
    }

    return checkMongoDB();

    async function checkMessages(messages) {
      const validMessages = messages.map(async message => {
        const valid = await mongoOperator.checkAndSetState({correlationId: message.properties.correlationId, state: PRIO_QUEUE_ITEM_STATE.IMPORTING});
        if (valid) {
          logger.log('debug', 'Valid message');
          return message;
        }
        logger.log('debug', 'Non-valid message');
        return false;
      });
      const checkedMessages = await Promise.all(validMessages);
      // Removes undefined ones
      const filteredMessages = checkedMessages.filter(message => message);
      const invalids = messages.filter(message => !filteredMessages.includes(message));
      const records = await amqpOperator.messagesToRecords(filteredMessages);
      logger.log('debug', `Found ${filteredMessages.length} valid and ${invalids.length} non-valid messages`);
      await amqpOperator.nackMessages(filteredMessages);
      await amqpOperator.ackMessages(invalids);
      return records;
    }
  }

  async function checkAmqpQueuePrio({queue, headers, correlationId, records, recordLoadParams}) {
    try {
      const {processId, pLogFile, pRejectFile} = await recordLoadOperator.loadRecord({correlationId, ...headers, records, recordLoadParams, prio: true});

      // Send to process queue {queue, correlationId, headers, data}
      await amqpOperator.sendToQueue({
        queue: `PROCESS.${queue}`, correlationId, headers: {queue}, data: {
          correlationId,
          pActiveLibrary: recordLoadParams.pActiveLibrary || recordLoadLibrary,
          processId, pRejectFile, pLogFile
        }
      });
    } catch (error) {
      logger.log('error', 'app/checkAmqpQueuePrio');
      logError(error);
      await sendErrorResponses(error, queue);
      return startCheck();
    }

    return startCheck();
  }

  async function checkAmqpQueueBulk({queue, headers, records, recordLoadParams}) {
    try {
      const {processId, correlationId, pLogFile, pRejectFile, prioCorrelationIds} = await recordLoadOperator.loadRecord({correlationId: queue, ...headers, records, recordLoadParams, prio: false});

      // Send to process queue {queue, correlationId, headers, data}
      await amqpOperator.sendToQueue({
        queue: `PROCESS.${queue}`, correlationId, headers: {queue}, data: {
          correlationId,
          pActiveLibrary: recordLoadParams.pActiveLibrary || recordLoadLibrary,
          processId, pRejectFile, pLogFile, prioCorrelationIds
        }
      });
    } catch (error) {
      logger.log('error', 'app/checkAmqpQueueBulk');
      logError(error);
      return checkAmqpQueue();
    }

    return startCheck();
  }

  async function checkMongoDB() {
    await handleQueueItem(await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS}));
    await handleQueueItem(await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_QUEUE}));

    await setTimeoutPromise(pollWaitTime);
    return checkAmqpQueue();

    async function handleQueueItem(queueItem) {
      if (queueItem) {
        const messagesAmount = await amqpOperator.checkQueue(queueItem.correlationId, 'messages', purgeQueues);
        if (messagesAmount) {
          if (queueItem.queueItemState === QUEUE_ITEM_STATE.IN_QUEUE) {
            logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IN_PROCESS})));

            return checkAmqpQueue(queueItem.correlationId, queueItem.recordLoadParams);
          }

          return checkAmqpQueue(queueItem.correlationId, queueItem.recordLoadParams);
        }

        if (queueItem.queueItemState === QUEUE_ITEM_STATE.IN_PROCESS) {
          logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE})));
          // Clean empty queues
          amqpOperator.removeQueue(queueItem.correlationId);

          return checkAmqpQueue();
        }

        logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.ERROR})));
        // Clean empty queues
        amqpOperator.removeQueue(queueItem.correlationId);
        return checkAmqpQueue();
      }
    }
  }

  async function sendErrorResponses(error, queue) {
    logger.log('debug', 'Sending error responses');
    const {messages} = await amqpOperator.checkQueue(queue, 'basic', false);
    if (messages) { // eslint-disable-line functional/no-conditional-statement
      logger.log('debug', `Got back messages (${messages.length})`);
      const status = error.status ? error.status : '500';
      const payloads = error.payload ? new Array(messages.length).fill(error.payload) : new Array(messages.length).fill(JSON.stringify.error);
      // Send response back if PRIO
      await amqpOperator.ackNReplyMessages({status, messages, payloads});
      await messages.forEach(async message => {
        await mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.ERROR});
      });
      logger.log('silly', `Status: ${status}, Messages: ${messages}, Payloads:${payloads}`);
    }
    logger.log('debug', `Did not get back any messages: ${messages}`);
  }
}
