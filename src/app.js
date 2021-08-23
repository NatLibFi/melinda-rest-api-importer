/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */
/* eslint-disable max-lines */
/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {amqpFactory, mongoFactory, logError, PRIO_QUEUE_ITEM_STATE, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {promisify} from 'util';
import recordLoadFactory from './interfaces/loadStarter';
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
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime});

  logger.log('info', `Started Melinda-rest-api-importer with operation ${operation}`);
  switchPrioBulk();

  /* Original
  async function startCheck() {
    logger.log('debug', `StartCheck for ${operation}!`);

    logger.log('silly', `app:startCheck -> checkProcess:intiCheck`);
    await processOperator.intiCheck(operation, true);
    logger.log('debug', 'app: Process queue checked!');

    logger.log('silly', `startCheck -> checkAmqpQueue`);
    return checkAmqpQueue();
  }
 */

  async function switchPrioBulk(prio = true, wait = false) {
    if (wait) {
      await setTimeoutPromise(5000);
      return switchPrioBulk();
    }
    if (prio) {
      return startCheck(mongoOperatorPrio, prio);
    }

    return startCheck(mongoOperatorBulk, prio);
  }

  async function startCheck(mongoOperator, prio) {
    logger.log('debug', `StartCheck for ${operation}, ${prio ? 'PRIO' : 'BULK'}!`);

    // Items in aleph-record-load-api
    const itemInProcess = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_PROCESS});
    if (itemInProcess) {
      return handleItemInProcess(itemInProcess, mongoOperator, prio);
    }
    // Items in importer to be send to aleph-record-load-api
    const itemImporting = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
    if (itemImporting) {
      return handleItemImporting(itemImporting, mongoOperator, prio);
    }

    // Items waiting to be imported
    const itemInQueue = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.VALIDATOR.IN_QUEUE});
    if (itemInQueue) {
      return handleItemInQueue(itemInQueue, mongoOperator);
    }

    if (!prio) {
      return switchPrioBulk(true, true);
    }

    return switchPrioBulk(!prio);
  }

  async function handleItemInProcess(item, mongoOperator, prio) {
    logger.debug(`App/handleInQueue: QueueItem: ${JSON.stringify(item)}`);
    await processOperator.intiCheck(item.correlationId, mongoOperator, prio);
    await setTimeoutPromise(100);
    return switchPrioBulk();
  }

  async function handleItemImporting(item, mongoOperator, prio) {
    logger.debug(`App/handleItempImporting: QueueItem: ${JSON.stringify(item)}`);
    const {operation, correlationId, recordLoadParams} = item;
    logger.debug(`Operation: ${operation}, correlation id: ${correlationId}`);

    /*
      { // Prio queue item
      "correlationId": "9c9f479a-55a5-4123-8f8a-2247f48e6536",
      "cataloger": "HELLE4017",
      "operation": "CREATE",
      "oCatalogerIn": "HELLE4017",
      "queueItemState": "DONE",
      "creationTime": "2021-08-18T06:38:33.182Z",
      "modificationTime": "2021-08-18T06:38:37.580Z",
      "handledId": ""
    },
    { // Bulk queue item
      "correlationId": "302c4274-0554-46fb-b2b0-5e560df6dd33",
      "cataloger": "LASTU0000",
      "oCatalogerIn": "LOAD-MRLB",
      "operation": "UPDATE",
      "contentType": "application/alephseq",
      "recordLoadParams": {
        "pActiveLibrary": "FIN01",
        "pOldNew": "OLD",
        "pRejectFile": "/exlibris/aleph/u23_3/fin01/scratch/legacy-api/017561703.0818_094714.rej",
        "pLogFile": "/exlibris/aleph/u23_3/alephe/scratch/legacy-api/017561703.0818_094714.syslog",
        "pCatalogerIn": "LASTU0000"
      },
      "queueItemState": "DONE",
      "creationTime": "2021-08-18T06:47:15.395Z",
      "modificationTime": "2021-08-18T06:47:19.336Z",
      "handledIds": [
        "017561703"
      ]
    },
    */

    try {
      const {headers, messages} = await amqpOperator.checkQueue(`${operation}.${correlationId}`, 'rawChunk', purgeQueues);
      /// 1-100 messages from 1-10000 messages
      // eslint-disable-next-line functional/no-conditional-statement
      if (headers && messages) {
        logger.log('debug', `Headers: ${JSON.stringify(headers)}, Messages (${messages.length}): ${messages}`);
        const records = await amqpOperator.messagesToRecords(messages);
        logger.log('debug', `Found ${records.length} records`);
        await amqpOperator.nackMessages(messages);

        await setTimeoutPromise(200); // (S)Nack time!
        // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}
        const {processId, pLogFile, pRejectFile} = await recordLoadOperator.loadRecord({correlationId, ...headers, records, recordLoadParams, prio});

        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IN_PROCESS});
        await amqpOperator.sendToQueue({
          queue: `PROCESS.${correlationId}`, correlationId, headers: {queue: `${operation}.${correlationId}`}, data: {
            correlationId,
            pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
            processId, pRejectFile, pLogFile
          }
        });
      }
    } catch (error) {
      logger.log('error', 'app/handleItemImporting');
      logError(error);
      await sendErrorResponses(error, `${operation}.${correlationId}`, mongoOperator, prio);
      return switchPrioBulk();
    }

    return switchPrioBulk();
  }

  async function handleItemInQueue(item, mongoOperator) {
    logger.debug(`App/handleItemInQueue: QueueItem: ${JSON.stringify(item)}`);
    await mongoOperator.setState({correlationId: item.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
    return switchPrioBulk();
  }


  async function checkAmqpQueue(queue = operation, recordLoadParams = {}) {
    logger.log('silly', `checkAmqpQueue: Checking queue for ${queue}, for params: ${recordLoadParams}`);

    if (OPERATION_TYPES.includes(queue)) {
      logger.log('silly', `checkAmqpQueue: Checking ${queue} - it is an operations queue`);
      const {headers, messages} = await amqpOperator.checkQueue(queue, 'rawChunk', purgeQueues);
      if (headers && messages) {
        logger.log('debug', `Headers: ${queue}, Messages (${messages.length}): ${messages}`);
        const {correlationId} = messages[0].properties;
        logger.log('debug', `Found correlationId: ${correlationId}`);
        logger.log('debug', `checkAmqpQueue -> checkMessages: for ${messages.length} messages for ${correlationId}`);
        const records = await checkMessages(messages);
        await setTimeoutPromise(200); // (S)Nack time!
        logger.log('silly', `checkAmqpQueue -> checkAmqpQueuePrio`);
        return checkAmqpQueuePrio({queue, headers, correlationId, records, recordLoadParams});
      }

      logger.log('silly', `checkAmqpQueue -> checkMongoDB`);
      return checkMongoDB();
    }

    logger.log('silly', `checkAmqpQueue: Checking ${queue} - it is not an operations queue`);
    const {headers, records, messages} = await amqpOperator.checkQueue(queue, 'basic', purgeQueues);
    logger.log('debug', `checkAmqpQueue: ${headers}, ${messages}, ${records}`);
    if (headers && records) {
      await amqpOperator.nackMessages(messages);
      await setTimeoutPromise(200); // (S)Nack time!
      logger.log('silly', `checkAmqpQueue -> checkAmqpQueueBulk`);
      return checkAmqpQueueBulk({queue, headers, records, recordLoadParams});
    }

    logger.log('silly', `checkAmqpQueue -> checkMongoDB`);
    return checkMongoDB();

    async function checkMessages(messages) {
      logger.log('debug', `checkMessages: IMPORTING -> ABORT?`);
      const validMessages = messages.map(async message => {
        const valid = await mongoOperator.checkAndSetState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
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

  async function checkAmqpQueueBulk({queue, headers, records, recordLoadParams}) {
    try {
      // actually start the loader
      logger.log('silly', `checkAmqpQueueBulk: starting loader`);
      const {processId, correlationId, pLogFile, pRejectFile, prioCorrelationIds} = await recordLoadOperator.loadRecord({correlationId: queue, ...headers, records, recordLoadParams, prio: false});

      // Send to process queue {queue, correlationId, headers, data}
      logger.log('silly', `checkAmqpQueueBulk: send to process queue: PROCESS.${queue}`);
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

    logger.log('silly', `checkAmqpQueueBulk -> startCheck`);
    return startCheck();
  }

  async function checkMongoDB() {
    logger.log('silly', `checking Mongo for IN_PROCESS`);
    await handleQueueItem(await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS}));
    logger.log('silly', `checking Mongo for IN_QUEUE`);
    await handleQueueItem(await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_QUEUE}));

    await setTimeoutPromise(pollWaitTime);

    logger.log('silly', `checkMongoDB -> checkAmqpQueue`);
    return checkAmqpQueue();

    async function handleQueueItem(queueItem) { // mongoOperator
      if (queueItem) {
        logger.log('debug', `Got queueItem from Mongo: ${JSON.stringify(queueItem)}`);
        logger.log('debug', `QueueItemState: ${JSON.stringify(queueItem.queueItemState)}`);
        logger.log('debug', `Checking queue for ${JSON.stringify(queueItem.correlationId)}`);
        const messagesAmount = await amqpOperator.checkQueue(queueItem.correlationId, 'messages', purgeQueues);
        logger.log('debug', `app:handleQueueItem: messagesAmount: ${messagesAmount}`);
        if (messagesAmount) {
          if (queueItem.queueItemState === QUEUE_ITEM_STATE.IN_QUEUE) {
            logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IN_PROCESS})));

            logger.log('silly', `checkMongo -> checkAmqpQueue for for (${queueItem.correlationId}`);
            return checkAmqpQueue(queueItem.correlationId, queueItem.recordLoadParams);
          }

          logger.log('silly', `checkMongo -> checkAmqpQueue for (${queueItem.correlationId}`);
          return checkAmqpQueue(queueItem.correlationId, queueItem.recordLoadParams);
        }

        if (queueItem.queueItemState === QUEUE_ITEM_STATE.IN_PROCESS) {
          logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.DONE})));
          // Clean empty queues
          amqpOperator.removeQueue(queueItem.correlationId);

          logger.log('silly', `checkMongo -> checkAmqpQueue`);
          return checkAmqpQueue();
        }

        logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.ERROR})));
        // Clean empty queues
        amqpOperator.removeQueue(queueItem.correlationId);

        logger.log('silly', `checkMongo -> checkAmqpQueue`);
        return checkAmqpQueue();
      }

      logger.log('silly', `app:handleQueueItem: No queueItem to handle: ${queueItem}`);

    }
  }

  async function sendErrorResponses(error, queue, mongoOperator, prio = false) {
    logger.log('debug', 'Sending error responses');
    const {messages} = await amqpOperator.checkQueue(queue, 'basic', false);
    if (messages) { // eslint-disable-line functional/no-conditional-statement
      logger.log('debug', `Got back messages (${messages.length})`);
      const status = error.status ? error.status : '500';
      const payloads = error.payload ? new Array(messages.length).fill(error.payload) : new Array(messages.length).fill(JSON.stringify.error);
      // Send response back if PRIO
      if (prio) { // eslint-disable-line functional/no-conditional-statement
        await amqpOperator.ackNReplyMessages({status, messages, payloads});
        await messages.forEach(async message => {
          await mongoOperator.setState({correlationId: message.properties.correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload});
        });
      }

      if (!prio && error.status !== 503) { // eslint-disable-line functional/no-conditional-statement
        await amqpOperator.ackMessages(messages);
      }
      logger.log('silly', `Status: ${status}, Messages: ${messages}, Payloads:${payloads}`);
    }
    logger.log('debug', `Did not get back any messages: ${messages}`);
  }
}
