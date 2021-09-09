/* eslint-disable max-lines */
/* eslint-disable max-statements */
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {amqpFactory, mongoFactory, logError, QUEUE_ITEM_STATE} from '@natlibfi/melinda-rest-api-commons';
import {promisify} from 'util';
import recordLoadFactory from './interfaces/loadStarter';
import checkProcess from './interfaces/checkProcess';

export default async function ({
  amqpUrl, operation, pollWaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl
}) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const purgeQueues = false;
  const amqpOperator = await amqpFactory(amqpUrl);
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, operation});

  logger.log('info', `Started Melinda-rest-api-importer with operation ${operation}`);
  startCheck();

  /*
  prio inProcess importing inqueue
  bulk inProcess importing inqueue

  |
  v

  inProcess prio bulk
  Prio importing inQueue
  Bulk importing inQueue
  */

  async function startCheck(checkInProcessItems = true, wait = false) {
    if (wait) {
      await setTimeoutPromise(5000);
      return startCheck();
    }

    if (checkInProcessItems) {
      return checkInProcess();
    }

    return checkItemImportingAndInQueue();
  }

  async function checkInProcess(prio = true) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;

    // Items in aleph-record-load-api
    const itemInProcess = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_PROCESS});
    if (itemInProcess) {
      return handleItemInProcess(itemInProcess, mongoOperator, prio);
    }

    if (prio) {
      return checkInProcess(false);
    }

    return startCheck(false, false);
  }

  async function checkItemImportingAndInQueue(prio = true) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;

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

    if (prio) {
      return checkItemImportingAndInQueue(false);
    }

    return startCheck(true, true);
  }

  async function handleItemInProcess(item, mongoOperator, prio) {
    logger.debug(`App/handleInProcess: QueueItem: ${JSON.stringify(item)}`);
    await processOperator.loopCheck({correlationId: item.correlationId, mongoOperator, prio});
    await setTimeoutPromise(100);
    return startCheck();
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
        const recordAmount = records.length;
        logger.log('debug', `Found ${records.length} records from ${messages.length} messages`);
        await amqpOperator.nackMessages(messages);

        await setTimeoutPromise(200); // (S)Nack time!
        // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}
        const {processId, pLogFile, pRejectFile} = await recordLoadOperator.loadRecord({correlationId, ...headers, records, recordLoadParams, prio});

        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IN_PROCESS});
        await amqpOperator.sendToQueue({
          queue: `PROCESS.${correlationId}`, correlationId, headers: {queue: `${operation}.${correlationId}`}, data: {
            correlationId,
            pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
            processId, pRejectFile, pLogFile,
            recordAmount
          }
        });
      }
    } catch (error) {
      logger.log('error', 'app/handleItemImporting');
      logError(error);
      // eslint-disable-next-line functional/no-conditional-statement
      await sendErrorResponses(error, `${operation}.${correlationId}`, mongoOperator, prio);

      return startCheck();
    }

    return startCheck();
  }

  async function handleItemInQueue(item, mongoOperator) {
    logger.debug(`App/handleItemInQueue: QueueItem: ${JSON.stringify(item)}`);
    await mongoOperator.setState({correlationId: item.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
    return startCheck();
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
        await mongoOperator.setState({correlationId: messages[0].properties.correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload});
      }
      logger.log('silly', `Status: ${status}, Messages: ${messages}, Payloads:${payloads}`);
    }
    logger.log('debug', `Did not get back any messages: ${messages}`);
  }
}
