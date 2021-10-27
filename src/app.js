import {createLogger} from '@natlibfi/melinda-backend-commons';
import {amqpFactory, mongoFactory, logError, QUEUE_ITEM_STATE} from '@natlibfi/melinda-rest-api-commons';
import {promisify} from 'util';
import recordLoadFactory from './interfaces/loadStarter';
import checkProcess from './interfaces/checkProcess';
import httpStatus from 'http-status';

export default async function ({
  amqpUrl, operation, pollWaitTime, error503WaitTime, mongoUri,
  recordLoadApiKey, recordLoadLibrary, recordLoadUrl, keepLoadProcessReports
}) {
  const setTimeoutPromise = promisify(setTimeout);
  const logger = createLogger();
  const purgeQueues = false;
  const amqpOperator = await amqpFactory(amqpUrl);
  const mongoOperatorPrio = await mongoFactory(mongoUri, 'prio');
  const mongoOperatorBulk = await mongoFactory(mongoUri, 'bulk');
  const recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
  const processOperator = await checkProcess({amqpOperator, recordLoadApiKey, recordLoadUrl, pollWaitTime, error503WaitTime, operation, keepLoadProcessReports});

  logger.info(`Started Melinda-rest-api-importer with operation ${operation}`);
  startCheck();

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
      // Do not spam logs
      logger.silly(`Found item in process ${itemInProcess.correlationId}`);
      return handleItemInProcess(itemInProcess, mongoOperator, prio);
    }

    if (prio) {
      logger.silly(`app/checkInProcess: Nothing found: PRIO -> checkInProcess `);
      return checkInProcess(false);
    }

    logger.silly(`app/checkInProcess: Nothing found: BULK -> starCheck `);
    return startCheck(false, false);
  }

  async function checkItemImportingAndInQueue(prio = true) {
    const mongoOperator = prio ? mongoOperatorPrio : mongoOperatorBulk;

    // Items in importer to be send to aleph-record-load-api
    const itemImporting = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
    if (itemImporting) {
      logger.debug(`Found item in importing ${itemImporting.correlationId}`);
      return handleItemImporting(itemImporting, mongoOperator, prio);
    }

    // Items waiting to be imported
    const itemInQueue = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IMPORTER.IN_QUEUE});
    if (itemInQueue) {
      logger.debug(`Found item in queue to be imported ${itemInQueue.correlationId}`);
      return handleItemInQueue(itemInQueue, mongoOperator);
    }

    if (prio) {
      logger.silly(`app/checkItemImportingAndInQueue: Nothing found: PRIO -> checkItemImportingAndInQueue `);
      return checkItemImportingAndInQueue(false);
    }

    logger.silly(`app/checkItemImportingAndInQueue: Nothing found: BULK -> startCheck `);
    return startCheck(true, true);
  }

  async function handleItemInProcess(item, mongoOperator, prio) {
    logger.silly(`Item in process: ${JSON.stringify(item)}`);
    await processOperator.loopCheck({correlationId: item.correlationId, mongoOperator, prio});
    await setTimeoutPromise(100);
    return startCheck();
  }

  // eslint-disable-next-line max-statements
  async function handleItemImporting(item, mongoOperator, prio) {
    logger.silly(`Item in importing: ${JSON.stringify(item)}`);
    const {operation, correlationId, recordLoadParams} = item;

    try {
      // basic: get next chunk of 100 messages {headers, records, messages}
      const {headers, records, messages} = await amqpOperator.checkQueue({queue: `${operation}.${correlationId}`, style: 'basic', toRecord: true, purge: purgeQueues});
      /// 1-100 messages from 1-10000 messages
      // eslint-disable-next-line functional/no-conditional-statement
      if (headers && messages) {
        logger.debug(`app/handleItemImporting: Headers: ${JSON.stringify(headers)}, Messages (${messages.length}), Records: ${records.length}`);
        const recordAmount = records.length;
        // messages nacked to wait results - should these go to some other queue IN_PROCESS.correaltionId ?
        await amqpOperator.nackMessages(messages);

        await setTimeoutPromise(200); // (S)Nack time!
        // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}

        const {processId, pLogFile, pRejectFile} = await recordLoadOperator.loadRecord({correlationId, ...headers, records, recordLoadParams, prio});

        logger.silly(`app/handleItemImporting: setState and send to process queue`);
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IN_PROCESS});
        await amqpOperator.sendToQueue({
          queue: `PROCESS.${correlationId}`, correlationId, headers: {queue: `${operation}.${correlationId}`}, data: {
            correlationId,
            pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
            processId, pRejectFile, pLogFile,
            recordAmount
          }
        });
        return startCheck();
      }

      logger.debug(`app/handleItemImporting: No messages found in ${operation}.${correlationId}`);
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Importer: empty queue: ${operation}.${correlationId}`});
      throw new Error(`Empty queue ${operation}.${correlationId}`);
      //return startCheck();

    } catch (error) {
      logger.error('app/handleItemImporting errored:');
      logError(error);
      // eslint-disable-next-line functional/no-conditional-statement
      await sendErrorResponses({error, correlationId, queue: `${operation}.${correlationId}`, mongoOperator, prio});

      return startCheck();
    }

  }

  async function handleItemInQueue(item, mongoOperator) {
    logger.silly(`app/handleItemInQueue: QueueItem: ${JSON.stringify(item)}`);
    await mongoOperator.setState({correlationId: item.correlationId, state: QUEUE_ITEM_STATE.IMPORTER.IMPORTING});
    return startCheck();
  }

  async function sendErrorResponses({error, correlationId, queue, mongoOperator, prio = false}) {
    logger.debug('app/sendErrorResponses: Sending error responses');

    // rawChunk: get next chunk of 100 messages {headers, messages} where cataloger is the same
    // no need for transforming messages to records
    const {messages} = await amqpOperator.checkQueue({queue, style: 'basic', toRecords: false, purge: false});

    if (messages) { // eslint-disable-line functional/no-conditional-statement
      logger.debug(`Got back messages (${messages.length}) for ${correlationId} from ${queue}`);

      const responseStatus = error.status ? error.status : httpStatus.INTERNAL_SERVER_ERROR;
      const responsePayload = error.payload ? error.payload : 'unknown error';

      logger.silly(`app/sendErrorResponses Status: ${responseStatus}, Messages: ${messages.length}, Payloads: ${responsePayload}`);
      // Send response back if PRIO
      // Send responses back if BULK and error is something else than 503

      // eslint-disable-next-line no-extra-parens
      if (prio || (!prio && error.status !== 503)) { // eslint-disable-line functional/no-conditional-statement

        await amqpOperator.ackMessages(messages);
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: responsePayload, errorStatus: responseStatus});
        return;
      }

      // Nack messages and sleep, if BULK and error is 503
      if (!prio && error.status === 503) { // eslint-disable-line functional/no-conditional-statement
        await amqpOperator.nackMessages(messages);
        logger.debug(`app/sendErrorResponses Got 503 for bulk. Nack messages to try loading/polling again after sleeping ${error503WaitTime} ms`);
        await setTimeoutPromise(error503WaitTime);
        return;
      }

      throw new Error('app/sendErrorMessages: What to do with these error responses?');
    }
    logger.debug(`app/sendErrorResponses: Did not get back any messages: ${messages} from ${queue}`);
    // should this throw an error?
  }
}
