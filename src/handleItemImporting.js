import httpStatus from 'http-status';
import {logError, QUEUE_ITEM_STATE, IMPORT_JOB_STATE} from '@natlibfi/melinda-rest-api-commons';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {promisify} from 'util';

export function createItemImportingHandler(amqpOperator, mongoOperator, recordLoadOperator, {prio, error503WaitTime, recordLoadLibrary}) {
  const purgeQueues = false;
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);

  return handleItemImporting;

  async function handleItemImporting({item, operation}) {
    logger.silly(`Item in importing: ${JSON.stringify(item)}`);
    const {correlationId} = item;

    try {
      // basic: get next chunk of 100 messages {headers, records, messages}
      const {headers, records, messages} = await amqpOperator.checkQueue({queue: `${operation}.${correlationId}`, style: 'basic', toRecord: true, purge: purgeQueues});
      /// 1-100 messages from 1-10000 messages
      if (headers && messages) {
        await importRecords({headers, operation, records, messages, item, correlationId});
        return;
      }

      logger.debug(`app/handleItemImporting: No messages found in ${operation}.${correlationId}`);
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Importer: empty queue: ${operation}.${correlationId}`});
      throw new Error(`Empty queue ${operation}.${correlationId}`);
    } catch (error) {
      logger.error('app/handleItemImporting errored: ');
      logError(error);
      await sendErrorResponses({error, correlationId, queue: `${operation}.${correlationId}`, mongoOperator});

      return;
    }
  }

  async function importRecords({headers, operation, records, messages, item}) {
    logger.debug(`app/handleItemImporting: Headers: ${JSON.stringify(headers)}, Messages (${messages.length}), Records: ${records.length}`);
    const recordAmount = records.length;
    // recordLoadParams have pOldNew - is this used or is operation caught from importer?
    const {correlationId, recordLoadParams} = item;
    // messages nacked to wait results - should these go to some other queue PROCESS.correaltionId ?
    await amqpOperator.nackMessages(messages);
    await setTimeoutPromise(200); // (S)Nack time!
    // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}

    const {processId, pLogFile, pRejectFile, loaderProcessId} = await recordLoadOperator.loadRecord({correlationId, ...headers, records, recordLoadParams, prio});

    logger.silly(`app/handleItemImporting: setState and send to process queue`);

    // set here importJobState: {<OPERATION>: PROCESSING}
    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.PROCESSING});

    // send here to queue PROCESS.<OPERATION>.correlationId
    const processQueue = `PROCESS.${operation}.${correlationId}`;

    await amqpOperator.sendToQueue({
      queue: processQueue,
      correlationId,
      headers: {queue: `${operation}.${correlationId}`},
      data: {
        correlationId,
        pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
        processId, pRejectFile, pLogFile, loaderProcessId,
        recordAmount
      }
    });
    return;
  }

  async function sendErrorResponses({error, correlationId, queue, mongoOperator}) {
    logger.debug('app/sendErrorResponses: Sending error responses');

    // get next chunk of 100 messages {headers, messages} where cataloger is the same
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
      if (prio || (!prio && error.status !== 503)) {

        await amqpOperator.ackMessages(messages);
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: responsePayload, errorStatus: responseStatus});
        return;
      }

      // Nack messages and sleep, if BULK and error is 503
      if (!prio && error.status === 503) {
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
