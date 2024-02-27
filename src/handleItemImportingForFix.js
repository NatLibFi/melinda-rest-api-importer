import httpStatus from 'http-status';
import {logError, QUEUE_ITEM_STATE, IMPORT_JOB_STATE} from '@natlibfi/melinda-rest-api-commons';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {promisify} from 'util';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons/dist/constants';

export function createItemImportingFixHandler(amqpOperator, mongoOperator, recordLoadOperator, {prio, error503WaitTime, recordLoadLibrary}) {
  const purgeQueues = false;
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);

  return handleItemImportingFix;

  async function handleItemImportingFix({item, operation}) {
    logger.silly(`Item in importing: ${JSON.stringify(item)}`);
    const {correlationId} = item;
    if (![OPERATIONS.FIX].includes(operation)) {
      throw new Error(`Wrong operation ${operation}`);
    }
    try {
      // basic: get next chunk of 100 messages {headers, records, messages}
      // how do we get only recordId? Do we have in checkQueue just id in record?
      const {headers, messages} = await amqpOperator.checkQueue({queue: `${operation}.${correlationId}`, style: 'basic', toRecord: false, purge: purgeQueues});
      /// 1-100 messages from 1-10000 messages
      if (headers && messages) {
        await importFixRecords({headers, operation, messages, item, correlationId});
        return;
      }

      logger.debug(`handleItemImportingForFix: No messages found in ${operation}.${correlationId}`);
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorStatus: '500', errorMessage: `Importer: empty queue: ${operation}.${correlationId}`});
      throw new Error(`Empty queue ${operation}.${correlationId}`);
    } catch (error) {
      logger.error('handleItemImportingForFix errored: ');
      logger.silly(JSON.stringify(error));
      logError(error);
      await sendErrorResponses({error, correlationId, queue: `${operation}.${correlationId}`, mongoOperator});

      return;
    }
  }

  async function importFixRecords({headers, operation, messages, item}) {
    logger.debug(`importFixRecords: Headers: ${JSON.stringify(headers)}, Messages (${messages.length})`);
    const recordList = messages.map(message => message.properties.headers.id);

    //logger.debug(`${JSON.stringify(messages[0].properties)}`);
    const recordAmount = messages.length;
    const {correlationId, recordLoadParams} = item;

    // messages nacked to wait results - should these go to some other queue PROCESS.correaltionId ?
    await amqpOperator.nackMessages(messages);
    await setTimeoutPromise(200); // (S)Nack time! - we need this timeout here to catch errors from loadRecord
    // Response: {"correlationId":"97bd7027-048c-425f-9845-fc8603f5d8ce","pLogFile":null,"pRejectFile":null,"processId":12014}

    const {fixType} = headers.operationSettings;
    // what happens if recordLoadOperator errors?
    const {processId, loaderProcessId} = await recordLoadOperator.fixLoadRecord({correlationId, cataloger: headers.cataloger, fixType, recordList, recordLoadParams, prio});

    logger.silly(`handleItemImporting: setState and send to process queue`);

    // send here to queue PROCESS.<OPERATION>.correlationId
    const processQueue = `PROCESS.${operation}.${correlationId}`;
    logger.debug(`Sending process information for loading process ${processId} / ${loaderProcessId} to ${processQueue}`);

    // what happens if sendToQueue errors?
    await amqpOperator.sendToQueue({
      queue: processQueue,
      correlationId,
      headers: {queue: `${operation}.${correlationId}`},
      data: {
        correlationId,
        pActiveLibrary: recordLoadParams ? recordLoadParams.pActiveLibrary : recordLoadLibrary,
        processId, loaderProcessId,
        recordAmount
      }
    });
    await setTimeoutPromise(200); // (S)Nack time! - wait here to avoid polling of a process queue that would have not received process message yet

    // set here importJobState: {<OPERATION>: PROCESSING}
    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.PROCESSING});

    return;
  }

  // eslint-disable-next-line max-statements
  async function sendErrorResponses({error, correlationId, queue, mongoOperator}) {
    logger.debug('app/sendErrorResponses: Sending error responses');
    logger.silly(`error: ${JSON.stringify(error)}`);
    logger.silly(`correalationId: ${correlationId}, queue: ${queue}`);

    // get next chunk of 100 messages {headers, messages} where cataloger is the same
    // no need for transforming messages to records
    const {messages} = await amqpOperator.checkQueue({queue, style: 'basic', toRecords: false, purge: false});

    if (messages) { // eslint-disable-line functional/no-conditional-statements
      logger.debug(`Got back messages (${messages.length}) for ${correlationId} from ${queue}`);

      const responseStatus = error.status ? error.status : httpStatus.INTERNAL_SERVER_ERROR;
      const responsePayload = error.payload ? error.payload : 'unknown error';

      logger.silly(`app/sendErrorResponses Status: ${responseStatus}, Messages: ${messages.length}, Payloads: ${responsePayload}`);
      // Send response back if PRIO
      // Send responses back if BULK and error is something else than 503

      // eslint-disable-next-line no-extra-parens
      if (prio || (!prio && error.status !== 503 && error.status !== 422)) {

        await amqpOperator.ackMessages(messages);
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: responsePayload, errorStatus: responseStatus});
        return;
      }

      // ack messages, if BULK and error is 422
      if (!prio && error.status === 422) {
        await amqpOperator.ackMessages(messages);
        await setTimeoutPromise(200); // wait for messages to get acked
        logger.debug(`app/sendErrorResponses Got 422 for bulk. Ack messages to try loading/polling next batch`);
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
