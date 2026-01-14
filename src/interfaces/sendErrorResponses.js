import httpStatus from 'http-status';
import {QUEUE_ITEM_STATE, IMPORT_JOB_STATE} from '@natlibfi/melinda-rest-api-commons';
import {createLogger} from '@natlibfi/melinda-backend-commons';
import {promisify} from 'util';

export async function sendErrorResponses({error, correlationId, queue, mongoOperator, prio, error503WaitTime, amqpOperator, operation}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);

  logger.debug('app/sendErrorResponses: Sending error responses');
  logger.debug(`error: ${JSON.stringify(error)}`);
  logger.debug(`correalationId: ${correlationId}, queue: ${queue}`);

  // get next chunk of 100 messages {headers, messages} where cataloger is the same
  // no need for transforming messages to records
  const {messages} = await amqpOperator.checkQueue({queue, style: 'basic', toRecords: false, purge: false});

  if (messages) {
    logger.debug(`Got back messages (${messages.length}) for ${correlationId} from ${queue}`);
    logger.debug(`${JSON.stringify(error)}`);
    const responseStatus = error.status ? error.status : httpStatus.INTERNAL_SERVER_ERROR;
    const responsePayload = error.payload ? error.payload : 'Unknown error';

    logger.silly(`app/sendErrorResponses Status: ${responseStatus}, Messages: ${messages.length}, Payloads: ${responsePayload}`);
    // Send response back if PRIO
    // Send responses back if BULK and error is something else than 503

    if (prio || (!prio && error.status !== 503 && error.status !== 422)) {

      await amqpOperator.ackMessages(messages);
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
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
