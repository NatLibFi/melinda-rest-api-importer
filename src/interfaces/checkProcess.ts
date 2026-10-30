import httpStatus from 'http-status';
import {promisify, inspect} from 'util';
import {createLogger, Error as CommonsError, toAlephId} from '@natlibfi/melinda-commons';
import type {Error as CommonsApiError} from '@natlibfi/melinda-commons';
import {IMPORT_JOB_STATE, OPERATIONS, QUEUE_ITEM_STATE, createRecordResponseItem, addRecordResponseItems, mongoLogFactory} from '@natlibfi/melinda-rest-api-commons';
import {logError} from '@natlibfi/melinda-rest-api-commons';
import type {AmqpOperator, LogItem, MongoOperator} from '@natlibfi/melinda-rest-api-commons';
import type {GetMessage, Message, MessagePropertyHeaders} from 'amqplib';
import processOperatorFactory from './processPoll.ts';
import type {LoadProcessReport, ProcessParamsData, ProcessPollResults} from './processPoll.ts';

// The published commons .d.ts constructor requires two arguments (found in C4), but
// the runtime constructor accepts one - separateMessages / handleProcessMessageError
// below throw with a single argument. Boundary-cast per the utils.ts (C4) precedent.
const ApiError = CommonsError as unknown as new (status: number, payload?: unknown) => InstanceType<typeof CommonsError>;

// Structural union for any error the handleProcessMessage try block can throw
// (an ApiError or a plain thrown Error); `message` is read in the unexpected branch.
type HandledProcessMessageError = CommonsApiError | {status?: number; payload?: unknown; message?: string};

// processParams as parsed from the AMQP PROCESS message content (H5)
export interface ProcessParams {
  data: ProcessParamsData;
}

// success shape of checkProcessQueue / handleProcessMessage
export interface ProcessQueueResults {
  processPollResults: ProcessPollResults;
  processParams: ProcessParams;
}

export default async function ({amqpOperator, recordLoadApiKey, recordLoadUrl, error503WaitTime, keepLoadProcessReports, mongoUri}: {
  amqpOperator: AmqpOperator;
  recordLoadApiKey: string;
  recordLoadUrl: string;
  error503WaitTime: number;
  keepLoadProcessReports: string;
  mongoUri: string;
}) {
  const logger = createLogger();
  const setTimeoutPromise = promisify(setTimeout);
  const processOperator = processOperatorFactory({recordLoadApiKey, recordLoadUrl});
  const mongoLogOperator = await mongoLogFactory(mongoUri);

  return {checkProcessQueueStart};

  async function checkProcessQueueStart({correlationId, operation, mongoOperator, prio}: {
    correlationId: string;
    operation: string;
    mongoOperator: MongoOperator;
    prio: boolean;
  }): Promise<boolean> {

    logger.silly(`loopCheck -> checkProcessQueue for ${operation} (${correlationId})`);
    const processQueueResults = await checkProcessQueue({correlationId, operation, mongoOperator, prio});
    // handle checkProcessQueue errors ???
    // if checkProcessQueue errored with error that's not an ApiError, processMessage is not acked/nacked
    logger.silly(`processQueueResults: ${JSON.stringify(processQueueResults)}`);

    if (!processQueueResults) {
      // false: there's a process in process queue that is not ready yet (importJobState: IN_PROCESS) or
      // false: processQueue errored (queueItemState was set to ERROR)
      return false;
    }

    await handleProcessQueueResults({processQueueResults, correlationId, operation, mongoOperator});

    await setTimeoutPromise(100); // (S)Nack time!

    // results: {payloads: {handledIds: handledIdList, rejectedIds: rejectedIdList, loadProcessReport}, ackOnlyLength: processedAmount};
    // if process poll results resulted in less processed results than processes recordAmount -> ackOnlyLength is recordAmount

    const {processPollResults, processParams} = processQueueResults;
    logger.silly(`loopCheck -> handleMessages`);
    // HandleMessages returns false if there are no messages in queue to handle
    // HandleMessages returns true, if there were messages and they were handled
    const messagesHandled = await handleMessages({operation, results: processPollResults, processParams, queue: `${operation}.${correlationId}`, mongoOperator, prio});
    logger.silly(`messagesHandled: ${messagesHandled}`);

    if (messagesHandled) {
      logger.verbose(`Requesting file cleaning for ${JSON.stringify(processParams.data)}`);
      // aleph-record-load-api probably doesn't actually do this...
      await processOperator.requestFileClear(processParams.data);
      return true;
    }

    // There was a message in processQueue for queueItem, but no messages in operationQueue for correlation id
    throw new ApiError(httpStatus.UNPROCESSABLE_ENTITY, `No messages in ${operation}.${correlationId}`);
  }

  async function handleProcessQueueResults({processQueueResults, correlationId, operation, mongoOperator}: {
    processQueueResults: ProcessQueueResults;
    correlationId: string;
    operation: string;
    mongoOperator: MongoOperator;
  }): Promise<void> {
    logger.silly(`processQueueResults: ${JSON.stringify(processQueueResults)}`);
    const results = processQueueResults.processPollResults;
    logger.silly(`results: ${JSON.stringify(results)}`);
    const {processParams} = processQueueResults;
    logger.silly(`processParams: ${JSON.stringify(processParams)}`);

    const {handledIds, erroredAmount} = results.payloads;
    // H3: the pollFix-shaped results lack rejectedIds / loadProcessReport; the
    // CREATE/UPDATE (pollLoad) shape that reaches here (the FIX shape returns
    // earlier) always has both. Boundary-cast at the poll boundary; the runtime
    // values are unchanged.
    const rejectedIds = results.payloads.rejectedIds as string[];
    const loadProcessReport = results.payloads.loadProcessReport as LoadProcessReport;
    const {status} = results;

    if (([OPERATIONS.FIX] as string[]).includes(operation)) {
      // DEVELOP: should we log process for FIXes too?
      logger.debug(`Status: ${status}`);
      return;
    }

    if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation)) {
      // Assume that all messages are for same correlation id, no need to pushId for every message
      logger.silly(`Pushing load process results to mongo for ${operation} ${correlationId}.`);

      // The published PushIdsParams (6.2) does not declare erroredAmount (it is
      // ignored by the runtime) - boundary-cast at the mongo edge.
      await mongoOperator.pushIds({correlationId, handledIds, rejectedIds, erroredAmount} as {
        correlationId: string;
        handledIds: string[];
        rejectedIds: string[];
        erroredAmount?: number | undefined;
      });

      // The 6.2 LogItem type does not declare the loadProcessReport fields -
      // boundary-cast at the mongo edge.
      const loadProcessLogItem = {
        logItemType: 'LOAD_PROCESS_LOG',
        correlationId,
        ...loadProcessReport
      } as LogItem;

      logger.silly(`${inspect(loadProcessLogItem, {depth: 6})}`);
      const result = mongoLogOperator.addLogItem(loadProcessLogItem);
      logger.silly(result);

      const keepLoadProcessReport = checkLoadProcessReport(keepLoadProcessReports, loadProcessReport);
      if (keepLoadProcessReport) {
        await mongoOperator.pushMessages({correlationId, messageField: 'loaderProcessReports', messages: [loadProcessReport]});
        return;
      }

      return;
    }
    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Unknown OPERATION: ${operation} in ${correlationId}`);
  }

  function checkLoadProcessReport(keepLoadProcessReports: string, loadProcessReport: LoadProcessReport): boolean {
    logger.silly(`Checking if loadProcessReport should be kept.`);

    if (keepLoadProcessReports === 'ALL') {
      logger.debug(`Keeping loadProcessReport. (${keepLoadProcessReports})`);
      return true;
    }
    if (keepLoadProcessReports === 'NON_PROCESSSED' && !loadProcessReport.processedAll) {
      logger.debug(`Keeping loadProcessReport. (${keepLoadProcessReports}): processedAll: ${loadProcessReport.processedAll}`);
      return true;
    }
    if (keepLoadProcessReports === 'NON_HANDLED' && loadProcessReport.handledAmount < loadProcessReport.recordAmount) {
      logger.debug(`Keeping loadProcessReport. (${keepLoadProcessReports}): ${loadProcessReport.handledAmount}/${loadProcessReport.recordAmount}`);
      return true;
    }
    logger.debug(`Not keeping loadProcessReport. (${keepLoadProcessReports}): ${loadProcessReport.processedAll}, ${loadProcessReport.handledAmount}/${loadProcessReport.recordAmount}`);
    return false;
  }

  async function checkProcessQueue({correlationId, operation, mongoOperator, prio, waited = false}: {
    correlationId: string;
    operation: string;
    mongoOperator: MongoOperator;
    prio: boolean;
    waited?: boolean;
  }): Promise<ProcessQueueResults | false | undefined> {
    const processQueue = `PROCESS.${operation}.${correlationId}`;
    logger.silly(`Checking process queue: ${processQueue} for ${correlationId}`);

    let processMessage: GetMessage | false | undefined;
    try {
      const checkQueueResult = await amqpOperator.checkQueue({queue: processQueue, style: 'one', toRecord: false, purge: false});
      // H2: checkQueue returns CheckQueueResult = number | boolean |
      // QueueItemMessagePayload | QueueItemRecordPayload | GetMessage. With
      // style 'one' + toRecord: false the runtime returns the raw GetMessage or
      // `false` (empty queue) - narrow via the `content` property, which only
      // GetMessage has (the `typeof === 'object'` step drops the primitive
      // members so the `in` check typechecks).
      processMessage = checkQueueResult && typeof checkQueueResult === 'object' && 'content' in checkQueueResult ? checkQueueResult : false;

      if (processMessage) {
        logger.silly(`We have processMessage: ${processMessage}`);
        logger.silly(`checkProcessQueue -> handleProcessMessage`);

        // handleProcessMessage returns: {results, processParams} if successful - ack processMessage
        // false if process is locked - nack processMessage
        // otherwise throws error
        const processMessageResult = await handleProcessMessage(operation, processMessage, correlationId, prio);
        return processMessageResult;
      }

      // wait and check once again, because we should have a message!
      if (!waited) {
        logger.debug(`app/checkProcessQueue: Waiting 200 ms and trying again`);
        await setTimeoutPromise(200); // Let's see if we have messages after waiting
        return checkProcessQueue({correlationId, operation, mongoOperator, prio, waited: true});
      }

      // This could remove empty PROCESS.operation.correlationId queue
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Empty ${processQueue} queue`);

    } catch (error) {

      logger.debug(`checkProcessQueue for queue ${operation} ${correlationId} errored: ${error}`);
      logError(error);

      if (error instanceof ApiError) {

        // We error the whole queueItem here
        await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
        await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: error.payload, errorStatus: error.status});

        // If we errored because we didn't have a process message, let's not try to ack non-existing message
        if (processMessage) {
          await amqpOperator.ackMessages([processMessage]);
          await setTimeoutPromise(100);
          return false;
        }

        return false;
      }
      // processMessage get un-(n)acked in this case?
      logger.debug(`Error is not ApiError, not sending error responses`);
      // H6: intentional fallthrough - a non-ApiError error leaves the queueItem
      // unerrored; make the `undefined` return explicit.
      return undefined;
    }
  }

  async function handleProcessMessage(operation: string, processMessage: GetMessage, correlationId: string, prio: boolean): Promise<ProcessQueueResults | false> {
    logger.silly(`handleProcessMessage: ${JSON.stringify(processMessage)} for ${correlationId}`);
    try {
      // H5: JSON.parse boundary - the AMQP PROCESS content is the serialized
      // {data: ProcessParamsData} payload.
      const processParams: ProcessParams = await JSON.parse(processMessage.content.toString());
      logger.silly(`handleProcessMessage:processParams: ${JSON.stringify(processParams)}`);

      // Ask aleph-record-load-api about the process
      // We should ask differently for loadProcess/vs fixProcess?
      // H3: poll only returns undefined for an operation outside OPERATIONS;
      // operation here is always one of OPERATIONS, so the results are present.
      const processPollResults = (await processOperator.poll({operation, params: processParams.data})) as ProcessPollResults;
      logger.silly(`ProcessPoll results: ${JSON.stringify(processPollResults)}`);
      // should this check that results exist/are sane?

      await amqpOperator.ackMessages([processMessage]);
      await setTimeoutPromise(100);

      return {processPollResults, processParams};
    } catch (error) {

      // Boundary-cast at the catch: the try block only throws Error instances
      // (JSON.parse SyntaxError, ApiError from the poll/ack, driver errors).
      return handleProcessMessageError(error as HandledProcessMessageError, processMessage, amqpOperator, prio);
    }

    async function handleProcessMessageError(
      error: HandledProcessMessageError,
      processMessage: GetMessage,
      amqpOperator: AmqpOperator,
      prio: boolean
    ): Promise<false> {
      if (error instanceof ApiError) {
        if (error.status === httpStatus.LOCKED) {
          // Nack message and loop back if process was ongoing
          await amqpOperator.nackMessages([processMessage]);
          logger.silly('Process in progress @ server, back to loop!');
          return false;
        }
        if (error.status === httpStatus.SERVICE_UNAVAILABLE) {
          await amqpOperator.nackMessages([processMessage]);
          logger.silly(`Server temporarily unavailable, sleeping ${error503WaitTime} and back to loop!`);
          await setTimeoutPromise(error503WaitTime);
          return false;
        }

        if (error.status === httpStatus.BAD_REQUEST) {
          if (prio) {
            throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
          }
          throw new ApiError(error.status, 'Bad request from record-load-api');
        }
        logger.error('checkProcess/handleProcessMessage errored');
        logError(error);
        // should we ack processMessage for other errors than LOCKED/SERVICE_UNAVAILABEL?
        // processMessage un-(n)acked
        throw error;
      }
      // Unexpected
      // processMessage un-(n)acked
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, error.message);
    }
  }

  async function handleMessages({mongoOperator, operation, results, processParams, queue, prio}: {
    mongoOperator: MongoOperator;
    operation: string;
    results: ProcessPollResults;
    processParams: ProcessParams;
    queue: string;
    prio: boolean;
  }): Promise<boolean> {
    const {correlationId} = processParams.data;
    logger.debug(`handleMessages for ${operation} ${correlationId}`);
    logger.silly(`handleMessages for ${JSON.stringify(results)}, ${JSON.stringify(JSON.stringify(processParams))}, ${queue}, ${correlationId}`);
    logger.silly(`Check queue: ${JSON.stringify(queue)}`);
    // note: headers are headers for the first message in chunk
    const checkQueueResult = await amqpOperator.checkQueue({queue, style: 'basic', toRecord: false, purge: false});
    // H2: checkQueue returns CheckQueueResult = number | boolean |
    // QueueItemMessagePayload | QueueItemRecordPayload | GetMessage. With
    // style 'basic' + toRecord: false the runtime returns a
    // QueueItemMessagePayload {headers, messages} (or false for an empty queue).
    // The `typeof === 'object'` step drops the primitive members so the `in`
    // check narrows to the payload (C8 precedent in sendErrorResponses.ts); the
    // existing `if (!messages)` guard below stays the effective type guard.
    const firstMessageHeaders = checkQueueResult && typeof checkQueueResult === 'object' && 'messages' in checkQueueResult ? checkQueueResult.headers : undefined;
    const messages = checkQueueResult && typeof checkQueueResult === 'object' && 'messages' in checkQueueResult ? checkQueueResult.messages : undefined;
    // checkQueue returns plain false for an empty queue: guard before any use of messages
    if (!messages) {
      logger.verbose(`No messages in ${queue} to handle. Continuing the loop`);
      return false;
    }

    logger.silly(`firstMessageHeaders: ${JSON.stringify(firstMessageHeaders)}, messages: ${messages.length}`);
    logger.silly(`messages: ${JSON.stringify(messages)}`);

    logger.verbose(`Handling ${operation}.${correlationId} messages (total: ${messages.length}) based on results got from process polling`);
    logger.debug(`${JSON.stringify(results)}`);
    logger.silly(`${JSON.stringify(results.ackOnlyLength)}`);
    // Handle separation of all ready done records
    const ackMessages = await separateMessages(messages, results.ackOnlyLength);

    if (ackMessages === undefined || ackMessages.length < 1) {
      // If there are no messages to ack, continue the loop
      logger.verbose(`There was no messages to ack!!!`);
      return false;
    }

    return handleMessagesBoth({operation, messages: ackMessages, results, queue, correlationId, mongoOperator, amqpOperator, prio});
  }

  async function separateMessages(messages: Message[], ackOnlyLength: number): Promise<Message[] | undefined> {
    //logger.silly(`We have ${messages.length} messages to separate`);
    //logger.silly(`We want to ack  ${ackOnlyLength} messages`);
    if (!ackOnlyLength || ackOnlyLength > messages.length) {
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR);
    }
    const ack = messages.slice(0, ackOnlyLength);
    const nack = messages.slice(ackOnlyLength);
    logger.debug(`Message separation: ack: ${ack.length}, nack: ${nack.length}`);
    await amqpOperator.nackMessages(nack);
    await setTimeoutPromise(100); // (S)Nack time!
    return ack;
  }

  async function handleMessagesBoth({operation, messages, results, queue, correlationId, mongoOperator, amqpOperator, prio}: {
    operation: string;
    messages: Message[];
    results: ProcessPollResults;
    queue: string;
    correlationId: string;
    mongoOperator: MongoOperator;
    amqpOperator: AmqpOperator;
    prio: boolean;
  }): Promise<boolean> {
    logger.debug(`Acking for ${messages.length} messages.`);
    logger.silly(JSON.stringify(results));

    await createRecordResponses({messages, operation, mongoOperator, correlationId, results});
    // ack messages
    await amqpOperator.ackMessages(messages);

    // handle queueItem and amqp queues
    if (prio) {
      const {prioStatus, prioPayloads} = getPrioStatusAndPayloads(operation, results);
      return prioEnd({prioStatus, prioPayloads, correlationId, operation, mongoOperator, amqpOperator});
    }
    return bulkEnd({correlationId, operation, queue, mongoOperator, amqpOperator});

  }

  function getPrioStatusAndPayloads(operation: string, results: ProcessPollResults): {prioStatus: number | string; prioPayloads: string} {
    const {handledIds, rejectedIds, erroredAmount} = results.payloads;
    // processPoll returns the record-load-api status as results.status
    const {status} = results;
    const prioStatus = getPrioStatus(operation, status, handledIds, erroredAmount);
    // H3: rejectedIds is optional on the pollFix-shaped results; only the
    // CREATE/UPDATE branch inside reads it, and that branch always has it
    // present. Boundary-cast; the runtime value is unchanged.
    const prioPayloads = getPrioPayloads(operation, handledIds, rejectedIds as string[]);

    return {prioStatus, prioPayloads};

    function getPrioStatus(operation: string, status: number | undefined, handledIds: string[] | undefined = undefined, erroredAmount: number | undefined = undefined): number | string {
      // OraErrors: return 503 so that the client can try again
      if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation) && erroredAmount !== undefined && erroredAmount > 0) {
        return httpStatus.SERVICE_UNAVAILABLE;
      }
      // We did not get an id, probably rejected
      if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation) && handledIds !== undefined && handledIds.length < 1) {
        return httpStatus.UNPROCESSABLE_ENTITY;
      }

      if (operation === OPERATIONS.CREATE) {
        return 'CREATED';
      }
      if (operation === OPERATIONS.UPDATE) {
        return 'UPDATED';
      }
      if (operation === OPERATIONS.FIX) {
        if (status === httpStatus.CONFLICT) {
          return 'UNKNOWN';
        }
        return 'FIXED';
      }
      throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Unknown OPERATION: ${operation}.`);
    }

    function getPrioPayloads(operation: string, handledIds: string[], rejectedIds: string[]): string {
      if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation)) {
        return handledIds[0] || rejectedIds[0] || 'No loadProcess information for record';
      }
      return handledIds[0] || 'No loadProcess information for record';
    }

  }

  function createRecordResponses({messages, operation, mongoOperator, correlationId, results}: {
    messages: Message[];
    operation: string;
    mongoOperator: MongoOperator;
    correlationId: string;
    results: ProcessPollResults;
  }): Promise<void> | undefined {
    // separate handling of UPDATE/CREATE loadProcess and FIX fixProcessResults
    if (([OPERATIONS.CREATE, OPERATIONS.UPDATE] as string[]).includes(operation)) {
      return createRecordResponsesForLoad({messages, operation, mongoOperator, correlationId, results});
    }
    if (([OPERATIONS.FIX] as string[]).includes(operation)) {
      return createRecordResponsesForFix({messages, rlaStatus: results.status, operation, mongoOperator, correlationId});
    }
    // H6: intentional fallthrough - an unknown operation creates no record
    // responses; make the `undefined` return explicit.
    return undefined;
  }

  async function createRecordResponsesForFix({messages, rlaStatus, operation, mongoOperator, correlationId}: {
    messages: Message[];
    rlaStatus: number | undefined;
    operation: string;
    mongoOperator: MongoOperator;
    correlationId: string;
  }): Promise<void> {
    logger.debug(`Creating recorResponses for FIX: ${messages.length}, ${rlaStatus}, ${operation}`);
    const status = rlaStatus === httpStatus.OK ? 'FIXED' : 'UNKNOWN';
    const recordResponseItems = await messages.map((message) => {
      logger.silly(JSON.stringify(message));
      // AMQP headers are always set on these messages by the importer itself;
      // boundary-cast amqplib's optional `headers`.
      const {id, recordMetadata, notes} = message.properties.headers as MessagePropertyHeaders;
      const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';
      //DEVELOP: get here Removed / Recovered / Fixed depending on FIX-operations pFixType
      const responsePayload = rlaStatus === httpStatus.OK ? {message: `${notesString}Fixed record ${id}.`} : {message: `${notesString}Tried to fix record ${id}. Result unknown.`};

      return createRecordResponseItem({responseStatus: status, responsePayload, recordMetadata, id});
    });

    addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
    return;
  }

  async function createRecordResponsesForLoad({messages, operation, mongoOperator, correlationId, results}: {
    messages: Message[];
    operation: string;
    mongoOperator: MongoOperator;
    correlationId: string;
    results: ProcessPollResults;
  }): Promise<void> {
    //{"payloads":{"handledIds":["000999999"],"rejectedIds":[],"loadProcessReport":{"status":200,"processId":31930,"processedAll":false,"recordAmount":2,"processedAmount":1,"handledAmount":1,"rejectedAmount":0,"rejectMessages":[]}},"ackOnlyLength":2, "handledAll": false}
    const {handledIds, erroredAmount} = results.payloads;
    // H3: the pollFix-shaped results lack rejectedIds / loadProcessReport; this
    // function only runs for the CREATE/UPDATE (pollLoad) shape, where both are
    // always present. Boundary-cast; the runtime values are unchanged.
    const rejectedIds = results.payloads.rejectedIds as string[];
    const {handledAll} = results.payloads.loadProcessReport as LoadProcessReport;

    logger.silly(`${messages.length}, ${operation}, ${handledAll}, ${handledIds.length}, ${rejectedIds.length}, ${erroredAmount}`);

    if (operation === OPERATIONS.CREATE) {
      if (handledAll) {
        await createRecordResponsesForCreateOperationHandledAll({mongoOperator, correlationId, messages, handledIds});
        return;
      }
      await createRecordResponsesForCreateOperationNotHandledAll({mongoOperator, correlationId, messages, handledIds, rejectedIds, erroredAmount});
      return;
    }

    if (operation === OPERATIONS.UPDATE) {
      await createRecordResponsesForUpdateOperation({mongoOperator, correlationId, messages, handledAll, handledIds, rejectedIds});
      return;
    }

    throw new ApiError(httpStatus.INTERNAL_SERVER_ERROR, `Unknown OPERATION: ${operation} in ${correlationId}`);

    async function createRecordResponsesForCreateOperationHandledAll({mongoOperator, correlationId, messages, handledIds}: {
      mongoOperator: MongoOperator;
      correlationId: string;
      messages: Message[];
      handledIds: string[];
    }): Promise<void> {
      // CREATEs which have handledId for all records and no oraErrors
      logger.debug(`We have a CREATE operation which handled all records normally.`);
      const status = 'CREATED';
      const recordResponseItems = await messages.map((message, index) => {
        logger.silly(JSON.stringify(message));
        // AMQP headers are always set on these messages by the importer itself;
        // boundary-cast amqplib's optional `headers`.
        const {id, recordMetadata, notes} = message.properties.headers as MessagePropertyHeaders;
        const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';

        // H4: noUncheckedIndexedAccess - a real undefined here (malformed
        // record-load-api response) stringified as "undefined" before; keep that.
        const idFromHandledIds: string | undefined = handledIds[index];

        logger.debug(`headers.id: ${id} got handledId for ${index}: ${idFromHandledIds}`);
        const responsePayload = {message: `${notesString}Created record ${idFromHandledIds}.`};

        return createRecordResponseItem({responseStatus: status, responsePayload, recordMetadata, id: idFromHandledIds});
      });

      addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
      return;
    }

    async function createRecordResponsesForCreateOperationNotHandledAll({mongoOperator, correlationId, messages, handledIds, rejectedIds, erroredAmount}: {
      mongoOperator: MongoOperator;
      correlationId: string;
      messages: Message[];
      handledIds: string[];
      rejectedIds: string[];
      erroredAmount?: number;
    }): Promise<void> {

      // CREATEs which have a (possible) handled id for all records (and !handledAll because of oraErrors)
      if (handledIds.length === messages.length) {
        logger.debug(`We have a CREATE operation which did not handle all records the records normally, but has a possible id for all records.`);
        const status = 'CREATED';
        const recordResponseItems = await messages.map((message, index) => {
          logger.silly(JSON.stringify(message));
          // AMQP headers are always set on these messages by the importer itself;
          // boundary-cast amqplib's optional `headers`.
          const {id, recordMetadata, notes} = message.properties.headers as MessagePropertyHeaders;
          const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';

          // H4: noUncheckedIndexedAccess - a real undefined here (malformed
          // record-load-api response) stringified as "undefined" before; keep that.
          const idFromHandledIds: string | undefined = handledIds[index];
          logger.debug(`headers.id: ${id} got handledId for ${index}: ${idFromHandledIds}`);

          const errorRegex = /^ERROR-/u;
          if ((idFromHandledIds && errorRegex.test(idFromHandledIds)) || erroredAmount === handledIds.length) {
            const responsePayload = {message: `${notesString}Errored creating record ${idFromHandledIds}.`};
            return createRecordResponseItem({responseStatus: 'ERROR', responsePayload, recordMetadata, id: '000000000'});
          }

          const responsePayload = {message: `${notesString}Created record ${idFromHandledIds}.`};
          return createRecordResponseItem({responseStatus: status, responsePayload, recordMetadata, id: idFromHandledIds});
        });

        addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
        return;
      }

      logger.debug(`We have a CREATE operation which did not handle all records normally - other cases.`);
      // remove errorIds from handledIds
      const errorRegex = /^ERROR-/u;
      const possibleIds = handledIds.filter((id) => id && !errorRegex.test(id));

      // We could add rejetedRecords to the handledIds based on the blobSequence (CREATEs always have blobSequence as sysnro)
      // for each rejectId add an REJECT-<id> to handledIds[id-1]
      // and the we could solve ids for cases where handledIds.length + rejectedIds.length === messages.length

      const recordResponseItems = await messages.map((message) => {
        // AMQP headers are always set on these messages by the importer itself;
        // boundary-cast amqplib's optional `headers`.
        const {recordMetadata, notes} = message.properties.headers as MessagePropertyHeaders;
        const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';
        const {blobSequence} = recordMetadata;
        const alephSeqId = toAlephId(blobSequence.toString());

        if (rejectedIds.includes(alephSeqId)) {
          const rejectedStatus = 'INVALID';
          const responsePayload = {message: `${notesString}LoaderProcess rejected record ${alephSeqId}`};
          return createRecordResponseItem({responseStatus: rejectedStatus, recordMetadata, id: '000000000', responsePayload});
        }

        const status = 'UNKNOWN';
        const ids = possibleIds;
        const responsePayload = {
          message: `LoaderProcess did not return databaseIds for all records in chunk.`,
          ids
        };

        return createRecordResponseItem({responseStatus: status, recordMetadata, id: '000000000', responsePayload});
      });
      await addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
      return;
    }

    async function createRecordResponsesForUpdateOperation({mongoOperator, correlationId, messages, handledAll, handledIds, rejectedIds}: {
      mongoOperator: MongoOperator;
      correlationId: string;
      messages: Message[];
      handledAll: boolean;
      handledIds: string[];
      rejectedIds: string[];
    }): Promise<void> {

      const recordResponseItems = await messages.map((message, index) => {
        logger.silly(`${index}: ${JSON.stringify(message)}`);
        // AMQP headers are always set on these messages by the importer itself;
        // boundary-cast amqplib's optional `headers`.
        const {id, recordMetadata, notes} = message.properties.headers as MessagePropertyHeaders;
        const notesString = notes && Array.isArray(notes) && notes.length > 0 ? `${notes.join(' - ')} - ` : '';
        const paddedId = toAlephId(id);

        // Note: if a record is in chunk to be updated several times, all of them get status UPDATED
        if (handledIds.includes(paddedId)) {
          const responseStatus = 'UPDATED';
          const responsePayload = {message: `${notesString}Updated record ${paddedId}`};
          return createRecordResponseItem({responseStatus, recordMetadata, id, responsePayload});
        }

        if (rejectedIds.includes(paddedId)) {
          const responseStatus = 'INVALID';
          const responsePayload = {message: `${notesString}LoaderProcess rejected record ${paddedId}`};
          return createRecordResponseItem({responseStatus, recordMetadata, id, responsePayload});
        }

        if (handledAll) {
          const status = 'UNKNOWN';
          // H4: noUncheckedIndexedAccess - a real undefined here (malformed
          // record-load-api response) stringified as "undefined" before; keep that.
          const idFromHandledIds: string | undefined = handledIds[index];
          const ids = [idFromHandledIds];
          const responsePayload = {
            message: `${notesString}LoaderProcess did not return result for record ${paddedId}. It might have updated ${idFromHandledIds} instead.`,
            ids
          };
          return createRecordResponseItem({responseStatus: status, recordMetadata, id, responsePayload});
        }

        const status = 'UNKNOWN';
        const responsePayload = {message: `${notesString}LoaderProcess did not return result for record ${paddedId}`};
        return createRecordResponseItem({responseStatus: status, recordMetadata, id, responsePayload});
      });

      addRecordResponseItems({recordResponseItems, mongoOperator, correlationId});
      return;
    }
  }
  async function prioEnd({prioStatus, prioPayloads, correlationId, operation, mongoOperator, amqpOperator}: {
    prioStatus: number | string;
    prioPayloads: string;
    correlationId: string;
    operation: string;
    mongoOperator: MongoOperator;
    amqpOperator: AmqpOperator;
  }): Promise<boolean> {
    logger.silly(`prioEnd`);
    // failed prios
    if (prioStatus !== 'UPDATED' && prioStatus !== 'CREATED' && prioStatus !== 'FIXED') {
      logger.debug(`prioStatus: failed ${prioStatus}`);
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.ERROR});
      await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.ERROR, errorMessage: prioPayloads, errorStatus: prioStatus});
      removeImporterQueues({amqpOperator, operation, correlationId});
      return true;
    }
    logger.debug(`prioStatus: successfull ${prioStatus}`);
    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
    await mongoOperator.setState({correlationId, state: QUEUE_ITEM_STATE.DONE});
    logger.silly(`Trying to remove queues`);
    removeImporterQueues({amqpOperator, operation, correlationId});
    logger.silly(`prioEnd done, returning true`);
    return true;
  }

  async function bulkEnd({correlationId, operation, queue, mongoOperator, amqpOperator}: {
    correlationId: string;
    operation: string;
    queue: string;
    mongoOperator: MongoOperator;
    amqpOperator: AmqpOperator;
  }): Promise<boolean> {
    // If Bulk queue has more records/messages waiting in the queue resume to them.
    //logger.silly(`Checking remaining items in ${queue}`);
    // H2: checkQueue with style 'messages' returns the raw queue messageCount
    // (a number) - boundary-cast at the AMQP edge.
    const queueMessagesCount = (await amqpOperator.checkQueue({queue, style: 'messages'})) as number;
    //logger.silly(`Remaining items in ${queue}: ${queueItemsCount}`);

    if (queueMessagesCount > 0) {
      logger.debug(`All messages in ${queue} NOT handled.`);
      // Note: this assumes that all messages in the queue are related to the same correlationId
      await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.IMPORTING});
      return true;
    }

    logger.debug(`All messages in ${queue} handled`);

    // Note: cases, where aleph-record-load-api has rejected all or some records get state DONE here
    // Note: this assumes that all messages in the queue are related to the same correlationId

    await mongoOperator.setImportJobState({correlationId, operation, importJobState: IMPORT_JOB_STATE.DONE});
    removeImporterQueues({amqpOperator, operation, correlationId});

    return true;
  }

  function removeImporterQueues({amqpOperator, operation, correlationId}: {
    amqpOperator: AmqpOperator;
    operation: string;
    correlationId: string;
  }): void {
    const operationQueue = `${operation}.${correlationId}`;
    const processQueue = `PROCESS.${operation}.${correlationId}`;
    logger.debug(`Removeing queues: ${operationQueue}, ${processQueue}`);
    amqpOperator.removeQueue(operationQueue);
    amqpOperator.removeQueue(processQueue);
    return;
  }
}
