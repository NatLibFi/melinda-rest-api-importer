/* eslint-disable no-unused-vars */

import ApiError, {Utils} from '@natlibfi/melinda-commons';
import {amqpFactory, mongoFactory, logError, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {promisify} from 'util';
import recordLoadFactory from './interfaces/datastore';
import processFactory from './interfaces/processPoll';

export default async function ({
	amqpUrl, operation, pollWaitTime, mongoUri,
	recordLoadApiKey, recordLoadLibrary, recordLoadUrl
}) {
	const setTimeoutPromise = promisify(setTimeout);
	const {createLogger} = Utils;
	const logger = createLogger(); // eslint-disable-line no-console
	const OPERATION_TYPES = [OPERATIONS.CREATE, OPERATIONS.UPDATE];
	const purgeQueues = false;
	let amqpOperator;
	let mongoOperator;
	let recordLoadOperator;
	let processOperator;

	const app = await initApp();
	// Soft shutdown function
	app.on('close', async () => {
		// Things that need soft shutdown
		// amqp disconnect?
		// mongo disconnect?
	});

	return app;

	async function initApp() {
		try {
			amqpOperator = await amqpFactory(amqpUrl);
			mongoOperator = await mongoFactory(mongoUri);
			recordLoadOperator = recordLoadFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
			processOperator = processFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);
			// Start loop
			await checkProcess(true);
		} catch (error) {
			logError(error);
			process.exit(1);
		}
	}

	async function checkProcess() {
		await checkProcessQueue(operation);

		const queueItem = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});
		if (queueItem) {
			await checkProcessQueue(queueItem.correlationId);
		}

		return checkAmqpQueue();
	}

	async function checkProcessQueue(queue) {
		let processMessage;
		try {
			processMessage = await amqpOperator.checkQueue('PROCESS.' + queue, 'raw', purgeQueues);
			if (processMessage) {
				const processParams = JSON.parse(processMessage.content.toString());
				const results = await processOperator.pollProcess(processParams.data);

				const {headers, messages} = await amqpOperator.checkQueue(queue, 'basic', purgeQueues);
				if (messages) {
					await handleMessages(results, headers, messages);
				}

				await processOperator.requestFileClear(processParams.data);
				await amqpOperator.ackMessages([processMessage]);
				await setTimeoutPromise(100); // (S)Nack time!

				return;
			}

			return;
		} catch (error) {
			if (error instanceof ApiError) {
				if (error.status === 423) {
					await amqpOperator.nackMessages([processMessage]);
					await setTimeoutPromise(pollWaitTime);

					return checkProcessQueue(queue);
				}

				if (error.status === 404) {
					await amqpOperator.ackMessages([processMessage]);

					return checkProcess();
				}
			}

			throw error;
		}

		async function handleMessages(results, headers, messages) {
			// Handle separation of all ready done records
			const ack = messages.splice(0, results.ackOnlyLength);
			await amqpOperator.nackMessages(messages);

			if (OPERATION_TYPES.includes(processMessage.properties.headers.queue)) {
				// Handle separation of all ready done records
				const status = (headers.operation === OPERATIONS.CREATE) ? 'CREATED' : 'UPDATED';
				await amqpOperator.ackNReplyMessages({status, messages: ack, payloads: results.payloads});

				return;
			}

			// Ids to mongo
			await mongoOperator.pushIds({correlationId: queue, ids: results.payloads});
			await amqpOperator.ackMessages(ack);
		}
	}

	async function checkAmqpQueue(queue = operation, recordLoadParams = {}) {
		const {headers, records, messages} = await amqpOperator.checkQueue(queue, 'basic', purgeQueues);

		if (headers && records) {
			await amqpOperator.nackMessages(messages);
			try {
				const {processId, correlationId} = (OPERATION_TYPES.includes(queue)) ? await recordLoadOperator.loadRecord({...headers, records, recordLoadParams, prio: true}) :
					await recordLoadOperator.loadRecord({correlationId: queue, ...headers, records, recordLoadParams, prio: false});

				// Send to process queue {queue, correlationId, headers, data}
				await amqpOperator.sendToQueue({
					queue: 'PROCESS.' + queue, correlationId: correlationId, headers: {queue}, data: {
						correlationId: correlationId,
						pActiveLibrary: recordLoadParams.pActiveLibrary || recordLoadLibrary,
						processId
					}
				});

				await checkProcess();

				return checkAmqpQueue();
			} catch (error) {
				logError(error);
				if (OPERATION_TYPES.includes(queue)) {
					// Send response back if PRIO
					const status = error.status;
					const payloads = (error.payload) ? new Array(messages.lenght).fill(error.payload) : [];
					await amqpOperator.ackNReplyMessages({status, messages, payloads});

					return checkAmqpQueue();
				}

				// Return bulk stuff back to queue
				await amqpOperator.nackMessages(messages);
				await setTimeoutPromise(200); // (S)Nack time!
				return checkAmqpQueue();
			}
		}

		if (!OPERATION_TYPES.includes(queue)) {
			mongoOperator.setState({correlationId: queue, state: QUEUE_ITEM_STATE.ERROR});
			return checkAmqpQueue();
		}

		return checkMongoDB();
	}

	async function checkMongoDB() {
		let queueItem = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});
		if (!queueItem) {
			queueItem = await mongoOperator.getOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_QUEUE});
		}

		if (queueItem) {
			const messagesAmount = await amqpOperator.checkQueue(queueItem.correlationId, 'messages', purgeQueues);
			if (messagesAmount) {
				if (queueItem.queueItemState === QUEUE_ITEM_STATE.IN_QUEUE) {
					logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.IN_PROCESS})));
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

		await setTimeoutPromise(pollWaitTime);
		return checkAmqpQueue();
	}
}
