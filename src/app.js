/* eslint-disable no-unused-vars */

import {Utils} from '@natlibfi/melinda-commons';
import {amqpFactory, mongoFactory, logError, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {promisify} from 'util';
import {datastoreFactory} from './interfaces/datastore';

export default async function ({
	amqpUrl, operation, pollWaitTime, mongoUri,
	recordLoadApiKey, recordLoadLibrary, recordLoadUrl
}) {
	const setTimeoutPromise = promisify(setTimeout);
	const {createLogger} = Utils;
	const logger = createLogger(); // eslint-disable-line no-console
	const OPERATION_TYPES = [OPERATIONS.CREATE, OPERATIONS.UPDATE];
	let amqpOperator;
	let mongoOperator;
	let datastoreOperator;

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
			datastoreOperator = datastoreFactory(recordLoadApiKey, recordLoadLibrary, recordLoadUrl);

			// Start loop
			await checkAmqpQueue();
		} catch (error) {
			logError(error);
			process.exit(1);
		}
	}

	async function checkAmqpQueue(queue = operation, recordLoadParams = {}) {
		const {headers, records, messages} = await amqpOperator.checkQueue(queue, 'basic', false);

		if (headers && records) {
			try {
				// Work with results
				if (OPERATION_TYPES.includes(queue)) {
					const status = (headers.operation === OPERATIONS.CREATE) ? 'CREATED' : 'UPDATED';
					const results = await datastoreOperator.set({...headers, records, recordLoadParams});

					// Handle separation of all ready done records
					const ack = (results.ackOnlyLength === undefined) ? messages.splice(0) : messages.splice(0, results.ackOnlyLength);
					await amqpOperator.ackNReplyMessages({status, messages: ack, payloads: results.payloads});

					if (messages.lenght > 0) {
						await amqpOperator.nackMessages(messages);
					}

					return checkAmqpQueue();
				}

				// Could send confirmation back to record load api that ids are saved to db and it is ok to clear the files
				const results = await datastoreOperator.set({correlationId: queue, ...headers, records, recordLoadParams});
				await mongoOperator.pushIds({correlationId: queue, ids: results.payloads});

				// Handle separation of all ready done records
				const ack = (results.ackOnlyLength === undefined) ? messages.splice(0) : messages.splice(0, results.ackOnlyLength);
				await amqpOperator.ackMessages(ack);

				if (messages.lenght > 0) {
					await amqpOperator.nackMessages(messages);
				}

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
			const messagesAmount = await amqpOperator.checkQueue(queueItem.correlationId, 'messages', false);
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
