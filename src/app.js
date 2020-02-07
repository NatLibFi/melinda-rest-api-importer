/* eslint-disable no-unused-vars */

import {Utils} from '@natlibfi/melinda-commons';
import {amqpFactory, mongoFactory, logError, QUEUE_ITEM_STATE, OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {AMQP_URL, OPERATION, POLL_WAIT_TIME, MONGO_URI} from './config';
import {promisify} from 'util';
import {datastoreFactory} from './interfaces/datastore';

const setTimeoutPromise = promisify(setTimeout);
const {createLogger, handleInterrupt} = Utils;
const OPERATION_TYPES = [OPERATIONS.CREATE, OPERATIONS.UPDATE];

run();

async function run() {
	const logger = createLogger(); // eslint-disable-line no-console
	registerSignalHandlers();
	logger.log('info', `Melinda-rest-import-queue has been started: ${OPERATION}`);
	const amqpOperator = await amqpFactory(AMQP_URL);
	const mongoOperator = await mongoFactory(MONGO_URI);
	const datastoreOperator = datastoreFactory();

	try {
		// Start loop
		await checkAmqpQueue(OPERATION);
	} catch (error) {
		logError(error);
		process.exit(0);
	}

	async function checkAmqpQueue(queue = OPERATION, recordLoadParams = {}) {
		const {headers, records, messages} = await amqpOperator.checkQueue(queue, 'basic', false);

		if (headers && records) {
			try {
				// Work with results
				if (OPERATION_TYPES.includes(queue)) {
					const status = (headers.operation === OPERATIONS.CREATE) ? 'CREATED' : 'UPDATED';
					const results = await datastoreOperator.set({...headers, records, recordLoadParams});

					// Handle separation of all ready done records
					if (results.ackOnlyLength === undefined) {
						await amqpOperator.ackNReplyMessages({status, messages, payloads: results.payloads});
					} else {
						const ack = messages.splice(0, results.ackOnlyLength);
						await amqpOperator.ackNReplyMessages({status, messages: ack, payloads: results.payloads});
						await amqpOperator.nackMessages(messages);
					}
				} else { // Could send confirmation back to record load api that ids are saved to db and it is ok to clear the files
					const results = await datastoreOperator.set({correlationId: queue, ...headers, records, recordLoadParams});
					await mongoOperator.pushIds({correlationId: queue, ids: results.payloads});

					// Handle separation of all ready done records
					if (results.ackOnlyLength === undefined) {
						await amqpOperator.ackMessages(messages);
					} else {
						const ack = messages.splice(0, results.ackOnlyLength);
						await amqpOperator.ackMessages(ack);
						await amqpOperator.nackMessages(messages);
					}
				}

				return checkAmqpQueue();
			} catch (error) {
				if (OPERATION_TYPES.includes(queue)) {
					// Send response back if PRIO
					const status = error.status;
					const payloads = (error.payload) ? new Array(messages.lenght).fill(error.payload) : [];
					await amqpOperator.ackNReplyMessages({status, messages, payloads});
				} else {
					// Return bulk stuff back to queue
					await amqpOperator.nackMessages(messages);
					await setTimeoutPromise(POLL_WAIT_TIME);
				}

				logError(error);
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
		let queueItem = await mongoOperator.getOne({operation: OPERATION, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});
		if (!queueItem) {
			queueItem = await mongoOperator.getOne({operation: OPERATION, queueItemState: QUEUE_ITEM_STATE.IN_QUEUE});
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
			} else {
				logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: queueItem.correlationId, state: QUEUE_ITEM_STATE.ERROR})));
				// Clean empty queues
				amqpOperator.removeQueue(queueItem.correlationId);
			}

			return checkAmqpQueue();
		}

		await setTimeoutPromise(POLL_WAIT_TIME);
		return checkAmqpQueue();
	}

	function registerSignalHandlers() {
		process
			.on('SIGINT', handleInterrupt)
			.on('uncaughtException', handleInterrupt)
			.on('unhandledRejection', handleInterrupt);
		// Nodemon
		// .on('SIGUSR2', handle);
	}
}
