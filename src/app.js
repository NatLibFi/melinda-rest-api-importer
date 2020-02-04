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
	logger.log('info', 'Melinda-rest-import-queue has been started');
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
				const status = (headers.operation === OPERATIONS.CREATE) ? 'CREATED' : 'UPDATED';
				const results = await datastoreOperator.set({...headers, records, recordLoadParams});
				if (OPERATION_TYPES.includes(queue)) {
					await amqpOperator.ackNReplyMessages({status, messages, payloads: results.payloads});
				} else {
					await mongoOperator.pushIds({correlationId: queue, ids: results.payloads});
					await amqpOperator.ackMessages(messages);
				}

				return checkAmqpQueue();
			} catch (error) {
				if (OPERATION_TYPES.includes(queue)) {
					// Send response back if PRIO
					const status = error.status;
					const payloads = (error.payload) ? new Array(messages.lenght).fill(error.payload) : [];
					await amqpOperator.ackNReplyMessages({status, messages, payloads});
				} else {
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
		let result = await mongoOperator.getOne({operation: OPERATION, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});
		if (!result) {
			result = await mongoOperator.getOne({operation: OPERATION, queueItemState: QUEUE_ITEM_STATE.IN_QUEUE});
		}

		if (result) {
			const amount = await amqpOperator.checkQueue(result.correlationId, 'messages', false);
			if (amount) {
				if (result.queueItemState === QUEUE_ITEM_STATE.IN_QUEUE) {
					logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: result.correlationId, state: QUEUE_ITEM_STATE.IN_PROCESS})));
				}

				return checkAmqpQueue(result.correlationId, result.recordLoadParams);
			}

			if (result.queueItemState === QUEUE_ITEM_STATE.IN_PROCESS) {
				logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: result.correlationId, state: QUEUE_ITEM_STATE.DONE})));
				// Clean empty queues
				amqpOperator.removeQueue(result.correlationId);
			} else {
				logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId: result.correlationId, state: QUEUE_ITEM_STATE.ERROR})));
				amqpOperator.removeQueue(result.correlationId);
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
