/* eslint-disable no-unused-vars */
import amqplib from 'amqplib';
import RabbitError, {Utils} from '@natlibfi/melinda-commons';
import {logError, checkIfOfflineHours} from '../utils';
import HttpStatus from 'http-status';
import {MarcRecord} from '@natlibfi/marc-record';
import {AMQP_URL, OFFLINE_BEGIN, OFFLINE_DURATION, QUEUE, OPERATION, POLL_WAIT_TIME} from '../config';
import {CHUNK_SIZE, PRIO_IMPORT_QUEUES, QUEUE_ITEM_STATE} from '@natlibfi/melinda-record-import-commons';
import mongoFactory from './mongo';
import {datastoreFactory} from './datastore';
import {promisify} from 'util';

const setTimeoutPromise = promisify(setTimeout);
const {createLogger} = Utils;

export default async function () {
	const {REPLY, CREATE, UPDATE} = PRIO_IMPORT_QUEUES;
	const connection = await amqplib.connect(AMQP_URL);
	const channel = await connection.createChannel();
	const mongoOperator = await mongoFactory();
	const datastoreOperator = datastoreFactory();
	const logger = createLogger();

	return {checkQueue, consume};

	async function checkQueue(init = false, purge = false) {
		try {
			if (init) {
				await channel.assertQueue(QUEUE);
			}

			// Only for testing purposes, Removed in production
			if (purge) {
				await channel.purgeQueue(QUEUE);
			}

			let channelInfo = await channel.checkQueue(QUEUE);
			logger.log('debug', `Queue ${QUEUE} has ${channelInfo.messageCount} records`);

			// If Service is in offline
			const isOfflineHours = checkIfOfflineHours();
			if (isOfflineHours && channelInfo.messageCount > 0) {
				replyErrors(HttpStatus.SERVICE_UNAVAILABLE, `${HttpStatus['503_MESSAGE']} Offline hours begin at ${OFFLINE_BEGIN} and will last next ${OFFLINE_DURATION} hours.`);
			} else if (isOfflineHours) {
				await setTimeoutPromise(POLL_WAIT_TIME);
				return checkQueue();
			}

			// No messages in rabbit. Check jobs from Mongo.
			if (channelInfo.messageCount < 1 && !isOfflineHours) {
				const result = await mongoOperator.checkDB(OPERATION);

				// No jobs in mongo.
				if (result === null) {
					await setTimeoutPromise(POLL_WAIT_TIME);
					return checkQueue();
				}

				logger.log('debug', 'Got job from mongo!');
				const {correlationId, cataloger, operation, queueItemState} = result;
				channelInfo = await channel.checkQueue(correlationId);

				if (channelInfo.messageCount > 0) {
					logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId, cataloger, operation, state: QUEUE_ITEM_STATE.IN_PROCESS})));
					// OPERATE QUEUE
					return await consume(correlationId);
				}

				if (queueItemState === QUEUE_ITEM_STATE.IN_QUEUE) {
					await mongoOperator.setState({correlationId, cataloger, operation, state: QUEUE_ITEM_STATE.ERROR});
					channel.deleteQueue(correlationId);
				} else {
					logger.log('info', JSON.stringify(await mongoOperator.setState({correlationId, cataloger, operation, state: QUEUE_ITEM_STATE.DONE})));
					channel.deleteQueue(correlationId);
				}

				// Back to loop
				return checkQueue();
			}

			consume(channelInfo.queue);
		} catch (error) {
			logError(error);
			checkQueue();
		}
	}

	// TODO!!! Split to parts
	async function consume(queue) {
		// Debug: logger.log('debug', `Prepared to consume from queue: ${queue}`);
		try {
			const queDatas = await getData(queue);

			const {cataloger, operation} = getHeaderInfo(queDatas[0]);

			// Check that cataloger match! headers
			// console.log(queDatas);
			const datas = queDatas.filter(data => {
				return (data.properties.headers.cataloger === cataloger);
			}).map(data => {
				data.content = JSON.parse(data.content.toString());
				data.content.record = new MarcRecord(data.content.data);
				return data;
			});

			// Collect queDatas.content.records to one array
			const records = datas.flatMap(data => {
				return new MarcRecord(data.content.record);
			});

			// Send to Record-Load-Api
			// console.log(records);
			const results = await datastoreOperator.set({records, operation, cataloger});
			logger.log('debug', `Operation ${operation} performed to records ${results.ids}`);

			// ACK records
			if (queue === CREATE || queue === UPDATE) {
				datas.forEach((data, index) => {
					if (queue === CREATE || queue === UPDATE) {
						sendReply({
							correlationId: data.properties.correlationId,
							cataloger,
							operation,
							data: results.ids[index]
						});
					}

					channel.ack(data);
				});

				// Bulk has no unwanted ones
				// Nack unwanted ones to back in queue;
				queDatas.filter(data => {
					return (!datas.includes(data));
				}).forEach(data => {
					// Message, allUpTo, reQueue
					channel.nack(data, false, true);
				});
			} else {
				// Add ids to mongo metadata?
				datas.forEach(data => {
					channel.ack(data);
				});
			}

			checkQueue();
		} catch (error) {
			logError(error);
			// TODO: If datastore throws error
			checkQueue();
		}
	}

	async function replyErrors(err) {
		try {
			const error = new RabbitError(err);
			const queDatas = await getData(QUEUE);
			const promises = [];

			queDatas.forEach(data => {
				const headers = getHeaderInfo(data);
				promises.push(sendReply({correlationId: data.properties.correlationId, ...headers, data: error}));
			});

			await Promise.all(promises);
			queDatas.forEach(data => {
				channel.ack(data);
			});

			checkQueue();
		} catch (err) {
			logError(err);
		}
	}

	async function sendReply({correlationId, cataloger, operation, data}) {
		try {
			// Logger.log('debug', `Record cataloger ${cataloger}`)
			// logger.log('debug', `Record correlationId ${correlationId}`);
			// logger.log('debug', `Record data ${data}`);
			// logger.log('debug', `Record operation ${operation}`);

			await channel.assertQueue(REPLY, {durable: true});
			channel.sendToQueue(
				REPLY,
				Buffer.from(JSON.stringify({data})),
				{
					correlationId,
					persistent: true,
					headers: {
						cataloger,
						operation
					}
				}
			);
		} catch (err) {
			logError(err);
		}
	}

	async function getData(queue) {
		let queDatas = [];

		try {
			const {messageCount} = await channel.checkQueue(queue);
			const messages = (messageCount >= CHUNK_SIZE) ? CHUNK_SIZE : messageCount;
			const promises = [];
			for (let i = 0; i < messages; i++) {
				promises.push(get());
			}

			await Promise.all(promises);
			return queDatas;
		} catch (err) {
			logError(err);
		}

		async function get() {
			const message = await channel.get(queue);
			if (!queDatas.includes(message)) {
				queDatas.push(message);
			}
		}
	}

	function getHeaderInfo(data) {
		return {cataloger: data.properties.headers.cataloger, operation: data.properties.headers.operation};
	}
}
