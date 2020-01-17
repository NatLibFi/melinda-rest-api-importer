/* eslint-disable no-unused-vars */
import amqplib from 'amqplib';
import {Utils} from '@natlibfi/melinda-commons';
import {toRecordLoadApi} from './toRecordLoadApi';
import {logError, checkIfOfflineHours} from '../utils';
import ApiError from './error';
import HttpStatus from 'http-status';
import {MarcRecord} from '@natlibfi/marc-record';
import {AMQP_URL, OFFLINE_BEGIN, OFFLINE_DURATION, QUEUE, OPERATION} from '../config';
import {CHUNK_SIZE, PRIO_IMPORT_QUEUES, QUEUE_ITEM_STATE} from '@natlibfi/melinda-record-import-commons';
import mongoFactory from './mongo';

const toRLA = toRecordLoadApi();
const {createLogger} = Utils;

export default async function () {
	const {REPLY, CREATE, UPDATE} = PRIO_IMPORT_QUEUES;
	const connection = await amqplib.connect(AMQP_URL);
	const channel = await connection.createChannel();
	const mongoOperator = await mongoFactory();
	const logger = createLogger();

	return {checkQueue, checkIfExists, consume};

	async function checkQueue(init = false, purge = false) {
		if (init) {
			await channel.assertQueue(QUEUE);
		}

		if (purge) {
			await channel.purgeQueue(QUEUE);
		}

		let channelInfo = await channel.checkQueue(QUEUE);
		logger.log('debug', `Queue ${QUEUE} has ${channelInfo.messageCount} records`);

		if (channelInfo.messageCount < 1) {
			const result = await mongoOperator.checkDB(OPERATION);
			if (result === null) {
				return setTimeout(checkQueue, 1000);
			}

			const {id, cataloger, operation, queueItemState} = result;
			channelInfo = await channel.checkQueue(id);

			if (channelInfo.messageCount > 0) {
				logger.log('info', JSON.stringify(await mongoOperator.setState({id, cataloger, operation, state: QUEUE_ITEM_STATE.IN_PROCESS})));
				// TODO: OPERATE QUEUE
				await consume(id);
			} else if (queueItemState === QUEUE_ITEM_STATE.IN_QUEUE) {
				await mongoOperator.setState({id, cataloger, operation, state: QUEUE_ITEM_STATE.ERROR});
			} else {
				logger.log('info', JSON.stringify(await mongoOperator.setState({id, cataloger, operation, state: QUEUE_ITEM_STATE.DONE})));
			}

			// Back to loop
			return checkQueue();
		}

		consume(channelInfo.queue);
	}

	async function checkIfExists(queue) {
		return channel.assertQueue(queue, {durable: true});
	}

	// TODO!!! Split to parts
	async function consume(queue) {
		// Debug: logger.log('debug', `Prepared to consume from queue: ${queue}`);
		try {
			if (checkIfOfflineHours()) {
				throw new ApiError(HttpStatus.SERVICE_UNAVAILABLE, `${HttpStatus['503_MESSAGE']} Offline hours begin at ${OFFLINE_BEGIN} and will last next ${OFFLINE_DURATION} hours.`);
			}

			const queDatas = await getData(CHUNK_SIZE);
			const {cataloger, operation} = getHeaderInfo(queDatas[0].properties.headers);

			// Check that cataloger match! headers
			// console.log(queDatas);
			const datas = queDatas.filter(data => {
				return (data.properties.headers.cataloger === cataloger);
			}).map(data => {
				data.content = JSON.parse(data.content.toString());
				data.content.record = new MarcRecord(data.content.record);
				return data;
			});

			// Nack unwanted ones to back in queue;
			queDatas.filter(data => {
				return (!datas.includes(data));
			}).forEach(data => {
				channel.nack(data, false, true);
			});

			// Collect queDatas.content.records to one array
			const records = datas.flatMap(data => {
				return data.content.record;
			});

			// Send to Record-Load-Api
			// console.log(records);
			const results = await toRLA({records, cataloger, operation});

			// ACK records
			datas.forEach((data, index) => {
				if (queue === CREATE || queue === UPDATE) {
					pushToQueue({
						id: data.properties.correlationId,
						cataloger,
						operation,
						data: results.ids[index]
					});
				}

				channel.ack(data);
			});

			// Ids to metadata?
			if (queue === CREATE || queue === UPDATE) {
				return checkQueue();
			}

			return results;
		} catch (error) {
			logError(error);
		}

		async function getData(amount) {
			let queDatas = [];
			const {messageCount} = await channel.checkQueue(queue);
			const messages = (messageCount >= amount) ? amount : messageCount;
			const promises = [];
			for (let i = 0; i < messages; i++) {
				promises.push(get());
			}

			await Promise.all(promises);
			return queDatas;

			async function get() {
				const message = await channel.get(queue);
				if (!queDatas.includes(message)) {
					queDatas.push(message);
				}
			}
		}

		function getHeaderInfo(headers) {
			return {cataloger: headers.cataloger, operation: headers.operation};
		}
	}

	async function pushToQueue({id, cataloger, operation, data}) {
		try {
			// Logger.log('debug', `Record cataloger ${cataloger}`)
			// logger.log('debug', `Record id ${id}`);
			// logger.log('debug', `Record record ${record}`);
			// logger.log('debug', `Record operation ${operation}`);

			await channel.assertQueue(id, {durable: true});
			channel.sendToQueue(
				REPLY,
				Buffer.from(JSON.stringify({data})),
				{
					correlationId: id,
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
}
