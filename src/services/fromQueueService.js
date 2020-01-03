import amqplib from 'amqplib';
import {Utils} from '@natlibfi/melinda-commons';
import {toRecordLoadApi} from './toRecordLoadApiService';
import {logError, checkIfOfflineHours} from '../utils';
import ServiceError from './error';
import HttpStatus from 'http-status';

import {checkQueues} from '../app';
import {AMQP_URL, OFFLINE_BEGIN, OFFLINE_DURATION} from '../config';
import {CHUNK_STATE, QUEUE_NAME_REPLY_BULK, QUEUE_NAME_REPLY_PRIO, QUEUE_NAME_PRIO} from '@natlibfi/melinda-record-import-commons';

const load = toRecordLoadApi();
const {createLogger} = Utils;

export async function consumeQueue(queue) {
	const logger = createLogger();
	let connection;
	let channel;
	let queData;
	let chunkInfo;
	const replyQueue = (queue === QUEUE_NAME_PRIO) ? QUEUE_NAME_REPLY_PRIO : QUEUE_NAME_REPLY_BULK;
	// Debug: logger.log('debug', `Prepared to consume from queue: ${queue}`);

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();
		channel.prefetch(1); // Per consumer limit
		queData = await channel.get(queue);
		chunkInfo = JSON.parse(queData.content.toString());
		if (chunkInfo) {
			if (queue === QUEUE_NAME_PRIO && checkIfOfflineHours) {
				throw new ServiceError(HttpStatus.SERVICE_UNAVAILABLE, `${HttpStatus['503_MESSAGE']} Offline hours begin at ${OFFLINE_BEGIN} and will last next ${OFFLINE_DURATION} hours.`);
			}

			const result = await load(queue, chunkInfo);
			if (result) {
				logger.log('debug', `Response from record-load-api ${JSON.stringify(result)}`);

				// Send message back to rest-api when done
				await channel.sendToQueue(
					replyQueue,
					Buffer.from(JSON.stringify(result)),
					{
						persistent: true,
						correlationId: queData.properties.correlationId
					}
				);

				channel.ack(queData); // TODO: DO NOT ACK BEFORE RECORD IS SAVED TO ALEPH & Reply is send
			} else {
				throw new Error('No result from datastore');
			}

			// Back to the loop
			checkQueues();
		} else {
			throw new Error(`No records in ${queue} queue`);
		}
	} catch (error) {
		logger.log('error', 'Error was thrown in "fromQueueService"');

		logError(error);

		// Send reply in case of failure
		channel.sendToQueue(
			replyQueue,
			Buffer.from(JSON.stringify({status: CHUNK_STATE.ERROR, chunkNumber: chunkInfo.chunkNumber, cataloger: chunkInfo.cataloger, operation: chunkInfo.operation, metadata: {error}})),
			{
				persistent: true,
				correlationId: queData.properties.correlationId
			}
		);

		// Priority requests get priority responses, no need to keep messages after user is notified
		if (error.code === 'ECONNREFUSED') {
			// When Record-load-api is down and !checkIfOfflineHours
			await channel.nack(queData);
		} else {
			await channel.ack(queData);
		}

		// Back to the loop
		checkQueues();
	} finally {
		if (channel) {
			await channel.close();
		}

		if (connection) {
			await connection.close();
		}
	}
}
