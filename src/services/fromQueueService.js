import amqplib from 'amqplib';
import {Utils} from '@natlibfi/melinda-commons';
import {toRecordLoadApi} from './toRecordLoadApiService';
import {logError} from '../utils';

import {checkQueues} from '../app';
import {AMQP_URL} from '../config';
import {CHUNK_STATE, QUEUE_NAME_REPLY_BULK, QUEUE_NAME_REPLY_PRIO, QUEUE_NAME_PRIO} from '@natlibfi/melinda-record-import-commons';

const load = toRecordLoadApi();
const {createLogger} = Utils;

export async function consumeQueue(queue) {
	const logger = createLogger();
	let connection;
	let channel;
	const chunkInfo = {};
	const replyQueue = (queue === QUEUE_NAME_PRIO) ? QUEUE_NAME_REPLY_PRIO : QUEUE_NAME_REPLY_BULK;
	// Debug: logger.log('debug', `Prepared to consume from queue: ${queue}`);

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();
		channel.prefetch(1); // Per consumer limit
		chunkInfo.queData = await channel.get(queue);
		if (chunkInfo.queData) {
			const data = JSON.parse(chunkInfo.queData.content.toString());
			chunkInfo.chunkNumber = data.chunkNumber;
			const result = await load(queue, data);
			if (result) {
				logger.log('debug', `Response from record-load-api ${JSON.stringify(result)}`);

				// Send message back to rest-api when done
				result.queue = queue;
				result.chunkNumber = chunkInfo.chunkNumber;
				await channel.sendToQueue(
					replyQueue,
					Buffer.from(JSON.stringify(result)),
					{
						persistent: true,
						correlationId: chunkInfo.queData.properties.correlationId
					}
				);

				channel.ack(chunkInfo.queData); // TODO: DO NOT ACK BEFORE RECORD IS SAVED TO ALEPH & Reply is send
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
			Buffer.from(JSON.stringify({status: CHUNK_STATE.ERROR, chunkNumber: chunkInfo.chunkNumber, metadata: {error}})),
			{
				persistent: true,
				correlationId: chunkInfo.queData.properties.correlationId
			}
		);

		// Do not ack if service is offline
		if (error.status === 503 && queue === QUEUE_NAME_REPLY_BULK) {
			// TODO Weit
			setTimeout(checkQueues, 10000);
		} else {
			channel.ack(chunkInfo.queData);
			// Back to the loop
			checkQueues();
		}
	} finally {
		if (channel) {
			await channel.close();
		}

		if (connection) {
			await connection.close();
		}
	}
}
