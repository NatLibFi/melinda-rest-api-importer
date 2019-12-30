import amqplib from 'amqplib';
import {Utils} from '@natlibfi/melinda-commons';
import {toRecordLoadApi} from './toRecordLoadApiService';
import {logError} from '../utils';

import {checkQueues} from '../app';
import {AMQP_URL, NAME_QUEUE_REPLY_BULK, NAME_QUEUE_REPLY_PRIO, CHUNK_STATE, NAME_QUEUE_PRIORITY} from '../config';

const load = toRecordLoadApi();
const {createLogger} = Utils;

export async function consumeQueue(queue) {
	const logger = createLogger();
	let connection;
	let channel;
	let correlationId;
	const replyQueue = (queue === NAME_QUEUE_PRIORITY) ? NAME_QUEUE_REPLY_PRIO : NAME_QUEUE_REPLY_BULK;
	// Logger.log('debug', `Prepared to consume from queue: ${queue}`);

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();
		channel.prefetch(1); // Per consumer limit
		const queData = await channel.get(queue);
		if (queData) {
			correlationId = queData.properties.correlationId;
			const data = JSON.parse(queData.content.toString());
			const result = await load(queue, data);
			if (result) {
				logger.log('debug', `Response from record-load-api ${JSON.stringify(result)}`);

				// Send message back to rest-api when done
				result.queue = queue;
				result.chunkNumber = data.chunkNumber;
				await channel.sendToQueue(
					replyQueue,
					Buffer.from(JSON.stringify(result)),
					{
						persistent: true,
						correlationId: correlationId
					}
				);

				channel.ack(queData); // TODO: DO NOT ACK BEFORE RECORD IS SAVED TO ALEPH & Reply is send
			}

			// Back to the loop
			checkQueues();
		} else {
			throw Error(`no records in ${queue} queue`);
		}
	} catch (err) {
		logger.log('error', 'from queue service');
		logError(err);
		// Send reply in case of failure
		await channel.sendToQueue(
			replyQueue,
			Buffer.from(JSON.stringify({status: CHUNK_STATE.ERROR, metadata: err})),
			{
				persistent: true,
				correlationId: correlationId
			}
		);
	} finally {
		if (channel) {
			await channel.close();
		}

		if (connection) {
			await connection.close();
		}
	}
}
