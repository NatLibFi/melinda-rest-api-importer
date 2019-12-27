import amqplib from 'amqplib';
import {Utils} from '@natlibfi/melinda-commons';
import {toRecordLoadApi} from './toRecordLoadApiService';
import {logError} from '../utils';

import {checkQueues} from '../app';
import {AMQP_URL, NAME_QUEUE_REPLY} from '../config';

const load = toRecordLoadApi();
const {createLogger} = Utils;

export async function consumeQueue(queue) {
	const logger = createLogger();
	let connection;
	let channel;
	let correlationId;
	// Logger.log('debug', `Prepared to consume from queue: ${queue}`);

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();
		channel.prefetch(1); // Per consumer limit
		const queData = await channel.get(queue);
		if (queData) {
			correlationId = queData.properties.correlationId;
			const content = JSON.parse(queData.content.toString()); // Add correlationId?*
			const res = await load(queue, content);
			if (res) {
				logger.log('debug', `Response from record-load-api ${JSON.stringify(res)}`);

				// Send message back to rest-api when done
				res.queue = queue;
				res.blobNumber = content.blobNumber;
				await channel.sendToQueue(
					NAME_QUEUE_REPLY,
					Buffer.from(JSON.stringify(res)), // *) or return inputfile name
					{
						persistent: true,
						correlationId: correlationId
					}
				);

				channel.ack(queData); // TODO: DO NOT ACK BEFORE RECORD IS SAVED TO ALEPH
			} else {
				throw new Error('Queued data did not contain records!');
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
			NAME_QUEUE_REPLY,
			Buffer.from(JSON.stringify(err)),
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
