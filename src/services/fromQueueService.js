import amqplib from 'amqplib';
import {Utils} from '@natlibfi/melinda-commons';
import {toRecordLoadApi} from './toRecordLoadApiService';
import {logError} from '../utils';

import {EMITTER} from '../app';
import {AMQP_URL, EMITTER_JOB_CHECK_QUEUE, NAME_QUEUE_REPLY} from '../config';

const load = toRecordLoadApi();
const {createLogger} = Utils;

export async function consumeQueue(queue) {
	const logger = createLogger();
	let connection;
	let channel;
	// Logger.log('debug', `Prepared to consume from queue: ${queue}`);

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();
		channel.prefetch(1); // Per consumer limit
		const record = await channel.get(queue);
		if (record) {
			// TODO: 2-part ack?
			const content = JSON.parse(record.content.toString());
			// Logger.log('debug', `Record has consumed from queue id: ${content.QUEUEID}`);
			const res = await load(content);
			// TODO: Send message back to rest-api when done
			console.log(res);
			res.queue = queue;
			await channel.sendToQueue(
				NAME_QUEUE_REPLY,
				Buffer.from(JSON.stringify(res)),
				{
					persistent: true,
					correlationId: record.properties.correlationId
				}
			);
			channel.ack(record); // TODO: DO NOT ACK BEFORE RECORD IS SAVED TO ALEPH
			EMITTER.emit(EMITTER_JOB_CHECK_QUEUE);
		} else {
			throw Error(`no records in ${queue} queue`);
		}
	} catch (err) {
		logger.log('error', 'from queue service');
		logError(err);
	} finally {
		if (channel) {
			await channel.close();
		}

		if (connection) {
			await connection.close();
		}
	}
}
