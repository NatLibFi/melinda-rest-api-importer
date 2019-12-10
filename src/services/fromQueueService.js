import amqplib from 'amqplib';
import {AMQP_URL} from '../config';
import {Utils} from '@natlibfi/melinda-commons';
import {EMITTER_CONSUM_ANNOUNSE} from '../config';
import {logError} from '../utils';

const {createLogger} = Utils;

export async function consumeQueue(type, EMITTER) {
	const logger = createLogger();
	let connection;
	let channel;
	// Logger.log('debug', `Prepared to consume from queue: ${type}`);

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();
		channel.prefetch(1); // Per consumer limit
		const record = await channel.get(type);
		if (record) {
			channel.ack(record);
			const content = JSON.parse(record.content.toString());
			// Logger.log('debug', `Record has consumed from queue id: ${content.QUEUEID}`);
			EMITTER.emit(EMITTER_CONSUM_ANNOUNSE, content);
		} else {
			throw Error(`no records in ${type} queue`);
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
