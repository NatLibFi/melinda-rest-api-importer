// THIS HAS BEEN MOVED TO REST-API

import amqplib from 'amqplib';
import {AMQP_URL} from '../config';
import {Utils} from '@natlibfi/melinda-commons';
import {logError} from '../utils';

const {createLogger} = Utils;

export async function pushToQueue({type, QUEUEID, format, data, operation}) {
	const logger = createLogger();
	let connection;
	let channel;

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();

		// Logger.log('debug', `Record type ${type}`);
		// logger.log('debug', `Record QUEUEID ${QUEUEID}`);
		// logger.log('debug', `Record format ${format}`);
		// logger.log('debug', `Record data ${data}`);
		// logger.log('debug', `Record operation ${operation}`);

		data.forEach(async record => {
			const message = JSON.stringify({
				format,
				type,
				operation,
				QUEUEID,
				record
			});

			// Logger.log('debug', `Record message ${message}`);

			await channel.sendToQueue(
				type,
				Buffer.from(message),
				{persistent: true}
			);
		});

		logger.log('debug', 'Record has been set in queue');
	} catch (err) {
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
