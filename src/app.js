/* eslint-disable no-unused-vars, no-unsafe-finally */

import {Utils} from '@natlibfi/melinda-commons';
import amqplib from 'amqplib';
import {logError} from './utils';

import {
	AMQP_URL,
	NAME_QUEUE_BULK,
	NAME_QUEUE_PRIORITY,
	PURGE_QUEUE_ON_LOAD
} from './config';

const {createLogger} = Utils;
const logger = createLogger();

process.on('UnhandledPromiseRejectionWarning', err => {
	logError(err);
	process.exit(1);
});

run();

async function run() {
	logger.log('info', 'Melinda-rest-import-queue has been started');
	await operateRabbitQueues(true, PURGE_QUEUE_ON_LOAD, true);
	checkQueues();
}

async function checkQueues() {
	const queueLenght = await operateRabbitQueues(false, false, true);
	setTimeout(checkQueues, 3000);
}

async function operateRabbitQueues(initQueues, purge, checkQueues) {
	let connection;
	let channel;
	let prioQueueCount;
	let bulkQueueCount;

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();

		if (initQueues) {
			await channel.assertQueue(NAME_QUEUE_PRIORITY, {durable: true, autoDelete: false});
			await channel.assertQueue(NAME_QUEUE_BULK, {durable: true, autoDelete: false});
			logger.log('info', 'Rabbitmq queues has been initiated');
		}

		if (purge) {
			await channel.purgeQueue(NAME_QUEUE_PRIORITY);
			await channel.purgeQueue(NAME_QUEUE_BULK);
			logger.log('info', 'Rabbitmq queues have been purged');
		}

		if (checkQueues) {
			const infoChannelPrio = await channel.checkQueue(NAME_QUEUE_PRIORITY);
			prioQueueCount = infoChannelPrio.messageCount;
			logger.log('debug', `${NAME_QUEUE_PRIORITY} queue: ${prioQueueCount} records`);
			const infoChannel = await channel.checkQueue(NAME_QUEUE_BULK);
			bulkQueueCount = infoChannel.messageCount;
			logger.log('debug', `${NAME_QUEUE_BULK} queue: ${bulkQueueCount} records`);
		}
	} catch (err) {
		logError(err);
	} finally {
		if (channel) {
			await channel.close();
		}

		if (connection) {
			await connection.close();
		}

		if (checkQueues) {
			return {PRIORITY: prioQueueCount, BULK: bulkQueueCount};
		}
	}
}
