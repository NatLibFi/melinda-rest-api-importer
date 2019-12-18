/* eslint-disable no-unused-vars, no-unsafe-finally */

import {Utils} from '@natlibfi/melinda-commons';
import amqplib from 'amqplib';
import {logError} from './utils';
import {consumeQueue} from './services/fromQueueService';

import {
	AMQP_URL,
	NAME_QUEUE_BULK,
	NAME_QUEUE_PRIORITY,
	PURGE_QUEUE_ON_LOAD,
	NAME_QUEUE_REPLY
} from './config';

const {createLogger} = Utils;
const logger = createLogger(); // eslint-disable-line no-console

process.on('UnhandledPromiseRejectionWarning', err => {
	logError(err);
	process.exit(1);
});

run();

async function run() {
	logger.log('info', 'Melinda-rest-import-queue has been started');
	await operateRabbitQueues(true, PURGE_QUEUE_ON_LOAD, false);
	checkQueues();
}

export async function checkQueues() {
	const queueLenghts = await operateRabbitQueues(false, false, true);
	if (queueLenghts.PRIORITY > 0) {
		consumeQueue(NAME_QUEUE_PRIORITY);
	} else if (queueLenghts.BULK > 0) {
		consumeQueue(NAME_QUEUE_BULK);
	} else {
		setTimeout(checkQueues, 3000);
	}
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
			await channel.assertQueue(NAME_QUEUE_REPLY, {durable: true, autoDelete: false});
			logger.log('info', 'Rabbitmq queues has been initiated');
		}

		if (purge) {
			await channel.purgeQueue(NAME_QUEUE_PRIORITY);
			await channel.purgeQueue(NAME_QUEUE_BULK);
			await channel.purgeQueue(NAME_QUEUE_REPLY);
			logger.log('info', 'Rabbitmq queues have been purged');
		}

		if (checkQueues) {
			const infoChannelPrio = await channel.checkQueue(NAME_QUEUE_PRIORITY);
			prioQueueCount = infoChannelPrio.messageCount;
			logger.log('debug', `${NAME_QUEUE_PRIORITY} queue: ${prioQueueCount} blobs`);
			const infoChannel = await channel.checkQueue(NAME_QUEUE_BULK);
			bulkQueueCount = infoChannel.messageCount;
			logger.log('debug', `${NAME_QUEUE_BULK} queue: ${bulkQueueCount} blobs`);
			const replyChannel = await channel.checkQueue(NAME_QUEUE_REPLY);
			logger.log('debug', `${NAME_QUEUE_REPLY} queue: ${replyChannel.messageCount} blobs`);
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
