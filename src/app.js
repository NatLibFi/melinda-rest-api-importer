/* eslint-disable no-unused-vars */

import {Utils} from '@natlibfi/melinda-commons';
import amqplib from 'amqplib';
import {logError, checkIfOfflineHours} from './utils';
import {consumeQueue} from './services/fromQueueService';
import {
	QUEUE_NAME_BULK,
	QUEUE_NAME_PRIO,
	QUEUE_NAME_REPLY_PRIO,
	QUEUE_NAME_REPLY_BULK
} from '@natlibfi/melinda-record-import-commons';
import {
	AMQP_URL,
	PURGE_QUEUE_ON_LOAD
} from './config';

const {createLogger, handleSignal} = Utils;
const logger = createLogger(); // eslint-disable-line no-console

// process.on('SIGTERM', handleSignal);
// Process.on('SIGINT', handleSignal);

run();

async function run() {
	logger.log('info', 'Melinda-rest-import-queue has been started');
	await operateRabbitQueues(true, PURGE_QUEUE_ON_LOAD, false);
	checkQueues();
}

export async function checkQueues() {
	const offlineHours = checkIfOfflineHours();
	const {prio, bulk} = await operateRabbitQueues(false, false, true);
	if (prio > 0) {
		consumeQueue(QUEUE_NAME_PRIO);
	} else if (bulk > 0 && !offlineHours) {
		consumeQueue(QUEUE_NAME_BULK);
	} else {
		setTimeout(checkQueues, 1000); // TODO Affects consume speed...
	}
}

async function operateRabbitQueues(initQueues, purge, checkQueues) {
	let connection;
	let channel;
	const channelInfos = {};

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();

		if (initQueues) {
			await channel.assertQueue(QUEUE_NAME_PRIO, {durable: true, autoDelete: false});
			await channel.assertQueue(QUEUE_NAME_BULK, {durable: true, autoDelete: false});
			await channel.assertQueue(QUEUE_NAME_REPLY_BULK, {durable: true, autoDelete: false});
			await channel.assertQueue(QUEUE_NAME_REPLY_PRIO, {durable: true, autoDelete: false});
			logger.log('info', 'Rabbitmq queues has been initiated');
		}

		if (purge) {
			await channel.purgeQueue(QUEUE_NAME_PRIO);
			await channel.purgeQueue(QUEUE_NAME_BULK);
			await channel.purgeQueue(QUEUE_NAME_REPLY_BULK);
			await channel.purgeQueue(QUEUE_NAME_REPLY_PRIO);
			logger.log('info', 'Rabbitmq queues have been purged');
		}

		if (checkQueues) {
			channelInfos.prio = await channel.checkQueue(QUEUE_NAME_PRIO);
			logger.log('debug', `${QUEUE_NAME_PRIO} queue: ${channelInfos.prio.messageCount} chunks`);
			channelInfos.bulk = await channel.checkQueue(QUEUE_NAME_BULK);
			logger.log('debug', `${QUEUE_NAME_BULK} queue: ${channelInfos.bulk.messageCount} chunks`);
			channelInfos.replyPrio = await channel.checkQueue(QUEUE_NAME_REPLY_PRIO);
			logger.log('debug', `${QUEUE_NAME_REPLY_PRIO} queue: ${channelInfos.replyPrio.messageCount} chunks`);
			channelInfos.replyBulk = await channel.checkQueue(QUEUE_NAME_REPLY_BULK);
			logger.log('debug', `${QUEUE_NAME_REPLY_BULK} queue: ${channelInfos.replyBulk.messageCount} chunks`);
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
	}

	if (checkQueues) {
		return {prio: channelInfos.prio.messageCount, bulk: channelInfos.bulk.messageCount};
	}
}
