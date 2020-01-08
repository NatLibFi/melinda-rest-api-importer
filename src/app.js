/* eslint-disable no-unused-vars */

import {Utils} from '@natlibfi/melinda-commons';
import amqplib from 'amqplib';
import {logError, checkIfOfflineHours} from './utils';
import {consumeQueue} from './services/fromQueueService';
import {IMPORT_QUEUES} from '@natlibfi/melinda-record-import-commons';
import {
	AMQP_URL,
	PURGE_QUEUE_ON_LOAD
} from './config';

const {BULK_CREATE, BULK_REPLY, BULK_UPDATE, PRIO_CREATE, PRIO_REPLY, PRIO_UPDATE} = IMPORT_QUEUES;
const {createLogger, handleSignal} = Utils;
const logger = createLogger(); // eslint-disable-line no-console

// process.on('SIGTERM', handleSignal);
// Process.on('SIGINT', handleSignal);

run();

async function run() {
	logger.log('info', 'Melinda-rest-import-queue has been started');
	await operateRabbitQueues(true, PURGE_QUEUE_ON_LOAD, false);
	checkCreateQueues();
	checkUpdateQueues();
}

export async function checkCreateQueues() {
	const {prioC, bulkC} = await operateRabbitQueues(false, false, true, false, true);

	// TODO: create loop
	if (prioC.messageCount > 0) {
		consumeQueue(PRIO_CREATE);
	} else if (bulkC.messageCount > 0 && checkIfOfflineHours()) {
		consumeQueue(BULK_CREATE);
	} else {
		setTimeout(checkCreateQueues, 1000); // TODO: Think better way, this affects consume speed...
	}
}

export async function checkUpdateQueues() {
	const {prioU, bulkU} = await operateRabbitQueues(false, false, false, true, true);

	// TODO: update loop
	if (prioU.messageCount > 0) {
		consumeQueue(PRIO_UPDATE);
	} else if (bulkU.messageCount > 0 && checkIfOfflineHours()) {
		consumeQueue(BULK_UPDATE);
	} else {
		setTimeout(checkUpdateQueues, 1000); // TODO: Think better way, this affects consume speed...
	}
}

async function operateRabbitQueues(initQueues, purge, checkCreateQueues, checkUpdateQueues, checkReplyQueues) {
	let connection;
	let channel;
	const channelInfos = {};

	try {
		connection = await amqplib.connect(AMQP_URL);
		channel = await connection.createChannel();

		if (initQueues) {
			await channel.assertQueue(BULK_CREATE, {durable: true, autoDelete: false});
			await channel.assertQueue(BULK_UPDATE, {durable: true, autoDelete: false});
			await channel.assertQueue(BULK_REPLY, {durable: true, autoDelete: false});
			await channel.assertQueue(PRIO_CREATE, {durable: true, autoDelete: false});
			await channel.assertQueue(PRIO_UPDATE, {durable: true, autoDelete: false});
			await channel.assertQueue(PRIO_REPLY, {durable: true, autoDelete: false});
			logger.log('info', 'Rabbitmq queues has been initiated');
		}

		if (purge) {
			await channel.purgeQueue(BULK_CREATE);
			await channel.purgeQueue(BULK_UPDATE);
			await channel.purgeQueue(BULK_REPLY);
			await channel.purgeQueue(PRIO_CREATE);
			await channel.purgeQueue(PRIO_UPDATE);
			await channel.purgeQueue(PRIO_REPLY);
			logger.log('info', 'Rabbitmq queues have been purged');
		}

		if (checkCreateQueues) {
			channelInfos.prioC = await channel.checkQueue(PRIO_CREATE);
			logger.log('debug', `${PRIO_CREATE} queue: ${channelInfos.prioC.messageCount} chunks`);
			channelInfos.bulkC = await channel.checkQueue(BULK_CREATE);
			logger.log('debug', `${BULK_CREATE} queue: ${channelInfos.bulkC.messageCount} chunks`);
		}

		if (checkUpdateQueues) {
			channelInfos.prioU = await channel.checkQueue(PRIO_UPDATE);
			logger.log('debug', `${PRIO_UPDATE} queue: ${channelInfos.prioU.messageCount} chunks`);
			channelInfos.bulkU = await channel.checkQueue(BULK_UPDATE);
			logger.log('debug', `${BULK_UPDATE} queue: ${channelInfos.bulkU.messageCount} chunks`);
		}

		if (checkReplyQueues) {
			channelInfos.prioR = await channel.checkQueue(PRIO_REPLY);
			logger.log('debug', `${PRIO_REPLY} queue: ${channelInfos.prioR.messageCount} chunks`);
			channelInfos.bulkR = await channel.checkQueue(BULK_REPLY);
			logger.log('debug', `${BULK_REPLY} queue: ${channelInfos.bulkR.messageCount} chunks`);
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

	if (checkCreateQueues || checkUpdateQueues) {
		return channelInfos;
	}
}
