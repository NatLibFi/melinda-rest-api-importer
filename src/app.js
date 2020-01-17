/* eslint-disable no-unused-vars */

import {Utils} from '@natlibfi/melinda-commons';
import {logError} from './utils';
import rabbitFactory from './interfaces/rabbit';

const {createLogger, handleSignal} = Utils;

// Process.on('SIGTERM', handleSignal);
// Process.on('SIGINT', handleSignal);

run();

async function run() {
	const logger = createLogger(); // eslint-disable-line no-console
	logger.log('info', 'Melinda-rest-import-queue has been started');
	try {
		// Do rabbit loop
		const rabbitOperator = await rabbitFactory();
		await rabbitOperator.checkQueue(true);
	} catch (error) {
		logError(error);
		// TODO crash
	}
}

