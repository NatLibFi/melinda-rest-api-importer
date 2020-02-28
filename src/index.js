import {Utils} from '@natlibfi/melinda-commons';
import * as config from './config';
import startApp from './app';
import {logError} from '@natlibfi/melinda-rest-api-commons';

run();

async function run() {
	const {handleInterrupt} = Utils;

	registerInterruptionHandlers();

	await startApp({...config});

	function registerInterruptionHandlers() {
		process
			.on('SIGTERM', handleSignal)
			.on('SIGINT', handleInterrupt)
			.on('uncaughtException', ({stack}) => {
				handleTermination({code: 1, message: stack});
			})
			.on('unhandledRejection', ({stack}) => {
				handleTermination({code: 1, message: stack});
			});

		function handleTermination({code = 0, message}) {
			if (message) {
				logError(message);
				process.exit(code);
			}

			process.exit(code);
		}

		function handleSignal(signal) {
			handleTermination({code: 1, message: `Received ${signal}`});
		}
	}
}
