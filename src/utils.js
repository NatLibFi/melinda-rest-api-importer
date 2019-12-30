import {Utils} from '@natlibfi/melinda-commons';

const {createLogger} = Utils;

export function logError(err) {
	const logger = createLogger();
	if (err !== 'SIGINT') {
		logger.log('error', 'stack' in err ? err.stack : err);
	}

	logger.log('error', err);
}
