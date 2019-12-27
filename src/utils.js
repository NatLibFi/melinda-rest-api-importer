import {Utils} from '@natlibfi/melinda-commons';

const {createLogger, toAlephId} = Utils;

export function logError(err) {
	const logger = createLogger();
	if (err !== 'SIGINT') {
		logger.log('error', 'stack' in err ? err.stack : err);
	}

	logger.log('error', err);
}

export async function validateLine(line, index, operation) {
	const logger = createLogger();
	const lineId = line.slice(0, 9).trim();
	const valid = /^\d{9}$/.test(lineId);
	const old = lineId > 0;

	if (operation === 'create') {
		return {valid, old: false, id: toAlephId(index)};
	}

	logger.log('debug', `Line is valid: ${valid}`);
	return {valid, old, id: lineId};
}
