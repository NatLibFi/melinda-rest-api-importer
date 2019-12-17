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

export function seqLineToMarcField(line) {
	// From '000606145 CAT   L $$aUEF4122$$b30$$c20191105$$lFIN01$$h1315'
	// To { tag: '100', ind1: '1', ind2: ' ', subfields: [ [Object] ] }
	const field = {
		tag: 'CAT',
		ind1: ' ',
		ind2: ' ',
		subfields: []
	};
	const subfieldsStr = line.substr(18);
	const subfields = subfieldsStr.split('$$');
	subfields.forEach(sub => {
		if (sub.length > 0) {
			field.subfields.push({code: sub.substr(0, 1), value: sub.substr(1)});
		}
	});
	return field;
}
