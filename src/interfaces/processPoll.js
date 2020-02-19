import fetch from 'node-fetch';
import HttpStatus from 'http-status';
import ApiError, {Utils} from '@natlibfi/melinda-commons';

export default function (recordLoadApiKey, recordLoadLibrary, recordLoadUrl) {
	const {createLogger, generateAuthorizationHeader} = Utils;
	const logger = createLogger(); // eslint-disable-line no-console

	return {pollProcess, requestFileClear};

	async function pollProcess({correlationId, pActiveLibrary, processId}) {
		const url = new URL(recordLoadUrl);

		// Pass correlationId to record-load-api so it can use same name in log files
		url.search = new URLSearchParams([
			['correlationId', correlationId],
			['pActiveLibrary', pActiveLibrary],
			['processId', processId]
		]);

		const response = await fetch(url, {
			method: 'GET',
			headers: {
				'Content-Type': 'text/plain',
				Accept: 'text/plain', // Might need change
				Authorization: generateAuthorizationHeader(recordLoadApiKey)
			}
		});

		logger.log('debug', 'Got response for process poll!');

		// R-L-A has crashed
		if (response.status === HttpStatus.CONFLICT) {
			const array = await response.json();
			const idList = array.filter(id => id.length > 0).map(id => formatRecordId(id));
			logger.log('info', `Got "conflict" (409) response from record-load-api. Ids:  ${idList}`);
			return {payloads: idList, ackOnlyLength: idList.length};
		}

		// Too early
		if (response.status === 423) {
			logger.log('info', 'Got "locked" (423) response from record-load-api. Process is still going on!');
			throw new ApiError(423, 'Not ready yet!');
		}

		// OK
		if (response.status === 200) {
			const array = await response.json();
			console.log(array);
			const idList = array.map(id => formatRecordId(pActiveLibrary, id));
			logger.log('info', `Got "OK" (200) response from record-load-api. Ids: ${idList}`);
			return {payloads: idList, ackOnlyLength: idList.length};
		}
	}

	async function requestFileClear({correlationId, pActiveLibrary, processId}) {
		const url = new URL(recordLoadUrl);

		// Pass correlationId to record-load-api so it can use same name in log files
		url.search = new URLSearchParams([
			['correlationId', correlationId],
			['pActiveLibrary', pActiveLibrary],
			['processId', processId]
		]);

		const response = await fetch(url, {
			method: 'delete',
			headers: {
				'Content-Type': 'text/plain',
				Accept: 'text/plain', // Might need change
				Authorization: generateAuthorizationHeader(recordLoadApiKey)
			}
		});

		logger.log('debug', 'Got response for file clear!');
		logger.log('debug', response.status);
	}

	function formatRecordId(library, id) {
		const pattern = new RegExp(`${library.toUpperCase()}$`);
		return id.replace(pattern, '');
	}
}
