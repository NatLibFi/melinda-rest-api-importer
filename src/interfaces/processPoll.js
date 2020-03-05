import fetch from 'node-fetch';
import HttpStatus from 'http-status';
import {Error as ApiError, Utils} from '@natlibfi/melinda-commons';

export default function (recordLoadApiKey, recordLoadLibrary, recordLoadUrl) {
	const {createLogger, generateAuthorizationHeader} = Utils;
	const logger = createLogger(); // eslint-disable-line no-console

	return {pollProcess, requestFileClear};

	async function pollProcess({correlationId, pActiveLibrary, processId, pLogFile, pRejectFile}) {
		const url = new URL(recordLoadUrl);

		// Pass correlationId to record-load-api so it can use same name in log files
		url.search = new URLSearchParams([
			['correlationId', correlationId],
			['pActiveLibrary', pActiveLibrary],
			['processId', processId],
			['pLogFile', pLogFile || null],
			['pRejectFile', pRejectFile || null]
		]);

		const response = await fetch(url, {
			method: 'GET',
			headers: {
				'Content-Type': 'text/plain',
				Authorization: generateAuthorizationHeader(recordLoadApiKey)
			}
		});

		logger.log('info', 'Got response for process poll!');
		logger.log('debug', `Status: ${response.status}`);

		// OK (200)
		if (response.status === HttpStatus.OK) {
			const array = await response.json();
			const idList = array.map(id => formatRecordId(pActiveLibrary, id));
			logger.log('info', `Got "OK" (200) response from record-load-api. Ids: ${idList}`);
			return {payloads: idList, ackOnlyLength: idList.length};
		}

		// R-L-A has crashed (409)
		if (response.status === HttpStatus.CONFLICT) {
			const array = await response.json();
			if (array.length > 0) {
				const idList = array.map(id => formatRecordId(pActiveLibrary, id));
				logger.log('info', `Got "conflict" (409) response from record-load-api. Ids:  ${idList}`);
				return {payloads: idList, ackOnlyLength: idList.length};
			}

			return {payloads: [], ackOnlyLength: 0};
		}

		// Not found (404)
		if (response.status === HttpStatus.NOT_FOUND) {
			logger.log('info', 'Got "not found" (404) response from record-load-api. Process log files missing!');
			throw new ApiError(HttpStatus.NOT_FOUND, 'Process log not found!');
		}

		// Locked (423) too early
		if (response.status === HttpStatus.LOCKED) {
			logger.log('info', 'Got "locked" (423) response from record-load-api. Process is still going on!');
			throw new ApiError(HttpStatus.LOCKED, 'Not ready yet!');
		}

		// Forbidden (403)
		if (response.status === HttpStatus.FORBIDDEN) {
			throw new ApiError(HttpStatus.FORBIDDEN);
		}

		// Unauthorized (401)
		if (response.status === HttpStatus.UNAUTHORIZED) {
			throw new ApiError(HttpStatus.UNAUTHORIZED);
		}

		throw new ApiError(HttpStatus.INTERNAL_SERVER_ERROR, 'Unexpected');
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
