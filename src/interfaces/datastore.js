/* eslint-disable no-unused-vars */
import HttpStatus from 'http-status';
import moment from 'moment';
import fetch from 'node-fetch';
import {URL} from 'url';
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import DatastoreError, {Utils} from '@natlibfi/melinda-commons';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';

export default function (recordLoadApiKey, recordLoadLibrary, recordLoadUrl) {
	const {createLogger, generateAuthorizationHeader} = Utils; // eslint-disable-line no-unused-vars
	const logger = createLogger();
	const INDEXING_PRIORITY = {
		HIGH: 1,
		LOW: 2
	};

	return {loadRecord};

	async function loadRecord({correlationId = undefined, records, operation, cataloger, recordLoadParams, prio}) {
		records = records.map(record => {
			return AlephSequential.to(record);
		});
		const recordData = records.join('');

		const url = new URL(recordLoadUrl);

		// Pass correlationId to record-load-api so it can use same name in log files
		url.search = new URLSearchParams([
			['correlationId', correlationId],
			['pActiveLibrary', recordLoadParams.pActiveLibrary || recordLoadLibrary],
			['pOldNew', operation === OPERATIONS.CREATE ? 'NEW' : 'OLD'],
			['pFixType', prio ? 'API' : 'INSB'],
			['pCatalogerIn', recordLoadParams.pCatalogerIn || cataloger],
			['pZ07PriorityYear', prio ? generateIndexingPriority(INDEXING_PRIORITY.HIGH, operation === OPERATIONS.CREATE) : 2099]
		]);

		if (recordLoadParams.pRejectFile) {
			url.searchParams.set('pRejectFile', recordLoadParams.pRejectFile);
		}

		if (recordLoadParams.pLogFile) {
			url.searchParams.set('pLogFile', recordLoadParams.pLogFile);
		}

		const response = await fetch(url, {
			method: 'POST',
			body: recordData,
			headers: {
				'Content-Type': 'text/plain',
				Accept: 'text/plain', // Might need change
				Authorization: generateAuthorizationHeader(recordLoadApiKey)
			}
		});

		if (response.status === HttpStatus.OK) {
			const processId = await response.json();
			return processId;
		}

		// Unexpected! Retry?
		throw new DatastoreError(response.status, await response.text());

		function generateIndexingPriority(priority, forCreated) {
			if (priority === INDEXING_PRIORITY.HIGH) {
				// These are values Aleph assigns for records modified in the cataloging GUI
				return forCreated ? '1990' : '1998';
			}

			return moment().add(1000, 'years').year();
		}
	}
}
