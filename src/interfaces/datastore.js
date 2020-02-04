/* eslint-disable no-unused-vars */
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import DatastoreError, {Utils} from '@natlibfi/melinda-commons';
import fetch from 'node-fetch';
import HttpStatus from 'http-status';
import {URL} from 'url';
import moment from 'moment';
import {promisify} from 'util';

import {RECORD_LOAD_API_KEY, RECORD_LOAD_LIBRARY, RECORD_LOAD_URL, DEFAULT_CATALOGER_ID} from '../config';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons/dist/constants';
const {createLogger, generateAuthorizationHeader} = Utils; // eslint-disable-line no-unused-vars

const setTimeoutPromise = promisify(setTimeout);

const FIX_ROUTINE = 'API'; // Henrillä ja esalla INSB
const UPDATE_ACTION = 'REP';
const MAX_RETRIES_ON_CONFLICT = 10;
const RETRY_WAIT_TIME_ON_CONFLICT = 1000;

export const INDEXING_PRIORITY = {
	HIGH: 1,
	LOW: 2
};

export function datastoreFactory() {
	const logger = createLogger();

	return {set};

	async function set({correlationId = undefined, records, operation, cataloger = DEFAULT_CATALOGER_ID, recordLoadParams, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		records = records.map(record => {
			return AlephSequential.to(record);
		});
		const recordData = records.join('');
		return loadRecord({correlationId, recordData, operation, cataloger, recordLoadParams, indexingPriority});
	}

	async function loadRecord({correlationId, recordData, operation, cataloger, recordLoadParams, indexingPriority, retriesCount = 0}) {
		const url = new URL(RECORD_LOAD_URL);

		// TODO: Pass correlationId to record-load-api so it can use same name in log files?
		url.search = new URLSearchParams([
			['correlationId', correlationId],
			['library', recordLoadParams.library || RECORD_LOAD_LIBRARY],
			['method', operation === OPERATIONS.CREATE ? 'NEW' : 'OLD'],
			['fixRoutine', recordLoadParams.fixRoutine || FIX_ROUTINE],
			['updateAction', recordLoadParams.updateAction || UPDATE_ACTION],
			['cataloger', recordLoadParams.cataloger || cataloger],
			['indexingPriority', recordLoadParams.indexingPriority || generateIndexingPriority(indexingPriority, operation === OPERATIONS.CREATE)]
		]);

		// Set Bulk loader settings
		if (recordLoadParams.indexing) {
			url.searchParams.set('indexing', recordLoadParams.indexing);
		}

		if (recordLoadParams.mode) {
			url.searchParams.set('mode', recordLoadParams.mode);
		}

		if (recordLoadParams.charConversion) {
			url.searchParams.set('charConversion', recordLoadParams.charConversion);
		}

		if (recordLoadParams.mergeRoutine) {
			url.searchParams.set('mergeRoutine', recordLoadParams.mergeRoutine);
		}

		if (recordLoadParams.catalogerLevel) {
			url.searchParams.set('catalogerLevel', recordLoadParams.catalogerLevel);
		}

		const response = await fetch(url, {
			method: 'POST',
			body: recordData,
			headers: {
				'Content-Type': 'text/plain',
				Accept: 'text/plain',
				Authorization: generateAuthorizationHeader(RECORD_LOAD_API_KEY)
			}
		});

		if (response.status === HttpStatus.OK) {
			const array = await response.json();
			const idList = array.filter(id => id.length > 0).map(id => formatRecordId(id));
			logger.log('debug', `Ids back from record-load-api: ${idList}`);
			return {payloads: idList};
		}

		// If record-load-api finds result file that should not be there!
		if (response.status === HttpStatus.CONFLICT) {
			const array = await response.json();
			logger.log('info', `Got conflict response. ${array}`);
			const idList = array.filter(id => id.length > 0).map(id => formatRecordId(id));
			console.log(idList);
			return {payloads: idList, ackOnlyLength: idList.length};
		}

		// Unexpected! Retry?
		throw new DatastoreError(response.status, await response.text());

		function formatRecordId(id) {
			const pattern = new RegExp(`${RECORD_LOAD_LIBRARY.toUpperCase()}$`);
			return id.replace(pattern, '');
		}

		function generateIndexingPriority(priority, forCreated) {
			if (priority === INDEXING_PRIORITY.HIGH) {
				// These are values Aleph assigns for records modified in the cataloging GUI
				return forCreated ? '1990' : '1998';
			}

			return moment().add(1000, 'years').year();
		}
	}
}
