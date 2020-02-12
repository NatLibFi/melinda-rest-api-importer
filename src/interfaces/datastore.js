/* eslint-disable no-unused-vars */
import HttpStatus from 'http-status';
import moment from 'moment';
import fetch from 'node-fetch';
import {URL} from 'url';
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import DatastoreError, {Utils} from '@natlibfi/melinda-commons';
import {OPERATIONS} from '@natlibfi/melinda-rest-api-commons';
import {RECORD_LOAD_API_KEY, RECORD_LOAD_LIBRARY, RECORD_LOAD_URL} from '../config';

const {createLogger, generateAuthorizationHeader} = Utils; // eslint-disable-line no-unused-vars

const FIX_ROUTINE = 'API';
const UPDATE_ACTION = 'REP';

export const INDEXING_PRIORITY = {
	HIGH: 1,
	LOW: 2
};

export function datastoreFactory() {
	const logger = createLogger();

	return {set};

	async function set({correlationId = undefined, records, operation, cataloger, recordLoadParams}) {
		records = records.map(record => {
			return AlephSequential.to(record);
		});
		const recordData = records.join('');
		return loadRecord({correlationId, recordData, operation, cataloger, recordLoadParams});
	}

	async function loadRecord({correlationId, recordData, operation, cataloger, recordLoadParams}) {
		const url = new URL(RECORD_LOAD_URL);

		// Pass correlationId to record-load-api so it can use same name in log files
		url.search = new URLSearchParams([
			['correlationId', correlationId],
			['library', recordLoadParams.library || RECORD_LOAD_LIBRARY],
			['method', operation === OPERATIONS.CREATE ? 'NEW' : 'OLD'],
			['fixRoutine', recordLoadParams.fixRoutine || FIX_ROUTINE],
			['updateAction', recordLoadParams.updateAction || UPDATE_ACTION],
			['cataloger', recordLoadParams.cataloger || cataloger],
			['indexingPriority', recordLoadParams.indexingPriority || generateIndexingPriority(INDEXING_PRIORITY.HIGH, operation === OPERATIONS.CREATE)]
		]);

		// Set Bulk loader optional settings

		if (recordLoadParams.rejectedFile) {
			url.searchParams.set('rejectedFile', recordLoadParams.rejectedFile);
		}

		if (recordLoadParams.resultFile) {
			url.searchParams.set('resultFile', recordLoadParams.resultFile);
		}

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

		// If record-load-api finds result file that should not be there! (e.g. record load api has crashed mid process)
		if (response.status === HttpStatus.CONFLICT) {
			const array = await response.json();
			logger.log('info', `Got conflict response. Ids: ${array}`);
			const idList = array.filter(id => id.length > 0).map(id => formatRecordId(id));
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
