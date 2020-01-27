/* eslint-disable no-unused-vars */
import {AlephSequential} from '@natlibfi/marc-record-serializers';
import DatastoreError, {Utils} from '@natlibfi/melinda-commons';
import fetch from 'node-fetch';
import HttpStatus from 'http-status';
import {URL} from 'url';
import moment from 'moment';
import {promisify} from 'util';

import {RECORD_LOAD_API_KEY, RECORD_LOAD_LIBRARY, RECORD_LOAD_URL, DEFAULT_CATALOGER_ID} from '../config';
const {createLogger, generateAuthorizationHeader} = Utils; // eslint-disable-line no-unused-vars

const setTimeoutPromise = promisify(setTimeout);

const FIX_ROUTINE = 'API';
const UPDATE_ACTION = 'REP';
const MAX_RETRIES_ON_CONFLICT = 10;
const RETRY_WAIT_TIME_ON_CONFLICT = 1000;

export const INDEXING_PRIORITY = {
	HIGH: 1,
	LOW: 2
};

export function datastoreFactory() {
	const logger = createLogger();
	const requestOptions = {
		headers: {
			Accept: 'text/plain',
			Authorization: generateAuthorizationHeader(RECORD_LOAD_API_KEY)
		}
	};

	return {set};

	async function set({records, operation, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		records = records.map(record => {
			return AlephSequential.to(record);
		});
		const recordData = records.join('');
		return loadRecord({recordData, operation, cataloger, indexingPriority});
	}

	async function loadRecord({recordData, operation, cataloger, indexingPriority, retriesCount = 0}) {
		const url = new URL(RECORD_LOAD_URL);

		// TODO: Pass correlationId to record-load-api so it can use same name in log files?
		url.search = new URLSearchParams([
			['library', RECORD_LOAD_LIBRARY],
			['method', operation === 'create' ? 'NEW' : 'OLD'],
			['fixRoutine', FIX_ROUTINE],
			['updateAction', UPDATE_ACTION],
			['cataloger', cataloger],
			['indexingPriority', generateIndexingPriority(indexingPriority, operation === 'update')]
		]);

		const response = await fetch(url, Object.assign({
			method: 'POST',
			body: recordData,
			headers: {'Content-Type': 'text/plain'}
		}, requestOptions));

		if (response.status === HttpStatus.OK) {
			const array = await response.json();
			const idList = array.map(id => formatRecordId(id));
			return {payloads: idList};
		}

		// Should not ever happen!
		if (response.status === HttpStatus.CONFLICT) {
			if (retriesCount === MAX_RETRIES_ON_CONFLICT) {
				throw new DatastoreError(response.status, await response.text());
			}

			logger.log('info', 'Got conflict response. Retrying...');
			await setTimeoutPromise(RETRY_WAIT_TIME_ON_CONFLICT);
			return loadRecord({recordData, operation, cataloger, indexingPriority, retriesCount: retriesCount + 1});
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
