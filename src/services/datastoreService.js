import {URL} from 'url';
import {promisify, isArray} from 'util';
import HttpStatus from 'http-status';
import fetch from 'node-fetch';
import createSruClient from '@natlibfi/sru-client';
import {MARCXML, AlephSequential} from '@natlibfi/marc-record-serializers';
import deepEqual from 'deep-eql';
import moment from 'moment';
import {Utils} from '@natlibfi/melinda-commons';

import {SRU_URL, RECORD_LOAD_API_KEY, RECORD_LOAD_LIBRARY, RECORD_LOAD_URL} from '../config';
const {createLogger, toAlephId, fromAlephId, generateAuthorizationHeader} = Utils; // eslint-disable-line no-unused-vars

const setTimeoutPromise = promisify(setTimeout);

const FIX_ROUTINE = 'API';
const UPDATE_ACTION = 'REP';
const SRU_VERSION = '2.0';
const DEFAULT_CATALOGER_ID = 'API';
const MAX_RETRIES_ON_CONFLICT = 10;
const RETRY_WAIT_TIME_ON_CONFLICT = 1000;

export const INDEXING_PRIORITY = {
	HIGH: 1,
	LOW: 2
};

export class DatastoreError extends Error {
	constructor(status, ...params) {
		super(params);
		this.status = status;
	}
}

export function createService() {
	const logger = createLogger();
	const requestOptions = {
		headers: {
			Accept: 'text/plain',
			Authorization: generateAuthorizationHeader(RECORD_LOAD_API_KEY)
		}
	};

	return {create, update, bulk};

	async function create({record, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		record = AlephSequential.to(record);
		return loadRecord({record, cataloger, indexingPriority});
	}

	async function update({record, id, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		// Bulk updates have pre set valid ids?
		if (!id) { // TODO if !id pick value from field 001
			id = record.get(/$001^/)[0].value;
		}
		const existingRecord = await fetchRecord(id);
		validateRecordState(record, existingRecord);
		record = AlephSequential.to(record);
		return loadRecord({record, isUpdate: true, cataloger, indexingPriority});
	}

	async function bulk({operation, records, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		const op = (operation === 'update');
		records = records.map(record => {
			// TODO if op -> Validate record state!
			// const id = record.get(/$001^/)[0].value;
			// const existingRecord = await fetchRecord(id);
			// validateRecordState(record, existingRecord);
			return AlephSequential.to(record);
		});
		return loadRecord({record: records, isUpdate: op, cataloger, indexingPriority});
	}

	async function fetchRecord(id) {
		return new Promise((resolve, reject) => {
			try {
				const sruClient = createSruClient({serverUrl: SRU_URL, version: SRU_VERSION, maximumRecords: 1});

				sruClient.searchRetrieve(`rec.id=${id}`)
					.on('record', record => {
						try {
							resolve(MARCXML.from(record));
						} catch (err) {
							reject(err);
						}
					})
					.on('end', () => {
						reject(new DatastoreError(HttpStatus.NOT_FOUND));
					})
					.on('error', err => {
						reject(err);
					});
			} catch (err) {
				reject(err);
			}
		});
	}

	async function loadRecord({record, isUpdate = false, cataloger, indexingPriority, retriesCount = 0}) {
		const url = new URL(RECORD_LOAD_URL);

		url.search = new URLSearchParams([
			['library', RECORD_LOAD_LIBRARY],
			['method', isUpdate === false ? 'NEW' : 'OLD'],
			['fixRoutine', FIX_ROUTINE],
			['updateAction', UPDATE_ACTION],
			['cataloger', cataloger],
			['indexingPriority', generateIndexingPriority(indexingPriority, isUpdate === false)]
		]);

		const response = await fetch(url, Object.assign({
			method: 'POST',
			body: record,
			headers: {'Content-Type': 'text/plain'}
		}, requestOptions));

		if (response.status === HttpStatus.OK) {
			const array = await response.json();
			const idList = array.map(id => formatRecordId(id));
			return {ids: idList};
		}

		if (response.status === HttpStatus.SERVICE_UNAVAILABLE) {
			throw new DatastoreError(HttpStatus.SERVICE_UNAVAILABLE);
		}

		if (response.status === HttpStatus.CONFLICT) {
			if (retriesCount === MAX_RETRIES_ON_CONFLICT) {
				throw new Error(`Unexpected response: ${response.status}: ${await response.text()}`);
			}

			logger.log('info', 'Got conflict response. Retrying...');
			await setTimeoutPromise(RETRY_WAIT_TIME_ON_CONFLICT);
			return loadRecord({record, isUpdate, cataloger, indexingPriority, retriesCount: retriesCount + 1});
		}

		throw new Error(`Unexpected response: ${response.status}: ${await response.text()}`);

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

	// Checks that the modification history is identical
	function validateRecordState(incomingRecord, existingRecord) {
		let incomingModificationHistory;
		if (isArray(incomingRecord)) {
			incomingModificationHistory = incomingRecord;
		} else {
			incomingModificationHistory = incomingRecord.get(/^CAT$/);
		}

		const existingModificationHistory = existingRecord.get(/^CAT$/);
		if (!deepEqual(incomingModificationHistory, existingModificationHistory)) {
			throw new DatastoreError(HttpStatus.CONFLICT);
		}
	}

	/* Has been moved to rest api
	function updateField001ToParamId(id, record) {
		const fields = record.get(/^001$/);

		if (fields.length === 0) {
			// Return to break out of function
			return record.insertField({tag: '001', value: toAlephId(id)});
		}

		fields.map(field => {
			field.value = toAlephId(id);
			return field;
		});
	} */
}
