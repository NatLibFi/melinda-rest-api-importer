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
import {seqLineToMarcCATField} from '../utils'; // eslint-disable-line no-unused-vars
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

	return {createJSON, createALEPH, updateJSON, updateALEPH};

	async function createALEPH({records, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		const record = records.map(record => {
			return record.join('\n');
		}).join('\n');

		return loadRecord({record, cataloger, indexingPriority});
	}

	async function createJSON({record, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		record = AlephSequential.to(record);
		return loadRecord({record, cataloger, indexingPriority});
	}

	async function updateALEPH({records, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		const record = records.map(record => {
			return record.join('\n');
		}).join('\n');

		return loadRecord({record, isUpdate: true, cataloger, indexingPriority});
		/* Problem => this returns before validated
		try {
			let failed = [];
			let valids = records.filter(async record => validateRecord(record));

			// Join arrays to one big string
			records = valids.map(record => {
				return record.join('\n');
			}).join('\n');
			if (records !== '') {
				return {records: await loadRecord({record: records, isUpdate: true, cataloger, indexingPriority}), failed};
			}

			return {error: new Error('No valid records'), failed};
		} catch (err) {
			return err;
		}

		async function validateRecord(record) {
			let id;
			try {
				// Get cat fields and transform them to marc style
				const cats = getCatFields(record);

				// Get record id
				id = fromAlephId(record[0].substr(0, 9));
				// Check if exists?
				const existingRecord = await fetchRecord(id);
				// Validate record state
				validateRecordState(cats, existingRecord);
				return true;
			} catch (err) {
				console.log(err);
				failed.push({id: toAlephId(id), error: err});
				return false;
			}
		}

		function getCatFields(record) {
			return record.filter(line => {
				return line.substr(10, 3) === 'CAT';
			}).map(line => {
				return seqLineToMarcCATField(line);
			});
		} */
	}

	async function updateJSON({record, id, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
		const existingRecord = await fetchRecord(id);
		updateField001ToParamId(id, record);
		await validateRecordState(record, existingRecord);
		record = AlephSequential.to(record);
		await loadRecord({record, isUpdate: true, cataloger, indexingPriority});
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
			const json = await response.json();
			const idList = json.map(id => {
				return formatRecordId(id);
			});
			return idList;
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
	}
}
