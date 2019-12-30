/* eslint-disable no-unused-vars, no-warning-comments */

import {Utils} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {CHUNK_STATE, NAME_QUEUE_BULK, NAME_QUEUE_PRIORITY} from '../config';
import {createService} from './datastoreService';

const {createLogger} = Utils;

export function toRecordLoadApi() {
	const logger = createLogger();
	const DatastoreService = createService();

	return async (queue, data) => {
		// Logger.log('info', `Has record ${records}`);
		// logger.log('info', `Has data ${JSON.stringify(data)}`);
		// logger.log('info', `Has format ${data.format}`);
		// logger.log('info', `Has operation ${data.operation}`);
		// logger.log('info', `Has cataloger: ${data.cataloger}`);

		// TODO if record type Alephsequental = needs changes in commons
		// TODO Json type record
		if (data.records) {
			const records = data.records.map(record => {
				return new MarcRecord(record);
			});
			if (queue === NAME_QUEUE_BULK) {
				const metadata = await DatastoreService.bulk({operation: data.operation, records, cataloger: data.cataloger});
				const status = generateStatus(data.operation, metadata.ids, metadata.failedRecords);
				logger.log('debug', `${data.operation} records ${metadata.ids}`);
				return {status, metadata};
			}

			if (queue === NAME_QUEUE_PRIORITY) {
				if (data.operation === 'update') {
					const record = new MarcRecord(data.records[0]);
					const id = getRecordId(record);
					const metadata = await DatastoreService.update({record, id, cataloger: data.cataloger});
					const status = generateStatus(data.operation, metadata.ids, metadata.failedRecords);
					logger.log('debug', `Updated records ${metadata.ids}`);
					return {status, metadata};
				}

				if (data.operation === 'create') {
					const record = new MarcRecord(data.records[0]);
					const metadata = await DatastoreService.create({record, cataloger: data.cataloger});
					const status = generateStatus(data.operation, metadata.ids, data.failedRecords);
					logger.log('debug', `Created new records ${metadata.ids}`);
					return {status, metadata};
				}
			}
		}

		return {status: CHUNK_STATE.ERROR, metadata: 'No records parsed from chunk data'};
	};

	function getRecordId(record) {
		const f001 = record.get(/^001$/)[0];
		return f001.value;
	}

	function generateStatus(operation, records, failedRecords) {
		if (records === undefined || records.length === 0) {
			return CHUNK_STATE.ERROR;
		}

		if (failedRecords.length > 0) {
			return CHUNK_STATE.ACTION_NEEDED;
		}

		if (operation === 'update') {
			return CHUNK_STATE.UPDATED;
		}

		if (operation === 'create') {
			return CHUNK_STATE.CREATED;
		}

		return CHUNK_STATE.INVALID;
	}
}
