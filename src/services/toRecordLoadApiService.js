/* eslint-disable no-unused-vars, no-warning-comments */

import {Utils} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {CHUNK_STATE, QUEUE_NAME_BULK, QUEUE_NAME_PRIO, OPERATIONS} from '@natlibfi/melinda-record-import-commons';
import {createService} from './datastoreService';
import ServiceError from './error';

const {createLogger} = Utils;

export function toRecordLoadApi() {
	const logger = createLogger();
	const DatastoreService = createService();

	return async (queue, data) => {
		const {operation, cataloger, chunkNumber} = data;
		// Debug:
		// logger.log('info', `Has data ${JSON.stringify(data)}`);
		// logger.log('info', `Has record ${data.records}`);
		// logger.log('info', `Has chunkNumber ${data.chunkNumber}`);
		// logger.log('info', `Has operation ${data.operation}`);
		// logger.log('info', `Has cataloger: ${data.cataloger}`);

		if (data.records) {
			const records = data.records.map(record => {
				return new MarcRecord(record);
			});

			if (queue === QUEUE_NAME_BULK) {
				const metadata = await DatastoreService.bulk({operation, records, cataloger});
				const status = await generateStatus(operation, metadata.ids, metadata.failedRecords);
				logger.log('debug', `${operation} records ${metadata.ids}`);
				return {status, operation, cataloger, chunkNumber, metadata};
			}

			if (queue === QUEUE_NAME_PRIO) {
				const record = data.records[0];
				let metadata;
				if (data.operation === 'update') {
					const id = getRecordId(record);
					metadata = await DatastoreService.update({record, id, cataloger});
				} else {
					metadata = await DatastoreService.create({record, cataloger});
				}

				const status = await generateStatus(operation, metadata.ids, metadata.failedRecords);
				logger.log('debug', `Operation ${operation} performed to records ${metadata.ids}`);
				return {status, operation, cataloger, chunkNumber, metadata};
			}
		}

		// 422 Unprocessable Entity
		return {status: CHUNK_STATE.ERROR, operation, cataloger, chunkNumber, metadata: {error: new ServiceError(422, 'No records parsed from chunk data')}};
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

		if (OPERATIONS.includes(operation)) {
			return CHUNK_STATE.DONE;
		}

		return CHUNK_STATE.INVALID;
	}
}
