/* eslint-disable no-unused-vars, no-warning-comments */

import {Utils} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {CHUNK_STATE, IMPORT_QUEUES, OPERATIONS} from '@natlibfi/melinda-record-import-commons';
import {createService} from './datastore';
import ApiError from './error';

const {createLogger} = Utils;
const {BULK_CREATE, BULK_UPDATE, PRIO_CREATE, PRIO_UPDATE} = IMPORT_QUEUES;

export function toRecordLoadApi() {
	const logger = createLogger();
	const Datastore = createService();

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

			if (queue === BULK_UPDATE || queue === BULK_CREATE) {
				const metadata = await Datastore.bulk({operation, records, cataloger});
				const status = await generateStatus(operation, metadata.ids, metadata.failedRecords);
				logger.log('debug', `${operation} records ${metadata.ids}`);
				return {status, operation, cataloger, chunkNumber, metadata};
			}

			if (queue === PRIO_CREATE || queue === PRIO_UPDATE) {
				const record = records[0];
				let metadata;
				if (data.operation === 'update') {
					const id = getRecordId(record);
					metadata = await Datastore.update({record, id, cataloger});
				} else {
					metadata = await Datastore.create({record, cataloger});
				}

				const status = await generateStatus(operation, metadata.ids, metadata.failedRecords);
				logger.log('debug', `Operation ${operation} performed to records ${metadata.ids}`);
				return {status, operation, cataloger, chunkNumber, metadata};
			}
		}

		// 422 Unprocessable Entity
		return {status: CHUNK_STATE.ERROR, operation, cataloger, chunkNumber, metadata: {error: new ApiError(422, 'No records parsed from chunk data')}};
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
