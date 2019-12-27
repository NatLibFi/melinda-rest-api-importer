/* eslint-disable no-unused-vars, no-warning-comments */

import {Utils} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {RECORD_STATE, NAME_QUEUE_BULK, NAME_QUEUE_PRIORITY} from '../config';
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
				const {ids, error} = await DatastoreService.bulk({operation: data.operation, records, cataloger: data.cataloger});
				if (error === undefined) {
					logger.log('debug', `Updated records ${ids}`);
					return {status: RECORD_STATE.UPDATED, metadata: {ids}};
				}

				return {status: RECORD_STATE.ERROR, metadata: {ids, error}};
			}

			if (queue === NAME_QUEUE_PRIORITY) {
				if (data.operation === 'update') {
					const record = new MarcRecord(data.records[0]);
					const id = getRecordId(record);
					const {ids, error} = await DatastoreService.update({record, id, cataloger: data.cataloger});
					if (error === undefined) {
						logger.log('debug', `Updated records ${ids}`);
						return {status: RECORD_STATE.UPDATED, metadata: {ids}};
					}

					return {status: RECORD_STATE.ERROR, metadata: {ids, error}};
				}

				if (data.operation === 'create') {
					const ids = await DatastoreService.create({records, cataloger: data.cataloger});
					logger.log('debug', `Created new records ${ids}`);
					return {status: RECORD_STATE.CREATED, metadata: {ids}};
				}
			}
		}

		return false;
	};

	function getRecordId(record) {
		const f001 = record.get(/^001$/)[0];
		return f001.value;
	}
}
