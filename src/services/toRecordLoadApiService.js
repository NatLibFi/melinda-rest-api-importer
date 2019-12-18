/* eslint-disable no-unused-vars, no-warning-comments */

import {Utils} from '@natlibfi/melinda-commons';
import {validateLine} from '../utils';
import MarcRecord from '@natlibfi/marc-record';
import {RECORD_STATE} from '../config';
import {createService} from './datastoreService';

const {createLogger} = Utils;

export function toRecordLoadApi() {
	const logger = createLogger();
	const DatastoreService = createService();

	return async data => {
		// Logger.log('info', `Has record ${records}`);
		// logger.log('info', `Has data ${JSON.stringify(data)}`);
		// logger.log('info', `Has format ${data.format}`);
		// logger.log('info', `Has operation ${data.operation}`);
		// logger.log('info', `Has cataloger: ${data.cataloger}`);

		// TODO if record type Alephsequental = needs changes in commons
		// TODO Json type record
		if (data.records) {
			if (data.operation === 'update') {
				// Async function update({record, id, cataloger = DEFAULT_CATALOGER_ID, indexingPriority = INDEXING_PRIORITY.HIGH}) {
				const {ids, error} = await DatastoreService.updateALEPH({records: data.records, cataloger: data.cataloger});
				if (error === undefined) {
					logger.log('debug', `Updated records ${ids}`);
					return {status: RECORD_STATE.UPDATED, metadata: {ids}};
				}

				return {status: RECORD_STATE.ERROR, metadata: {ids, error}};
			}

			if (data.operation === 'create') {
				const ids = await DatastoreService.createALEPH({records: data.records, cataloger: data.cataloger});
				logger.log('debug', `Created new records ${ids}`);
				return {status: RECORD_STATE.CREATED, metadata: {ids}};
			}
		}

		return false;
	};
}
