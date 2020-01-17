/* eslint-disable no-unused-vars, no-warning-comments */

import {Utils} from '@natlibfi/melinda-commons';
import {MarcRecord} from '@natlibfi/marc-record';
import {createService} from './datastore';
import ApiError from './error';

const {createLogger} = Utils;

export function toRecordLoadApi() {
	const logger = createLogger();
	const Datastore = createService();

	return async ({records, cataloger, operation}) => {
		// Debug:
		// logger.log('info', `Has record ${records}`);
		// logger.log('info', `Has operation ${operation}`);
		// logger.log('info', `Has cataloger: ${cataloger}`);

		if (records) {
			let metadata;
			records = records.map(record => {
				return new MarcRecord(record);
			});

			metadata = await Datastore.set({records, operation, cataloger});

			logger.log('debug', `Operation ${operation} performed to records ${metadata.ids}`);
			return {...metadata};
		}

		// 422 Unprocessable Entity
		return new ApiError(422, 'No records parsed from data');
	};
}
// TODO: UNNESSESSARY FILE.. REMOVE!
