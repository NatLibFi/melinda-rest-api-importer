/* eslint-disable no-unused-vars */

import {MongoClient} from 'mongodb';
import {Utils} from '@natlibfi/melinda-commons';
import {QUEUE_ITEM_STATE} from '@natlibfi/melinda-record-import-commons';
import {MONGO_URI} from '../config';
import {logError} from '../utils';
import moment from 'moment';

const {createLogger} = Utils;
/* QueueItem:
{
	"correlationId":"test",
	"cataloger":"xxx0000",
	"operation":"update",
	"contentType":"application/json",
	"queueItemState":"PENDING_QUEUING",
	"creationTime":"2020-01-01T00:00:00.000Z",
	"modificationTime":"2020-01-01T00:00:01.000Z"
}
*/
// Send to Record-Load-Api
// Make reply
// Back to loop

export default async function () {
	const logger = createLogger(); // eslint-disable-line no-unused-vars
	// Connect to mongo (MONGO)
	const client = await MongoClient.connect(MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true, logger: logMongo});
	const db = client.db('rest-api');

	return {checkDB, setState};

	async function checkDB(operation) {
		let result;
		logger.log('debug', `Checking DB for ${operation}`);

		try {
			// Check mongo if any QUEUE_ITEM_STATE.IN_PROCESS (MONGO) (Job that has not completed)
			result = await db.collection('queue-items').findOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_PROCESS});

			if (result === null) {
				// Checking if any QUEUE_ITEM_STATE.IN_QUEUE to be new job!
				result = await db.collection('queue-items').findOne({operation, queueItemState: QUEUE_ITEM_STATE.IN_QUEUE});
				if (result === null) {
					// Back to loop
					logger.log('debug', 'No jobs in mongo');
					return null;
				}
			}

			return result;
		} catch (error) {
			logError(error);
			checkDB();
		}
	}

	async function setState({correlationId, cataloger, operation, state}) {
		logger.log('debug', 'Setting queue item state');
		const db = client.db('rest-api');
		await db.collection('queue-items').updateOne({
			cataloger,
			correlationId,
			operation
		}, {
			$set: {
				queueItemState: state,
				modificationTime: moment().toDate()
			}
		});
		const result = await db.collection('queue-items').findOne({
			cataloger,
			correlationId,
			operation
		}, {projection: {_id: 0}});
		return result;
	}

	function logMongo({error, log, debug}) {
		if (error) {
			logger.log('error', error);
		}

		if (log) {
			logger.log('info', log);
		}

		if (debug) {
			logger.log('debug', debug);
		}
	}
}
