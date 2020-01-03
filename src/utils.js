import {Utils} from '@natlibfi/melinda-commons';
import {OFFLINE_BEGIN, OFFLINE_DURATION} from './config';
import moment from 'moment';

const {createLogger} = Utils;
const logger = createLogger();

export function logError(err) {
	if (err === 'SIGINT') {
		logger.log('error', err);
	} else {
		logger.log('error', 'stack' in err ? err.stack : err);
	}
}

export function checkIfOfflineHours() {
	const now = moment();
	const start = moment(now).startOf('day').add(OFFLINE_BEGIN, 'hours');
	const end = moment(start).add(OFFLINE_DURATION, 'hours');
	if (now.hours() < OFFLINE_BEGIN && start.format('DDD') < end.format('DDD')) { // Offline hours pass midnight (DDD = day of the year)
		start.subtract(1, 'days');
		end.subtract(1, 'days');
	}

	if (now.format('x') >= start.format('x') && now.format('x') < end.format('x')) {
		// TODO: logger.log('info', `Offline hours begin at ${OFFLINE_BEGIN} and will last next ${OFFLINE_DURATION} hours. Time is now ${moment().format('HH:mm')}`);
		return true;
	}

	return false;
}
