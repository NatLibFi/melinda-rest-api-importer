import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils;

export const CHUNK_SIZE = readEnvironmentVariable('CHUNK_SIZE', {defaultValue: 50, format: v => Number(v)});

export const AMQP_URL = {
	protocol: 'amqp',
	hostname: 'localhost',
	port: 5672,
	username: 'melinda',
	password: 'test12',
	frameMax: 0,
	heartbeat: 0,
	vhost: '/'
};

export const NAME_QUEUE_PRIORITY = 'PRIORITY';
export const NAME_QUEUE_BULK = 'BULK';
export const PURGE_QUEUE_ON_LOAD = readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: false, format: parseBoolean});

export const EMITTER_JOB_CONSUME = 'EMITTER_JOB_CONSUME';
export const EMITTER_CONSUM_ANNOUNSE = 'RECORD_CONSUMED';

export const RECORD_STATE = {
	CREATED: 'CREATED',
	UPDATED: 'UPDATED',
	INVALID: 'INVALID',
	DUPLICATE: 'DUPLICATE',
	ERROR: 'ERROR',
	SKIPPED: 'SKIPPED'
};

export const SRU_URL = readEnvironmentVariable('SRU_URL', '[".*"]');
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL', '');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY', '');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY', '');
