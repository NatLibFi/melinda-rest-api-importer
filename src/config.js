import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils;


export const IP_FILTER_BIB = readEnvironmentVariable('IP_FILTER_BIB', '[".*"]');
export const HTTP_PORT = readEnvironmentVariable('HTTP_PORT', {defaultValue: 3005, format: v => Number(v)});
export const SOCKET_KEEP_ALIVE_TIMEOUT = readEnvironmentVariable('SOCKET_KEEP_ALIVE_TIMEOUT', {defaultValue: 0, format: v => Number(v)});

export const CHUNK_SIZE = readEnvironmentVariable('CHUNK_SIZE', {defaultValue: 50, format: v => Number(v)});
export const TMP_FILE_LOCATION = readEnvironmentVariable('TMP_FILE_LOCATION', {defaultValue: '/tmp/'});

export const AMQP_URL = readEnvironmentVariable('AMQP_URL', {defaultValue: {
    protocol: 'amqp',
    hostname: 'localhost',
    port: 5672,
    username: 'melinda',
    password: 'test12',
    frameMax: 0,
    heartbeat: 0,
    vhost: '/',
  }});

export const NAME_QUEUE_PRIORITY = 'PRIORITY';
export const NAME_QUEUE_BULK = 'BULK';
export const PURGE_QUEUE_ON_LOAD = readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: false, format: parseBoolean});

export const FORMATS = {
    JSON: 1,
	ISO2709: 2,
	MARCXML: 3,
    TEXT: 4
};

export const EMITTER_JOB_CONSUME = 'EMITTER_JOB_CONSUME';
export const EMITTER_CONSUM_ANNOUNSE = 'RECORD_CONSUMED';

export const SRU_URL = readEnvironmentVariable('SRU_URL', '[".*"]');
export const RECORD_LOAD_URL = readEnvironmentVariable('RECORD_LOAD_URL', '');
export const RECORD_LOAD_API_KEY = readEnvironmentVariable('RECORD_LOAD_API_KEY', '');
export const RECORD_LOAD_LIBRARY = readEnvironmentVariable('RECORD_LOAD_LIBRARY', '');

export const RECORD_STATE = {
	CREATED: 'CREATED',
	UPDATED: 'UPDATED',
	INVALID: 'INVALID',
	DUPLICATE: 'DUPLICATE',
	ERROR: 'ERROR',
	SKIPPED: 'SKIPPED'
};