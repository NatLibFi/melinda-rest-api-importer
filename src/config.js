import {Utils} from '@natlibfi/melinda-commons/';

const {readEnvironmentVariable, parseBoolean} = Utils; // eslint-disable-line no-unused-vars

export const CHUNK_SIZE = readEnvironmentVariable('CHUNK_SIZE', {defaultValue: 50, format: v => Number(v)});

export const AMQP_URL = JSON.parse(readEnvironmentVariable('AMQP_URL'));

export const NAME_QUEUE_PRIORITY = 'PRIORITY';
export const NAME_QUEUE_BULK = 'BULK';
export const NAME_QUEUE_REPLY = 'REPLY';
export const PURGE_QUEUE_ON_LOAD = readEnvironmentVariable('PURGE_QUEUE_ON_LOAD', {defaultValue: 0}); // ParseBoolean works?

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
